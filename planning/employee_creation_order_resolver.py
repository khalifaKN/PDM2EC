from utils.logger import get_logger
from typing import Dict, List, Set, Tuple
from collections import defaultdict, deque
import pandas as pd

logger = get_logger("employee_creation_order_resolver")


class EmployeeCreationOrderResolver:
    """
    Resolves the creation order for new employees based on HARD dependencies.
    Uses level-based topological sorting (Kahn’s algorithm).

    HARD dependencies:
      - manager
      - matrix_manager

    SOFT dependencies (do not block creation):
      - hr

    Enhancements:
      - Detects real cycle groups using SCC (Tarjan algorithm)
      - Provides separate circular dependency groups (only hard deps)
    """

    HARD_DEPENDENCY_FIELDS = ("manager", "matrix_manager")
    SOFT_DEPENDENCY_FIELDS = ("hr",)

    def __init__(self, new_employees: pd.DataFrame, existing_employees: pd.DataFrame):
        self.new_employees = new_employees.copy()

        if not self.new_employees.empty:
            self.new_employees["userid"] = (
                self.new_employees["userid"].astype(str).str.lower()
            )

        self.existing_employees = existing_employees.copy()
        if not self.existing_employees.empty:
            self.existing_employees["userid"] = (
                self.existing_employees["userid"].astype(str).str.lower()
            )

        self.existing_ids: Set[str] = (
            set(self.existing_employees["userid"])
            if not self.existing_employees.empty
            else set()
        )

        self.new_ids: Set[str] = (
            set(self.new_employees["userid"])
            if not self.new_employees.empty
            else set()
        )

        # This is IMPORTANT: we store soft dependency candidates here
        # so downstream pipeline can decide to defer HR relationship updates.
        self.hr_retry_candidates: Set[str] = set()

    # -------------------------------------------------------------------------
    # Dependency extraction
    # -------------------------------------------------------------------------

    def _extract_hard_dependencies(self, row) -> Set[str]:
        """
        Extract HARD dependencies for a single employee.
        HARD deps affect ordering.

        A dependency exists only if:
          - The dependency userid is also a NEW employee
          - The dependency is not self
        """
        deps = set()

        for field in self.HARD_DEPENDENCY_FIELDS:
            if not hasattr(row, field):
                continue

            value = getattr(row, field)
            if pd.isna(value) or value in ("", "None", None):
                continue

            dep_id = str(value).lower()

            if dep_id in self.new_ids and dep_id != row.userid:
                deps.add(dep_id)

        return deps

    def _extract_soft_hr_dependency(self, row) -> str | None:
        """
        Extract HR dependency (soft).
        This does NOT block ordering, but if HR points to a NEW employee,
        then this user needs HR retry later.
        """
        if not hasattr(row, "hr"):
            return None

        value = getattr(row, "hr")
        if pd.isna(value) or value in ("", "None", None):
            return None

        hr_id = str(value).lower()

        if hr_id in self.new_ids and hr_id != row.userid:
            return hr_id

        return None

    # -------------------------------------------------------------------------
    # Graph build (HARD deps only)
    # -------------------------------------------------------------------------

    def _build_graph(self) -> Tuple[Dict[str, Set[str]], Dict[str, int]]:
        """
        Build adjacency graph and in-degree map for Kahn sorting.

        graph[A] = {B, C} means A must be created before B and C.
        """
        graph: Dict[str, Set[str]] = defaultdict(set)
        in_degree: Dict[str, int] = {uid: 0 for uid in self.new_ids}

        for row in self.new_employees.itertuples(index=False):
            hard_deps = self._extract_hard_dependencies(row)
            in_degree[row.userid] = len(hard_deps)

            for dep in hard_deps:
                graph[dep].add(row.userid)

            # Track HR retry candidates
            hr_dep = self._extract_soft_hr_dependency(row)
            if hr_dep is not None:
                self.hr_retry_candidates.add(row.userid)

        return graph, in_degree

    def _build_dependency_map(self) -> Dict[str, Set[str]]:
        """
        Build a dependency map (HARD deps only):

        deps_map[uid] = {manager_uid, matrix_uid}

        This is used for SCC cycle detection.
        """
        deps_map: Dict[str, Set[str]] = {uid: set() for uid in self.new_ids}

        for row in self.new_employees.itertuples(index=False):
            deps_map[row.userid] = self._extract_hard_dependencies(row)

        return deps_map

    # -------------------------------------------------------------------------
    # Cycle detection (HARD deps only)
    # -------------------------------------------------------------------------

    def _find_cycle_groups(self) -> List[List[str]]:
        """
        Finds real circular dependency groups using Tarjan SCC.

        Returns:
            List of SCC groups where size > 1 (real cycles)
            or size == 1 with a self-loop.
        """
        deps_map = self._build_dependency_map()

        index = 0
        stack: List[str] = []
        on_stack: Set[str] = set()
        indices: Dict[str, int] = {}
        lowlink: Dict[str, int] = {}
        sccs: List[List[str]] = []

        def strongconnect(v: str):
            nonlocal index
            indices[v] = index
            lowlink[v] = index
            index += 1

            stack.append(v)
            on_stack.add(v)

            for w in deps_map.get(v, set()):
                if w not in indices:
                    strongconnect(w)
                    lowlink[v] = min(lowlink[v], lowlink[w])
                elif w in on_stack:
                    lowlink[v] = min(lowlink[v], indices[w])

            if lowlink[v] == indices[v]:
                scc = []
                while True:
                    w = stack.pop()
                    on_stack.remove(w)
                    scc.append(w)
                    if w == v:
                        break
                sccs.append(scc)

        for uid in self.new_ids:
            if uid not in indices:
                strongconnect(uid)

        cycle_groups = []
        for group in sccs:
            if len(group) > 1:
                cycle_groups.append(sorted(group))
            elif len(group) == 1:
                u = group[0]
                if u in deps_map.get(u, set()):
                    cycle_groups.append([u])

        cycle_groups.sort(key=len, reverse=True)
        return cycle_groups

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def get_ordered_batches(self) -> List[pd.DataFrame]:
        """
        Returns employees ordered in creation batches.
        Each batch can be processed in parallel.

        IMPORTANT:
          - Only manager/matrix affect ordering.
          - HR does NOT block creation.
        """
        if self.new_employees.empty:
            return []

        graph, original_in_degree = self._build_graph()
        in_degree = original_in_degree.copy()

        queue = deque(uid for uid, deg in in_degree.items() if deg == 0)
        processed: Set[str] = set()
        batches: List[pd.DataFrame] = []

        while queue:
            level_size = len(queue)
            current_batch_ids = []

            for _ in range(level_size):
                uid = queue.popleft()
                processed.add(uid)
                current_batch_ids.append(uid)

                for dependent in graph.get(uid, []):
                    in_degree[dependent] -= 1
                    if in_degree[dependent] == 0:
                        queue.append(dependent)

            batch_df = self.new_employees[
                self.new_employees["userid"].isin(current_batch_ids)
            ].copy()

            # Mark HR retry candidates in the batch df (super useful downstream)
            if "needs_hr_retry" not in batch_df.columns:
                batch_df["needs_hr_retry"] = batch_df["userid"].isin(
                    self.hr_retry_candidates
                )

            batches.append(batch_df)

        remaining = self.new_ids - processed
        if remaining:
            cycle_groups = self._find_cycle_groups()

            logger.warning(
                f"HARD circular dependencies detected for {len(remaining)} employees "
                f"(grouped into {len(cycle_groups)} cycles)"
            )

            for i, group in enumerate(cycle_groups, start=1):
                logger.warning(f"Cycle {i} ({len(group)} users): {group}")

            logger.warning(
                "Manager/matrix references referencing cycle members will be temporarily cleared"
            )

            cycle_df = self.new_employees[
                self.new_employees["userid"].isin(remaining)
            ].copy()

            # Only clear HARD dependency fields (NOT HR!)
            for field in self.HARD_DEPENDENCY_FIELDS:
                if field not in cycle_df.columns:
                    continue

                cycle_df[field] = cycle_df.apply(
                    lambda r: None
                    if pd.notna(r[field])
                    and str(r[field]).lower() in remaining
                    else r[field],
                    axis=1,
                )

            # Mark HR retry candidates
            if "needs_hr_retry" not in cycle_df.columns:
                cycle_df["needs_hr_retry"] = cycle_df["userid"].isin(
                    self.hr_retry_candidates
                )

            batches.append(cycle_df)

        logger.info(f"Resolved {len(self.new_ids)} employees into {len(batches)} batches")
        for i, batch in enumerate(batches, start=1):
            logger.info(f"Batch {i}: {len(batch)} employees")

        return batches

    def get_dependency_summary(self) -> Dict:
        """
        Provides a summary of HARD dependencies among new employees.
        Includes real cycle groups (hard only).
        """
        if self.new_employees.empty:
            return {
                "total_new_employees": 0,
                "employees_with_no_dependencies": 0,
                "employees_with_dependencies": 0,
                "employees_in_hard_cycles": 0,
                "hard_cycle_userids": [],
                "hard_cycle_groups": [],
                "missing_dependencies_hard_only": [],
                "missing_dependency_count_hard_only": 0,
                "hr_retry_candidates": [],
                "hr_retry_candidate_count": 0,
            }

        graph, in_degree = self._build_graph()

        no_deps = sum(1 for d in in_degree.values() if d == 0)

        queue = deque(uid for uid, deg in in_degree.items() if deg == 0)
        processed: Set[str] = set()

        while queue:
            uid = queue.popleft()
            processed.add(uid)

            for dependent in graph.get(uid, []):
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        hard_cycles = self.new_ids - processed

        hard_cycle_groups = self._find_cycle_groups()
        hard_cycle_userids = sorted(list(hard_cycles))

        # Missing HARD dependencies only
        missing = []
        for row in self.new_employees.itertuples(index=False):
            for field in self.HARD_DEPENDENCY_FIELDS:
                if not hasattr(row, field):
                    continue

                value = getattr(row, field)
                if pd.isna(value) or value in ("", "None", None):
                    continue

                dep_id = str(value).lower()

                if dep_id not in self.new_ids and dep_id not in self.existing_ids:
                    missing.append(
                        {
                            "userid": row.userid,
                            "field": field,
                            "missing_dependency": dep_id,
                        }
                    )

        return {
            "total_new_employees": len(self.new_ids),
            "employees_with_no_dependencies": no_deps,
            "employees_with_dependencies": len(self.new_ids) - no_deps,
            "employees_in_hard_cycles": len(hard_cycles),
            "hard_cycle_userids": hard_cycle_userids,
            "hard_cycle_groups": hard_cycle_groups,
            "missing_dependencies_hard_only": missing,
            "missing_dependency_count_hard_only": len(missing),
            "hr_retry_candidates": sorted(list(self.hr_retry_candidates)),
            "hr_retry_candidate_count": len(self.hr_retry_candidates),
        }
