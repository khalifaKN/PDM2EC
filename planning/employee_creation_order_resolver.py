from utils.logger import get_logger
from typing import Dict, List, Set, Tuple
from collections import defaultdict, deque
import pandas as pd

logger = get_logger("employee_creation_order_resolver")


class EmployeeCreationOrderResolver:
    """
    Resolves the creation order for new employees based on manager dependencies.
    Uses level-based topological sorting (Kahn’s algorithm).
    """

    DEPENDENCY_FIELDS = ("manager", "matrix_manager", "hr")

    def __init__(self, new_employees: pd.DataFrame, existing_employees: pd.DataFrame):
        self.new_employees = new_employees.copy()

        # Normalize userids only if DataFrames are not empty
        if not self.new_employees.empty:
            self.new_employees["userid"] = (
                self.new_employees["userid"].astype(str).str.lower()
            )
        self.existing_employees = existing_employees.copy()
        if not self.existing_employees.empty:
            self.existing_employees["userid"] = (
                self.existing_employees["userid"].astype(str).str.lower()
            )
        self.existing_ids: Set[str] = set(self.existing_employees["userid"]) if not self.existing_employees.empty else set()
        self.new_ids: Set[str] = set(self.new_employees["userid"]) if not self.new_employees.empty else set()

    def _extract_dependencies(self, row) -> Set[str]:
        """Extract valid dependencies for a single employee."""
        deps = set()

        for field in self.DEPENDENCY_FIELDS:
            if not hasattr(row, field):
                continue

            value = getattr(row, field)
            if pd.isna(value) or value in ("", "None", None):
                continue

            dep_id = str(value).lower()

            # Only depend on other *new* employees
            if dep_id in self.new_ids and dep_id != row.userid:
                deps.add(dep_id)

        return deps

    def _build_graph(self) -> Tuple[Dict[str, Set[str]], Dict[str, int]]:
        """
        Build adjacency graph and in-degree map.
        """
        graph: Dict[str, Set[str]] = defaultdict(set)
        in_degree: Dict[str, int] = {uid: 0 for uid in self.new_ids}

        for row in self.new_employees.itertuples(index=False):
            deps = self._extract_dependencies(row)
            in_degree[row.userid] = len(deps)

            for dep in deps:
                graph[dep].add(row.userid)

        return graph, in_degree

    def get_ordered_batches(self) -> List[pd.DataFrame]:
        """
        Returns employees ordered in creation batches.
        Each batch can be processed in parallel.
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

            batches.append(batch_df)

        # Cycle handling (unprocessed nodes)
        remaining = self.new_ids - processed
        if remaining:
            logger.warning(
                f"Circular dependencies detected for {len(remaining)} employees: {remaining}"
            )
            logger.warning(
                "Manager fields referencing cycle members will be temporarily cleared"
            )

            cycle_df = self.new_employees[
                self.new_employees["userid"].isin(remaining)
            ].copy()

            for field in self.DEPENDENCY_FIELDS:
                if field not in cycle_df.columns:
                    continue

                cycle_df[field] = cycle_df.apply(
                    lambda r: None
                    if pd.notna(r[field])
                    and str(r[field]).lower() in remaining
                    else r[field],
                    axis=1,
                )

            batches.append(cycle_df)

        logger.info(
            f"Resolved {len(self.new_ids)} employees into {len(batches)} batches"
        )
        for i, batch in enumerate(batches, start=1):
            logger.info(f"Batch {i}: {len(batch)} employees")

        return batches

    def get_dependency_summary(self) -> Dict:
        """
        Provides a summary of dependencies among new employees.
        Description:
        Provides a summary of dependencies among new employees.
        Returns:
        A dictionary containing:
            - total_new_employees: Total number of new employees.
            - employees_with_no_dependencies: Count of employees without dependencies.
            - employees_with_dependencies: Count of employees with dependencies.
            - employees_in_cycles: Count of employees involved in dependency cycles. 
              Note:
                A circular dependency occurs when employees reference each other
                (e.g. A → B → C → A). In such cases, none of them can be created first
                without temporarily clearing one or more manager fields.
                
            - cycle_userids: List of userids involved in cycles.
            - missing_dependencies: List of missing dependencies with details.
            - missing_dependency_count: Count of missing dependencies.
        """
        graph, in_degree = self._build_graph()

        no_deps = sum(1 for d in in_degree.values() if d == 0)
        cycles = self.new_ids - set(
            uid for uid, deg in in_degree.items() if deg == 0
        )

        missing = []
        for row in self.new_employees.itertuples(index=False):
            for field in self.DEPENDENCY_FIELDS:
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
            "employees_with_dependencies": len(in_degree) - no_deps,
            "employees_in_cycles": len(cycles),
            "cycle_userids": list(cycles),
            "missing_dependencies": missing,
            "missing_dependency_count": len(missing),
        }
