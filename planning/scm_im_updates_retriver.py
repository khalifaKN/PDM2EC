from typing import Iterator
from utils.logger import get_logger
from mapper.field_mappings import EC_TABLE_TO_PDM_FIELD_MAPPING_SCM_IM, get_pdm_column_value
from planning.base_users_updates_retriever import BaseUsersUpdatesRetriever
from planning.field_change_data import FieldChange
import pandas as pd

Logger = get_logger("scm_im_updates_retriever")

class SCM_IM_UpdatesRetriever(BaseUsersUpdatesRetriever):
    """
    Compares PDM data against EC data to identify field-level changes for SCM and IM users.
    The fields compared are defined in EC_TABLE_TO_PDM_FIELD_MAPPING_SCM_IM.
    Detected changes are persisted in the employee_field_changes table in chunks via BaseUsersUpdatesRetriever.
    """

    def __init__(self, pdm_data: pd.DataFrame, ec_data: pd.DataFrame, scm_users_ids: set, im_users_ids: set,
                 postgres_connector, table_names, chunk_size: int = 1000, sap_email_data=None, run_id=None, batch_context=None):
        all_users_ids = scm_users_ids.union(im_users_ids)
        super().__init__(
            pdm_data=pdm_data,
            ec_data=ec_data,
            user_ids=all_users_ids,
            postgres_connector=postgres_connector,
            table_names=table_names,
            sap_email_data=sap_email_data,
            chunk_size=chunk_size,
            run_id=run_id,
            batch_context=batch_context
        )
        self.scm_users_ids = set(scm_users_ids)
        self.im_users_ids = set(im_users_ids)

    def generate_changes(self) -> Iterator[FieldChange]:
        """
        Vectorized comparison of non-email fields for SCM/IM users.
        Yields FieldChange objects.
        Email logic is handled separately by _control_email_updates.
        """
        try:
            # Normalize user IDs
            self.pdm_data["userid"] = self.pdm_data["userid"].astype(str).str.lower()
            self.ec_data["userid"] = self.ec_data["userid"].astype(str).str.lower()

            self.pdm_data = self.pdm_data.drop_duplicates(subset=["userid"], keep="first")
            self.ec_data = self.ec_data.drop_duplicates(subset=["userid"], keep="first")

            # Map EC fields to PDM columns
            field_mapping = {
                ec_field: get_pdm_column_value(ec_field)
                for ec_field in EC_TABLE_TO_PDM_FIELD_MAPPING_SCM_IM.keys()
            }

            # Filter SCM/IM users
            pdm_common = self.pdm_data[self.pdm_data["userid"].isin(self.user_ids)].set_index("userid")
            ec_common = self.ec_data[self.ec_data["userid"].isin(self.user_ids)].set_index("userid")

            common_users = pdm_common.index.intersection(ec_common.index)
            pdm_common = pdm_common.loc[common_users]
            ec_common = ec_common.loc[common_users]

            scm_users = self.scm_users_ids
            im_users = self.im_users_ids

            # Compare non-email fields vectorized
            for ec_field, pdm_col in field_mapping.items():
                # Skip email fields
                if ec_field in ("email", "email_2") or pdm_col in ("email", "private_email"):
                    continue

                # Skip if column missing
                if not pdm_col or pdm_col not in pdm_common.columns or ec_field not in ec_common.columns:
                    continue

                pdm_series = pdm_common[pdm_col]
                ec_series = ec_common[ec_field]

                # Normalize sentinel values: treat "N/A", "NO_MANAGER", "NO_HR" as equivalent to null
                # This prevents false positives when PDM has "N/A" and EC has "NO_MANAGER"/"NO_HR"
                sentinel_values = ["N/A", "NO_MANAGER", "NO_HR", "n/a", "no_manager", "no_hr"]
                pdm_normalized = pdm_series.where(~pdm_series.astype(str).str.strip().str.upper().isin([v.upper() for v in sentinel_values]))
                ec_normalized = ec_series.where(~ec_series.astype(str).str.strip().str.upper().isin([v.upper() for v in sentinel_values]))

                # PDM has value AND differs from EC (after normalization)
                diff_mask = pdm_normalized.notna() & (pdm_normalized != ec_normalized)

                # Exclude rows where both are null after normalization
                diff_mask &= ~(pdm_normalized.isna() & ec_normalized.isna())

                changed_users = common_users[diff_mask.fillna(False)]

                for userid in changed_users:
                    yield FieldChange(
                        userid=userid,
                        field_name=pdm_col,
                        ec_value=ec_common.at[userid, ec_field],
                        pdm_value=pdm_common.at[userid, pdm_col],
                        is_scm_user=userid in scm_users,
                        is_im_user=userid in im_users
                    )

            # Email logic stays per user since it has many sap rules behind for each user
            for userid in common_users:
                yield from self._control_email_updates(
                    userid=userid,
                    pdm_row=pdm_common.loc[userid],
                    is_scm_user=userid in scm_users,
                    is_im_user=userid in im_users
                )

            Logger.info("Completed generating changes for SCM/IM users (vectorized).")

        except Exception as e:
            Logger.error(f"Error generating SCM/IM changes: {e}")
            raise
