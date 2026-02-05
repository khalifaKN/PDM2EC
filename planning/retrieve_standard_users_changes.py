from mapper.field_mappings import EC_TABLE_TO_PDM_FIELD_MAPPING_STANDARD_USERS, get_pdm_column_value
from utils.logger import get_logger
from planning.base_users_updates_retriever import BaseUsersUpdatesRetriever
from planning.field_change_data import FieldChange

Logger = get_logger("standard_users_updates_retriever")

class StandardUsersUpdatesRetriever(BaseUsersUpdatesRetriever):
    """
    Compares PDM data against EC data to identify field-level changes for standard users.
    The fields compared are defined in the EC_TABLE_TO_PDM_FIELD_MAPPING_STANDARD_USERS.
    Detected changes are persisted in the employee_field_changes table in chunks.
    """

    def __init__(self, pdm_data, ec_data, standard_users_ids, postgres_connector, table_names, chunk_size=1000, sap_email_data=None, run_id=None, batch_context=None):
        super().__init__(
            pdm_data=pdm_data,
            ec_data=ec_data,
            user_ids=standard_users_ids,
            postgres_connector=postgres_connector,
            table_names=table_names,
            sap_email_data=sap_email_data,
            chunk_size=chunk_size,
            run_id=run_id,
            batch_context=batch_context
        )
        self.standard_users_ids = standard_users_ids

    def generate_changes(self):
        """
        Vectorized comparison of non-email fields for Standard Users.
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
                ec_field: get_pdm_column_value(ec_field, scm_im=False)
                for ec_field in EC_TABLE_TO_PDM_FIELD_MAPPING_STANDARD_USERS.keys()
            }

            # Filter standard users
            pdm_common = self.pdm_data[self.pdm_data["userid"].isin(self.standard_users_ids)].set_index("userid")
            ec_common = self.ec_data[self.ec_data["userid"].isin(self.standard_users_ids)].set_index("userid")

            common_users = pdm_common.index.intersection(ec_common.index)
            pdm_common = pdm_common.loc[common_users]
            ec_common = ec_common.loc[common_users]

            # Compare fields vectorized
            for ec_field, pdm_col in field_mapping.items():

                # Skip email fields
                if ec_field in ("email", "email_2", "private_email") or pdm_col in ("email", "private_email"):
                    continue

                # Skip if column missing
                if not pdm_col or pdm_col not in pdm_common.columns or ec_field not in ec_common.columns:
                    continue

                pdm_series = pdm_common[pdm_col]
                ec_series = ec_common[ec_field]

                # Normalize sentinel values: treat "N/A", "NO_MANAGER", "NO_HR" as equivalent to null
                sentinel_values = ["N/A", "NO_MANAGER", "NO_HR", "n/a", "no_manager", "no_hr"]
                pdm_normalized = pdm_series.where(~pdm_series.astype(str).str.strip().str.upper().isin([v.upper() for v in sentinel_values]))
                ec_normalized = ec_series.where(~ec_series.astype(str).str.strip().str.upper().isin([v.upper() for v in sentinel_values]))

                # PDM has value AND differs from EC (after normalization)
                diff_mask = pdm_normalized.notna() & (pdm_normalized != ec_normalized)

                # Exclude rows where both are null after normalization
                diff_mask &= ~(pdm_normalized.isna() & ec_normalized.isna())
                # Fill na here to avoid having NaN in index
                changed_users = common_users[diff_mask.fillna(False)]

                for userid in changed_users:
                    yield FieldChange(
                        userid=userid,
                        field_name=pdm_col,
                        ec_value=ec_common.at[userid, ec_field],
                        pdm_value=pdm_common.at[userid, pdm_col],
                        is_scm_user=False,
                        is_im_user=False
                    )

            # Email logic stays per user since it has many sap rules behind for each user
            for userid in common_users:
                yield from self._control_email_updates(
                    userid=userid,
                    pdm_row=pdm_common.loc[userid],
                    is_scm_user=False,
                    is_im_user=False
                )

            Logger.info("Completed generating changes for standard users.")

        except Exception as e:
            Logger.error(f"Error generating changes: {e}")
            raise