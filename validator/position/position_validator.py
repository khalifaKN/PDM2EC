from utils.logger import get_logger
import pandas as pd

logger = get_logger('position_validator')

class PositionValidator:
    """
    Validator class to check:
    - If the position code already exists in SAP system from the cached position mappings.
    - If the required PDM fields for position creation are present.
    """

    def __init__(self,record: pd.Series, pos_data: pd.DataFrame,emp_data: pd.DataFrame, user_id: str, ec_user_id, results: dict, required_fields: list, raise_if_missing=False):
        self.record = record
        self.pos_data = pos_data
        self.emp_data = emp_data
        self.user_id = user_id
        self.ec_user_id = ec_user_id
        self.results = results
        self.required_fields = required_fields
        self.raise_if_missing = raise_if_missing

    def position_code_exists_in_employees(self) -> str:
        """
        Checks if the position code exists for the given user ID in the employment data.
        Returns:
            str: The position code if it exists, else an empty string.
        """
        try:
            mask = self.emp_data['userid'].astype(str).str.lower().eq(self.ec_user_id.lower())
            result = self.emp_data[mask]
            if not result.empty:
                position_code = result['position'].values[0]
                return position_code
            return None
        except Exception as e:
            msg = f"Error checking position code existence for user ID {self.user_id}: {str(e)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            logger.info(msg)
            return None
        
    def position_code_exists_in_positions(self) -> str | None:
        """
        Returns the position code if it exists and is not assigned to another user.
        """
        try:
            job_code = str(self.record.get('jobcode')).strip().lower()
            location_code = str(self.record.get('address_code')).strip().lower()
            cost_center = str(self.record.get('cost_center')).strip().lower()
            company = str(self.record.get('company')).strip().lower()
            if 'positioncriticality' in self.pos_data.columns:
                self.pos_data['_is_critical'] = (
                    pd.to_numeric(
                        self.pos_data['positioncriticality'],
                        errors='coerce'
                    )
                    .fillna(0)
                    .astype(int)
                    == 1
                )
            else:
                self.pos_data['_is_critical'] = False
            mask = (
                (self.pos_data['jobcode'].astype(str).str.lower() == job_code) &
                (self.pos_data['location'].astype(str).str.lower() == location_code) &
                (self.pos_data['costcenter'].astype(str).str.lower() == cost_center) &
                (self.pos_data['company'].astype(str).str.lower() == company) &
                (~self.pos_data['_is_critical'])  # Exclude critical positions which belong maybe to SCM
            )

            result = self.pos_data[mask]

            if not result.empty:
                for code in result['code']:
                    emp_mask = self.emp_data['position'].astype(str).str.lower().eq(code.lower())
                    emp_result = self.emp_data[emp_mask]
                    # Check if position is assigned to another user
                    if not emp_result.empty:
                        assigned_userid = emp_result['userid'].values[0]
                        # Check if this position is assigned to a different user
                        if assigned_userid.lower() != self.ec_user_id.lower():
                            pass
                    # Check if position code is assigned to another user in current batch
                    in_batch = False
                    results_df = pd.DataFrame.from_dict(
                        {
                            user_id: ctx.position_code
                            for user_id, ctx in self.results.items()
                            if ctx.position_code
                        },
                        orient="index",
                        columns=["position_code"]
                    ).reset_index(names="user_id")
                    results_df["user_id"] = results_df["user_id"].astype(str).str.lower()
                    results_df["position_code"] = results_df["position_code"].astype(str).str.lower()
                    code_l = code.lower()
                    in_batch = (
                        (results_df["position_code"] == code_l) &
                        (results_df["user_id"] != self.user_id.lower())
                    ).any()

                    if in_batch:
                        continue
                    return code

            return None

        except Exception as e:
            msg = f"Error checking position code existence for user ID {self.user_id}: {str(e)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            logger.info(msg)
            return None

    
    def _retrieve_position_data(self, position_code) -> dict:
        """
        Retrieves position data from cache including organizational fields.
        Returns:
            dict: Position data including job_code, cost_center, company, location, 
                  division (HR BU/FU), cust_subunit, and cust_geographicalscope.
        """
        try:
            mask = self.pos_data['code'].astype(str).str.lower().eq(position_code.lower())
            result = self.pos_data[mask]
            if not result.empty:
                job_code = result['jobcode'].values[0]
                cost_center = result['costcenter'].values[0]
                company = result['company'].values[0]
                location = result['location'].values[0]
                # Get organizational fields (may be missing/null in SAP)
                division = result.get('division', pd.Series([None])).values[0] if 'division' in result.columns else None
                cust_subunit = result.get('cust_subunit', pd.Series([None])).values[0] if 'cust_subunit' in result.columns else None
                cust_geoscope = result.get('cust_geographicalscope', pd.Series([None])).values[0] if 'cust_geographicalscope' in result.columns else None
                
                return {
                    "job_code": job_code,
                    "cost_center": cost_center,
                    "company": company,
                    "location": location,
                    "division": division,
                    "cust_subunit": cust_subunit,
                    "cust_geographicalscope": cust_geoscope
                }
            return None
        except Exception as e:
            msg = f"Error retrieving position data for position code {position_code}: {str(e)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            return None
    
    def position_to_update_exists(self) -> bool:
        """
        Checks if the position needs to be updated.
        Returns True if position exists but has missing/incorrect fields.
        Returns:
            bool: True if the position needs update, else False.
        """
        try:
            # First, find the position code from employees or positions
            position_code = self.position_code_exists_in_employees()
            # if not position_code:
            #     position_code = self.position_code_exists_in_positions()
            
            # If no position code found at all, no update needed (will create new)
            if not position_code:
                return False
            
            # Now retrieve the Position entity data to validate its fields
            position_data = self._retrieve_position_data(position_code)
            if not position_data:
                return False
            
            job_code = position_data.get('job_code')
            location = position_data.get('location')
            cost_center = position_data.get('cost_center')
            company = position_data.get('company')
            division = position_data.get('division')  # HR BU/FU
            cust_subunit = position_data.get('cust_subunit')
            cust_geoscope = position_data.get('cust_geographicalscope')
            
            # Handle both 'cost_center' (from PDM) and 'costcenter' (from record)
            record_cost_center = self.record.get('cost_center') or self.record.get('costcenter')
            
            # Check if update is needed
            needs_update = False
            
            # Check if jobCode is missing in SAP Position (critical!)
            if not job_code or str(job_code).strip() in ['', 'None', 'null']:
                needs_update = True
            
            # Check basic field changes
            if position_data and (
                (self.record.get('address_code') and location and str(self.record.get('address_code')).lower() != str(location).lower()) or
                (record_cost_center and cost_center and str(record_cost_center).lower() != str(cost_center).lower()) or
                (self.record.get('company') and company and str(self.record.get('company')).lower() != str(company).lower()) or
                (self.record.get('jobcode') and job_code and str(self.record.get('jobcode')).lower() != str(job_code).lower())
            ):
                needs_update = True
            
            # Check if organizational fields are missing in SAP
            if not division or str(division).strip() in ['', 'None', 'null']:
                needs_update = True
            
            if not cust_subunit or str(cust_subunit).strip() in ['', 'None', 'null']:
                needs_update = True
            
            if not cust_geoscope or str(cust_geoscope).strip() in ['', 'None', 'null']:
                needs_update = True
            
            if needs_update:
                return True
            
            return False
        except Exception as e:
            msg = f"Error checking position update existence for user ID {self.user_id}: {str(e)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            logger.info(msg)
            return False
    def validate_required_fields(self) -> bool:
        """
        Validates that all required fields are present and not NaN in the record.
        Returns:
            bool: True if all required fields are present, else False.
        """
        missing_fields = [field for field in self.required_fields if field not in self.record or pd.isna(self.record[field])]
        self.missing_fields = missing_fields
        if missing_fields:
            msg = f"Missing required fields for user ID {self.user_id}: {', '.join(missing_fields)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            logger.error(msg)
            return False
        return True