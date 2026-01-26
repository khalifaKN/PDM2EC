from cache.postgres_cache import PostgresDataCache

def get_userid_from_personid(person_id):
        """
        Retrieve EC USERID from PDM USERID using cached data if they are different.
        Args:
            person_id (str): The PDM user ID to look up.
        Returns:
            str: The corresponding USERID if different, else the original PERSON_ID_EXTERNAL.
        """
        postgres_cache = PostgresDataCache()
        different_userid_personid_df =postgres_cache.get(
            "different_userid_personid_data_df"
        )
        if different_userid_personid_df is not None:
            match = different_userid_personid_df[
                different_userid_personid_df["person_id_external"] == person_id
            ]
            if not match.empty:
                return match["userid"].values[0]
        return person_id