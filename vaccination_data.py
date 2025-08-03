import pandas as pd
from mysql_handler import MySQLManager

class VaccinationCSVLoader:
    def __init__(self, db: MySQLManager):
        self.db = db

    def load_csv(self, file_path: str):
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            print(f"[ERROR] Could not read CSV file: {e}")
            return

        # Rename and normalize columns
        df = df.rename(columns={
            "location": "country_name",
            "date": "report_date",
            "total_vaccinations": "total_vaccinations",
            "people_vaccinated": "people_vaccinated",
            "people_fully_vaccinated": "people_fully_vaccinated"
        })

        # Clean data
        df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce")
        df = df.dropna(subset=["report_date", "country_name"])
        df = df.sort_values("report_date").drop_duplicates(subset=["country_name"], keep="last")

        # Prepare records
        data_to_insert = df[[
            "report_date", "country_name", "total_vaccinations",
            "people_vaccinated", "people_fully_vaccinated"
        ]].to_dict(orient="records")

        if not data_to_insert:
            print("No valid records found to load.")
            return

        # Insert into database
        try:
            self.db.insert_records('vaccination_data', data_to_insert)
            print(f"\n Loaded {len(data_to_insert)} vaccination records.\n")
        except Exception as e:
            print(f"[ERROR] Failed to insert records into DB: {e}")

