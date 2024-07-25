import os
from dotenv import load_dotenv
import requests
import pandas as pd
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()


class JobicyAPI:
    def __init__(self, base_url: str, industry: str, count: int):
        self.base_url = base_url
        self.industry = industry
        self.count = count
        self.data = None

    def fetch_data(self):
        url = f"{self.base_url}?count={self.count}&industry={self.industry}"
        response = requests.get(url)

        if response.status_code == 200:
            self.data = response.json()
        else:
            response.raise_for_status()

    def get_jobs_data(self):
        if self.data and 'jobs' in self.data:
            return pd.DataFrame(self.data['jobs'])
        else:
            return pd.DataFrame()


class Snowflake:
    def __init__(self, account: str, user: str, password: str, database: str, schema: str, warehouse: str):
        self.engine = create_engine(URL(
            account=account,
            user=user,
            password=password,
            database=database,
            schema=schema,
            warehouse=warehouse
        ))

    def save_to_snowflake(self, df: pd.DataFrame, table_name: str):
        # Clean missing value
        df = df.fillna('')

        print(
            f"Saving to Snowflake: {df.shape[0]} rows and {df.shape[1]} columns")

        # Convert data type
        df = df.astype(str)

        # Save to Snowflake
        try:
            df.to_sql(table_name, self.engine, if_exists='replace',
                      index=False, method='multi')
        except Exception as e:
            print(f"Erro saving to Snowflake: {e}")


def main():
    api = JobicyAPI(
        base_url="https://jobicy.com/api/v2/remote-jobs",
        industry="data-science",
        count=5
    )

    api.fetch_data()
    jobs_df = api.get_jobs_data()

    if not jobs_df.empty:
        snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT')
        snowflake_user = os.getenv('SNOWFLAKE_USER')
        snowflake_password = os.getenv('SNOWFLAKE_PASSWORD')
        snowflake_database = os.getenv('SNOWFLAKE_DATABASE')
        snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA')
        snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')

        saver = Snowflake(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
            database=snowflake_database,
            schema=snowflake_schema,
            warehouse=snowflake_warehouse
        )

        saver.save_to_snowflake(jobs_df, table_name='jobs_list')
        print("Data saved successfully!")
    else:
        print("There is no data to save in Snowflake!")


if __name__ == "__main__":
    main()
