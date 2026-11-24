"""Small ETL example: CSV -> validation -> PostgreSQL.

Designed for a learning/demo environment, not production deployment.
"""

import csv
import os
from datetime import datetime

import psycopg
from airflow.sdk import dag, task


@dag(
    dag_id="employee_etl_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Trigger manually in the Airflow UI.
    catchup=False,
    tags=["etl", "csv", "postgresql"],
)
def employee_etl_pipeline():
    @task
    def extract() -> list[dict]:
        with open("/opt/airflow/data/employees.csv", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.DictReader(f))
        print(f"Extracted {len(rows)} rows")
        return rows

    @task
    def transform(rows: list[dict]) -> list[dict]:
        cleaned = []
        for row in rows:
            try:
                employee_id = int(row["id"])
                name = row["name"].strip()
                department = row["department"].strip()
                salary = int(row["salary"])
                if employee_id <= 0 or not name or not department or salary <= 0:
                    print(f"Skipping invalid row: {row}")
                    continue
                cleaned.append({
                    "id": employee_id,
                    "name": name,
                    "department": department,
                    "salary": salary,
                })
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                print(f"Skipping malformed row {row}: {exc}")
        print(f"Cleaned {len(cleaned)} rows")
        return cleaned

    @task
    def load(rows: list[dict]) -> None:
        with psycopg.connect(
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"],
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS employees (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        department VARCHAR(100) NOT NULL,
                        salary INTEGER NOT NULL CHECK (salary > 0)
                    )
                """)
                for row in rows:
                    cur.execute("""
                        INSERT INTO employees (id, name, department, salary)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            department = EXCLUDED.department,
                            salary = EXCLUDED.salary
                    """, (row["id"], row["name"], row["department"], row["salary"]))
        print(f"Loaded {len(rows)} rows")

    load(transform(extract()))


employee_etl_pipeline()
