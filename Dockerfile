FROM apache/airflow:slim-3.4.0b2

# Airflow's official image already runs as the airflow user.
RUN pip install --no-cache-dir "psycopg[binary]>=3.2,<4"
