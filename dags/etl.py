from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="ETL_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:

    # Step 1 : Create table if it does not exist
    @task
    def create_table():
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    # Step 2 : Extract the NASA API data (APOD)
    extract_apod = HttpOperator(
        task_id="extract_apod",
        http_conn_id="nasa_api",  # Connection ID defined in Airflow for the NASA API
        endpoint="planetary/apod",  # NASA API endpoint for APOD
        method="GET",
        data={"api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},  # API key from the connection
        response_filter=lambda response: response.json(),  # Convert response to JSON
    )

    # Step 3 : Transform the data (keep only the fields to save)
    @task
    def transform_apod_data(response: dict) -> dict:
        return {
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""),
            "date": response.get("date", ""),
            "media_type": response.get("media_type", ""),
        }

    # Step 4 : Load the data into PostgreSQL
    @task
    def load_data_to_postgres(apod_data: dict):
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(
            insert_query,
            parameters=(
                apod_data["title"],
                apod_data["explanation"],
                apod_data["url"],
                apod_data["date"],
                apod_data["media_type"],
            ),
        )

    # Step 5 : Task dependencies
    # The table must exist before the extraction
    create_table() >> extract_apod
    # Transform, then load
    transformed_data = transform_apod_data(extract_apod.output)
    load_data_to_postgres(transformed_data)