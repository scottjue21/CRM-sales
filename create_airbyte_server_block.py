import os
from prefect import flow, task
from prefect_airbyte.server import AirbyteServer
from prefect_airbyte.connections import AirbyteConnection, AirbyteSyncResult
from prefect_airbyte.flows import run_connection_sync
from dotenv import load_dotenv
from prefect_dbt.cli.commands import DbtCoreOperation

# Load environment variables from .env
load_dotenv()

AIRBYTE_API_KEY = os.getenv("AIRBYTE_API_KEY")
AIRBYTE_SERVER_HOST = os.getenv("AIRBYTE_SERVER_HOST")
AIRBYTE_SERVER_PORT = os.getenv("AIRBYTE_SERVER_PORT")
AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")

remote_airbyte_server = AirbyteServer(
    api_key= AIRBYTE_API_KEY,
    server_host= AIRBYTE_SERVER_HOST,
    server_port= AIRBYTE_SERVER_PORT,
    use_https= True
)

remote_airbyte_server.save("my-remote-airbyte-server", overwrite=True)

airbyte_connection = AirbyteConnection(
    airbyte_server=remote_airbyte_server,
    connection_id= AIRBYTE_CONNECTION_ID
)

@task(name="Extract, Load with Airbyte")
def run_airbyte_sync() -> AirbyteSyncResult:
    try:
        print("Triggering Airbyte sync...")
        job_run = airbyte_connection.trigger()
        print("Airbyte trigger succeeded. Waiting for completion...")
        job_run.wait_for_completion()
        return job_run.fetch_result()

    except Exception as e:
        # Print full details Prefect normally hides
        print("\n===== AIRBYTE ERROR DETAILS =====")
        print(type(e))
        print(str(e))
        print("=================================\n")
        raise

@task(name="dbt build models")
def run_dbt_commands(prev_task_result):
    run_dbt_build = DbtCoreOperation(
        commands=["dbt build"],
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        wait_for=prev_task_result
    )
    return run_dbt_build.run()

@flow(log_prints=True)
def my_data_pipeline():
    # 1) sync data via Airbyte
    airbyte_sync_result = run_airbyte_sync(airbyte_connection)

    # 2) run dbt after Airbyte finishes
    dbt_result = run_dbt_commands(airbyte_sync_result)

    print(f"Airbyte result: {airbyte_sync_result}")
    print(f"dbt result: {dbt_result}")

if __name__ == "__main__":
    my_data_pipeline()