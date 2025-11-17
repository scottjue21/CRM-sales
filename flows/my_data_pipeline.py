import os
# import time          # ← uncomment if using the Airbyte API section
# import httpx         # ← uncomment if using the Airbyte API section

from dotenv import load_dotenv
from prefect import flow, task
from prefect_dbt import DbtCoreOperation

load_dotenv()

# ======================================================================
# Airbyte API configuration (COMMENTED OUT – FREE TIER LIMITATIONS)
# ----------------------------------------------------------------------
# In a production setup, you could use these env vars and functions
# to trigger Airbyte Cloud syncs via API from Prefect.
# ======================================================================

# AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
# AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
# AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID")
# AIRBYTE_BASE_URL = "https://api.airbyte.com"
#
# if not AIRBYTE_CLIENT_ID or not AIRBYTE_CLIENT_SECRET:
#     raise ValueError(
#         "Missing AIRBYTE_CLIENT_ID or AIRBYTE_CLIENT_SECRET in environment. "
#         "These would be required for programmatic Airbyte authentication."
#     )
#
# if not AIRBYTE_CONNECTION_ID:
#     raise ValueError(
#         "Missing AIRBYTE_CONNECTION_ID in environment. "
#         "This should be the ID of the Airbyte connection to sync."
#     )
#
#
# def get_airbyte_access_token() -> str:
#     """
#     Production-style helper: exchange client_id/client_secret
#     for a short-lived access token using Airbyte's /applications/token endpoint.
#     """
#     token_url = f"{AIRBYTE_BASE_URL}/api/v1/applications/token"
#     payload = {
#         "client_id": AIRBYTE_CLIENT_ID,
#         "client_secret": AIRBYTE_CLIENT_SECRET,
#     }
#
#     resp = httpx.post(token_url, json=payload, timeout=30)
#     print("Token endpoint status:", resp.status_code)
#     print("Token endpoint raw body:", resp.text)
#     resp.raise_for_status()
#
#     data = resp.json()
#     access_token = data["access_token"]
#     return access_token
#
#
# @task(name="Extract & Load with Airbyte (API)")
# def run_airbyte_sync_via_api() -> dict:
#     """
#     Production-style task: trigger an Airbyte Cloud sync via API and
#     poll until the job reaches a terminal status.
#     """
#     access_token = get_airbyte_access_token()
#
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json",
#         "Accept": "application/json",
#     }
#
#     # 1) Trigger sync job
#     trigger_resp = httpx.post(
#         f"{AIRBYTE_BASE_URL}/v1/jobs",
#         headers=headers,
#         json={
#             "connectionId": AIRBYTE_CONNECTION_ID,
#             "jobType": "sync",
#         },
#         timeout=30,
#     )
#     trigger_resp.raise_for_status()
#     trigger_data = trigger_resp.json()
#
#     job_id = trigger_data.get("jobId") or trigger_data.get("id")
#     if not job_id:
#         raise RuntimeError(
#             f"Unexpected response when triggering Airbyte sync: {trigger_data}"
#         )
#
#     print(f"Triggered Airbyte sync, job_id={job_id}")
#
#     # 2) Poll job status until it's finished
#     terminal_statuses = {"succeeded", "failed", "cancelled", "incomplete"}
#
#     while True:
#         status_resp = httpx.get(
#             f"{AIRBYTE_BASE_URL}/v1/jobs/{job_id}",
#             headers=headers,
#             timeout=30,
#         )
#         status_resp.raise_for_status()
#         job_data = status_resp.json()
#         status = job_data.get("status")
#
#         print(f"Airbyte job {job_id} status: {status}")
#
#         if status in terminal_statuses:
#             break
#
#         time.sleep(10)
#
#     if status != "succeeded":
#         raise RuntimeError(
#             f"Airbyte sync did not succeed. Final status={status}, payload={job_data}"
#         )
#
#     print(f"Airbyte sync job {job_id} succeeded")
#     return job_data


# ======================================================================
# ACTIVE PIPELINE: MANUAL AIRBYTE + DBT VIA PREFECT
# ======================================================================

@task(name="Extract & Load with Airbyte (manual)")
def airbyte_manual_step():
    """
    Placeholder task used in this project.

    Because of Airbyte Cloud free-trial limitations on API authentication,
    the Airbyte sync is triggered manually from the Airbyte UI instead of
    programmatically from Prefect.

    This task exists to:
    - Document where an automated Airbyte API call would normally run.
    - Show the orchestration step in Prefect's UI / logs.
    """
    print(
        "Skipping Airbyte API call.\n"
        "Assuming the latest Airbyte sync has already been run manually "
        "in the Airbyte Cloud UI for the target connection."
    )


@task(name="dbt build (Core)")
def run_dbt_build():
    """
    Run dbt Core to build models on top of the ingested data.
    """
    op = DbtCoreOperation(
        commands=["dbt build"],
        project_dir="crm_sales_project",  # relative to repo root
        profiles_dir=".",                 # uses profiles.yml in repo root
    )
    return op.run()


@flow(name="airbyte-to-dbt-core-pipeline", log_prints=True)
def my_data_pipeline():
    # 1) log the manual Airbyte step:
    airbyte_manual_step()

    # In a production environment, you could instead do:
    # airbyte_result = run_airbyte_sync_via_api()
    # print(f"Airbyte sync finished: {airbyte_result}")

    # 2) Run dbt transformations
    dbt_result = run_dbt_build()
    print(f"dbt build finished: {dbt_result}")

    return {
        # documents that Airbyte is manual.
        "airbyte": "Manual sync via Airbyte UI (no API call due to free-tier limits)",
        "dbt": dbt_result,
    }


if __name__ == "__main__":
    my_data_pipeline()
