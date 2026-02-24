# Python to run failed dags
# It will run the dag from scratch, not only failed tasks

import httpx

DAG_ID = "emol"
ENDPOINT = "http://localhost:8080"
HEADERS = {
    "Content-Type": "application/json"
}


def get_token():
    auth = {
        "username": "admin",
        "password": "admin"
    }
    with httpx.Client(headers=HEADERS) as client:
        request = client.post(f"{ENDPOINT}/auth/token", json=auth)
        request.raise_for_status()
        token = request.json()["access_token"]
        return token


def get_num_dags(access_token, state=["failed"]):
    params = {
        "state": state
    }
    with httpx.Client(
            headers={
                **HEADERS,
                "Authorization": f"Bearer {access_token}"
            },
            params={
                **params, "limit": 1
            }) as client:
        request = client.get(f"{ENDPOINT}/api/v2/dags/{DAG_ID}/dagRuns")
        request.raise_for_status()
        data = request.json()
        total_entries = data["total_entries"]

    return total_entries


def get_dag_runs(access_token, state=["failed"]):
    params = {
        "state": state
    }

    with httpx.Client(
            headers={
                **HEADERS,
                "Authorization": f"Bearer {access_token}"
            },
            params=params) as client:
        request = client.get(f"{ENDPOINT}/api/v2/dags/{DAG_ID}/dagRuns")
        request.raise_for_status()
        data = request.json()
        dag_runs = data["dag_runs"]

    return dag_runs


def rerun_failed_dags():
    access_token = get_token()
    params = {
        "dry_run": "false"
    }

    num_dags = get_num_dags(access_token, ["failed"])
    while num_dags > 0:
        dag_runs = get_dag_runs(access_token, ["failed"])

        for dag_run in dag_runs:
            dag_run_id = dag_run["dag_run_id"]
            dag_state = dag_run["state"]

            with httpx.Client(
                    headers={
                        **HEADERS,
                        "Authorization": f"Bearer {access_token}"
                    }) as client:

                print(
                    f"Rerunning dag run \"{dag_run_id}\", with state \"{dag_state}\""
                )

                request = client.post(
                    f"{ENDPOINT}/api/v2/dags/{DAG_ID}/dagRuns/{dag_run_id}/clear",
                    json=params
                )
                request.raise_for_status()

        num_dags = get_num_dags(access_token, ["failed"])


if __name__ == "__main__":
    try:
        rerun_failed_dags()
    except Exception as e:
        print(e)
