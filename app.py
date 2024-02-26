from prefect import task, flow
from prefect.tasks import task_input_hash
import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
import httpx


@task(retries=1)
def get_repo_info(repo_owner: str = "gabrieldinizsm", repo_name: str = "prefect"):
    """Get info about a repo - will retry twice after failing"""
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}"
    api_response = httpx.get(url)
    api_response.raise_for_status()
    repo_info = api_response.json()
    return repo_info


@task
def extract(num_rows: int = 100) -> pd.DataFrame:

    data = {}
    data['string_column'] = np.random.choice(
        ['abc', 'def', 'ghi'], size=num_rows)
    data['int_column'] = np.random.randint(1, 100, size=num_rows)
    data['float_column'] = np.random.uniform(1.0, 100.0, size=num_rows)
    data['datetime_column'] = pd.to_datetime(np.random.choice(
        pd.date_range('2010-01-01', '2024-12-31'), size=num_rows))
    return pd.DataFrame(data)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def transform(df: pd.DataFrame):

    df['int_column'] = df['int_column']-2
    df['float_column'] = df['float_column']*2
    return df


@task
def load(df: pd.DataFrame, path: str):
    df.to_csv(path, sep=';')


@flow
def main():
    print(f"Started at: {datetime.now()}")
    load(transform(extract(100)), str(os.getcwd()) + '\\' + 'etl.csv')
    print(f"Finished at: {datetime.now()}")


if __name__ == '__main__':
    main()
