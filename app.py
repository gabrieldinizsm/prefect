from prefect import task, flow
import numpy as np
import pandas as pd
import os


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


@task
def transform(df: pd.DataFrame):

    df['int_column'] = df['int_column']-2
    df['float_column'] = df['float_column']*2
    print(df.head())
    return df


@task
def load(df: pd.DataFrame, path: str):
    print(path)
    df.to_csv(path, sep=';')


@flow(log_prints=True)
def main():
    load(transform(extract(100)), str(os.getcwd()) + '\\' + 'etl.csv')


if __name__ == '__main__':
    main()
