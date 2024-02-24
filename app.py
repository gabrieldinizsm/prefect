from prefect import flow
import numpy as np
import pandas as pd


def extract(num_rows: int = 100) -> dict:

    data = {}

    data['string_column'] = np.random.choice(
        ['apple', 'banana', 'grape'], size=num_rows)

    data['int_column'] = np.random.randint(1, 100, size=num_rows)

    data['float_column'] = np.random.uniform(1.0, 100.0, size=num_rows)

    data['datetime_column'] = pd.to_datetime(np.random.choice(
        pd.date_range('2010-01-01', '2024-12-31'), size=num_rows))

    return pd.DataFrame(data)


def transform(data: list):
    pass


def load(data: list, path: str):
    pass


df = extract()

print(df.head())
