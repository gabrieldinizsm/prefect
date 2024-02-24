from prefect import flow
import numpy as np
import pandas as pd


def extract(num_rows: int = 100) -> pd.DataFrame:

    data = {}

    data['string_column'] = np.random.choice(
        ['apple', 'banana', 'grape'], size=num_rows)

    data['int_column'] = np.random.randint(1, 100, size=num_rows)

    data['float_column'] = np.random.uniform(1.0, 100.0, size=num_rows)

    data['datetime_column'] = pd.to_datetime(np.random.choice(
        pd.date_range('2010-01-01', '2024-12-31'), size=num_rows))

    return pd.DataFrame(data)


def transform(df: pd.DataFrame):

    df['int_column'] = df['int_column']-2
    df['float_column'] = df['float_column']*2
    pass


def load(data: list, path: str):
    pass


if __name__ == '__main__':

    df = extract()

    print(df.head())
