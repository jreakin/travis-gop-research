from __future__ import annotations
import tomli
import pandas as pd
from dataclasses import dataclass
from functools import partial

CONFIG = tomli.load(open("config.toml", 'rb'))

def change_non_ints_to_string(func):
    """ Decorator processes a DataFrame returned by the decorated
    function and converts specific columns to float and rounds them to 2 decimal places;
    converts other columns to strings."""

    def wrapper(*args, **kwargs):
        df = func(*args, **kwargs)
        for col in df.columns:
            if col in ['expendAmount', 'Payment_Amount', 'contribution_receipt_amount', 'Total']:
                df[col] = df[col].astype(float).round(2)
            elif df[col].dtype == 'float64' or df[col].dtype == 'int64':
                df[col] = df[col].astype(str)
        return df
    return wrapper

@change_non_ints_to_string
def set_datetime(df: pd.DataFrame, col: str = 'expendDt', fmt: str = '%Y%m%d') -> pd.DataFrame:
    """converts a specified column in a DataFrame to datetime format."""
    df[col] = pd.to_datetime(df[col], format=fmt)
    return df

@dataclass
class CreateCrosstab:
    """
    CreateCrosstab
    Creates a crosstab (pivot table) from a DataFrame and rounds float columns to 2 decimal places.
    """
    index_fields: list | str
    column_fields: list | str
    amount_field: str

    @staticmethod
    def _round_floats(func):
        """static method decorator that rounds float columns in the DataFrame to 2 decimal places."""
        def wrapper(*args, **kwargs):
            df: pd.DataFrame = func(*args, **kwargs)
            for col in df.columns:
                if df[col].dtype == 'float64':
                    df[col] = df[col].round(2)
            return df
        return wrapper

    @_round_floats
    def create(self, df: pd.DataFrame):
        """Creates a crosstab from the DataFrame."""
        ct = partial(pd.crosstab, aggfunc='sum', margins=True, margins_name='Total')
        if isinstance(self.index_fields, str):
            _index_fields = partial(ct, index=df[self.index_fields])
        elif isinstance(self.index_fields, list):
            _index_fields = partial(ct, index=[df[x] for x in self.index_fields])
        else:
            raise ValueError("index_fields must be a list or string")

        if isinstance(self.column_fields, str):
            _column_fields = partial(_index_fields, columns=df[self.column_fields].dt.year)
        elif isinstance(self.column_fields, list):
            _column_fields = partial(_index_fields, columns=[df[x] for x in self.column_fields])
        else:
            raise ValueError("column_fields must be a list or string")

        return _column_fields(
            values=df[self.amount_field].astype(float).round(2)
        ).sort_values(
            by='Total',
            ascending=False
        )
