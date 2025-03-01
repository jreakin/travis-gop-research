import tomli
import pandas as pd

CONFIG = tomli.load(open("config.toml", 'rb'))

def change_non_ints_to_string(func):

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
    df[col] = pd.to_datetime(df[col], format=fmt)
    return df