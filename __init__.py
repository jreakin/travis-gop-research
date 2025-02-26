import tomli
import pandas as pd

CONFIG = tomli.load(open("config.toml", 'rb'))

def set_datetime(df: pd.DataFrame, col: str = 'expendDt', fmt: str = '%Y%m%d') -> pd.DataFrame:
    df[col] = pd.to_datetime(df[col], format=fmt)
    return df