import tomli
import pandas as pd

CONFIG = tomli.load(open("config.toml", 'rb'))

def set_datetime(df: pd.DataFrame, col: str = 'expendDt') -> pd.DataFrame:
    df[col] = pd.to_datetime(df[col], format='%Y%m%d')
    return df