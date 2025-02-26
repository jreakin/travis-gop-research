import pandas as pd
import polars as pl
from pathlib import Path
from __init__ import CONFIG, set_datetime

atx_df = set_datetime(pd.read_csv("data/austin_cf.csv"), 'Payment_Date', '%m/%d/%Y')
fight_for_austin_df = set_datetime(pd.read_csv("data/fight_for_austin_expenses_atx.csv"), 'Payment_Date', '%m/%d/%Y')
save_austin_now_df = set_datetime(pd.read_csv("data/save_austin_now_expenses_atx.csv"), 'Payment_Date', '%m/%d/%Y')

atx_ct = pd.crosstab(
    index=[atx_df['Paid_By'], atx_df['Payee']],
    columns=atx_df['Payment_Date'].dt.year,
    values=atx_df['Payment_Amount'].astype(float).round(2),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

fight_for_austin_ct = pd.crosstab(
    index=[fight_for_austin_df['Paid_By'], fight_for_austin_df['Payee']],
    columns=fight_for_austin_df['Payment_Date'].dt.year,
    values=fight_for_austin_df['Payment_Amount'].astype(float).round(2),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

save_austin_now_ct = pd.crosstab(
    index=[save_austin_now_df['Paid_By'], save_austin_now_df['Payee']],
    columns=save_austin_now_df['Payment_Date'].dt.year,
    values=save_austin_now_df['Payment_Amount'].astype(float).round(2),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)