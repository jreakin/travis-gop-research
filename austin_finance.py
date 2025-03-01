import pandas as pd
import polars as pl
from pathlib import Path
from __init__ import CONFIG, set_datetime, CreateCrosstab

atx_df = set_datetime(
    pd.read_csv("data/austin_cf.csv"), 'Payment_Date', '%m/%d/%Y')
fight_for_austin_df = set_datetime(
    pd.read_csv("data/fight_for_austin_expenses_atx.csv"), 'Payment_Date', '%m/%d/%Y')
save_austin_now_df = set_datetime(
    pd.read_csv("data/save_austin_now_expenses_atx.csv"), 'Payment_Date', '%m/%d/%Y')

ATXExpenseCrosstab = CreateCrosstab(
    index_fields=['Paid_By', 'Payee'],
    column_fields='Payment_Date',
    amount_field='Payment_Amount'
)

atx_ct = ATXExpenseCrosstab.create(atx_df)

fight_for_austin_ct = ATXExpenseCrosstab.create(fight_for_austin_df)

save_austin_now_ct = ATXExpenseCrosstab.create(save_austin_now_df)