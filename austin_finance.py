import pandas as pd
from __init__ import set_datetime, CreateCrosstab

atx_df = set_datetime(
    pd.read_csv("data/austin_cf.csv"), 'Payment_Date', '%m/%d/%Y')  # Austin Campaign Finance
fight_for_austin_df = set_datetime(
    pd.read_csv("data/fight_for_austin_expenses_atx.csv"), 'Payment_Date', '%m/%d/%Y')  # Fight for Austin
save_austin_now_df = set_datetime(
    pd.read_csv("data/save_austin_now_expenses_atx.csv"), 'Payment_Date', '%m/%d/%Y') # Save Austin Now

ATXExpenseCrosstab = CreateCrosstab(
    index_fields=['Paid_By', 'Payee'],
    column_fields='Payment_Date',
    amount_field='Payment_Amount'
) # Austin Expense Crosstab Base

atx_ct = ATXExpenseCrosstab.create(atx_df)  # Austin Expense Crosstab

fight_for_austin_ct = ATXExpenseCrosstab.create(fight_for_austin_df) # Fight for Austin Expense Crosstab

save_austin_now_ct = ATXExpenseCrosstab.create(save_austin_now_df) # Save Austin Now Expense Crosstab