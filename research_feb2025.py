import pandas as pd
import polars as pl
from pathlib import Path
from tec_research import (
    tec_expenses, tec_contributions,potomac_frame, wab_frame, potomac_ct, wab_ct,
    keep_round_rock_safe_contributors, fight_for_tomorrow_expenses, fight_for_tomorrow_expense_ct, travis_cec_expenses,
    travis_cec_expense_ct, keep_round_rock_safe_payroll_ct, travis_cec_payroll_ct, mackowiak_ct, potomac_all_years
)
from austin_finance import fight_for_austin_ct, fight_for_austin_df, save_austin_now_ct, save_austin_now_df

# Amount Made Before/After 2018
before2018_cols = [col for col in potomac_all_years.columns if isinstance(col, int) and col < 2018]
after2018_cols = [col for col in potomac_all_years.columns if isinstance(col, int) and col >= 2018]
before2018_frame = potomac_all_years.loc[:, before2018_cols]
total_before2018 = before2018_frame.loc['Total'].sum(axis=1)
avg_before2018 = total_before2018 / len(before2018_cols)

after2018_frame = potomac_all_years.loc[:, after2018_cols]
total_after2018 = after2018_frame.loc['Total'].sum(axis=1)
avg_after2018 = total_after2018 / len(after2018_cols)

potomac_frame['expendDt'] = pd.to_datetime(potomac_frame['expendDt'], format='%Y%m%d')
potomac_frame = potomac_frame[potomac_frame['expendDt'] > '2018-01-01']

make_liberty_win_expenses = tec_expenses.filter(pl.col('filerName').str.contains('Make Liberty Win')).collect().to_pandas()
make_liberty_win_contributions = tec_contributions.filter(pl.col('filerName').str.contains('Make Liberty Win')).collect().to_pandas()

# Exports
keep_round_rock_safe_payroll_ct.to_csv('data/keep_round_rock_safe_payroll.csv')
travis_cec_payroll_ct.to_csv('data/travis_cec_payroll.csv')
wab_ct.to_csv('data/wab_pac_payments.csv')
wab_frame.to_csv('data/wab_pac_payments_raw.csv')
potomac_ct.to_csv('data/potomac_pac_payments.csv')
keep_round_rock_safe_contributors.to_csv('data/keep_round_rock_safe_contributors.csv')
fight_for_tomorrow_expense_ct.to_csv('data/fight_for_tomorrow_expenses.csv')
fight_for_tomorrow_expenses.to_csv('data/fight_for_tomorrow_expenses_raw.csv')
travis_cec_expense_ct.to_csv('data/travis_cec_expenses.csv')
travis_cec_expenses.to_csv('data/travis_cec_expenses_raw.csv')
mackowiak_ct.to_csv('data/mackowiak_entities_payments.csv')
fight_for_austin_ct.to_csv('data/fight_for_austin_expenses.csv')
fight_for_austin_df.to_csv('data/fight_for_austin_expenses_raw.csv')
save_austin_now_df.to_csv('data/save_austin_now_expenses_raw.csv')
save_austin_now_ct.to_csv('data/save_austin_now_expenses.csv')
potomac_all_years.to_csv('data/potomac_all_years.csv')