import pandas as pd
import polars as pl
from __init__ import CONFIG
from tec_research import (
    tec_expenses, tec_contributions,
    tec_tcrp, potomac_frame,
    wab_frame, tec_beyond2018,
    mackowiak_frame, tec_ct, potomac_ct, wab_ct,
    keep_round_rock_safe_contributors, texas_emergency_network, keep_round_rock_safe_expenses,
    keep_round_rock_expense_ct, fight_for_tomorrow_contributors,
    fight_for_tomorrow_expenses, fight_for_tomorrow_expense_ct, travis_cec_expenses, travis_cec_contributions,
    travis_cec_expense_ct, keep_round_rock_safe_payroll_ct, travis_cec_payroll_ct, travis_cec_payroll, mackowiak_ct
)
from fec_research import (
    travis_county_fec_contributions,
    make_liberty_win_fec_contributions,
    travis_county_fec_contributions_ct
)


potomac_frame['expendDt'] = pd.to_datetime(potomac_frame['expendDt'], format='%Y%m%d')
potomac_frame = potomac_frame[potomac_frame['expendDt'] > '2018-01-01']



make_liberty_win_expenses = tec_expenses.filter(pl.col('filerName').str.contains('Make Liberty Win')).collect().to_pandas()
make_liberty_win_contributions = tec_contributions.filter(pl.col('filerName').str.contains('Make Liberty Win')).collect().to_pandas()

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