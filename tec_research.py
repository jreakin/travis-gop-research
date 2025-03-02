import pandas as pd
import polars as pl
from pathlib import Path
from __init__ import CONFIG, set_datetime, CreateCrosstab

""" === File Paths === """
tec_expenses_path = Path(__file__).parents[4] / 'campaignfinance/tmp/texas/expend_20250211w.parquet'
tec_contributions_path = Path(__file__).parents[4] / 'campaignfinance/tmp/texas/contribs_20250211w.parquet'

""" === Load Data === """
tec_contributions = pl.scan_parquet(tec_contributions_path)
tec_expenses = pl.scan_parquet(tec_expenses_path)
tec_expenses = tec_expenses.with_columns([pl.col('expendDt').str.strptime(pl.Date, '%Y%m%d')])

""" === Data Frames === """
tec_tcrp = set_datetime(
    tec_expenses.filter(
    (
        pl.col('filerIdent').is_in(CONFIG['TRAVIS_GOP_POSSIBLE'])
    ))
    .collect()
    .to_pandas()
)

potomac_frame_raw = set_datetime(
    tec_expenses.filter(
        pl.col('payeeNameOrganization').str.contains('Potomac')
    )
    .collect()
    .to_pandas()
)
wab_frame = set_datetime(
    tec_expenses.filter(
        pl.col('payeeNameOrganization').str.starts_with('WAB ')
    )
    .collect()
    .to_pandas()
)

""" Filter Data """
tec_beyond2018 = tec_tcrp[tec_tcrp['expendDt'] > '2018-01-01']
potomac_frame = potomac_frame_raw[potomac_frame_raw['expendDt'] > '2018-01-01']
wab_frame = wab_frame[wab_frame['expendDt'] > '2018-01-01']
mackowiak_frame = tec_beyond2018[tec_beyond2018['payeeNameOrganization'].isin(CONFIG['MACKOWIAK_ENTITIES'])]

""" === Data Analysis === """
def filter_vendor(vendor: str, frame: pd.DataFrame = mackowiak_frame) -> pd.DataFrame:
    return frame[frame['payeeNameOrganization'].str.contains(vendor)]

TECExpenseCrosstab = CreateCrosstab(
    index_fields= ['payeeNameOrganization', 'filerName'],
    column_fields= 'expendDt',
    amount_field= 'expendAmount'
)

TECPayrollCrosstab = CreateCrosstab(
    index_fields= ['payeeNameFirst', 'payeeNameLast'],
    column_fields= 'expendDt',
    amount_field= 'expendAmount'
)
tec_ct = TECExpenseCrosstab.create(tec_beyond2018)  # TCRP Expenses 2018+

mackowiak_ct = TECExpenseCrosstab.create(mackowiak_frame) # Mackowiak Entities

potomac_all_years = TECExpenseCrosstab.create(potomac_frame_raw) # Potomac Expenses All Years

potomac_ct = TECExpenseCrosstab.create(potomac_frame) # Potomac Expenses 2018+

wab_ct = TECExpenseCrosstab.create(wab_frame) # WAB PAC Payments

keep_round_rock_safe_contributors = tec_contributions.filter(
    pl.col('filerName').str.contains('Keep Round Rock Safe')
).collect().to_pandas() # Keep Round Rock Safe Contributors

texas_emergency_network = tec_contributions.filter(
    pl.col('contributorNameOrganization').str.contains('Texas Emergency Network')
).collect().to_pandas()  # Texas Emergency Network, LLC

keep_round_rock_safe_expenses = set_datetime(tec_expenses.filter(
    pl.col('filerName').str.contains('Keep Round Rock Safe')
).collect().to_pandas())  # Keep Round Rock Safe Expenses

keep_round_rock_expense_ct = TECExpenseCrosstab.create(keep_round_rock_safe_expenses) # Keep Round Rock Safe Expenses

keep_round_rock_safe_payroll = keep_round_rock_safe_expenses[keep_round_rock_safe_expenses['payeeNameOrganization'].isna()]  # Keep Round Rock Safe Payroll

keep_round_rock_safe_payroll_ct = TECPayrollCrosstab.create(keep_round_rock_safe_payroll)  # Keep Round Rock Safe Payroll

# Fight for Tomorrow
fight_for_tomorrow_contributors = set_datetime(
    tec_contributions.filter(
    pl.col('filerName').str.contains('Fight for Tomorrow'))
    .collect()
    .to_pandas(),
    'contributionDt'
)  # Fight for Tomorrow Contributors

fight_for_tomorrow_expenses = set_datetime(
    tec_expenses.filter(
        pl.col('filerName').str.contains('Fight for Tomorrow')
    )
    .collect()
    .to_pandas()
)  # Fight for Tomorrow Expenses

fight_for_tomorrow_expense_ct = TECExpenseCrosstab.create(fight_for_tomorrow_expenses)  # Fight for Tomorrow Expense Crosstab

fight_for_tomorrow_payroll = fight_for_tomorrow_expenses[fight_for_tomorrow_expenses['payeeNameOrganization'].isna()]  # Fight for Tomorrow Payroll

# Travis County Republican Party (CEC)
travis_cec_contributions = set_datetime(
    tec_contributions.filter(
        pl.col('filerIdent').str.contains("00039023")
    )
    .collect()
    .to_pandas(),
    'contributionDt'
)  # Travis County Republican Party Contributions
travis_cec_contributions = travis_cec_contributions[travis_cec_contributions['contributionDt'] > '2018-01-01']

travis_cec_expenses = set_datetime(
    tec_expenses.filter(
        pl.col('filerIdent').str.contains('00039023')
    )
    .collect()
    .to_pandas()
)  # Travis County Republican Party Expenses
travis_cec_expenses = travis_cec_expenses[travis_cec_expenses['expendDt'] > '2018-01-01']  # Travis County Republican Party Expenses 2018+

travis_cec_expense_ct = TECExpenseCrosstab.create(travis_cec_expenses) # Travis County Republican Party Expenses Crosstab

travis_cec_payroll = travis_cec_expenses[travis_cec_expenses['payeeNameOrganization'].isna()]  # Travis County Republican Party Payroll

travis_cec_payroll_ct = TECPayrollCrosstab.create(travis_cec_payroll)  # Travis County Republican Party Payroll