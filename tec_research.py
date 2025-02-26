import pandas as pd
import polars as pl
from pathlib import Path
from __init__ import CONFIG, set_datetime

""" === File Paths === """
tec_expenses_path = Path(__file__).parents[4] / 'campaignfinance/tmp/texas/expend_20250211w.parquet'
tec_contributions_path = Path(__file__).parents[4] / 'campaignfinance/tmp/texas/contribs_20250211w.parquet'

""" === Load Data === """
tec_contributions = pl.scan_parquet(tec_contributions_path)
tec_expenses = pl.scan_parquet(tec_expenses_path)

""" === Data Frames === """
tec_tcrp = set_datetime(tec_expenses.filter(
    (pl.col('filerIdent').is_in(CONFIG['TRAVIS_GOP_POSSIBLE']))
).collect().to_pandas())
potomac_frame = set_datetime(tec_expenses.filter(pl.col('payeeNameOrganization').str.contains('Potomac')).collect().to_pandas())
wab_frame = set_datetime(tec_expenses.filter(pl.col('payeeNameOrganization').str.starts_with('WAB ')).collect().to_pandas())

""" Filter Data """
tec_beyond2018 = tec_tcrp[tec_tcrp['expendDt'] > '2018-01-01']
potomac_frame = potomac_frame[potomac_frame['expendDt'] > '2018-01-01']
wab_frame = wab_frame[wab_frame['expendDt'] > '2018-01-01']
mackowiak_frame = tec_beyond2018[tec_beyond2018['payeeNameOrganization'].isin(CONFIG['MACKOWIAK_ENTITIES'])]

""" === Data Analysis === """

tec_ct = pd.crosstab(
    index=[tec_beyond2018['payeeNameOrganization'], tec_beyond2018['filerName']],
    columns=tec_beyond2018['expendDt'].dt.year,
    values=tec_beyond2018['expendAmount'].astype(float),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

print("Total Expenses: ", tec_beyond2018['expendAmount'].astype(float).sum())
print("Expenses paid to Mackowiak Entities: ", mackowiak_frame['expendAmount'].astype(float).sum())


potomac_ct = pd.crosstab(
    index=[potomac_frame['filerName'], potomac_frame['payeeNameOrganization']],
    columns=potomac_frame['expendDt'].dt.year,
    values=potomac_frame['expendAmount'].astype(float),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

def filter_vendor(vendor: str, frame: pd.DataFrame = mackowiak_frame) -> pd.DataFrame:
    return frame[frame['payeeNameOrganization'].str.contains(vendor)]

print("Travis GOP Groups Paid to GoodBuzz Solutions: ", filter_vendor('Good')['expendAmount'].astype(float).sum())
print("Travis GOP Groups Paid to WAB Holdings: ", filter_vendor('WAB ')['expendAmount'].astype(float).sum())
print("Travis GOP Groups Paid to Potomac: ", filter_vendor('Potomac')['expendAmount'].astype(float).sum())
print("Travis GOP Groups Paid to Save Austin Now: ", filter_vendor('Save')['expendAmount'].astype(float).sum())
print("Travis GOP Groups Paid to Victory Solutions: ", filter_vendor('Victory')['expendAmount'].astype(float).sum())


wab_ct = pd.crosstab(
    index=[wab_frame['filerIdent'], wab_frame['filerName'], wab_frame['payeeNameOrganization']],
    columns=wab_frame['expendDt'].dt.year,
    values=wab_frame['expendAmount'].astype(float),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

keep_round_rock_safe_contributors = tec_contributions.filter(
    pl.col('filerName').str.contains('Keep Round Rock Safe')
).collect().to_pandas()

# Only one contributor, Texas Emergency Network, LLC
texas_emergency_network = tec_contributions.filter(pl.col('contributorNameOrganization').str.contains('Texas Emergency Network')).collect().to_pandas()

keep_round_rock_safe_expenses = set_datetime(tec_expenses.filter(
    pl.col('filerName').str.contains('Keep Round Rock Safe')
).collect().to_pandas())

keep_round_rock_expense_ct = pd.crosstab(
    index=[keep_round_rock_safe_expenses['payeeNameOrganization']],
    columns=keep_round_rock_safe_expenses['expendDt'].dt.year,
    values=keep_round_rock_safe_expenses['expendAmount'].astype(float),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

# Fight for Tomorrow
fight_for_tomorrow_contributors = set_datetime(tec_contributions.filter(
    pl.col('filerName').str.contains('Fight for Tomorrow')
).collect().to_pandas(), 'contributionDt')

fight_for_tomorrow_expenses = set_datetime(tec_expenses.filter(
    pl.col('filerName').str.contains('Fight for Tomorrow')
).collect().to_pandas())

fight_for_tomorrow_expense_ct = pd.crosstab(
    index=[fight_for_tomorrow_expenses['payeeNameOrganization']],
    columns=fight_for_tomorrow_expenses['expendDt'].dt.year,
    values=fight_for_tomorrow_expenses['expendAmount'].astype(float),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)

# Travis County Republican Party (CEC)
travis_cec_contributions = set_datetime(tec_contributions.filter(
    pl.col('filerIdent').str.contains("00039023")
).collect().to_pandas(), 'contributionDt')
travis_cec_contributions = travis_cec_contributions[travis_cec_contributions['contributionDt'] > '2018-01-01']

travis_cec_expenses = set_datetime(tec_expenses.filter(
    pl.col('filerIdent').str.contains('00039023')
).collect().to_pandas())
travis_cec_expenses = travis_cec_expenses[travis_cec_expenses['expendDt'] > '2018-01-01']

travis_cec_expense_ct = pd.crosstab(
    index=[travis_cec_expenses['payeeNameOrganization']],
    columns=travis_cec_expenses['expendDt'].dt.year,
    values=travis_cec_expenses['expendAmount'].astype(float),
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)