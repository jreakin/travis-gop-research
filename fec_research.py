import pandas as pd
import polars as pl
from pathlib import Path

travis_county_fec_contributions = pd.read_csv("data/travis_gop_fec.csv")
make_liberty_win_fec_contributions = pd.read_csv("data/make_liberty_win_fec.csv")

travis_county_fec_contributions_ct = pd.crosstab(
    index=travis_county_fec_contributions['contributor_name'],
    columns=travis_county_fec_contributions['report_year'],
    values=travis_county_fec_contributions['contribution_receipt_amount'],
    aggfunc='sum',
    margins=True,
    margins_name='Total',
).sort_values(by='Total', ascending=False)
