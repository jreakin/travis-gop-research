import pandas as pd
from __init__ import CreateCrosstab

travis_county_fec_contributions = pd.read_csv("data/travis_gop_fec.csv")  # Travis County GOP Contributions
make_liberty_win_fec_contributions = pd.read_csv("data/make_liberty_win_fec.csv")  # Make Liberty Win Contributions

travis_county_fec_contributions['report_year'] = pd.to_datetime(travis_county_fec_contributions['report_year'], format='%Y')

FECExpenseCrossTab = CreateCrosstab(
    index_fields='contributor_name',
    column_fields='report_year',
    amount_field='contribution_receipt_amount'
)  # FEC Expense Crosstab Base

travis_county_fec_contributions_ct = FECExpenseCrossTab.create(travis_county_fec_contributions)  # Travis County GOP FEC Contributions
