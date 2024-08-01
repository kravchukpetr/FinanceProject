import simfin as sf
from Utils import merge_df_to_pg, get_conn_pg_engine
from dotenv import load_dotenv
import os
import warnings
warnings.filterwarnings('ignore')

load_dotenv()
PG_SCHEMA_NAME = 'finance'
SIMFIN_REFRESH_DAYS = 3
# SIMFIN_DATASETS = ['income', 'balance']
SIMFIN_DATASETS = ['balance']


def load_simfin():
    """
    Load fundamental statistic data from simfin
    """

    simfin_api_key = os.getenv("SIMFIN_API_KEY")
    # Set your API-key for downloading data. This key gets the free data.
    sf.set_api_key(simfin_api_key)
    # Set the local directory where data-files are stored.
    # The directory will be created if it does not already exist.
    sf.set_data_dir('~/simfin_data/')
    # Create SQLAlchemy engine
    engine = get_conn_pg_engine(False)
    table_name = 'income'
    merge_key_list = ['report_date', 'ticker']
    index_list = ['Report Date']
    parse_dates_list = ['Report Date', 'Publish Date', 'Restated Date']
    for dataset in SIMFIN_DATASETS:
        print(dataset)
        load_simfin_income(
                           engine,
                           PG_SCHEMA_NAME,
                           dataset,
                           index_list,
                           parse_dates_list,
                           merge_key_list,
                           SIMFIN_REFRESH_DAYS)


def load_simfin_income(engine, schema_name, dataset, index_list, parse_dates_list, merge_key_list, refresh_days):
    """
    Load fundamental statistic data from simfin - income dataset
    """

    df_quarterly = sf.load(dataset=dataset,
                           variant='quarterly',
                           market='us',
                           refresh_days=refresh_days,
                           index=index_list,
                           parse_dates=parse_dates_list)
    income_tickers = df_quarterly.groupby('Ticker').agg({'Ticker': 'count'}).rename(columns={'Ticker': 'CountRows'})
    for idx, ticker in enumerate(income_tickers.index):
        print(idx, ticker)
        if '_delisted' not in ticker:
            df_ticker = df_quarterly[df_quarterly['Ticker'] == ticker]
            df_ticker = simfin_rename_columns(dataset, df_ticker)
            df_ticker = df_ticker.reset_index("report_date")
            merge_df_to_pg(df_ticker, engine, dataset, schema_name, merge_key_list)


def simfin_rename_columns(dataset, df):
    if dataset == 'income':
        df = df.rename(columns={
            'Ticker': 'ticker',
            'SimFinId': 'simfin_id',
            'Currency': 'currency',
            'Fiscal Year': 'fiscal_year',
            'Fiscal Period': 'fiscal_period',
            'Publish Date': 'publish_date',
            'Restated Date': 'restated_date',
            'Shares (Basic)': 'shares_basic',
            'Shares (Diluted)': 'shares_diluted',
            'Revenue': 'revenue',
            'Cost of Revenue': 'cost_of_revenue',
            'Gross Profit': 'gross_profit',
            'Operating Expenses': 'operating_expenses',
            'Selling, General & Administrative': 'selling_general_administrative',
            'Research & Development': 'research_development',
            'Depreciation & Amortization': 'depreciation_amortization',
            'Operating Income (Loss)': 'operating_income',
            'Non-Operating Income (Loss)': 'non_operating_income',
            'Interest Expense, Net': 'interest_expense_net',
            'Pretax Income (Loss), Adj.': 'pretax_income_adj',
            'Abnormal Gains (Losses)': 'abnormal_gains',
            'Pretax Income (Loss)': 'pretax_income',
            'Income Tax (Expense) Benefit, Net': 'income_tax_benefit_net',
            'Income (Loss) from Continuing Operations': 'income_from_continuing_operations',
            'Net Extraordinary Gains (Losses)': 'net_extraordinary_gains',
            'Net Income': 'net_income',
            'Net Income (Common)': 'net_income_common'
        })
        df.rename_axis('report_date', inplace=True)
    elif dataset == 'balance':
        df = df.rename(columns={
            'Ticker': 'ticker',
            'SimFinId': 'simfin_id',
            'Currency': 'currency',
            'Fiscal Year': 'fiscal_year',
            'Fiscal Period': 'fiscal_period',
            'Publish Date': 'publish_date',
            'Restated Date': 'restated_date',
            'Shares (Basic)': 'shares_basic',
            'Shares (Diluted)': 'shares_diluted',
            'Cash, Cash Equivalents & Short Term Investments': 'cash_cash_equivalents_short_term_investments',
            'Accounts & Notes Receivable': 'accounts_notes_receivable',
            'Inventories': 'inventories',
            'Total Current Assets': 'total_current_assets',
            'Property, Plant & Equipment, Net': 'property_plant_equipment_net',
            'Long Term Investments & Receivables': 'long_term_investments_receivables',
            'Other Long Term Assets': 'other_long_term_assets',
            'Total Noncurrent Assets': 'total_noncurrent_assets',
            'Total Assets': 'total_assets',
            'Payables & Accruals': 'payables_accruals',
            'Short Term Debt': 'short_term_debt',
            'Total Current Liabilities': 'total_current_liabilities',
            'Long Term Debt': 'long_term_debt',
            'Total Noncurrent Liabilities': 'total_noncurrent_liabilities',
            'Total Liabilities': 'total_liabilities',
            'Share Capital & Additional Paid-In Capital': 'share_capital_additional_paid_in_capital',
            'Treasury Stock': 'treasury_stock',
            'Retained Earnings': 'retained_earnings',
            'Total Equity': 'total_equity',
            'Total Liabilities & Equity': 'total_liabilities_equity'
        })
        df.rename_axis('report_date', inplace=True)
    return df


if __name__ == "__main__":
    load_simfin()
