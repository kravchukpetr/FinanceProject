import simfin as sf
from Utils import merge_df_to_pg, get_conn_pg_engine
from dotenv import load_dotenv
import os
import warnings
import pandas as pd
warnings.filterwarnings('ignore')
import shutil



load_dotenv()
PG_SCHEMA_NAME = 'finance'
SIMFIN_REFRESH_DAYS = 3
SIMFIN_REFRESH_DAYS_HUB = 30
# SIMFIN_DATASETS = ['income', 'balance', 'cashflow', 'simfin_stats']
SIMFIN_DATASETS = ['simfin_stats']
SIMFIN_FOLDER = '~/simfin_data/'


def load_simfin():
    """
    Load fundamental statistic data from simfin
    """

    simfin_api_key = os.getenv("SIMFIN_API_KEY")
    # Set your API-key for downloading data. This key gets the free data.
    sf.set_api_key(simfin_api_key)
    # Set the local directory where data-files are stored.
    # The directory will be created if it does not already exist.
    sf.set_data_dir(SIMFIN_FOLDER)
    market = 'us'
    cache_name = market + '-all'
    cache_refresh_days = SIMFIN_REFRESH_DAYS
    cache_args = {'cache_name': cache_name,
                  'cache_refresh': cache_refresh_days}
    offset = pd.DateOffset(days=SIMFIN_REFRESH_DAYS_HUB)
    hub = sf.StockHub(market=market,
                      offset=offset,
                      refresh_days=SIMFIN_REFRESH_DAYS_HUB,
                      refresh_days_shareprices=SIMFIN_REFRESH_DAYS)

    # Create SQLAlchemy engine
    engine = get_conn_pg_engine(False)
    merge_key_list = ['report_date', 'ticker']
    index_list = ['Report Date']
    parse_dates_list = ['Report Date', 'Publish Date', 'Restated Date']
    for dataset in SIMFIN_DATASETS:
        print(dataset)
        if dataset == 'simfin_stats':
            load_simfin_stats_to_db(
                hub,
                engine,
                PG_SCHEMA_NAME,
                dataset,
                merge_key_list)
        else:
            load_simfin_dataset_to_db(
                               engine,
                               PG_SCHEMA_NAME,
                               dataset,
                               index_list,
                               parse_dates_list,
                               merge_key_list,
                               SIMFIN_REFRESH_DAYS)


def load_simfin_dataset_to_db(engine, schema_name, dataset, index_list, parse_dates_list, merge_key_list, refresh_days):
    """
    Load fundamental statistic data from simfin - income dataset
    """

    df_quarterly = sf.load(dataset=dataset,
                           variant='quarterly',
                           market='us',
                           refresh_days=refresh_days,
                           index=index_list,
                           parse_dates=parse_dates_list)
    tickers = df_quarterly.groupby('Ticker').agg({'Ticker': 'count'}).rename(columns={'Ticker': 'CountRows'})
    for idx, ticker in enumerate(tickers.index):
        print(idx, ticker)
        if '_delisted' not in ticker:
            df_ticker = df_quarterly[df_quarterly['Ticker'] == ticker]
            df_ticker = simfin_rename_columns(dataset, df_ticker)
            df_ticker = df_ticker.reset_index("report_date")
            merge_df_to_pg(df_ticker, engine, dataset, schema_name, merge_key_list)


def load_simfin_stats_to_db(hub, engine, schema_name, dataset, merge_key_list, is_clear_cache=True):
    df_signals = get_sim_fin_hub_df(hub, 'daily')
    tickers = df_signals.groupby(level='Ticker').agg({'Current Ratio': 'count'})
    tickers = tickers.rename(columns={'Current Ratio': 'CountRows'})
    for ticker in tickers.index:
        print(ticker)
        if '_delisted' not in ticker:
            df_ticker = df_signals.xs(ticker, level='Ticker')
            df_ticker['ticker'] = ticker
            df_ticker = simfin_rename_columns('stats', df_ticker)
            df_ticker = df_ticker.reset_index("report_date")
            merge_df_to_pg(df_ticker, engine, dataset, schema_name, merge_key_list)

    if is_clear_cache:
        simfin_clear_cache()


def simfin_clear_cache():
    unix_path = os.path.join(SIMFIN_FOLDER, 'cache')
    expanded_path = os.path.expanduser(unix_path)
    # Remove the entire directory and its contents
    shutil.rmtree(expanded_path)
    # Recreate the directory
    os.makedirs(expanded_path)

def get_sim_fin_hub_df(hub,  variant):
    """
    Get fundamental stats from simfin hub
    """

    df_fin_signals = hub.fin_signals(variant=variant)
    df_growth_signals = hub.growth_signals(variant=variant)
    df_val_signals = hub.val_signals(variant=variant)
    dfs = [df_fin_signals, df_growth_signals, df_val_signals]
    df_signals = pd.concat(dfs, axis=1)
    return df_signals


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
    elif dataset == "cashflow":
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
            'Net Income/Starting Line': 'net_income_starting_line',
            'Depreciation & Amortization': 'depreciation_and_amortization',
            'Non-Cash Items': 'non_cash_items',
            'Change in Working Capital': 'change_in_working_capital',
            'Change in Accounts Receivable': 'change_in_accounts_receivable',
            'Change in Inventories': 'change_in_inventories',
            'Change in Accounts Payable': 'change_in_accounts_payable',
            'Change in Other': 'change_in_other',
            'Net Cash from Operating Activities': 'net_cash_from_operating_activities',
            'Change in Fixed Assets & Intangibles': 'change_in_fixed_assets_and_intangibles',
            'Net Change in Long Term Investment': 'net_change_in_long_term_investment',
            'Net Cash from Acquisitions & Divestitures': 'net_cash_from_acquisitions_and_divestitures',
            'Net Cash from Investing Activities': 'net_cash_from_investing_activities',
            'Dividends Paid': 'dividends_paid',
            'Cash from (Repayment of) Debt': 'cash_from_repayment_of_debt',
            'Cash from (Repurchase of) Equity': 'cash_from_repurchase_of_equity',
            'Net Cash from Financing Activities': 'net_cash_from_financing_activities',
            'Net Change in Cash': 'net_change_in_cash'
        })
    elif dataset == "stats":
        df = df.rename(columns={
            '(Dividends + Share Buyback) / FCF': 'dividends_and_share_buyback_divide_fcf',
            'Asset Turnover': 'asset_turnover',
            'CapEx / (Depr + Amor)': 'capex_divide_depr_amor',
            'Current Ratio': 'current_ratio',
            'Debt Ratio': 'debt_ratio',
            'Dividends / FCF': 'dividends_divide_fcf',
            'Gross Profit Margin': 'gross_profit_margin',
            'Interest Coverage': 'interest_coverage',
            'Inventory Turnover': 'inventory_turnover',
            'Log Revenue': 'log_revenue',
            'Net Acquisitions / Total Assets': 'net_acquisitions_divide_total_assets',
            'Net Profit Margin': 'net_profit_margin',
            'Quick Ratio': 'quick_ratio',
            'R&D / Gross Profit': 'rnd_divide_gross_profit',
            'R&D / Revenue': 'rnd_divide_revenue',
            'Return on Assets': 'return_on_assets',
            'Return on Equity': 'return_on_equity',
            'Return on Research Capital': 'return_on_research_capital',
            'Share Buyback / FCF': 'share_buyback_divide_fcf',
            'Assets Growth': 'assets_growth',
            'Assets Growth QOQ': 'assets_growth_qqq',
            'Assets Growth YOY': 'assets_growth_yoy',
            'Earnings Growth': 'earnings_growth',
            'Earnings Growth QOQ': 'earnings_growth_qqq',
            'Earnings Growth YOY': 'earnings_growth_yoy',
            'FCF Growth': 'fcf_growth',
            'FCF Growth QOQ': 'fcf_growth_qqq',
            'FCF Growth YOY': 'fcf_growth_yoy',
            'Sales Growth': 'sales_growth',
            'Sales Growth QOQ': 'sales_growth_qqq',
            'Sales Growth YOY': 'sales_growth_yoy',
            'Dividend Yield': 'dividend_yield',
            'Earnings Yield': 'earnings_yield',
            'FCF Yield': 'fcf_yield',
            'Market-Cap': 'market_cap',
            'P/Cash': 'p_divide_cash',
            'P/E': 'p_divide_e',
            'P/FCF': 'p_divide_fcf',
            'P/NCAV': 'p_divide_ncav',
            'P/NetNet': 'p_divide_netnet',
            'P/Sales': 'p_divide_sales',
            'Price to Book Value': 'price_to_book_value'
        })
    df.rename_axis('report_date', inplace=True)
    return df


if __name__ == "__main__":
    load_simfin()
