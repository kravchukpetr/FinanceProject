import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
# import datetime
import time
import os
from datetime import date, datetime, timedelta
import logging
import numpy as np
from scipy.stats import jarque_bera
import requests
import bs4 as bs
import pickle
import psycopg2
# from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
from tradingview_ta import TA_Handler, Interval
from tradernet import NtApi
import psycopg2.extras as extras
import pandas_ta
import shutil

CONFIG_FILE = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/") + '/' + 'DB.config'
LOG_DIR = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/")


def read_conn_config(filename):
    config_dict = {}
    with open(filename) as f:
        for line in f:
            (key, val) = line.split('=')
            config_dict[key] = val.replace('\n', '')
    return config_dict


def get_conn_to_pg():
    config_dict = read_conn_config(CONFIG_FILE)
    return psycopg2.connect(
        database=config_dict['database'], user=config_dict['user'], password=config_dict['pwd'], host=config_dict['host'], port=config_dict['port']
    )
# database=config_dict['database'], user=config_dict['user'], password=os.environ['PG_PWD'], host=config_dict['host'], port=config_dict['port']


def save_stock_quote_to_db(stock, screener, date_from, date_to):
    query = ""
    result = 0
    if screener == 'Forex':
        try:
            data = yf.Ticker(f"{stock}=x")
            df_to_lst = data.history(period="1d", interval="1h").reset_index().values.tolist()
        except Exception as e:
            print(stock + ' Error in load quotes from datasource: ' + str(e))
            result = 1
            return None
        conn = get_conn_to_pg()
        cursor = conn.cursor()
        try:
            for row in df_to_lst:
                dt = row[0].strftime('%Y-%m-%d %H:%M:%S')
                open_value = row[1]
                high_value = row[2]
                low_value = row[3]
                close_value = row[4]
                adj_close_value = row[4]
                volume = row[5]
                query = f"CALL finance.p_load_forex('{dt}', '{stock}', {open_value}, {high_value}, {low_value}, {close_value}, {adj_close_value}, {volume})"
                # print(query)
                cursor.execute(query)
            conn.commit()
            conn.close()
            result = 0
        except Exception as e:
            print(stock + 'Error in save data to database: ' + str(e))
            print(query)
            result = 1
    else:
        try:
            data = yf.download(stock, date_from, date_to)
            df_to_lst = data.reset_index().values.tolist()
        except Exception as e:
            print(stock + ' Error in load quotes from datasource: ' + str(e))
            result = 1
            return None
        conn = get_conn_to_pg()
        cursor = conn.cursor()
        try:
            for row in df_to_lst:
                dt = row[0]
                open_value = row[1]
                high_value = row[2]
                low_value = row[3]
                close_value = row[4]
                adj_close_value = row[5]
                volume = row[6]
                query = f"CALL finance.p_load_quote('{dt}', '{stock}', {open_value}, {high_value}, {low_value}, {close_value}, {adj_close_value}, {volume})"
                cursor.execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
            print(stock + 'Error in save data to database: ' + str(e))
            result = 1
    return result


def get_df_recomendation(stock, screener, exchange):
    """
    Get recomendation data from trading view API and return pandas dataframe
    """
    df = pd.DataFrame()
    for interval in [Interval.INTERVAL_1_DAY, Interval.INTERVAL_1_WEEK, Interval.INTERVAL_1_MONTH]:
        try:
            ticker = TA_Handler(
                symbol=stock,
                screener=screener,
                exchange=exchange,
                interval=interval
            )
            # print(interval, ticker.get_analysis().summary)
            df_tmp = pd.DataFrame([ticker.get_analysis().summary])
            df_tmp['period'] = interval
            df = pd.concat([df, df_tmp])
        except Exception:
            pass
    return df


def save_stock_recomendation_to_db(stock, screener, exchange):
    """
    Save tradingview recomendation by stock to db
    """
    conn = get_conn_to_pg()
    query = ""
    try:
        df = get_df_recomendation(stock, screener, exchange)
        df_to_lst = df.reset_index().values.tolist()
    except Exception as e:
        print(stock + ' Error in load recomendation from datasource: ' + str(e))
        result = 1
        return None
    cursor = conn.cursor()
    try:
        for row in df_to_lst:
            period = row[5]
            recomendation = row[1]
            buy_count = row[2]
            sell_count = row[3]
            neutral_count = row[4]
            query = f"CALL finance.p_load_recomendation('{stock}', '{period}', '{recomendation}', {buy_count}, {sell_count}, {neutral_count})"
            cursor.execute(query)
        conn.commit()
        conn.close()
        result = 0
    except Exception as e:
        print(stock + 'Error in save data to database: ' + str(e))
        print(query)
        result = 1
    return result


def get_all_stock_recomendation(sleep_time=2, is_debug=0):
    """
    Get recomendation for all stocks and save to db
    """
    today_dt = date.today().strftime('%Y%m%d')
    log_dir = LOG_DIR + '/logs/' + today_dt
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename=log_dir + '/app.log', filemode='a+', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S', level=logging.DEBUG)
    logger = logging.getLogger()
    logger.info('DailyUpdaterecomendation')
    logger.info('log_dir: ' + log_dir)

    stock_df = get_stock_list_from_db()
    cnt_error = 0
    lst_error = []
    result_dic = {}
    num_ticker = 0
    for ind, row in stock_df.iterrows():
        stock = row.values[0]
        screener = row.values[7]
        exchange = row.values[4]
        print(num_ticker, stock)
        try:
            save_stock_recomendation_to_db(stock, screener, exchange)
            logging.info(row.values[0] + ' Success')
            time.sleep(sleep_time)
        except Exception as e:
            cnt_error += 1
            lst_error.append(row.values[0])
            logging.info(row.values[0] + ' Error: ' + str(e))
        if is_debug == 1 and num_ticker == 5:
            break
        num_ticker += 1

    state = 3 if cnt_error == 0 else 4

    result_dic['State'] = state
    result_dic['CntError'] = cnt_error
    result_dic['lst_error'] = lst_error
    return result_dic


def get_stock_quote_from_db(stock, screener, date_from=None, date_to=None, is_dt_index=1, is_stock_index=0):
    """
    Returns qultes from DB
    """

    conn = get_conn_to_pg()

    stock_str = "'" + stock + "'" if stock else "NULL"
    date_from_str = "'" + date_from + "'" if date_from else "NULL"
    date_to_str = "'" + date_to + "'" if date_to else "NULL"

    query = f"select * from finance.f_get_quote ({date_from_str}, {date_to_str}, {stock_str}, '{screener}')"
    print(query)
    lst_index = []
    # if Stock.find(',') > 0:
    if is_dt_index == 1:
        lst_index.append('dt')
    if is_stock_index == 1:
        lst_index.append('stock')

    if len(lst_index) > 0:
        df = pd.read_sql(query, conn, parse_dates='Dt', index_col = lst_index)
    else:
        df = pd.read_sql(query, conn, parse_dates='Dt')
    return df



def get_recomendation_from_db():
    """
    Returns qultes from DB
    """

    conn = get_conn_to_pg()

    query = "select * from finance.v_get_rec"
    df = pd.read_sql(query, conn)
    return df


def get_freedom_open_position():
    pub_ = os.environ['FREEDOM_PUBLIC_KEY']
    sec_ = os.environ['FREEDOM_SECRET_KEY']

    res = NtApi(pub_, sec_, NtApi.V2)
    json_position = res.sendRequest('getPositionJson')
    df = pd.DataFrame(json_position['result']['ps']['pos'])
    return df


def save_sp500_tickers():
    conn = get_conn_to_pg()
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    cursor = conn.cursor()

    for row in table.findAll('tr')[1:]:
        ticker = [row.findAll('td')[0].text.replace('\n', ''), row.findAll('td')[1].text.replace("'", "''"),
                  row.findAll('td')[2].text, row.findAll('td')[3].text]
        tickers.append(ticker)

    with open("../sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers,f)

    for ticker in tickers:
        print(ticker)
        stock = ticker[0]
        security = ticker[1]
        sector = ticker[2]
        sub_industry = ticker[3]
        query = f"CALL finance.p_load_stock_list('{stock}', '{security}' , '{sector}', '{sub_industry}');"
        print(query)
        cursor.execute(query)
    conn.commit()
    conn.close()
    return tickers


def get_stock_list_from_db(screener=None):
    """
    Get list of quotes from db
    """

    conn = get_conn_to_pg()
    screener_str = f"'{screener}'" if screener else 'NULL'
    query = f"select * from finance.f_get_stock_list({screener_str})"
    df = pd.read_sql(query, conn)
    return df


def get_all_stock_by_period(dt_from, dt_to, logger, sleep_time=5, is_debug=0):
    """
    Get quotes for all stocks by time period from dt_from to dt_to
    """
    stock_df = get_stock_list_from_db()
    cnt_error = 0
    lst_error = []
    result_dic = {}
    num_ticker = 0
    for ind, row in stock_df.iterrows():
        try:
            print(num_ticker, row.values[0])
            save_stock_quote_to_db(row.values[0], row.values[7], dt_from, dt_to)
            logging.info(row.values[0] + ' Success')
            time.sleep(sleep_time) 
        except Exception as e:
            cnt_error += 1
            lst_error.append(row.values[0]) 
            logging.info(row.values[0] + ' Error: ' + str(e))
        if is_debug == 1 and num_ticker == 5:
            break
        num_ticker += 1
    
    state = 3 if cnt_error == 0 else 4

    result_dic['State'] = state
    result_dic['CntError'] = cnt_error
    result_dic['lst_error'] = lst_error
    return result_dic


def daily_update_quote(dt_from=None, sleep_time=5, is_debug=0):
    """
    Daily update all quotes
    """
    # conn = get_conn_to_pg()

    today_dt = date.today().strftime('%Y%m%d')    
    log_dir = LOG_DIR + '/logs/' + today_dt

    dt_to = date.today()
    if not dt_from:
        dt_from = date.today() + timedelta(days=-3)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename=log_dir + '/app.log', filemode='a+', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S', level=logging.DEBUG)
    logger = logging.getLogger()
    logger.info('DailyUpdateQuote')
    logger.info('Parameters: ' + 'DtFrom = ' + str(dt_from) + '; DtTo = ' + str(dt_to))
    logger.info('log_dir: ' + log_dir)
    
    write_log(1, 1, 1, 0, 'NULL', logger)
    result_dict = get_all_stock_by_period(dt_from.strftime('%Y-%m-%d'),
                                          dt_to.strftime('%Y-%m-%d'),
                                          logger,
                                          sleep_time,
                                          is_debug
                                          )
    
    write_log(2, 1, result_dict['State'], result_dict['CntError'], ', '.join(result_dict['lst_error']), logger)


def write_log(type_write, wf_id, wf_status, cnt_error, error_msg, logger):
    """
    Write log to db
    """
    try:
        conn = get_conn_to_pg()
        cursor = conn.cursor()
        error_msg_str = error_msg if error_msg == 'NULL' else "'" + error_msg + "'"
        query = f"CALL wf.pWriteLog({type_write}, {wf_id} , {wf_status}, {cnt_error}, {error_msg_str});"
        cursor.execute(query)
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error('Error in Write Log: ' + str(e)) 


# def get_candle_plot(df_input, stock, re_sample='10D'):
#     """
#     return CandlePlot
#     """
#
#     df = df_input[df_input['Stock'] == stock]
#     df_ohlc = df['AdjClose'].resample(re_sample).ohlc()
#     df_volume = df['Volume'].resample(re_sample).sum()
#     df_ohlc.reset_index(inplace=True)
#     df_ohlc['Dt'] = df_ohlc['Dt'].map(mdates.date2num)
#
#     fig = plt.figure(figsize=(12, 7))
#     print(fig)
#     ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)
#     ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)
#     ax1.xaxis_date()
#
#     candlestick_ohlc(ax1, df_ohlc.values, width=5, colorup='g')
#     ax2.fill_between(df_volume.index.map(mdates.date2num), df_volume.values, 0)
#     plt.show()


def get_ma_plot(df_input, stock, window_value=100, min_per_value=0):
    
    df = df_input[df_input['Stock'] == stock]
    df['ma'] = df['AdjClose'].rolling(window=window_value, min_periods=min_per_value).mean()

    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)
    ax1.xaxis_date()
    
    ax1.plot(df.index, df['AdjClose'])
    ax1.plot(df.index, df['ma'])
    ax2.bar(df.index, df['Volume'])
    
    plt.show()


def get_adj_close_in_col(tickers_list, df):
    data = pd.DataFrame(columns=tickers_list)
    for ticker in tickers_list:
        data[ticker] = df[df['Stock'] == ticker]['AdjClose']
    return data


def get_pct_change(df):
    # df_change = df.drop(['OpenValue', 'HighValue', 'LowValue', 'CloseValue', 'Volume', 'LoadDt'], axis=1)
    df_change = df
    df_change['change'] = df_change.groupby('stock').adjclose.pct_change()
    df_change['change7'] = df_change.groupby('stock').adjclose.pct_change(7)
    df_change['change30'] = df_change.groupby('stock').adjclose.pct_change(30)
    df_change['change90'] = df_change.groupby('stock').adjclose.pct_change(90)
    df_change['change180'] = df_change.groupby('stock').adjclose.pct_change(180)
    df_change['change360'] = df_change.groupby('stock').adjclose.pct_change(360)
    df_change['change'] = df['change'].round(4)
    df_change['change7'] = df['change7'].round(4)
    df_change['change30'] = df['change30'].round(4)
    df_change['change90'] = df['change90'].round(4)
    df_change['change180'] = df['change180'].round(4)
    df_change['change360'] = df['change360'].round(4)
    return df_change


def get_returns(data):
    return data.pct_change()


def get_total_returns(data):
    returns = data.pct_change()
    returns.reset_index(drop=True, inplace=True)
    return (((returns+1).prod()-1)*100).round(2)


def skewness(r):
    """
    Alternative to scipy.stats.skew()
    Computes the skewness of the supplied Series or DataFrame
    Returns a float or a Series
    Коэффициент асимметрии - Величина, характеризующая асимметрию распределения данной 
    случайной величины
    """
    demeaned_r = r - r.mean()
    # use the population standard deviation, so set dof=0
    sigma_r = r.std(ddof=0)
    exp = (demeaned_r**3).mean()
    return exp/sigma_r**3


def kurtosis(r):
    """
    Alternative to scipy.stats.kurtosis()
    Computes the kurtosis of the supplied Series or DataFrame
    Returns a float or a Series
    Коэффициент эксцесса - Мера остроты пика распределения случайной величины
    """
    demeaned_r = r - r.mean()
    # use the population standard deviation, so set dof=0
    sigma_r = r.std(ddof=0)
    exp = (demeaned_r**4).mean()
    return exp/sigma_r**4


def compound(r):
    """
    returns the result of compounding the set of returns in r
    """
    return np.expm1(np.log1p(r).sum())

                         
def annualize_rets(r, periods_per_year):
    """
    Annualizes a set of returns
    We should infer the periods per year
    but that is currently left as an exercise
    to the reader :-)
    """
    compounded_growth = (1+r).prod()
    n_periods = r.shape[0]
    return compounded_growth**(periods_per_year/n_periods)-1


def annualize_vol(r, periods_per_year):
    """
    Annualizes the vol of a set of returns
    We should infer the periods per year
    but that is currently left as an exercise
    to the reader :-)
    """
    return r.std()*(periods_per_year**0.5)


def sharpe_ratio(r, riskfree_rate, periods_per_year):
    """
    Computes the annualized sharpe ratio of a set of returns
    """
    # convert the annual riskfree rate to per period
    rf_per_period = (1+riskfree_rate)**(1/periods_per_year)-1
    excess_ret = r - rf_per_period
    ann_ex_ret = annualize_rets(excess_ret, periods_per_year)
    ann_vol = annualize_vol(r, periods_per_year)
    return ann_ex_ret/ann_vol


def is_normal(r, level=0.01):
    """
    Applies the Jarque-Bera test to determine if a Series is normal or not
    Test is applied at the 1% level by default
    Returns True if the hypothesis of normality is accepted, False otherwise
    """
    if isinstance(r, pd.DataFrame):
        return r.aggregate(is_normal)
    else:
        statistic, p_value = jarque_bera(r)
        return p_value > level


def get_df_actual_price_and_metrics(screener="america"):
    dt_from = datetime.today() - timedelta(days=365)
    dt_to = datetime.today() - timedelta(days=1)
    print(screener, dt_from.strftime("%Y-%m-%d"), dt_to.strftime("%Y-%m-%d"))
    df_actual_price = get_stock_quote_from_db(None, screener, dt_from.strftime("%Y-%m-%d"), dt_to.strftime("%Y-%m-%d"), is_dt_index=0, is_stock_index=0)
    df_actual_price = get_pct_change(df_actual_price)
    df_actual_price['rsi'] = df_actual_price.ta.rsi()
    df_actual_price = df_actual_price[df_actual_price['dt'] == dt_to.strftime("%Y-%m-%d")]
    return df_actual_price


def get_rec_and_metric(screener="america"):
    """
    Get dataframe with actual recomendation and price on previous day
    """
    df_rec = get_recomendation_from_db()
    df_actual = get_df_actual_price_and_metrics(screener)
    df_result = pd.merge(df_actual, df_rec, how='left', left_on=['stock'], right_on=['stock'])
    return df_result


def get_pos_and_rec():
    """
    Get dataframe with position in freedom, recomendation and price on previous day
    """
    df_pos = get_freedom_open_position()
    df_pos['stock'] = df_pos['i'].apply(lambda x: x.split('.')[0])
    df_rec = get_recomendation_from_db()
    df = pd.merge(df_pos, df_rec,  how='left', left_on=['stock'], right_on=['stock'])
    df = df.rename(columns={'s': 'balance_value', 'q': 'count', 'price_a': 'in_price'})
    df_actual = get_df_actual_price_and_metrics("america")
    df_result = pd.merge(df, df_actual, how='left', left_on=['stock'], right_on=['stock'])

    columns_show = ['stock',
                    'name',
                    'count',
                    'in_price',
                    'balance_value',
                    'mkt_price',
                    'profit_price',
                    'profit_close',
                    'market_value',
                    'rec_1d',
                    'rec_1w',
                    'rec_1m',
                    'rec_loaddt',
                    'change7',
                    'change30',
                    'change90',
                    'change180',
                    'change360',
                    'rsi'
                    ]
    return df_result[columns_show]


def execute_values(conn, df, table):
    """
    Generate SQL scripts and execute it for fast load stock history
    """
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    # print(query, tuples)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    # print("execute_values() done")
    cursor.close()


def load_stock_history_to_db(dt_from, dt_to, stock_input=None, check_is_load=1, sleep_time=2, screener_input=None):
    """
    Fast load stock history in DB
    """
    stock_df = get_stock_list_from_db(screener=screener_input)
    conn = get_conn_to_pg()
    df = pd.DataFrame()
    for ind, row in stock_df.iterrows():
        try:
            stock = row.values[0]
            screener = row.values[7]
            is_load = 1 if check_is_load == 0 else row.values[6]
            if is_load == 1 and (stock_input is None or (stock_input and stock == stock_input)):
                print(stock)
                if screener == 'america' and (screener_input == screener or screener_input is None):
                    data = yf.download(stock, dt_from, dt_to)
                    df = data.reset_index()
                    df = df.rename(columns={"Adj Close": "AdjClose",
                                            "Date": "dt",
                                            "Open": "OpenValue",
                                            "Close": "CloseValue",
                                            "High": "HighValue",
                                            "Low": "LowValue",
                                            "Volume": "Volume"
                                            })
                    tbl = 'finance.quotes'

                if screener == 'Forex' and (screener_input == screener or screener_input is None):
                    data = yf.Ticker(f"{stock}=x")
                    data = data.history(period="2Y", interval="1h")
                    df = data.reset_index()
                    df = df.rename(columns={
                        "Datetime": "dt",
                        "Open": "OpenValue",
                        "Close": "CloseValue",
                        "High": "HighValue",
                        "Low": "LowValue"
                    })
                    df["AdjClose"] = df["CloseValue"]
                    tbl = 'finance.forex'
                df["Stock"] = stock
                columns = ["Stock", "dt", "OpenValue", "CloseValue", "HighValue", "LowValue", "AdjClose", "Volume"]
                execute_values(conn, df[columns], tbl)
                time.sleep(sleep_time)
        except Exception as e:
            print(f"Error in loading {stock}: ", e)
    conn.close()


def main():
    save_sp500_tickers()


if __name__ == "__main__":
    daily_update_quote()
