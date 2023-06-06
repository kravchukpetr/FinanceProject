import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import time
import os
from datetime import date
import logging
import numpy as np
import scipy.stats
import requests
import bs4 as bs
import pickle
import psycopg2
from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates

CONFIG_FILE = os.path.dirname(os.path.realpath(__file__)).replace("\\","/") + '/' + 'DB.config'
LOG_DIR = os.path.dirname(os.path.realpath(__file__)).replace("\\","/")


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
       database=config_dict['database'], user=config_dict['user'], password=os.environ['PG_PWD'], host=config_dict['host'], port=config_dict['port']
    )


def save_stock_quote_to_db(stock, date_from, date_to):
    conn = get_conn_to_pg()
    try:
        data = yf.download(stock, date_from, date_to)
        df_to_lst = data.reset_index().values.tolist()
    except Exception as e:
        print(stock + ' Error in load quotes from datasource: ' + str(e))
        result = 1
        return None
    cursor = conn.cursor()
    try:
        for row in df_to_lst:
            date = row[0]
            open = row[1]
            high = row[2]
            low = row[3]
            close = row[4]
            adj_close = row[5]
            volume = row[6]
            query = f"CALL finance.pLoadQuote('{date}', '{stock}', {open}, {high}, {low}, {close}, {adj_close}, {volume})"
            cursor.execute(query)
        conn.commit()
        conn.close()
        result = 0
    except Exception as e:
        print(stock + 'Error in save data to database: ' + str(e))
        result = 1
    return result


def GetStockQuoteFromDB(con, Stock, IsDtIndex = 1, IsStockIndex = 0, DateFrom = 'NULL', DateTo = 'NULL'):
    """
    Возвращает котировки по акции из база SQL SERVER
    """

    StockStr = Stock if Stock == 'NULL' else "'" + ', '.join(Stock) + "'"        
    DateFromStr = DateFrom if DateFrom == 'NULL' else "'" + DateFrom + "'"
    DateToStr = DateTo if DateTo == 'NULL' else "'" + DateTo + "'"

    query = "exec pGetQuote " + DateFromStr + ", " + DateToStr + ", " + StockStr
    
    lst_index = []
    # if Stock.find(',') > 0:
    if IsDtIndex == 1:
        lst_index.append('Dt')
    if IsStockIndex == 1:
        lst_index.append('Stock')

    if len(lst_index) > 0:
        df = pd.read_sql(query, con, parse_dates='Dt', index_col = lst_index)
    else:
        df = pd.read_sql(query, con, parse_dates='Dt')
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

    with open("sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers,f)

    for ticker in tickers:
        print(ticker)
        stock = ticker[0]
        security = ticker[1]
        sector = ticker[2]
        sub_industry = ticker[3]
        query = ("CALL finance.pLoadStockList('" + str(stock) + "', '" + str(security) + "' , '" + str(sector) + "', '" + str(sub_industry) + "');")
        print(query)
        cursor.execute(query)
    conn.commit()
    conn.close()
    return tickers


def get_stock_list_from_db(market=None):
    """
    Get list of quotes from db
    """

    conn = get_conn_to_pg()
    market_str = f"'{market}'" if market else'NULL'
    query = f"select * from finance.pGetStockList({market_str})"
    df = pd.read_sql(query, conn)
    return df


def get_all_stock_by_period(dt_from, dt_to, logger, sleep_time = 5, is_debug = 0):
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
            save_stock_quote_to_db(row.values[0], dt_from, dt_to)
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


def daily_update_quote(sleep_time=5, is_debug=0):
    """
    Daily update all quotes
    """
    conn = get_conn_to_pg()

    today_dt = date.today().strftime('%Y%m%d')    
    log_dir = LOG_DIR + '/logs/' + today_dt

    dt_to = date.today()
    dt_from = date.today() + datetime.timedelta(days=-2)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename=log_dir + '/app.log', filemode='a+', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S', level=logging.DEBUG)
    logger = logging.getLogger()
    logger.info('DailyUpdateQuote')
    logger.info('Parameters: ' + 'DtFrom = ' + str(dt_from) + '; DtTo = ' + str(dt_to))
    logger.info('log_dir: ' + log_dir)
    
    write_log(1, 1, 1, 0, 'NULL', logger)
    result_dict = get_all_stock_by_period(dt_from.strftime('%Y-%m-%d'), dt_to.strftime('%Y-%m-%d'), logger, sleep_time, is_debug)
    
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


def get_candle_plot(df_input, stock, re_sample = '10D'):
    """
    return CandlePlot
    """
    
    df = df_input[df_input['Stock'] == stock]
    df_ohlc = df['AdjClose'].resample(re_sample).ohlc()
    df_volume = df['Volume'].resample(re_sample).sum()
    df_ohlc.reset_index(inplace=True)
    df_ohlc['Dt'] = df_ohlc['Dt'].map(mdates.date2num)

    fig = plt.figure(figsize=(12, 7))
    ax1 = plt.subplot2grid((6, 1), (0, 0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((6, 1), (5, 0), rowspan=1, colspan=1, sharex=ax1)
    ax1.xaxis_date()

    candlestick_ohlc(ax1, df_ohlc.values, width=5, colorup='g')
    ax2.fill_between(df_volume.index.map(mdates.date2num), df_volume.values, 0)
    plt.show()


def get_ma_plot(df_input, Stock, WindowValue=100, MinPerValue = 0):
    
    df = df_input[df_input['Stock'] == Stock]
    df['ma'] = df['AdjClose'].rolling(window=WindowValue,min_periods=MinPerValue).mean()

    fig = plt.figure(figsize=(12, 7))
    ax1 = plt.subplot2grid((6,1), (0,0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((6,1), (5,0), rowspan=1, colspan=1, sharex=ax1)
    ax1.xaxis_date()
    
    ax1.plot(df.index, df['AdjClose'])
    ax1.plot(df.index, df['ma'])
    ax2.bar(df.index, df['Volume'])
    
    plt.show()


def get_adj_close_in_col(tickers_list, df):
    data = pd.DataFrame(columns=tickers_list)
    for ticker in tickers_list:
        data[ticker] = df[df['Stock']==ticker]['AdjClose']
    return data


def get_pct_change(df):
    df_change = df.drop(['OpenValue', 'HighValue', 'LowValue', 'CloseValue', 'Volume', 'LoadDt'], axis=1)
    
    df_change['Change'] = df_change.groupby('Stock').AdjClose.pct_change()
    df_change['Change7'] = df_change.groupby('Stock').AdjClose.pct_change(7)
    df_change['Change30'] = df_change.groupby('Stock').AdjClose.pct_change(30)
    df_change['Change90'] = df_change.groupby('Stock').AdjClose.pct_change(90)
    df_change['Change180'] = df_change.groupby('Stock').AdjClose.pct_change(180)
    df_change['Change360'] = df_change.groupby('Stock').AdjClose.pct_change(360)
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
        statistic, p_value = scipy.stats.jarque_bera(r)
        return p_value > level


def main():
    save_sp500_tickers()


if __name__ == "__main__":
    main()
