import pyodbc
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import datetime
import time
import os
import sys
from datetime import timedelta, date
import logging
import numpy as np
import seaborn as sns
import scipy.stats

from mpl_finance import candlestick_ohlc
import matplotlib.dates as mdates
# style.use('ggplot')

# con = pyodbc.connect(driver = '{SQL Server}',server = '77.221.144.170', port = '1433', database = 'Finance', UID = 'SA', PWD = 'Sauth1900$', autocommit=True)

def SaveStockQuoteToDB(con, Stock, DateFrom, DateTo):
    try:
        data = yf.download(Stock, DateFrom, DateTo)
        df_to_lst = data.reset_index().values.tolist()
    except Exception as e:
        print(Stock + ' Ошибка при получении данных из источника: ' + str(e)) 
        result = 1
    try:
        for row in df_to_lst:
            Date = row[0]
            Open = row[1]
            High = row[2]
            Low = row[3]
            Close = row[4]
            AdjClose = row[5]
            Volume = row[6]
            query = ("exec pLoadQuote '" + str(Date) + "', '" + str(Stock) + "' , " + str(Open) + ', ' + str(High) + ', ' + 
                       str(Low) + ', ' + str(Close) + ', '  + str(AdjClose) + ', ' + str(Volume))
            con.execute(query)
            con.commit()
        result = 0
    except Exception as e:
        print(Stock + ' Ошибка при сохранении в базу: ' + str(e)) 
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

def save_sp500_tickers(con):
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = []
        ticker.append(row.findAll('td')[0].text.replace('\n', ''))
        ticker.append(row.findAll('td')[1].text.replace("'", "''"))
        ticker.append(row.findAll('td')[3].text)
        ticker.append(row.findAll('td')[4].text)
        tickers.append(ticker)
        
    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(tickers,f)
        
    for ticker in tickers:
        Stock = ticker[0]
        Security =  ticker[1]
        Sector = ticker[2]
        SubIndustry = ticker[3]

        query = ("exec pLoadStockList '" + str(Stock) + "', '" + str(Security) + "' , '" + str(Sector) +  "', '" + str(SubIndustry) + "'")
        print(query)
        con.execute(query)
        con.commit()
        
    return tickers

def GetStockListFromDB(con, Market):
    if Market == 'NULL':
        MarketStr = Market
    else:
        MarketStr = "'" + Market + "'"
    query = "exec pGetStockList " +  MarketStr
    df = pd.read_sql(query, con)
    return df

def GetAllStockByPeriod(con, DtFrom, DtTo, logger, sleep_time = 5, IsDebug = 0):
    """
    По всем компаниям считывает данные из интернета по котировкам за временной интервал
    """
    stock_df = GetStockListFromDB(con, 'NULL')
    CntError = 0
    lst_error = []
    result_dic = {}
    num_ticker = 0
    for ind, row in stock_df.iterrows():
        try:
            SaveStockQuoteToDB(con, row.values[0], DtFrom, DtTo)
            logging.info(row.values[0] + ' Success')
            time.sleep(sleep_time) 
        except Exception as e:
            CntError += 1
            lst_error.append(row.values[0]) 
            logging.info(row.values[0] + ' Error: ' + str(e))
        if IsDebug ==1 and num_ticker == 5:
            break
        num_ticker +=1
    
    State = 3 if CntError == 0 else 4

    result_dic['State'] = State
    result_dic['CntError'] = CntError
    result_dic['lst_error'] = lst_error
    return result_dic

def ReadConnConfig(filename):
    config_dict = {}
    with open(filename) as f:
        for line in f:
            (key, val) = line.split(' = ')
            config_dict[key] = val.replace('\n', '')
    return config_dict

def DailyUpdateQuote(fname, log_dir, sleep_time = 5, IsDebug = 0):
    """
    Обновление в ежедневном режиме котировок по всем акциям
    """
    config_dict = ReadConnConfig(fname)
    con = pyodbc.connect(driver = config_dict['DRIVER'],server = config_dict['SERVER'], port = config_dict['PORT'], database = config_dict['DATABASE'], UID = config_dict['UID'], PWD = config_dict['PWD'], autocommit=True)

    today_dt = date.today().strftime('%Y%m%d')    
    log_dir = log_dir + '/logs/' + today_dt

    DtTo = date.today()
    DtFrom = date.today() + datetime.timedelta(days=-2)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(filename= log_dir + '/app.log', filemode='a+', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S', level=logging.DEBUG)
    logger = logging.getLogger()
    logger.info('DailyUpdateQuote')
    logger.info('Parameters: ' + 'DtFrom = ' + str(DtFrom) + '; DtTo = ' + str(DtTo))
    logger.info('fname: ' + fname)
    logger.info('log_dir: ' + log_dir)
    
    WtiteLog(con, 1, 1, 1, 0, 'NULL', logger)
    result_dict = GetAllStockByPeriod(con, DtFrom.strftime('%Y-%m-%d'), DtTo.strftime('%Y-%m-%d'), logger, sleep_time, IsDebug)
    
    WtiteLog(con, 2, 1, result_dict['State'], result_dict['CntError'], ', '.join(result_dict['lst_error']), logger)

def WtiteLog(con, TypeWrite, WfId, WfStatus, CntError, ErrorMsg, logger):
    """
    Записывает лог ежедневного обновления в базу
    """
    try:
        ErrorMsgStr = ErrorMsg if ErrorMsg == 'NULL' else "'" + ErrorMsg + "'"
        query = ("exec pWriteLog " + str(TypeWrite) + ", " + str(WfId) + " , " + str(WfStatus) + ", "  + str(CntError) + ", "  + str(ErrorMsgStr))
        con.execute(query)
        con.commit()
    except Exception as e:
        logger.error('Error in Write Log: ' + str(e)) 

def GetCandlePlot(df_input, Stock, ReSample = '10D'):
    """
    Записывает лог ежедневного обновления в базу
    """
    
    df = df_input[df_input['Stock'] == Stock]
    df_ohlc = df['AdjClose'].resample(ReSample).ohlc()
    df_volume = df['Volume'].resample(ReSample).sum()
    df_ohlc.reset_index(inplace=True)
    df_ohlc['Dt'] = df_ohlc['Dt'].map(mdates.date2num)

    fig = plt.figure(figsize=(12, 7))
    ax1 = plt.subplot2grid((6,1), (0,0), rowspan=5, colspan=1)
    ax2 = plt.subplot2grid((6,1), (5,0), rowspan=1, colspan=1, sharex=ax1)
    ax1.xaxis_date()

    candlestick_ohlc(ax1, df_ohlc.values, width=5, colorup='g')
    ax2.fill_between(df_volume.index.map(mdates.date2num), df_volume.values, 0)
    plt.show()

def GetMAPlot(df_input, Stock, WindowValue=100, MinPerValue = 0):
    
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

def GetAdjCloseInCol(tickers_list, df):
    data = pd.DataFrame(columns=tickers_list)
    for ticker in tickers_list:
        data[ticker] =  df[df['Stock']==ticker]['AdjClose']
    return data

def GetPctChange(df):
    df_change = df.drop(['OpenValue', 'HighValue', 'LowValue', 'CloseValue', 'Volume', 'LoadDt'], axis=1)
    
    df_change['Change'] = df_change.groupby('Stock').AdjClose.pct_change()
    df_change['Change7'] = df_change.groupby('Stock').AdjClose.pct_change(7)
    df_change['Change30'] = df_change.groupby('Stock').AdjClose.pct_change(30)
    df_change['Change90'] = df_change.groupby('Stock').AdjClose.pct_change(90)
    df_change['Change180'] = df_change.groupby('Stock').AdjClose.pct_change(180)
    df_change['Change360'] = df_change.groupby('Stock').AdjClose.pct_change(360)
    return df_change

def GetReturns(data):
    return data.pct_change()

def GetTotalReturns(data):
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