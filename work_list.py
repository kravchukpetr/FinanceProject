import FinanceLib as fl
from datetime import datetime
# import pandas_ta

OPERATION_LIST = [
    "OTHER",
    "LOAD_HISTORY",
    "LOAD_DAILY",
    "LOAD_CURRENT_STOCK"
]
OPERATION = OPERATION_LIST[3]

if __name__ == "__main__":
    print(OPERATION)
    dt_from = "2010-01-01"
    current_date = datetime.now().strftime('%Y-%m-%d')

    if OPERATION == "LOAD_CURRENT_STOCK":
        fl.load_stock_history_to_db(dt_from=dt_from,
                                    dt_to=current_date,
                                    stock_input="AAPL",
                                    check_is_load=1,
                                    sleep_time=1,
                                    screener_input="america"
                                    )
    # df = fl.get_stock_quote_from_db("AAPL", "america", "2024-03-12", "2024-03-12")
    # df = fl.get_pct_change(df)
    # print(len(df))
    # print(dir(pandas_ta))
    # load recomendation
    # fl.get_all_stock_recomendation()

    # load history
    if OPERATION == "LOAD_HISTORY":
        fl.save_sp500_tickers()
        fl.load_stock_history_to_db(dt_from=dt_from, dt_to=current_date, screener_input="america")

    #daily load
    if OPERATION == "LOAD_DAILY":
        fl.daily_update_quote()
        fl.get_all_stock_recomendation()