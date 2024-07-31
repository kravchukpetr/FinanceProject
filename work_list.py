from app import FinanceLib as fl
from app import MaintenanceLib as mntl
from datetime import datetime

OPERATION_LIST = {
    0: "OTHER",
    1: "LOAD_HISTORY",
    2: "LOAD_DAILY",
    3: "LOAD_CURRENT_STOCK",
    4: "LOGS_FOLDER_RM",
    5: "DEFINE_EXCHANGE_FOR_STOCK"
}
OPERATION = OPERATION_LIST[4]

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
        # fl.save_sp500_tickers()
        fl.load_stock_history_to_db(dt_from=dt_from, dt_to=current_date, screener_input="america", check_is_load=1)

    # daily load
    if OPERATION == "LOAD_DAILY":
        fl.daily_update_quote()
        fl.get_all_stock_recomendation()
    # check function for remove old logs directories
    if OPERATION == "LOGS_FOLDER_RM":
        mntl.rm_old_logs()
    # check function to define exchange for stock symbols
    if OPERATION == "DEFINE_EXCHANGE_FOR_STOCK":
        screener = "america"
        stock = fl.get_exchange_for_stock(screener)
        print(stock)
