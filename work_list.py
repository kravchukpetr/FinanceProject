import FinanceLib as fl
# import pandas_ta


if __name__ == "__main__":
    fl.load_stock_history_to_db(dt_from="2024-03-11",
                                dt_to="2024-03-12",
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
    # fl.load_stock_history_to_db("2018-01-01", "2023-07-05")