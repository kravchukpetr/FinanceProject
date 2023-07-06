import FinanceLib as fl
import pandas_ta


if __name__ == "__main__":
    df = fl.get_stock_quote_from_db("AAPL", "america", "2023-01-01", "2023-07-05")
    df = fl.get_pct_change(df)
    print(len(df))
    print(dir(pandas_ta))
    # load recomendation
    # fl.get_all_stock_recomendation()
    # load history
    # fl.load_stock_history_to_db("2018-01-01", "2023-07-05")