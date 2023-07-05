import FinanceLib as fl
import pandas_ta


if __name__ == "__main__":
    df = fl.get_stock_quote_from_db("AAPL", "america", "2023-01-01", "2023-07-05")
    df['Change'] = df.groupby('stock').adjclose.pct_change() * 100
    df['Change7'] = df.groupby('stock').adjclose.pct_change(7) * 100
    df['Change30'] = df.groupby('stock').adjclose.pct_change(30) * 100
    df['Change90'] = df.groupby('stock').adjclose.pct_change(90) * 100
    df['Change180'] = df.groupby('stock').adjclose.pct_change(180) * 100
    df['Change360'] = df.groupby('stock').adjclose.pct_change(360) * 100
    df['Change'] = df['Change'].round(2)
    df['Change7'] = df['Change7'].round(2)
    df['Change30'] = df['Change30'].round(2)
    df['Change90'] = df['Change90'].round(2)
    df['Change180'] = df['Change180'].round(2)
    df['Change360'] = df['Change360'].round(2)

    print(len(df))
    print(dir(pandas_ta))
    # load recomendation
    # fl.get_all_stock_recomendation()
    # load history
    # fl.load_stock_history_to_db("2018-01-01", "2023-07-05")