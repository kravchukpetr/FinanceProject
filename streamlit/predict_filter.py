import streamlit as st
import pandas as pd
import os
import json
import plotly.graph_objects as go
import sys
import yfinance as yf

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'app')))
from FinanceLib import get_stock_quote_from_db


# Конфигурация страницы
st.set_page_config(page_title="DataFrame Viewer", layout="wide")

# Директория для хранения фильтров
FILTERS_DIR = "filters"
os.makedirs(FILTERS_DIR, exist_ok=True)

# Инициализация session_state
if 'loaded_filter' not in st.session_state:
    st.session_state.loaded_filter = {}
if 'reset_filters' not in st.session_state:
    st.session_state.reset_filters = False

# Загрузка DataFrame
df = pd.read_pickle('df_with_predict.pkl').drop(columns=['symbol_encoded', 'Target'])

st.title("📊 DataFrame Explorer")

# --- Загрузка и сброс фильтров ---
st.subheader("📂 Загрузка / сброс фильтров")

saved_filters = [f for f in os.listdir(FILTERS_DIR) if f.endswith(".json")]
selected_filter_file = st.selectbox("Выберите фильтр для загрузки:", ["<не выбрано>"] + saved_filters)

col1, col2 = st.columns(2)

with col1:
    if st.button("🔄 Загрузить выбранный фильтр") and selected_filter_file != "<не выбрано>":
        with open(os.path.join(FILTERS_DIR, selected_filter_file), "r") as f:
            st.session_state.loaded_filter = json.load(f)
        st.session_state.reset_filters = False
        st.success(f"Фильтр {selected_filter_file} загружен!")
        st.rerun()

with col2:
    if st.button("🚫 Сбросить все фильтры"):
        st.session_state.loaded_filter = {}
        st.session_state.reset_filters = True
        st.rerun()

# --- Фильтрация ---
st.subheader("🔍 Фильтрация и сортировка")

filters = {}
df_original = df.copy()

for col in df_original.columns:
    if pd.api.types.is_numeric_dtype(df_original[col]):
        min_val, max_val = float(df_original[col].min()), float(df_original[col].max())
        default = tuple(st.session_state.loaded_filter.get(col, (min_val, max_val)))
        selected_range = st.slider(
            f"{col}",
            min_value=min_val,
            max_value=max_val,
            value=default,
            key=f"{col}_slider"
        )
        filters[col] = df_original[col].between(*selected_range)

    elif pd.api.types.is_datetime64_any_dtype(df_original[col]):
        default = st.session_state.loaded_filter.get(col, [])
        selected_range = st.date_input(
            f"{col}",
            value=default,
            key=f"{col}_date"
        )
        if len(selected_range) == 2:
            start_date, end_date = selected_range
            filters[col] = df_original[col].between(start_date, end_date)

    else:
        unique_vals = df_original[col].dropna().unique().tolist()
        unique_vals.sort()
        options = ["All"] + unique_vals
        default = st.session_state.loaded_filter.get(col, ["All"])
        selected_vals = st.multiselect(
            f"{col}",
            options=options,
            default=default,
            key=f"{col}_select"
        )
        if "All" not in selected_vals:
            filters[col] = df_original[col].isin(selected_vals)

# --- Применение фильтров ---
if filters:
    mask = pd.Series(True, index=df_original.index)
    for condition in filters.values():
        mask &= condition
    df = df_original[mask]
else:
    df = df_original.copy()

# --- Настройка отображаемых колонок ---
st.subheader("🧩 Настройка отображения колонок")

all_columns = df.columns.tolist()
default_columns = st.session_state.loaded_filter.get("selected_columns", all_columns)

selected_columns = st.multiselect(
    "Выберите колонки для отображения:",
    options=all_columns,
    default=default_columns,
    key="selected_columns",
    help="Уберите лишние, чтобы сосредоточиться на нужных метриках"
)

df_to_show = df[selected_columns]

# --- Вывод таблицы ---
st.subheader("📈 Отфильтрованный DataFrame")
st.markdown(f"**Количество строк:** {len(df_to_show):,}")
st.dataframe(df_to_show, use_container_width=True)

# --- Сохранение фильтра ---
st.subheader("💾 Сохранение фильтра")

filter_name = st.text_input("Название фильтра")

if st.button("Сохранить фильтр"):
    export_values = {}

    for col in df_original.columns:
        if pd.api.types.is_numeric_dtype(df_original[col]):
            export_values[col] = st.session_state.get(f"{col}_slider")
        elif pd.api.types.is_datetime64_any_dtype(df_original[col]):
            export_values[col] = st.session_state.get(f"{col}_date")
        else:
            export_values[col] = st.session_state.get(f"{col}_select")

    # 🔹 Сохраняем выбранные колонки
    export_values["selected_columns"] = selected_columns

    with open(os.path.join(FILTERS_DIR, f"{filter_name}.json"), "w") as f:
        json.dump(export_values, f, default=str)
    st.success(f"Фильтр '{filter_name}.json' сохранён!")

st.subheader("🕯️ График котировок по выбранному инструменту")

selected_symbol = st.selectbox(
    "Выберите тикер (symbol) для просмотра графика:",
    options=df['symbol'].unique().tolist()
)

if selected_symbol:
    # Загружаем данные с Yahoo Finance (или замените на свой источник)
    # hist = yf.download(selected_symbol, period="6mo", interval="1d")
    
    hist = get_stock_quote_from_db(selected_symbol, "america")
    hist = hist.rename_axis('Date')
    hist = hist.rename(columns={'stock': 'symbol', 
                                    'openvalue': 'Open', 
                                    'highvalue': 'High', 
                                    'lowvalue': 'Low', 
                                    'closevalue': 'Close', 
                                    'adjclose': 'Adj Close', 
                                    'volume': 'Volume'})
    # hist.dropna(inplace=True)
        
    # hist = yf.download(selected_symbol, period="6mo", interval="1d")
    hist = hist.reset_index()
    # ✅ Подготовка: Сбросить MultiIndex, если есть
    if isinstance(hist.columns, pd.MultiIndex):
        # hist.columns = ['_'.join(col).strip() for col in hist.columns]
        hist.columns = hist.columns.get_level_values(0)

    # ✅ Проверка наличия нужных колонок
    required_columns = ['Date', 'Open', 'High', 'Low', 'Close']
    for col in required_columns:
        if col not in hist.columns:
            st.error(f"Колонка {col} отсутствует в данных.")
            st.stop()

    # ✅ Приведение типов и сортировка
    hist["Date"] = pd.to_datetime(hist["Date"])
    hist = hist.sort_values("Date")
    hist.set_index("Date", inplace=True)

    hist["EMA_50"] = hist["Close"].ewm(span=50, adjust=False).mean()
    hist["EMA_200"] = hist["Close"].ewm(span=200, adjust=False).mean()

    for col in ["Open", "High", "Low", "Close"]:
        hist[col] = pd.to_numeric(hist[col], errors="coerce")

    hist = hist.dropna(subset=["Open", "High", "Low", "Close"])

    # ✅ Построение графика
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=hist.index,
        open=hist["Open"],
        high=hist["High"],
        low=hist["Low"],
        close=hist["Close"],
        name="Candles"
    ))

    # EMA линии (если есть)
    if "EMA_50" in hist.columns:
        fig.add_trace(go.Scatter(x=hist.index, y=hist["EMA_50"], name="EMA 50", line=dict(color="blue")))

    if "EMA_200" in hist.columns:
        fig.add_trace(go.Scatter(x=hist.index, y=hist["EMA_200"], name="EMA 200", line=dict(color="red")))

    fig.update_layout(
        title="График свечей",
        xaxis_title="Дата",
        yaxis_title="Цена",
        xaxis_rangeslider_visible=False,
        height=600
    )

    st.plotly_chart(fig, use_container_width=True)
