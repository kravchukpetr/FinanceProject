import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="DataFrame Viewer", layout="wide")

st.title("📊 DataFrame Explorer")

# --- Загрузка файла ---
# uploaded_file = st.file_uploader("Выберите файл", type=["csv", "parquet", "pkl"])

df = pd.read_pickle('df_with_predict.pkl').drop(columns=['symbol_encoded', 'Target'])

    
# --- Фильтрация по столбцам ---
st.subheader("🔍 Фильтрация и сортировка")


# Показываем все фильтры
filters = {}
df_original = df.copy()

for col in df_original.columns:
    if pd.api.types.is_numeric_dtype(df_original[col]):
        min_val, max_val = float(df_original[col].min()), float(df_original[col].max())
        selected_range = st.slider(f"{col}", min_val, max_val, (min_val, max_val))
        filters[col] = df_original[col].between(*selected_range)

    elif pd.api.types.is_datetime64_any_dtype(df_original[col]):
        selected_range = st.date_input(f"{col}", [])
        if len(selected_range) == 2:
            start_date, end_date = selected_range
            filters[col] = df_original[col].between(start_date, end_date)

    else:
        unique_vals = df_original[col].dropna().unique()
        selected_vals = st.multiselect(f"{col}", options=unique_vals, default=unique_vals)
        filters[col] = df_original[col].isin(selected_vals)

# Применяем фильтры
if filters:
    mask = pd.Series(True, index=df_original.index)
    for condition in filters.values():
        mask &= condition
    df = df_original[mask]


st.subheader("🧩 Настройка отображения колонок")

all_columns = df.columns.tolist()

selected_columns = st.multiselect(
    "Выберите колонки для отображения:",
    options=all_columns,
    default=all_columns,
    help="Вы можете убрать лишние колонки, чтобы сосредоточиться на нужных метриках"
)

# Отображаем только выбранные колонки
df_to_show = df[selected_columns]

# --- Вывод таблицы ---
st.markdown(f"**Количество строк:** {len(df):,}")
st.subheader("📈 Отфильтрованный DataFrame")
st.dataframe(df_to_show, use_container_width=True)

