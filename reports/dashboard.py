# reports/dashboard.py

import streamlit as st
import pandas as pd
import psycopg2
from datetime import date
from config.settings import DB_CONFIG

@st.cache_data(ttl=3600)
def load_data():
    """Load dims + fact_pricing from the star schema into a single DataFrame."""
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
      SELECT
        fp.pricing_sk,
        dd.full_date,
        dc.category_name,
        dp.product_name,
        fp.actual_price,
        fp.discounted_price,
        fp.discount_percentage,
        fp.currency,
        fp.rate_to_base
      FROM star.fact_pricing fp
      JOIN star.dim_date     dd ON fp.date_sk    = dd.date_sk
      JOIN star.dim_product  dp ON fp.product_sk = dp.product_sk
      JOIN star.dim_category dc ON dp.category_sk = dc.category_sk;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    # convert to native date
    df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
    return df

st.set_page_config(page_title="Pricing Dashboard", layout="wide")
st.title("📊 Pricing & Discount Dashboard")

# 1) Load data
df = load_data()

# 2) Sidebar filters
st.sidebar.header("Filters")
min_date, max_date = st.sidebar.date_input(
    "Date range",
    value=[df["full_date"].min(), df["full_date"].max()],
    key="date_range"
)
cats = st.sidebar.multiselect(
    "Category",
    options=sorted(df["category_name"].unique()),
    default=None
)
prods = st.sidebar.multiselect(
    "Product",
    options=sorted(df["product_name"].unique()),
    default=None
)

# 3) Apply filters
mask = (df["full_date"] >= min_date) & (df["full_date"] <= max_date)
if cats:
    mask &= df["category_name"].isin(cats)
if prods:
    mask &= df["product_name"].isin(prods)
filtered = df[mask]

st.markdown(f"**Showing {len(filtered)} records** from {df.shape[0]} total.")

# 4) KPIs
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Avg. Actual Price", f"${filtered['actual_price'].mean():.2f}")
with col2:
    st.metric("Avg. Discount", f"{filtered['discount_percentage'].mean():.1f}%")
with col3:
    st.metric("Avg. Rate to Base", f"{filtered['rate_to_base'].mean():.4f}")

# 5) Bar chart: Average actual price by category
st.subheader("Average Actual Price by Category")
bar_data = (
    filtered
    .groupby("category_name")["actual_price"]
    .mean()
    .sort_values(ascending=False)
    .reset_index()
)
st.bar_chart(
    data=bar_data.set_index("category_name"),
    use_container_width=True
)

# 6) Line chart: Selected product price over time
st.subheader("Price Over Time by Product")
if prods:
    line_data = (
        filtered
        .groupby(["full_date","product_name"])["actual_price"]
        .mean()
        .unstack("product_name")
    )
    st.line_chart(line_data, use_container_width=True)
else:
    st.info("Select at least one product to see its price trend.")

# 7) Raw data table
st.subheader("Underlying Data")
st.dataframe(filtered, height=300, use_container_width=True)
