import streamlit as st
import pandas as pd
import psycopg2
from datetime import date
from config.settings import DB_CONFIG

@st.cache_data(ttl=3600)
def load_data():
    """Load dims + fact_pricing from the star schema into a single DataFrame."""
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
      SELECT
        fp.pricing_sk,
        dd.full_date,
        dc.category_name,
        dp.product_name,
        dl.country,
        dl.city,
        fp.actual_price,
        fp.discounted_price,
        fp.discount_percentage,
        fp.currency,
        fp.rate_to_base
      FROM star.fact_pricing AS fp
      JOIN star.dim_date     AS dd ON fp.date_sk     = dd.date_sk
      JOIN star.dim_product  AS dp ON fp.product_sk  = dp.product_sk
      JOIN star.dim_category AS dc ON fp.category_sk = dc.category_sk
      JOIN star.dim_location AS dl ON fp.location_sk = dl.location_sk
      ;
    """, conn)
    conn.close()

    # ensure full_date is a native date for Streamlit widgets
    df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
    return df

st.set_page_config(page_title="Pricing Dashboard", layout="wide")
st.title("📊 Pricing & Discount Dashboard")

# 1) Load data
df = load_data()

# 2) Sidebar filters
st.sidebar.header("Filters")

# date range
if not df.empty:
    min_d, max_d = df["full_date"].min(), df["full_date"].max()
else:
    min_d = max_d = date.today()

min_date, max_date = st.sidebar.date_input(
    "Date range",
    value=(min_d, max_d),
    key="date_range"
)

# category & product
cats  = st.sidebar.multiselect("Category", sorted(df["category_name"].unique()))
prods = st.sidebar.multiselect("Product",  sorted(df["product_name"].unique()))

# country & city
countries = st.sidebar.multiselect("Country", sorted(df["country"].unique()))
cities    = st.sidebar.multiselect("City",    sorted(df["city"].unique()))

# 3) Apply filters
mask = (df["full_date"] >= min_date) & (df["full_date"] <= max_date)
if cats:      mask &= df["category_name"].isin(cats)
if prods:     mask &= df["product_name"].isin(prods)
if countries: mask &= df["country"].isin(countries)
if cities:    mask &= df["city"].isin(cities)
filtered = df[mask]

st.markdown(f"**Showing {len(filtered)} records** from {len(df)} total.")

# 4) KPIs
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Avg. Actual Price",      f"${filtered['actual_price'].mean():.2f}")
with col2:
    st.metric("Avg. Discount %",        f"{filtered['discount_percentage'].mean():.1f}%")
with col3:
    st.metric("Avg. Rate to Base",      f"{filtered['rate_to_base'].mean():.4f}")
with col4:
    st.metric("Distinct Countries Shown", filtered["country"].nunique())

# 5) Bar chart: Average actual price by category
st.subheader("Average Actual Price by Category")
bar_cat = (
    filtered
    .groupby("category_name")["actual_price"]
    .mean()
    .sort_values(ascending=False)
)
st.bar_chart(bar_cat)

# 6) Bar chart: Average actual price by country
st.subheader("Average Actual Price by Country")
bar_country = (
    filtered
    .groupby("country")["actual_price"]
    .mean()
    .sort_values(ascending=False)
)
st.bar_chart(bar_country)

# 7) Line chart: Selected product price over time
st.subheader("Price Over Time by Product")
if prods:
    line = (
        filtered
        .pivot_table(
            index="full_date",
            columns="product_name",
            values="actual_price",
            aggfunc="mean"
        )
    )
    st.line_chart(line)
else:
    st.info("Select at least one product to see its price trend.")

# 8) Raw data table
st.subheader("Underlying Data")
st.dataframe(filtered, height=300, use_container_width=True)
