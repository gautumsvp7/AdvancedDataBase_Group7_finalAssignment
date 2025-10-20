import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
from snowflake.snowpark.context import get_active_session

# -------------------------------------------------
# Session & page
# -------------------------------------------------
session = get_active_session()
try:
    # Force fresh results when controls change
    session.sql("ALTER SESSION SET USE_CACHED_RESULT = FALSE").collect()
except Exception:
    pass

st.set_page_config(page_title="UPay Dashboard : Key Metric", layout="wide")
st.title("UPay Dashboard : Key Metric")

# -------------------------------------------------
# Controls
# -------------------------------------------------
c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    lookback_days = st.selectbox("Lookback window (days)", [30, 90, 180, 365], index=1)
with c2:
    agg_grain = st.selectbox("Time grain", ["day", "week", "month"], index=1)
with c3:
    currency = st.selectbox("Currency", ["AUD"], index=0)

grain_sql = {"day": "day", "week": "week", "month": "month"}[agg_grain]

def run_df(sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query failed: {e}")
        return pd.DataFrame()

st.markdown("---")

# -------------------------------------------------
# KPIs
# -------------------------------------------------
kpi_sql = f"""
WITH base AS (
  SELECT
    CAST(transaction_date AS DATE) AS dt,
    UPPER(transaction_status) AS status,
    amount_aud
  FROM gold.fact_transactions
  WHERE CAST(transaction_date AS DATE)
        BETWEEN DATEADD('day', -{lookback_days}, CURRENT_DATE()) AND CURRENT_DATE()
)
SELECT
  ROUND(100.0 * SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 2) AS success_rate_pct,
  ROUND(SUM(CASE WHEN status = 'APPROVED' THEN amount_aud ELSE 0 END), 2) AS approved_revenue_aud,
  SUM(CASE WHEN status <> 'APPROVED' THEN 1 ELSE 0 END) AS failure_count,
  SUM(CASE WHEN status = 'FRAUD_DETECTED' THEN 1 ELSE 0 END) AS fraud_incidents
FROM base
"""
kdf = run_df(kpi_sql)
if not kdf.empty:
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Success Rate", f"{kdf.iloc[0]['SUCCESS_RATE_PCT']:.2f}%")
    k2.metric(f"Approved Revenue ({currency})", f"{kdf.iloc[0]['APPROVED_REVENUE_AUD']:,.0f}")
    k3.metric("Failure Count", int(kdf.iloc[0]['FAILURE_COUNT']))
    k4.metric("Fraud Incidents", int(kdf.iloc[0]['FRAUD_INCIDENTS']))
else:
    st.info("No KPI data in the selected window.")

st.markdown("---")

# -------------------------------------------------
# Donut & Revenue bar
# -------------------------------------------------
donut_sql = f"""
WITH base AS (
  SELECT
    UPPER(transaction_status) AS status,
    amount_aud
  FROM gold.fact_transactions
  WHERE CAST(transaction_date AS DATE)
        BETWEEN DATEADD('day', -{lookback_days}, CURRENT_DATE()) AND CURRENT_DATE()
)
SELECT
  CASE WHEN status = 'APPROVED' THEN 'APPROVED' ELSE 'FAILED' END AS outcome,
  COUNT(*) AS txn_count,
  ROUND(SUM(CASE WHEN status = 'APPROVED' THEN amount_aud ELSE 0 END), 2) AS revenue_gained_aud,
  ROUND(SUM(CASE WHEN status <> 'APPROVED' THEN amount_aud ELSE 0 END), 2) AS revenue_lost_aud
FROM base
GROUP BY 1
"""
ddf = run_df(donut_sql)

lc, rc = st.columns([1, 1])
with lc:
    st.subheader("Success vs Failure")
    if not ddf.empty:
        # pastel green for APPROVED, dark red for FAILED
        color_map = {"APPROVED": "#9AE19D", "FAILED": "#8B0000"}
        fig = px.pie(ddf, names="OUTCOME", values="TXN_COUNT", hole=0.55,
                     color="OUTCOME", color_discrete_map=color_map)
        fig.update_traces(textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No transactions found in the selected window.")

with rc:
    st.subheader(f"Revenue Gained vs Lost ({currency})")
    if not ddf.empty:
        rev_df = pd.DataFrame({
            "Impact": ["Revenue Gained", "Revenue Lost"],
            "Amount": [
                float(ddf.loc[ddf["OUTCOME"]=="APPROVED","REVENUE_GAINED_AUD"].values[0]) if (ddf["OUTCOME"]=="APPROVED").any() else 0.0,
                float(ddf["REVENUE_LOST_AUD"].max()) if "REVENUE_LOST_AUD" in ddf.columns else 0.0
            ]
        })
        # pastel green & dark red to match donut
        bar_colors = {"Revenue Gained": "#9AE19D", "Revenue Lost": "#8B0000"}
        bar = alt.Chart(rev_df).mark_bar().encode(
            x=alt.X("Amount:Q", title=currency),
            y=alt.Y("Impact:N", sort="-x", title=""),
            color=alt.Color("Impact:N",
                            scale=alt.Scale(domain=list(bar_colors.keys()),
                                            range=list(bar_colors.values())))
        )
        st.altair_chart(bar, use_container_width=True)
    else:
        st.info("No revenue data found.")

st.markdown("---")

# ---------- Global Failure View (bar fallback for Snowsight) ----------
st.subheader("Global Failure View (Top Failure Countries)")

map_sql = f"""
WITH base AS (
  SELECT
    UPPER(c.customer_country) AS iso2,
    UPPER(f.transaction_status) AS status
  FROM gold.fact_transactions f
  LEFT JOIN gold.dim_customer c ON f.customer_id = c.customer_id
  WHERE CAST(f.transaction_date AS DATE)
        BETWEEN DATEADD('day', -{lookback_days}, CURRENT_DATE()) AND CURRENT_DATE()
)
SELECT
  iso2,
  COUNT_IF(status <> 'APPROVED') AS failures,
  COUNT_IF(status = 'DECLINED')  AS declined,
  COUNT_IF(status = 'TIMED_OUT') AS timed_out,
  COUNT_IF(status = 'FRAUD_DETECTED') AS fraud_detected,
  COUNT_IF(status = 'ERROR') AS error_events
FROM base
WHERE iso2 IS NOT NULL
GROUP BY iso2
"""
df = run_df(map_sql)

if df.empty:
    st.info("No country-level failure data for the selected window.")
else:
    iso2_to_name = {
        'AU':'Australia','US':'United States','UK':'United Kingdom','GB':'United Kingdom',
        'CA':'Canada','FR':'France','DE':'Germany','BR':'Brazil','IN':'India','JP':'Japan','SG':'Singapore'
    }
    df["ISO2"] = df["ISO2"].astype(str).str.strip().str.upper()
    df["COUNTRY_NAME"] = df["ISO2"].map(iso2_to_name).fillna(df["ISO2"])
    df["FAILURES"] = pd.to_numeric(df["FAILURES"], errors="coerce").fillna(0)

    top_df = df.sort_values("FAILURES", ascending=False)

    bar = (
        alt.Chart(top_df)
        .mark_bar()
        .encode(
            x=alt.X("FAILURES:Q", title="Failure Count"),
            y=alt.Y("COUNTRY_NAME:N", sort='-x', title="Country"),
            tooltip=[
                "COUNTRY_NAME","FAILURES","DECLINED",
                "TIMED_OUT","FRAUD_DETECTED","ERROR_EVENTS"
            ],
            color=alt.Color("FAILURES:Q", scale=alt.Scale(scheme='reds'))
        )
        .properties(height=420)
    )
    st.altair_chart(bar, use_container_width=True)

# -------------------------------------------------
# High-Risk Customer Analysis (Fraud)
# -------------------------------------------------
st.subheader("High-Risk Customer Analysis by Fraud Presence")

risk_sql = f"""
WITH base AS (
  SELECT
    f.customer_id,
    UPPER(f.transaction_status) AS status,
    f.amount_aud
  FROM gold.fact_transactions f
  WHERE CAST(f.transaction_date AS DATE)
        BETWEEN DATEADD('day', -{lookback_days}, CURRENT_DATE()) AND CURRENT_DATE()
),
agg AS (
  SELECT
    customer_id,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN status = 'FRAUD_DETECTED' THEN 1 ELSE 0 END) AS fraud_txns,
    SUM(CASE WHEN status = 'APPROVED' THEN amount_aud ELSE 0 END) AS approved_amount_aud,
    SUM(amount_aud) AS total_amount_aud,
    AVG(amount_aud) AS avg_amount_aud
  FROM base
  GROUP BY 1
)
SELECT
  customer_id,
  total_txns,
  fraud_txns,
  ROUND(100.0 * fraud_txns / NULLIF(total_txns,0), 2) AS fraud_share_pct,
  ROUND(approved_amount_aud, 2) AS approved_amount_aud,
  ROUND(total_amount_aud, 2) AS total_amount_aud,
  ROUND(avg_amount_aud, 2) AS avg_amount_aud,
  CASE
    WHEN fraud_txns >= 5 OR (100.0 * fraud_txns / NULLIF(total_txns,0)) >= 20 THEN 'HIGH'
    WHEN fraud_txns >= 2 OR (100.0 * fraud_txns / NULLIF(total_txns,0)) >= 10 THEN 'MEDIUM'
    WHEN fraud_txns >= 1 THEN 'LOW'
    ELSE 'NONE'
  END AS risk_band
FROM agg
WHERE fraud_txns >= 1
"""
risk_df = run_df(risk_sql)

if risk_df.empty:
    st.info("No customers with fraud in this window.")
else:
    scatter = alt.Chart(risk_df).mark_circle(opacity=0.85).encode(
        x=alt.X("FRAUD_SHARE_PCT:Q", title="Fraud Share (%)"),
        y=alt.Y("TOTAL_TXNS:Q", title="Total Transactions"),
        size=alt.Size("TOTAL_AMOUNT_AUD:Q", title="Total Amount (AUD)", scale=alt.Scale(type='sqrt')),
        color=alt.Color("RISK_BAND:N", legend=alt.Legend(title="Risk Band")),
        tooltip=[
            "CUSTOMER_ID","FRAUD_TXNS","FRAUD_SHARE_PCT","TOTAL_TXNS",
            "APPROVED_AMOUNT_AUD","TOTAL_AMOUNT_AUD","AVG_AMOUNT_AUD"
        ]
    ).properties(height=420)
    st.altair_chart(scatter, use_container_width=True)

    st.markdown("Top customers by fraud share")
    st.dataframe(
        risk_df.sort_values(["FRAUD_SHARE_PCT","FRAUD_TXNS","TOTAL_TXNS"],
                            ascending=[False, False, False]).head(20),
        use_container_width=True
    )
