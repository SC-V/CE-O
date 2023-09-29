import streamlit as st
import pandas
import psycopg2
import io
import datetime
from pytz import timezone

st.set_page_config(page_title="CE orders – 2023-09-29", layout="wide")

FILE_BUFFER_REPORT = io.BytesIO()


@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


connection = init_connection()


@st.cache_data(ttl=60)
def get_historical_orders(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


proxy_orders = get_historical_orders(rf"""
    SELECT 
        client_order_number,
        routing_order_number,
        market_order_id,
        request_id,
        claim_id,
        tariff,
        logistic_status,
        claim_status,
        created_at AT TIME ZONE 'America/Santiago',
        client_id
    FROM orders
    WHERE client_id = '8FCBA125-637E-4365-95C4-17E5659EA485' AND created_at >= '2023-09-28 12:00:00';
    """)

proxy_frame = pandas.DataFrame(proxy_orders,
                               columns=["barcode", "external_id", "lo_code", "request_id", "claim_id",
                                        "tariff", "platform_status", "cargo_status", "created_at", "proxy_client_id"])

st.markdown(f"# CE Orders 2023-09-29")
st.markdown("This is an app to **get a list of CE orders to be received through the mass-processing SC page**. "
            "To do so you need a list of LO- codes.")
st.markdown("Here are the steps to get them correctly:")
st.markdown("1. Check **Bad address** solution and fix all CE orders. :red[Not fixed orders do not have LO-codes]")
st.markdown("2. Check the app. If the metric **Missing LO codes** is not **0**, find the missing orders in proxy with "
            "**Find and fix** solution and press **:green[Force sync Log Platform]** for each such order. To find such "
            "orders press on the column *lo_code*, so the orders are sorted and the missing codes are at the top. "
            "The other option is to use the filter **Show only missing orders**. :red[Note! It takes ~5 minutes to get LO- code for a newly geofixed order]")
st.markdown("3. Press **Reload data** button below, so the app reloads with an updated data")
st.markdown("4. When there's no missing orders, download the report pressing **Download orders** button and make an "
            "upload file to post in /mass-processing page of SC. Don't forget to select **Accept orders**!")
st.divider()

total_orders = len(proxy_frame)
missing_orders = len(proxy_frame[proxy_frame["lo_code"].isna()])

col_total, col_missing, col_show_only_missing, _ = st.columns(4)
with col_total:
    total_order_metric = st.metric("Total orders #", total_orders)
with col_missing:
    missing_orders_metric = st.metric("Missing orders #", missing_orders)
with col_show_only_missing:
    show_only_missing_orders = st.checkbox('Show only missing orders')

if show_only_missing_orders:
    proxy_frame = proxy_frame[proxy_frame["lo_code"].isna()]

st.dataframe(proxy_frame)

if st.button("Reload data", type="primary"):
    st.cache_data.clear()
    st.experimental_rerun()

with pandas.ExcelWriter(FILE_BUFFER_REPORT, engine='xlsxwriter') as writer:
    proxy_frame.to_excel(writer, sheet_name='ce_pick_report')
    writer.close()

    TODAY = datetime.datetime.now(timezone("America/Santiago")).strftime("%Y-%m-%d")
    st.download_button(
        label="Download orders",
        data=FILE_BUFFER_REPORT,
        file_name=f"ce_orders_{TODAY}.xlsx",
        mime="application/vnd.ms-excel"
    )
