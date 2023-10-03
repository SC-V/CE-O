import streamlit as st
import pandas
import psycopg2
import io
import datetime
import requests
import asyncio
import aiohttp
import json
from pytz import timezone

st.set_page_config(page_title=f"CE orders {datetime.datetime.now(timezone('America/Santiago')).strftime('%Y-%m-%d')}",
                   layout="wide")

FILE_BUFFER_REPORT = io.BytesIO()
BATCH: int = 1
TODAY = datetime.datetime.now(timezone("America/Santiago")).strftime("%Y-%m-%d")
YESTERDAY = (datetime.datetime.now(timezone("America/Santiago")) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

SYNC_URL = st.secrets["SYNC_URL"]
SYNC_REFERER = st.secrets["SYNC_REFERER"]
force_sync_creds = st.secrets["SYNC_KEY"]

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
semaphore = asyncio.Semaphore(10)

@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


connection = init_connection()


@st.cache_data(ttl=60)
def get_historical_orders(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def refactor_lo_code(row):
    if not pandas.isna(row["lo_code"]):
        row["lo_code"] = "LO-" + str(row["lo_code"])
    return row


async def force_sync_platform(proxy_frame: pandas.DataFrame):
    async with semaphore:
        aiohttp_client = aiohttp.ClientSession()
        msg = st.toast(f"Syncing {len(proxy_frame)} orders. Wait for the page to reload")
        tasks = [aiohttp_client.post(SYNC_URL, data=json.dumps({"id": order_id}),
                                     headers={'Content-Type': 'application/json',
                                              'Accept-Language': 'en',
                                              'Authorization': force_sync_creds,
                                              'Referer': f'{SYNC_REFERER}{order_id}'
                                              },
                                     ssl=False
                                     )
                 for order_id
                 in proxy_frame["proxy_order_id"].unique()]
        await asyncio.gather(*tasks)
        await aiohttp_client.close()
        msg.toast(f"Sync completed")
    return
  

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
        id,
        client_id
    FROM orders
    WHERE client_id = '8FCBA125-637E-4365-95C4-17E5659EA485' 
    AND created_at AT TIME ZONE 'America/Santiago' >= '{YESTERDAY} 00:00:00'
    ORDER BY created_at ASC;
    """)

proxy_frame = pandas.DataFrame(proxy_orders,
                               columns=["barcode", "external_id", "lo_code", "request_id", "claim_id",
                                        "tariff", "platform_status", "cargo_status", "created_at", "proxy_order_id", "proxy_client_id"])
proxy_frame["prev_created_at"] = proxy_frame["created_at"].shift(1)
proxy_frame = proxy_frame.apply(lambda row: refactor_lo_code(row), axis=1)
for index, row in proxy_frame.iterrows():
    time_delta = (row["created_at"] - row["prev_created_at"]).total_seconds()
    if time_delta <= 3600.0 or pandas.isna(row["prev_created_at"]):
        proxy_frame.loc[index, "batch"] = BATCH
    else:
        BATCH += 1
        proxy_frame.loc[index, "batch"] = BATCH

st.markdown(f"# CE Orders – {TODAY}")
st.markdown("This is an app to **get a list of CE orders to be received through the mass-processing SC page**. "
            "To do so you need a list of LO- codes.")
st.markdown("Here are the steps to get them correctly:")
st.markdown("0. CE load orders in batches throughout the day. Use **Select batch** to review orders by those batches. "
            "The orders are considered to be in 1 batch if the time between their creation is less than 1 hour. "
            ":red[Keep in mind: batches numbers are recalculated automatically]")
st.markdown("1. Check **Bad address** solution in the proxy and fix all CE orders. :red[Not fixed orders do not have LO-codes]")
st.markdown("2. Check the app. If the metric **Missing LO codes** is not **0**, find the missing orders in the proxy with "
            "**Find and fix** solution and press **:green[Force sync Log Platform]** for each such order. "
            "To get a list of such orders use the filter **Show only missing orders** below. :red[Note! It takes ~5 minutes to get LO- code for a geofixed order]")
st.markdown("3. Press **Reload data** button below, so the app reloads with an updated data")
st.markdown("4. When there's no missing orders, download the report pressing **Download orders** button and make an "
            "upload file to post in /mass-processing page of SC. Don't forget to select **Accept sortables** option!")
st.markdown("5. :red[Keep in mind that **Accept sortables** creates claims in the selected bucket immediately. Mind the timings and the bucket selected!]")
st.divider()

total_orders = len(proxy_frame)

col_total, col_missing, col_selected_orders, col_select_batch = st.columns(4)
with col_total:
    total_order_metric = st.metric("Total orders #", total_orders)
with col_select_batch:
    selected_batches = st.multiselect('Select batch', proxy_frame["batch"].unique()) if total_orders > 0 else None
show_only_missing_orders = st.checkbox('Show only missing orders')

if selected_batches:
    proxy_frame = proxy_frame[proxy_frame['batch'].isin(selected_batches)]

if show_only_missing_orders:
    proxy_frame = proxy_frame[proxy_frame["lo_code"].isna()]

if total_orders > 0:
    st.dataframe(proxy_frame)
    missing_orders = len(proxy_frame[proxy_frame["lo_code"].isna()])
    with col_selected_orders:
        selected_orders_metric = st.metric("Selected orders #", len(proxy_frame))
    with col_missing:
        missing_orders_metric = st.metric("Missing orders #", missing_orders)
    with pandas.ExcelWriter(FILE_BUFFER_REPORT, engine='xlsxwriter') as writer:
        proxy_frame.to_excel(writer, sheet_name='ce_pick_report')
        writer.close()

        st.download_button(
            label="Download orders",
            data=FILE_BUFFER_REPORT,
            file_name=f"ce_orders_{TODAY}.xlsx",
            mime="application/vnd.ms-excel"
        )
else:
    st.info("There are no orders for this period")

if st.button("Reload data", type="primary"):
    st.cache_data.clear()
    st.rerun()

with st.expander("🔄 Mass force sync platform"):
    st.markdown(f":green[Click only once and wait for the completion. Don't refresh the page until completed! It allows to sync all selected missing orders instead of clicking 1 by 1 (check **Show only missing orders** option to enable).]")
    if st.button("Force sync orders", disabled=False if show_only_missing_orders else True):
        asyncio.run(force_sync_platform(proxy_frame))
        st.cache_data.clear()
        st.rerun()
