import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
import plotly.graph_objects as go

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Axelar's Squid Bridge",
    page_icon="https://pbs.twimg.com/profile_images/1938625911743524864/ppNPPF84_400x400.jpg",
    layout="wide"
)

# --- Title with Logo -----------------------------------------------------------------------------------------------------
st.title("📜Overall Stats")

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ----------------------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user=st.secrets["snowflake"]["user"],
    password=st.secrets["snowflake"]["password"],
    account=st.secrets["snowflake"]["account"],
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="AXELAR",
    schema="PUBLIC"
)
# --- Date Inputs ---------------------------------------------------------------------------------------------------
timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))
# --- Query Function: Row1 --------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers,
        ROUND(avg(amount_usd)) as AVG_BRIDGES_VOLUME
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(timeframe, start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

def format_value(value, unit):
    if unit == 'B':
        return f"${value / 1_000_000_000:.2f}B"
    elif unit == 'M':
        return f"{value / 1_000_000:.2f}M Txns"
    elif unit == 'K':
        return f"{value / 1_000:.2f}K"
    return str(value)

col1.metric(
    label="Bridged Volume",
    value=format_value(df_kpi['VOLUME_OF_TRANSFERS'][0], 'B')
)

col2.metric(
    label="Bridges",
    value=format_value(df_kpi['NUMBER_OF_TRANSFERS'][0], 'M')
)

col3.metric(
    label="Bridgors",
    value=f"{df_kpi['NUMBER_OF_USERS'][0] / 1_000:.2f}K Addresses"
)

col4.metric(
    label="Avg Bridge Volume",
    value=f"${df_kpi['AVG_BRIDGES_VOLUME'][0] / 1_000:.2f}K"
)



# --- Row (2) ----------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_chart_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user,
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
        UNION ALL
        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        date_trunc('{timeframe}', created_at) as "Date",
        count(distinct id) as "Bridges", 
        sum(count(distinct id)) over (order by date_trunc('{timeframe}', created_at)) as "Cumulative Bridges Count", 
        round(sum(amount_usd)) as "Volume",
        sum(round(sum(amount_usd))) over (order by date_trunc('{timeframe}', created_at)) as "Cumulative Bridges Volume",
        round(sum(amount_usd)/count(distinct user)) as "Avg Bridges Volume per User",
        round(avg(amount_usd)) as "Avg Bridges Volume per Txn"
    FROM axelar_service
    WHERE created_at::date >= '{start_str}'
      AND created_at::date <= '{end_str}'
    GROUP BY 1
    ORDER BY 1
    """
    return pd.read_sql(query, conn)

df_chart = load_chart_data(timeframe, start_date, end_date)

# --- Row 2: Bar + Line Charts ------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(x=df_chart["Date"], y=df_chart["Bridges"], name="Bridges", yaxis="y1"))
    fig1.add_trace(go.Scatter(x=df_chart["Date"], y=df_chart["Cumulative Bridges Count"], name="Cumulative Bridges Count", mode="lines+markers", yaxis="y2"))
    fig1.update_layout(
        title="Number of Bridges Over Time",
        yaxis=dict(title="Bridges"),
        yaxis2=dict(title="Cumulative Bridges Count", overlaying="y", side="right"),
        barmode="group"
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=df_chart["Date"], y=df_chart["Volume"], name="Volume", yaxis="y1"))
    fig2.add_trace(go.Scatter(x=df_chart["Date"], y=df_chart["Cumulative Bridges Volume"], name="Cumulative Bridges Volume", mode="lines+markers", yaxis="y2"))
    fig2.update_layout(
        title="Bridges Volume Over Time",
        yaxis=dict(title="Volume (USD)"),
        yaxis2=dict(title="Cumulative Volume (USD)", overlaying="y", side="right"),
        barmode="group"
    )
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 3: Scatter Charts ---------------------------------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    fig3 = px.scatter(
        df_chart,
        x="Date",
        y="Avg Bridges Volume per User",
        size="Avg Bridges Volume per User",
        title="Avg Bridges Volume Per User Over Time"
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    fig4 = px.scatter(
        df_chart,
        x="Date",
        y="Avg Bridges Volume per Txn",
        size="Avg Bridges Volume per Txn",
        title="Avg Volume of Bridges Over Time"
    )
    st.plotly_chart(fig4, use_container_width=True)

# --- Row (4) ---------------------------------------------------------------------------------
@st.cache_data
def load_bridgors_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with table1 as (
        WITH axelar_service AS (
            SELECT created_at, recipient_address AS user
            FROM axelar.axelscan.fact_transfers
            WHERE status = 'executed' AND simplified_status = 'received'
              AND (
                sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
                OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' 
                OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' 
                OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' 
                OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
              )
            UNION ALL
            SELECT created_at, data:call.transaction.from::STRING AS user
            FROM axelar.axelscan.fact_gmp 
            WHERE status = 'executed' AND simplified_status = 'received'
              AND (
                data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
                OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' 
                OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' 
                OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' 
                OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
              )
        )
        SELECT 
            date_trunc('{timeframe}', created_at) as "Date",
            count(distinct user) as "Total Bridgors"
        FROM axelar_service
        WHERE created_at::date >= '{start_str}'
          AND created_at::date <= '{end_str}'
        GROUP BY 1
    ), 

    table2 as (
        with tab1 as (
            WITH axelar_service AS (
                SELECT created_at, recipient_address AS user
                FROM axelar.axelscan.fact_transfers
                WHERE status = 'executed' AND simplified_status = 'received'
                  AND (
                    sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
                    OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' 
                    OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' 
                    OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' 
                    OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
                  )
                UNION ALL
                SELECT created_at, data:call.transaction.from::STRING AS user
                FROM axelar.axelscan.fact_gmp 
                WHERE status = 'executed' AND simplified_status = 'received'
                  AND (
                    data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
                    OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' 
                    OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' 
                    OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' 
                    OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
                  )
            )
            SELECT user, min(created_at::date) as first_date
            FROM axelar_service
            GROUP BY 1
        )
        SELECT date_trunc('{timeframe}', first_date) as "Date", count(distinct user) as "New Bridgors"
        FROM tab1
        WHERE first_date >= '{start_str}' AND first_date <= '{end_str}'
        GROUP BY 1
    )

    SELECT 
        t1."Date" as "Date", 
        "Total Bridgors", 
        "New Bridgors", 
        "Total Bridgors" - "New Bridgors" as "Active Bridgors",
        sum("New Bridgors") over (order by t1."Date") as "Bridgors Growth"
    FROM table1 t1
    LEFT JOIN table2 t2 ON t1."Date" = t2."Date"
    ORDER BY 1
    """
    return pd.read_sql(query, conn)

df_brg = load_bridgors_data(timeframe, start_date, end_date)

# --- Row (4): Charts ------------------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig_b1 = go.Figure()
    # Stacked Bars
    fig_b1.add_trace(go.Bar(x=df_brg["Date"], y=df_brg["New Bridgors"], name="New Bridgors"))
    fig_b1.add_trace(go.Bar(x=df_brg["Date"], y=df_brg["Active Bridgors"], name="Active Bridgors"))
    # Line for Total Bridgors
    fig_b1.add_trace(go.Scatter(
        x=df_brg["Date"], y=df_brg["Total Bridgors"], name="Total Bridgors",
        mode="lines+markers", line=dict(color="black", width=2)
    ))
    fig_b1.update_layout(
        barmode="stack",
        title="Number of Bridgors Over Time",
        yaxis=dict(title="Number of Bridgors")
    )
    st.plotly_chart(fig_b1, use_container_width=True)

with col2:
    fig_b2 = go.Figure()
    fig_b2.add_trace(go.Bar(
        x=df_brg["Date"], y=df_brg["Bridgors Growth"], name="Bridgors Growth"
    ))
    fig_b2.update_layout(
        title="Total New Bridgors Over Time",
        yaxis=dict(title="Bridgors Growth")
    )
    st.plotly_chart(fig_b2, use_container_width=True)

