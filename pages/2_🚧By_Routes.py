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
st.title("🚧By Routes")

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

# --- Row (1) --------------------------------------------------------------------------------
@st.cache_data(ttl=86400)
def load_data(start_date, end_date):
    query = f"""
    WITH axelar_service AS (
      
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
        'Token Transfers' AS "Service", 
        data:link:asset::STRING AS raw_asset

      FROM axelar.axelscan.fact_transfers
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
        sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
        or sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
        or sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
        or sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
        or sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
    ) 

      UNION ALL

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
        'GMP' AS "Service", 
        data:symbol::STRING AS raw_asset

      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
            or data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
            or data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
            or data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
            or data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
            ) 
    )

    SELECT 
    source_chain || '➡' || destination_chain as "Route",
    round(sum(amount_usd)) as "Volume",
    round((avg(amount_usd)),1) as "Avg Volume per Txn",
    count(distinct id) as "Bridges", 
    count(distinct user) as "Bridgors",
    round((sum(amount_usd)/count(distinct user)),1) as "Avg Volume per Bridgor",
    round(count(distinct id)/count(distinct user)) as "Avg Bridge Count per User"
    FROM axelar_service
    WHERE created_at::date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1
    ORDER BY 4 DESC
    """
    df = pd.read_sql(query, conn)
    df.index = df.index + 1  
    return df

df = load_data(start_date, end_date)

# --- Row (1). Display Table -------------------------------------------------------------------------------------------------
st.write("### Squid Bridging Routes' Stats")
st.dataframe(df)

# --- Row (2,3). Plot Horizontal Bar Charts -----------------------------------------------------------------------------------
top_10_volume = df.nlargest(10, "Volume")
top_10_bridges = df.nlargest(10, "Bridges")

fig_volume = px.bar(
    top_10_volume[::-1],  
    x="Volume",
    y="Route",
    orientation='h',
    text="Volume",
    color="Route",
    title="Top 10 Routes by Volume"
)
fig_volume.update_traces(texttemplate='%{text}', textposition='outside')
fig_volume.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))

fig_bridges = px.bar(
    top_10_bridges[::-1],
    x="Bridges",
    y="Route",
    orientation='h',
    text="Bridges",
    color="Route",
    title="Top 10 Routes by Bridges"
)
fig_bridges.update_traces(texttemplate='%{text}', textposition='outside')
fig_bridges.update_layout(showlegend=False, yaxis=dict(autorange="reversed"))

st.plotly_chart(fig_volume, use_container_width=True)
st.plotly_chart(fig_bridges, use_container_width=True)

# --- Row 4 -----------------------------------------------------------------------------------------------------------------------------
@st.cache_data(ttl=86400)
def load_bridge_size_data(start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH axelar_service AS (
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
          'Token Transfers' AS "Service", 
          data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
            or sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
            or sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
            or sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
            or sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
          ) 
        UNION ALL
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
          'GMP' AS "Service", 
          data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
            or data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
            or data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
            or data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
            or data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
          ) 
      )
      SELECT 
        source_chain || '➡' || destination_chain as "Route", 
        sum(amount_usd) as total_amount_usd,
        CASE 
          WHEN sum(amount_usd)<=10 THEN '<=$10'
          WHEN sum(amount_usd)>10 AND sum(amount_usd)<=100 THEN '$10-$100'
          WHEN sum(amount_usd)>100 AND sum(amount_usd)<=1000 THEN '$100-$1K'
          WHEN sum(amount_usd)>1000 AND sum(amount_usd)<=10000 THEN '$1K-$10K'
          WHEN sum(amount_usd)>10000 AND sum(amount_usd)<=100000 THEN '$10K-$100K'
          WHEN sum(amount_usd)>100000 AND sum(amount_usd)<=1000000 THEN '$100K-$1M'
          WHEN sum(amount_usd)>1000000 THEN '>$1M'
        END as "Bridge Size"
      FROM axelar_service
      WHERE created_at::date BETWEEN '{start_date}' AND '{end_date}'
        AND amount_usd IS NOT NULL
      GROUP BY 1
    )
    SELECT "Bridge Size", COUNT(DISTINCT "Route") AS "Number of Routes"
    FROM overview
    GROUP BY 1
    ORDER BY
      CASE 
        WHEN "Bridge Size" = '<=$10' THEN 1
        WHEN "Bridge Size" = '$10-$100' THEN 2
        WHEN "Bridge Size" = '$100-$1K' THEN 3
        WHEN "Bridge Size" = '$1K-$10K' THEN 4
        WHEN "Bridge Size" = '$10K-$100K' THEN 5
        WHEN "Bridge Size" = '$100K-$1M' THEN 6
        WHEN "Bridge Size" = '>$1M' THEN 7
        ELSE 8
      END
    """
    df = pd.read_sql(query, conn)
    return df

@st.cache_data(ttl=86400)
def load_bridge_count_data(start_date, end_date):
    query = f"""
    WITH overview AS (
      WITH axelar_service AS (
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
          'Token Transfers' AS "Service", 
          data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
            or sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
            or sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
            or sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
            or sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
          ) 
        UNION ALL
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
          'GMP' AS "Service", 
          data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' -- Squid
            or data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%' -- Squid-blast
            or data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%' -- Squid-fraxtal
            or data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%' -- Squid coral
            or data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%' -- Squid coral hub
          ) 
      )
      SELECT 
        source_chain || '➡' || destination_chain as "Route", 
        COUNT(DISTINCT id) AS txn_count,
        CASE 
          WHEN COUNT(DISTINCT id)<=10 THEN '<=10 Txns'
          WHEN COUNT(DISTINCT id)>10 AND COUNT(DISTINCT id)<=100 THEN '11-100 Txns'
          WHEN COUNT(DISTINCT id)>100 AND COUNT(DISTINCT id)<=1000 THEN '101-1000 Txns'
          WHEN COUNT(DISTINCT id)>1000 AND COUNT(DISTINCT id)<=10000 THEN '1001-10000 Txns'
          WHEN COUNT(DISTINCT id)>10000 THEN '>10000 Txns'
          ELSE 'Unknown'
        END as "Bridge Count"
      FROM axelar_service
      WHERE created_at::date BETWEEN '{start_date}' AND '{end_date}'
      GROUP BY 1
    )
    SELECT "Bridge Count", COUNT(DISTINCT "Route") AS "Number of Routes"
    FROM overview
    GROUP BY 1
    ORDER BY
      CASE 
        WHEN "Bridge Count" = '<=10 Txns' THEN 1
        WHEN "Bridge Count" = '11-100 Txns' THEN 2
        WHEN "Bridge Count" = '101-1000 Txns' THEN 3
        WHEN "Bridge Count" = '1001-10000 Txns' THEN 4
        WHEN "Bridge Count" = '>10000 Txns' THEN 5
        ELSE 6
      END
    """
    df = pd.read_sql(query, conn)
    return df

df_size = load_bridge_size_data(start_date, end_date)
df_count = load_bridge_count_data(start_date, end_date)

col1, col2 = st.columns(2)

with col1:
    fig1 = px.pie(df_size, names="Bridge Size", values="Number of Routes", hole=0.5,
                  title="Distribution of Routes By Total Bridges Volume")
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.pie(df_count, names="Bridge Count", values="Number of Routes", hole=0.5,
                  title="Distribution of Routes By Total Bridges Count")
    st.plotly_chart(fig2, use_container_width=True)
