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

# --- Query execution ----------------------------------------------------------------------------------------------
df = pd.read_sql(query, conn)

# --- Show Table ---------------------------------------------------------------------------------------------------

st.write("### Squid Bridging Routes' Stats")
st.dataframe(df)

# --- Plot Horizontal Bar Charts -----------------------------------------------------------------------------------
top_10_volume = df.nlargest(10, "Volume")
top_10_bridges = df.nlargest(10, "Bridges")

fig_volume = px.bar(
    top_10_volume[::-1],  # معکوس برای نمایش از بالا به پایین
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

