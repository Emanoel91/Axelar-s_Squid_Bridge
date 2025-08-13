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
st.title("💰By Assets")

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned ---------------------------------------------------------------------------------------------------------
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px; # -- MOVE LEFT
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

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

# --- Row 1 ---------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data(ttl=86400)
def load_data(start_date, end_date):
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

SELECT created_at, id, user, source_chain, destination_chain, CASE 
      WHEN raw_asset='arb-wei' THEN 'ARB'
      WHEN raw_asset='avalanche-uusdc' THEN 'Avalanche USDC'
      WHEN raw_asset='avax-wei' THEN 'AVAX'
      WHEN raw_asset='bnb-wei' THEN 'BNB'
      WHEN raw_asset='busd-wei' THEN 'BUSD'
      WHEN raw_asset='cbeth-wei' THEN 'cbETH'
      WHEN raw_asset='cusd-wei' THEN 'cUSD'
      WHEN raw_asset='dai-wei' THEN 'DAI'
      WHEN raw_asset='dot-planck' THEN 'DOT'
      WHEN raw_asset='eeur' THEN 'EURC'
      WHEN raw_asset='ern-wei' THEN 'ERN'
      WHEN raw_asset='eth-wei' THEN 'ETH'
      WHEN raw_asset ILIKE 'factory/sei10hub%' THEN 'SEILOR'
      WHEN raw_asset='fil-wei' THEN 'FIL'
      WHEN raw_asset='frax-wei' THEN 'FRAX'
      WHEN raw_asset='ftm-wei' THEN 'FTM'
      WHEN raw_asset='glmr-wei' THEN 'GLMR'
      WHEN raw_asset='hzn-wei' THEN 'HZN'
      WHEN raw_asset='link-wei' THEN 'LINK'
      WHEN raw_asset='matic-wei' THEN 'MATIC'
      WHEN raw_asset='mkr-wei' THEN 'MKR'
      WHEN raw_asset='mpx-wei' THEN 'MPX'
      WHEN raw_asset='oath-wei' THEN 'OATH'
      WHEN raw_asset='op-wei' THEN 'OP'
      WHEN raw_asset='orbs-wei' THEN 'ORBS'
      WHEN raw_asset='factory/sei10hud5e5er4aul2l7sp2u9qp2lag5u4xf8mvyx38cnjvqhlgsrcls5qn5ke/seilor' THEN 'SEILOR'
      WHEN raw_asset='pepe-wei' THEN 'PEPE'
      WHEN raw_asset='polygon-uusdc' THEN 'Polygon USDC'
      WHEN raw_asset='reth-wei' THEN 'rETH'
      WHEN raw_asset='ring-wei' THEN 'RING'
      WHEN raw_asset='shib-wei' THEN 'SHIB'
      WHEN raw_asset='sonne-wei' THEN 'SONNE'
      WHEN raw_asset='stuatom' THEN 'stATOM'
      WHEN raw_asset='uatom' THEN 'ATOM'
      WHEN raw_asset='uaxl' THEN 'AXL'
      WHEN raw_asset='ukuji' THEN 'KUJI'
      WHEN raw_asset='ulava' THEN 'LAVA'
      WHEN raw_asset='uluna' THEN 'LUNA'
      WHEN raw_asset='ungm' THEN 'NGM'
      WHEN raw_asset='uni-wei' THEN 'UNI'
      WHEN raw_asset='uosmo' THEN 'OSMO'
      WHEN raw_asset='usomm' THEN 'SOMM'
      WHEN raw_asset='ustrd' THEN 'STRD'
      WHEN raw_asset='utia' THEN 'TIA'
      WHEN raw_asset='uumee' THEN 'UMEE'
      WHEN raw_asset='uusd' THEN 'USTC'
      WHEN raw_asset='uusdc' THEN 'USDC'
      WHEN raw_asset='uusdt' THEN 'USDT'
      WHEN raw_asset='vela-wei' THEN 'VELA'
      WHEN raw_asset='wavax-wei' THEN 'WAVAX'
      WHEN raw_asset='wbnb-wei' THEN 'WBNB'
      WHEN raw_asset='wbtc-satoshi' THEN 'WBTC'
      WHEN raw_asset='weth-wei' THEN 'WETH'
      WHEN raw_asset='wfil-wei' THEN 'WFIL'
      WHEN raw_asset='wftm-wei' THEN 'WFTM'
      WHEN raw_asset='wglmr-wei' THEN 'WGLMR'
      WHEN raw_asset='wmai-wei' THEN 'WMAI'
      WHEN raw_asset='wmatic-wei' THEN 'WMATIC'
      WHEN raw_asset='wsteth-wei' THEN 'wstETH'
      WHEN raw_asset='yield-eth-wei' THEN 'yieldETH' 
      else raw_asset end as "Symbol",
     "Service", amount, amount_usd, fee

FROM axelar_service
    )
    SELECT "Symbol", 
           ROUND(SUM(amount_usd)) AS "Volume (USD)", 
           ROUND(AVG(amount_usd)) AS "Avg Volume per Txn (USD)",
           COUNT(DISTINCT id) AS "Bridges", 
           COUNT(DISTINCT user) AS "Bridgors",
           ROUND((SUM(amount_usd)/COUNT(DISTINCT user)),1) AS "Avg Volume per Bridgor (USD)",
           ROUND(COUNT(DISTINCT id)/COUNT(DISTINCT user)) AS "Avg Bridge Count per User"
    FROM overview
    WHERE created_at BETWEEN '{start_date}' AND '{end_date}' AND "Symbol" is not null
    GROUP BY 1
    ORDER BY 4 DESC
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
df = load_data(start_date, end_date)

# --- Format Numbers ---
df_display = df.copy()
for col in df_display.columns[1:]:
    df_display[col] = df_display[col].apply(lambda x: f"{x:,.0f}")

# --- Reset Index to Start from 1 ---
df_display.index = df_display.index + 1

# --- Show Table ---
st.write("### Squid's Bridged Assets Stats")
st.dataframe(df_display)

# --- Charts ---
col1, col2 = st.columns(2)

# Top 10 by Volume
top_volume = df.nlargest(10, "Volume (USD)")
fig1 = px.bar(
    top_volume,
    x="Symbol",
    y="Volume (USD)",
    text="Volume (USD)",
    color="Symbol"
)
fig1.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
fig1.update_layout(title="Top 10 Tokens by Volume (USD)")

# Top 10 by Bridges
top_bridges = df.nlargest(10, "Bridges")
fig2 = px.bar(
    top_bridges,
    x="Symbol",
    y="Bridges",
    text="Bridges",
    color="Symbol"
)
fig2.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
fig2.update_layout(title="Top 10 Tokens by Bridges")

col1.plotly_chart(fig1, use_container_width=True)
col2.plotly_chart(fig2, use_container_width=True)

# --- Row 3 ----------------------------------------------------------------------------------------------------------
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

SELECT created_at, id, user, source_chain, destination_chain, CASE 
      WHEN raw_asset='arb-wei' THEN 'ARB'
      WHEN raw_asset='avalanche-uusdc' THEN 'Avalanche USDC'
      WHEN raw_asset='avax-wei' THEN 'AVAX'
      WHEN raw_asset='bnb-wei' THEN 'BNB'
      WHEN raw_asset='busd-wei' THEN 'BUSD'
      WHEN raw_asset='cbeth-wei' THEN 'cbETH'
      WHEN raw_asset='cusd-wei' THEN 'cUSD'
      WHEN raw_asset='dai-wei' THEN 'DAI'
      WHEN raw_asset='dot-planck' THEN 'DOT'
      WHEN raw_asset='eeur' THEN 'EURC'
      WHEN raw_asset='ern-wei' THEN 'ERN'
      WHEN raw_asset='eth-wei' THEN 'ETH'
      WHEN raw_asset ILIKE 'factory/sei10hub%' THEN 'SEILOR'
      WHEN raw_asset='fil-wei' THEN 'FIL'
      WHEN raw_asset='frax-wei' THEN 'FRAX'
      WHEN raw_asset='ftm-wei' THEN 'FTM'
      WHEN raw_asset='glmr-wei' THEN 'GLMR'
      WHEN raw_asset='hzn-wei' THEN 'HZN'
      WHEN raw_asset='link-wei' THEN 'LINK'
      WHEN raw_asset='matic-wei' THEN 'MATIC'
      WHEN raw_asset='mkr-wei' THEN 'MKR'
      WHEN raw_asset='mpx-wei' THEN 'MPX'
      WHEN raw_asset='oath-wei' THEN 'OATH'
      WHEN raw_asset='op-wei' THEN 'OP'
      WHEN raw_asset='orbs-wei' THEN 'ORBS'
      WHEN raw_asset='factory/sei10hud5e5er4aul2l7sp2u9qp2lag5u4xf8mvyx38cnjvqhlgsrcls5qn5ke/seilor' THEN 'SEILOR'
      WHEN raw_asset='pepe-wei' THEN 'PEPE'
      WHEN raw_asset='polygon-uusdc' THEN 'Polygon USDC'
      WHEN raw_asset='reth-wei' THEN 'rETH'
      WHEN raw_asset='ring-wei' THEN 'RING'
      WHEN raw_asset='shib-wei' THEN 'SHIB'
      WHEN raw_asset='sonne-wei' THEN 'SONNE'
      WHEN raw_asset='stuatom' THEN 'stATOM'
      WHEN raw_asset='uatom' THEN 'ATOM'
      WHEN raw_asset='uaxl' THEN 'AXL'
      WHEN raw_asset='ukuji' THEN 'KUJI'
      WHEN raw_asset='ulava' THEN 'LAVA'
      WHEN raw_asset='uluna' THEN 'LUNA'
      WHEN raw_asset='ungm' THEN 'NGM'
      WHEN raw_asset='uni-wei' THEN 'UNI'
      WHEN raw_asset='uosmo' THEN 'OSMO'
      WHEN raw_asset='usomm' THEN 'SOMM'
      WHEN raw_asset='ustrd' THEN 'STRD'
      WHEN raw_asset='utia' THEN 'TIA'
      WHEN raw_asset='uumee' THEN 'UMEE'
      WHEN raw_asset='uusd' THEN 'USTC'
      WHEN raw_asset='uusdc' THEN 'USDC'
      WHEN raw_asset='uusdt' THEN 'USDT'
      WHEN raw_asset='vela-wei' THEN 'VELA'
      WHEN raw_asset='wavax-wei' THEN 'WAVAX'
      WHEN raw_asset='wbnb-wei' THEN 'WBNB'
      WHEN raw_asset='wbtc-satoshi' THEN 'WBTC'
      WHEN raw_asset='weth-wei' THEN 'WETH'
      WHEN raw_asset='wfil-wei' THEN 'WFIL'
      WHEN raw_asset='wftm-wei' THEN 'WFTM'
      WHEN raw_asset='wglmr-wei' THEN 'WGLMR'
      WHEN raw_asset='wmai-wei' THEN 'WMAI'
      WHEN raw_asset='wmatic-wei' THEN 'WMATIC'
      WHEN raw_asset='wsteth-wei' THEN 'wstETH'
      WHEN raw_asset='yield-eth-wei' THEN 'yieldETH' 
      else raw_asset end as "Symbol", case 
      when amount_usd<=10 then '<=$10'
      when amount_usd>10 and amount_usd<=100 then '$10-$100'
      when amount_usd>100 and amount_usd<=1000 then '$100-$1K'
      when amount_usd>1000 and amount_usd<=10000 then '$1K-$10K'
      when amount_usd>10000 and amount_usd<=100000 then '$10K-$100K'
      when amount_usd>100000 then '>$100K'
      end as "Bridge Size"
      FROM axelar_service
        WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT "Symbol", "Bridge Size", COUNT(DISTINCT id) AS "Bridges"
    FROM overview
    GROUP BY 1, 2
    ORDER BY 1, 3 DESC
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
df_bridge_size = load_bridge_size_data(start_date, end_date)

# --- Normalize to percentages ---
df_bridge_size['Total'] = df_bridge_size.groupby('Symbol')['Bridges'].transform('sum')
df_bridge_size['Percentage'] = df_bridge_size['Bridges'] / df_bridge_size['Total'] * 100

# --- Ensure Bridge Size order ---
bridge_size_order = ['<=$10', '$10-$100', '$100-$1K', '$1K-$10K', '$10K-$100K', '>$100K']
df_bridge_size['Bridge Size'] = pd.Categorical(df_bridge_size['Bridge Size'], categories=bridge_size_order, ordered=True)

# --- Chart ---
fig = px.bar(
    df_bridge_size,
    y="Symbol",
    x="Percentage",
    color="Bridge Size",
    orientation='h',
    color_discrete_sequence=px.colors.sequential.Blues,  
    category_orders={"Bridge Size": bridge_size_order},
    text="Bridges"
)

fig.update_layout(
    barmode='stack',
    title="Distribution of Squid's Bridged Assets By Volume",
    xaxis_title="Percentage of Bridges",
    yaxis_title="Symbol",
    height=len(df_bridge_size['Symbol'].unique()) * 25 + 400
)

fig.update_traces(texttemplate='%{text}', textposition='inside')

st.plotly_chart(fig, use_container_width=True)

# --- Row 4 --------------------------------------------------------------------------------------------------------------------------------------
# ----------------------------- Bridges By Asset Over Time -----------------------------
@st.cache_data(ttl=86400)
def load_bridges_by_asset(start_date, end_date, timeframe):
    s = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    e = pd.to_datetime(end_date).strftime('%Y-%m-%d')
    tf = timeframe.lower()  # 'day' | 'week' | 'month'

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
        created_at, id, user, source_chain, destination_chain, 
        CASE 
          WHEN raw_asset='arb-wei' THEN 'ARB'
          WHEN raw_asset='avalanche-uusdc' THEN 'Avalanche USDC'
          WHEN raw_asset='avax-wei' THEN 'AVAX'
          WHEN raw_asset='bnb-wei' THEN 'BNB'
          WHEN raw_asset='busd-wei' THEN 'BUSD'
          WHEN raw_asset='cbeth-wei' THEN 'cbETH'
          WHEN raw_asset='cusd-wei' THEN 'cUSD'
          WHEN raw_asset='dai-wei' THEN 'DAI'
          WHEN raw_asset='dot-planck' THEN 'DOT'
          WHEN raw_asset='eeur' THEN 'EURC'
          WHEN raw_asset='ern-wei' THEN 'ERN'
          WHEN raw_asset='eth-wei' THEN 'ETH'
          WHEN raw_asset ILIKE 'factory/sei10hub%' THEN 'SEILOR'
          WHEN raw_asset='fil-wei' THEN 'FIL'
          WHEN raw_asset='frax-wei' THEN 'FRAX'
          WHEN raw_asset='ftm-wei' THEN 'FTM'
          WHEN raw_asset='glmr-wei' THEN 'GLMR'
          WHEN raw_asset='hzn-wei' THEN 'HZN'
          WHEN raw_asset='link-wei' THEN 'LINK'
          WHEN raw_asset='matic-wei' THEN 'MATIC'
          WHEN raw_asset='mkr-wei' THEN 'MKR'
          WHEN raw_asset='mpx-wei' THEN 'MPX'
          WHEN raw_asset='oath-wei' THEN 'OATH'
          WHEN raw_asset='op-wei' THEN 'OP'
          WHEN raw_asset='orbs-wei' THEN 'ORBS'
          WHEN raw_asset='factory/sei10hud5e5er4aul2l7sp2u9qp2lag5u4xf8mvyx38cnjvqhlgsrcls5qn5ke/seilor' THEN 'SEILOR'
          WHEN raw_asset='pepe-wei' THEN 'PEPE'
          WHEN raw_asset='polygon-uusdc' THEN 'Polygon USDC'
          WHEN raw_asset='reth-wei' THEN 'rETH'
          WHEN raw_asset='ring-wei' THEN 'RING'
          WHEN raw_asset='shib-wei' THEN 'SHIB'
          WHEN raw_asset='sonne-wei' THEN 'SONNE'
          WHEN raw_asset='stuatom' THEN 'stATOM'
          WHEN raw_asset='uatom' THEN 'ATOM'
          WHEN raw_asset='uaxl' THEN 'AXL'
          WHEN raw_asset='ukuji' THEN 'KUJI'
          WHEN raw_asset='ulava' THEN 'LAVA'
          WHEN raw_asset='uluna' THEN 'LUNA'
          WHEN raw_asset='ungm' THEN 'NGM'
          WHEN raw_asset='uni-wei' THEN 'UNI'
          WHEN raw_asset='uosmo' THEN 'OSMO'
          WHEN raw_asset='usomm' THEN 'SOMM'
          WHEN raw_asset='ustrd' THEN 'STRD'
          WHEN raw_asset='utia' THEN 'TIA'
          WHEN raw_asset='uumee' THEN 'UMEE'
          WHEN raw_asset='uusd' THEN 'USTC'
          WHEN raw_asset='uusdc' THEN 'USDC'
          WHEN raw_asset='uusdt' THEN 'USDT'
          WHEN raw_asset='vela-wei' THEN 'VELA'
          WHEN raw_asset='wavax-wei' THEN 'WAVAX'
          WHEN raw_asset='wbnb-wei' THEN 'WBNB'
          WHEN raw_asset='wbtc-satoshi' THEN 'WBTC'
          WHEN raw_asset='weth-wei' THEN 'WETH'
          WHEN raw_asset='wfil-wei' THEN 'WFIL'
          WHEN raw_asset='wftm-wei' THEN 'WFTM'
          WHEN raw_asset='wglmr-wei' THEN 'WGLMR'
          WHEN raw_asset='wmai-wei' THEN 'WMAI'
          WHEN raw_asset='wmatic-wei' THEN 'WMATIC'
          WHEN raw_asset='wsteth-wei' THEN 'wstETH'
          WHEN raw_asset='yield-eth-wei' THEN 'yieldETH' 
          ELSE raw_asset 
        END AS "Symbol",
        amount_usd
      FROM axelar_service
      WHERE created_at::date BETWEEN '{s}' AND '{e}'
    )
    SELECT DATE_TRUNC('{tf}', created_at) AS "Date",
           "Symbol",
           COUNT(DISTINCT id) AS "Number of Bridges",
           ROUND(SUM(amount_usd)) AS "Volume of Bridges (USD)"
    FROM overview
    where "Symbol" is not null
    GROUP BY 1, 2
    ORDER BY 1
    """
    return pd.read_sql(query, conn)

# دیتـا را با تابع کش شده بگیر
with st.spinner("Loading Bridges By Asset..."):
    df = load_bridges_by_asset(start_date, end_date, timeframe)

if df.empty:
    st.warning("No data for the selected range.")
else:
    df["Date"] = pd.to_datetime(df["Date"])
df = df.sort_values("Date")
symbols = sorted(df["Symbol"].unique())

# اگر USDC در لیست وجود داشت، اون رو پیشفرض انتخاب کن
default_symbol = "USDC"
default_index = symbols.index(default_symbol) if default_symbol in symbols else 0

selected_symbol = st.selectbox("Select Asset", symbols, index=default_index)
df_filtered = df[df["Symbol"] == selected_symbol]


    # --- Bar + Line Chart ---
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_filtered["Date"],
        y=df_filtered["Volume of Bridges (USD)"],
        name="Volume of Bridges (USD)",
        yaxis="y1",
        marker_color="skyblue"
    ))

    fig.add_trace(go.Scatter(
        x=df_filtered["Date"],
        y=df_filtered["Number of Bridges"],
        name="Number of Bridges",
        yaxis="y2",
        mode="lines+markers",
        marker=dict(color="orange")
    ))

    fig.update_layout(
        title="Bridges By Asset Over Time",
        xaxis=dict(title="Date"),
        yaxis=dict(
            title="Volume of Bridges (USD)",
            side="left"
        ),
        yaxis2=dict(
            title="Number of Bridges",
            overlaying="y",
            side="right"
        ),
        barmode="group",
        legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0)")
    )

    st.plotly_chart(fig, use_container_width=True)
