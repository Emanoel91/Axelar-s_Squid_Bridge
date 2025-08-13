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
st.title("⛓️Specific Chain")

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
# --- Filters ----------------------------------------------------------------------------------------------------------
timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])
start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))
end_date = st.date_input("End Date", value=pd.to_datetime("2025-07-31"))

chain_options = ["All", "Ethereum", "Agoric", "Arbitrum", "Archway", "Avalanche", "Babylon", "Base", "Binance", "Blast", 
                 "C4e", "Celestia", "Celo", "Chihuahua", "Comdex", "Carbon", "Crescent", "Cosmoshub", "Elys", "Evmos", "Fetch", "Fantom", "Filecoin", "Fraxtal", "Immutable",
                  "Injective", "Juno", "Kava", "Kujira", "Lava", "Linea", "Mantle", "Moonbeam", "Neutron", "Nolus", "Optimism",
                  "Osmosis", "Persistence", "Polygon", "Regen", "Saga", "Scroll", "Sei", "Sommelier", "Stargaze", "Stride", "Teritori",
                  "Terra", "Terra-2", "Umee", "Secret", "Secret-snip", "Xpla", "Xion", "Xrol-evm"]
chain_filter = st.selectbox(
    "Select Source Chain",
    options=chain_options,
    index=chain_options.index("Ethereum")
)
# --- Row (1) ------------------------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data(start_date, end_date, chain):
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

SELECT created_at, id, user, source_chain, destination_chain
     "Service", amount, amount_usd, fee

FROM axelar_service
    )
    SELECT 
        source_chain AS "Source Chain", 
        ROUND(SUM(amount_usd)) AS "Volume (USD)",
        ROUND(AVG(amount_usd), 1) AS "Avg Volume per Bridge (USD)",
        COUNT(DISTINCT id) AS "Bridges",
        COUNT(DISTINCT user) AS "Bridgors"
    FROM overview
    WHERE created_at::date >= '{start_date}'
      AND created_at::date <= '{end_date}'
      {"AND LOWER(source_chain) = LOWER('" + chain + "')" if chain != "All" else ""}
    GROUP BY 1
    ORDER BY 4 DESC
    """
    return pd.read_sql(query, conn)

# --- Load Data ---
df = load_data(start_date, end_date, chain_filter)

# --- KPIs -------
if not df.empty:
    total_volume = df["Volume (USD)"].sum()
    avg_volume = df["Avg Volume per Bridge (USD)"].mean()
    total_bridges = df["Bridges"].sum()
    total_bridgors = df["Bridgors"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Volume (USD)", f"${total_volume:,.0f}")
    col2.metric("Avg Volume per Bridge (USD)", f"${avg_volume:,.1f}")
    col3.metric("Bridges", f"{total_bridges:,} Txns")
    col4.metric("Bridgors", f"{total_bridgors:,} Wallets")
else:
    st.warning("No data available for the selected filters.")

# --- Row (2) --------------------------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data_volume_bridges(start_date, end_date, chain, timeframe):
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
                  sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
                  or sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
                  or sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
                  or sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
                  or sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
              )
            UNION ALL
            SELECT  
                created_at,
                LOWER(data:call.chain::STRING) AS source_chain,
                LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
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
                  data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
                  or data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
                  or data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
                  or data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
                  or data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
              )
        )
        SELECT created_at, id, user, source_chain, destination_chain,
               "Service", amount, amount_usd, fee
        FROM axelar_service
    )
    SELECT 
        DATE_TRUNC('{timeframe}', created_at) AS "DATE",
        source_chain AS "SOURCE CHAIN", 
        ROUND(SUM(amount_usd)) AS "VOLUME (USD)",
        COUNT(DISTINCT id) AS "BRIDGES"
    FROM overview
    WHERE created_at::date >= '{start_date}'
      AND created_at::date <= '{end_date}'
      {"AND LOWER(source_chain) = LOWER('" + chain + "')" if chain != "All" else ""}
    GROUP BY 1, 2
    ORDER BY 1
    """
    return pd.read_sql(query, conn)

# --- Load Data -----
df_vol_bridges = load_data_volume_bridges(start_date, end_date, chain_filter, timeframe)

# --- Chart ---------
# Normalize column names to lowercase
df_vol_bridges.columns = [col.lower() for col in df_vol_bridges.columns]

if df_vol_bridges.empty:
    st.warning("No data found for the selected filters.")
else:
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df_vol_bridges["date"],
            y=df_vol_bridges["volume (usd)"],
            name="Volume (USD)",
            yaxis="y1"
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df_vol_bridges["date"],
            y=df_vol_bridges["bridges"],
            name="Bridges",
            mode="lines+markers",
            yaxis="y2"
        )
    )

    fig.update_layout(
        title="Volume & Bridges Over Time",
        xaxis=dict(title=" "),
        yaxis=dict(title="$USD", side="left"),
        yaxis2=dict(title="Txns count", overlaying="y", side="right"),
        legend=dict(x=0, y=1.1, orientation="h"),
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)
