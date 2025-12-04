import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库配置 ---
# ⚠️ 请确认密码正确
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'

DB_NAME_OI = 'open_interest_db'
DB_NAME_SUPPLY = 'circulating_supply'
DATA_LIMIT = 4000 

# --- B. 数据库功能 ---

@st.cache_resource
def get_db_connection_params(db_name):
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。")
        st.stop()
    return {
        'host': DB_HOST,
        'port': DB_PORT,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'db': db_name,
        'charset': DB_CHARSET,
        'autocommit': True,
        'connect_timeout': 10
    }

@contextmanager
def get_connection(db_name):
    params = get_db_connection_params(db_name)
    conn = pymysql.connect(**params)
    try:
        yield conn
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def fetch_circulating_supply():
    """读取流通量数据"""
    try:
        with get_connection(DB_NAME_SUPPLY) as conn:
            sql = f"SELECT symbol, circulating_supply, market_cap FROM `{DB_NAME_SUPPLY}`"
            df = pd.read_sql(sql, conn)
            return df.set_index('symbol').to_dict('index')
    except Exception as e:
        print(f"⚠️ 流通量数据读取失败: {e}")
        return {}

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    """获取 OI 排名"""
    try:
        with get_connection(DB_NAME_OI) as conn:
            sql = f"SELECT symbol FROM `hyperliquid` GROUP BY symbol ORDER BY MAX(oi_usd) DESC;"
            df = pd.read_sql(sql, conn)
            return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 列表获取失败: {e}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    """批量获取行情数据"""
    if not symbol_list: return {}
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    sql_query = f"""
    WITH RankedData AS (
        SELECT symbol, `time`, `price`, `oi`,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `hyperliquid`
        WHERE symbol IN ({placeholders})
    )
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM RankedData
    WHERE rn <= %s
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        with get_connection(DB_NAME_OI) as conn:
            df_all = pd.read_sql(sql_query, conn, params=tuple(symbol_list) + (DATA_LIMIT,))
        
        if df_all.empty: return {}
        return {sym: group for sym, group in df_all.groupby('symbol')}
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 辅助与绘图 ---

def format_number(num):
    if abs(num) >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000: return f"{num / 1_000:.1f}K"
    else: return f"{num:.0f}"

def downsample_data(df, target_points=400):
    if len(df) <= target_points: return df
    step = len(df) // target_points
    df_sampled = df.iloc[::step].copy()
    if df.index[-1] not in df_sampled.index:
        df_sampled = pd.concat([df_sampled, df.iloc[[-1]]])
    return df_sampled

axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
format(datum.value, ',.0f')
"""

def create_dual_axis_chart(df, symbol):
    if df.empty: return None
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'])
    
    df = df.reset_index(drop=True)
    df['index'] = df.index

    tooltip_fields = [
        alt.Tooltip('time', title='时间', format="%m-%d %H:%M"),
        alt.Tooltip('标记价格 (USDC)', title='价格', format='$,.4f'),
        alt.Tooltip('未平仓量', title='OI', format=',.0f') 
    ]
    
    base = alt.Chart(df).encode(alt.X('index', title=None, axis=alt.Axis(labels=False)))
    
    line_price = base.mark_line(color='#d62728', strokeWidth=2).encode(
        alt.Y('标记价格 (USDC)', axis=alt.Axis(title='', titleColor='#d62728', orient='right'), scale=alt.Scale(zero=False))
    )

    line_oi = base.mark_line(color='purple', strokeWidth=2).encode(
        alt.Y('未平仓量', axis=alt.Axis(title='OI', titleColor='purple', orient='right', offset=45, labelExpr=axis_format_logic), scale=alt.Scale(zero=False))
    )
    
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(height=450)

    return chart

# --- 【新增】图表渲染组件 (用于复用) ---
def render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False):
    """封装了单个图表和标题栏的渲染逻辑"""
    raw_df = bulk_data.get(symbol)
    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
    title_color = "black"
    chart = None
    info_html = ""
    
    if raw_df is not None and not raw_df.empty:
        start_p = raw_df['标记价格 (USDC)'].iloc[0]
        end_p = raw_df['标记价格 (USDC)'].iloc[-1]
        title_color = "#009900" if end_p >= start_p else "#D10000"
        
        # 获取强度信息
        item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)
        if item_stats:
            int_val = item_stats['intensity'] * 100
            int_color = "#d62728" if int_val > 5 else ("#009900" if int_val > 1 else "#555")
            inflow = item_stats['oi_change_usd']
            inflow_str = format_number(inflow)
            prefix = "+" if inflow > 0 else ""
            
            info_html = (
                f'<span style="font-size: 16px; margin-left: 15px; color: #666;">'
                f'强度: <span style="color: {int_color}; font-weight: bold;">{int_val:.2f}%</span>'
                f'<span style="margin: 0 8px;">|</span>'
                f'净流入: <span style="color: {"green" if inflow>0 else "red"};">{prefix}${inflow_str}</span>'
                f'</span>'
            )

        chart_df = downsample_data(raw_df, target_points=400)
        chart = create_dual_axis_chart(chart_df, symbol)

    # Top Mover 特殊标识
    fire_icon = "🔥" if is_top_mover else ""

    expander_title_html = (
        f'<div style="text-align: center; margin-bottom: 5px;">'
        f'{fire_icon} '
        f'<a href="{coinglass_url}" target="_blank" '
        f'style="text-decoration:none; color:{title_color}; font-weight:bold; font-size:22px;">'
        f' {symbol} </a>' # 移除排名，Top区域不需要
        f'{info_html}'
        f'</div>'
    )
    
    # Top Mover 默认使用红色边框强调 (Streamlit 原生不支持改边框色，用 emoji 代替)
    label = f"🔥 {symbol} (强度 Top {rank})" if is_top_mover else f"#{rank} {symbol}"

    with st.expander(label, expanded=True):
        st.markdown(expander_title_html, unsafe_allow_html=True)
        if chart:
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("暂无数据")


# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("⚡ OI 强度监控 (OI Growth vs Supply)")
    
    # 1. 准备数据
    with st.spinner("正在读取流通量数据库..."):
        supply_data = fetch_circulating_supply()
        
    with st.spinner("正在加载市场数据..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        if not sorted_symbols: st.stop()
        
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据"); st.stop()

    # --- 计算强度 ---
    ranking_data = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        token_info = supply_data.get(sym)
        
        start_oi = df['未平仓量'].iloc[0]
        end_oi = df['未平仓量'].iloc[-1]
        price = df['标记价格 (USDC)'].iloc[-1]
        oi_change_tokens = end_oi - start_oi
        oi_change_usd = oi_change_tokens * price
        
        intensity = 0
        mc = 0
        if token_info and token_info.get('market_cap') and token_info['market_cap'] > 0:
            mc = token_info['market_cap']
            intensity = oi_change_usd / mc
        elif token_info and token_info.get('circulating_supply') and token_info['circulating_supply'] > 0:
            supply = token_info['circulating_supply']
            intensity = oi_change_tokens / supply
        else:
             if start_oi > 0: intensity = (oi_change_tokens / start_oi) * 0.1

        ranking_data.append({"symbol": sym, "intensity": intensity, "oi_change_usd": oi_change_usd, "market_cap": mc})

    # --- 【顶部展示】 Top 5 强度榜单 (指标卡片) ---
    st.markdown("### 🔥 Top 5 强度榜 (OI增量占市值比)")
    st.caption("反映主力资金相对于代币体量的介入程度。")

    top_movers = []
    if ranking_data:
        top_movers = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:5]
        cols = st.columns(5)
        for i, item in enumerate(top_movers):
            sym = item['symbol']
            intensity_pct = item['intensity'] * 100
            mc_str = format_number(item['market_cap']) if item['market_cap'] > 0 else "N/A"
            cols[i].metric(
                label=f"No.{i+1} {sym}",
                value=f"{intensity_pct:.2f}%",
                delta=f"MC: ${mc_str}", delta_color="off"
            )
    
    st.markdown("---")
    
    # --- 【新增】直接展示 Top 5 的图表 ---
    st.subheader("📈 Top 5 强度币种走势速览")
    if top_movers:
        for i, item in enumerate(top_movers, 1):
            # 调用复用组件
            render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True)
    else:
        st.info("暂无强度数据")

    st.markdown("---")
    st.subheader("📋 全部合约列表")

    # --- 大列表渲染 (剔除 Top 5) ---
    top_mover_symbols = [item['symbol'] for item in top_movers]
    # 过滤掉已经在上面展示过的 symbol
    remaining_symbols = [s for s in target_symbols if s not in top_mover_symbols]

    for rank, symbol in enumerate(remaining_symbols, len(top_movers) + 1):
        # 调用复用组件
        render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False)

if __name__ == '__main__':
    main_app()
