import streamlit as st
import pandas as pd
import altair as alt
import os
import connectorx as cx  # 🚀 Rust 编写的高性能数据加载库
from urllib.parse import quote_plus # 用于处理密码中的特殊字符

# --- A. 数据库配置 ----

DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
# 注意：ConnectorX 自动处理 UTF8，不需要显式配置 DB_CHARSET

DB_NAME_OI = 'open_interest_db'
DB_NAME_SUPPLY = 'circulating_supply'

# 策略：即使我们只取 ~400 个点，我们仍查看过去 4000 个周期的数据范围，但在 SQL 中进行过滤
DATA_LIMIT_RAW = 4000 
SAMPLE_STEP = 4  # 每 10 行取 1 行

# --- B. 数据库功能 (Rust 加速版) ---

@st.cache_resource
def get_db_uri(db_name):
    """构建 connectorx 需要的连接字符串 (mysql://...)"""
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。")
        st.stop()
    
    # 1. URL 编码密码，防止 @ 或 / 等符号破坏连接串
    safe_pwd = quote_plus(DB_PASSWORD)
    
    # 2. 移除 charset 参数，修复 'Unknown URL parameter' 错误
    return f"mysql://{DB_USER}:{safe_pwd}@{DB_HOST}:{DB_PORT}/{db_name}"

@st.cache_data(ttl=300)
def fetch_circulating_supply():
    try:
        uri = get_db_uri(DB_NAME_SUPPLY)
        query = f"SELECT symbol, circulating_supply, market_cap FROM `{DB_NAME_SUPPLY}`"
        # 使用 Rust 引擎读取
        df = cx.read_sql(uri, query)
        return df.set_index('symbol').to_dict('index')
    except Exception as e:
        print(f"⚠️ 流通量数据读取失败: {e}")
        return {}

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    try:
        uri = get_db_uri(DB_NAME_OI)
        # 仅获取列表，极快
        query = "SELECT symbol FROM `hyperliquid` GROUP BY symbol ORDER BY MAX(oi_usd) DESC"
        df = cx.read_sql(uri, query)
        return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 列表获取失败: {e}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    if not symbol_list: return {}
    
    symbols_str = "', '".join(symbol_list)
    
    # 🌟 SQL 优化核心：只回传 rn=1 (最新) 以及 rn % 10 == 0 (每隔10条) 的数据
    # 这将减少 90% 的网络传输量，极大提升速度
    sql_query = f"""
    WITH RankedData AS (
        SELECT symbol, `time`, `price`, `oi`,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `hyperliquid`
        WHERE symbol IN ('{symbols_str}')
    )
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM RankedData
    WHERE rn <= {DATA_LIMIT_RAW} 
    AND (rn = 1 OR rn % {SAMPLE_STEP} = 0)
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        uri = get_db_uri(DB_NAME_OI)
        # Rust 零拷贝读取
        df_all = cx.read_sql(uri, sql_query)
        
        if df_all.empty: return {}
        
        # 确保时间格式正确
        if not pd.api.types.is_datetime64_any_dtype(df_all['time']):
            df_all['time'] = pd.to_datetime(df_all['time'])

        # 确保数值格式正确 (防止数据库返回 Decimal 类型导致 Altair 报错)
        df_all['标记价格 (USDC)'] = df_all['标记价格 (USDC)'].astype(float)
        df_all['未平仓量'] = df_all['未平仓量'].astype(float)

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
    # 因为我们在 SQL 里已经做了降采样，这里主要做一个保险
    if len(df) <= target_points * 1.5: 
        return df
    # 简单的步长切片
    step = len(df) // target_points
    return df.iloc[::step]

axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
format(datum.value, ',.0f')
"""

def create_dual_axis_chart(df, symbol):
    if df.empty: return None
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

def render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False, list_type=""):
    raw_df = bulk_data.get(symbol)
    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
    title_color = "black"
    chart = None
    info_html = ""
    
    if raw_df is not None and not raw_df.empty:
        start_p = raw_df['标记价格 (USDC)'].iloc[0]
        end_p = raw_df['标记价格 (USDC)'].iloc[-1]
        title_color = "#009900" if end_p >= start_p else "#D10000"
        
        item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)
        if item_stats:
            int_val = item_stats['intensity'] * 100
            int_color = "#d62728" if int_val > 5 else ("#009900" if int_val > 1 else "#555")
            growth_usd = item_stats['oi_growth_usd']
            growth_str = format_number(growth_usd)
            
            info_html = (
                f'<span style="font-size: 14px; margin-left: 10px; color: #666;">'
                f'强度:<span style="color: {int_color}; font-weight: bold;">{int_val:.1f}%</span>'
                f'<span style="margin: 0 4px;">|</span>'
                f'增量:<span style="color: #009900; font-weight: bold;">+${growth_str}</span>'
                f'</span>'
            )

        chart_df = downsample_data(raw_df, target_points=400)
        chart = create_dual_axis_chart(chart_df, symbol)

    fire_icon = "🔥" if list_type == "strength" else ("🐳" if list_type == "whale" else "")
    expander_title_html = (
        f'<div style="text-align: center; margin-bottom: 5px;">'
        f'{fire_icon} '
        f'<a href="{coinglass_url}" target="_blank" '
        f'style="text-decoration:none; color:{title_color}; font-weight:bold; font-size:20px;">'
        f' {symbol} </a>'
        f'{info_html}'
        f'</div>'
    )
    
    if is_top_mover:
        label = f"{fire_icon} {symbol}"
    else:
        label = f"#{rank} {symbol}"

    with st.expander(label, expanded=True):
        st.markdown(expander_title_html, unsafe_allow_html=True)
        if chart:
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("暂无数据")

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("⚡ OI 双塔监控 (强度 vs 巨鲸) - Rust Accelerated")
    
    with st.spinner("正在读取流通量数据库..."):
        supply_data = fetch_circulating_supply()
        
    with st.spinner("正在加载市场数据..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        if not sorted_symbols: st.stop()
        # 即使只取100个，现在的速度也应该很快
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据"); st.stop()

    # --- 计算统计数据 ---
    ranking_data = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        
        token_info = supply_data.get(sym)
        current_price = df['标记价格 (USDC)'].iloc[-1]
        
        min_oi = df['未平仓量'].min()
        current_oi = df['未平仓量'].iloc[-1]
        oi_growth_tokens = current_oi - min_oi
        oi_growth_usd = oi_growth_tokens * current_price
        
        intensity = 0
        market_cap = 0
        if token_info and token_info.get('market_cap') and token_info['market_cap'] > 0:
            market_cap = token_info['market_cap']
            intensity = oi_growth_usd / market_cap
        elif token_info and token_info.get('circulating_supply') and token_info['circulating_supply'] > 0:
            supply = token_info['circulating_supply']
            intensity = oi_growth_tokens / supply
        else:
            if min_oi > 0: intensity = (oi_growth_tokens / min_oi) * 0.1

        ranking_data.append({
            "symbol": sym,
            "intensity": intensity, 
            "oi_growth_usd": oi_growth_usd,
            "market_cap": market_cap
        })

    # ==========================
    # 榜单指标区 (Metric Lists)
    # ==========================
    col_left, col_right = st.columns(2)
    
    top_intensity = []
    top_whales = []
    if ranking_data:
        top_intensity = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:10]
        top_whales = sorted(ranking_data, key=lambda x: x['oi_growth_usd'], reverse=True)[:10]

    # --- 左侧指标 ---
    with col_left:
        st.subheader("🔥 Top 10 强度榜 (相对比例)")
        st.caption("逻辑：(当前OI - 最低OI) / 市值。")
        st.markdown("---")
        for i, item in enumerate(top_intensity):
            st.metric(
                label=f"No.{i+1} {item['symbol']}",
                value=f"{item['intensity']*100:.2f}%",
                delta=f"MC: ${format_number(item['market_cap'])}",
                delta_color="off"
            )
            st.markdown("""<hr style="margin: 5px 0; border-top: 1px dashed #eee;">""", unsafe_allow_html=True)
    
    # --- 右侧指标 ---
    with col_right:
        st.subheader("🐳 Top 10 巨鲸榜 (绝对金额)")
        st.caption("逻辑：(当前OI - 最低OI) * 价格。")
        st.markdown("---")
        for i, item in enumerate(top_whales):
            st.metric(
                label=f"No.{i+1} {item['symbol']}",
                value=f"+${format_number(item['oi_growth_usd'])}",
                delta="资金净流入",
                delta_color="normal"
            )
            st.markdown("""<hr style="margin: 5px 0; border-top: 1px dashed #eee;">""", unsafe_allow_html=True)
    
    st.markdown("---")
    
    # ==========================
    # 双塔图表区
    # ==========================
    chart_col_left, chart_col_right = st.columns(2)
    
    with chart_col_left:
        st.subheader("📈 强度 Top 10 走势")
        if top_intensity:
            for i, item in enumerate(top_intensity, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True, list_type="strength")
        else:
            st.info("暂无数据")

    with chart_col_right:
        st.subheader("📈 巨鲸 Top 10 走势")
        if top_whales:
            for i, item in enumerate(top_whales, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True, list_type="whale")
        else:
            st.info("暂无数据")
    
    st.markdown("---")
    st.subheader("📋 其他合约列表 (已去重)")

    # --- 底部：剩余列表 ---
    shown_symbols = set()
    for item in top_intensity: shown_symbols.add(item['symbol'])
    for item in top_whales: shown_symbols.add(item['symbol'])
    
    remaining_symbols = [s for s in target_symbols if s not in shown_symbols]

    for rank, symbol in enumerate(remaining_symbols, 1):
        render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False)

if __name__ == '__main__':
    main_app()




