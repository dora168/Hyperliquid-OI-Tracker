import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库配置 ---
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

def create_dual_axis_chart(df, symbol, height=450, sparkline=False):
    """
    绘制图表
    :param height: 图表高度
    :param sparkline: 是否为迷你图模式（迷你图不显示坐标轴文字，更紧凑）
    """
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
    
    # 根据是否是 sparkline 调整坐标轴显示
    if sparkline:
        # 隐藏坐标轴标签和标题，只保留线条
        y_axis_price = alt.Axis(labels=False, title=None, tickCount=0)
        y_axis_oi = alt.Axis(labels=False, title=None, tickCount=0)
    else:
        # 正常显示
        y_axis_price = alt.Axis(title='', titleColor='#d62728', orient='right')
        y_axis_oi = alt.Axis(title='OI', titleColor='purple', orient='right', offset=45, labelExpr=axis_format_logic)

    base = alt.Chart(df).encode(alt.X('index', title=None, axis=alt.Axis(labels=False)))
    
    line_price = base.mark_line(color='#d62728', strokeWidth=2).encode(
        alt.Y('标记价格 (USDC)', axis=y_axis_price, scale=alt.Scale(zero=False))
    )
    line_oi = base.mark_line(color='purple', strokeWidth=2).encode(
        alt.Y('未平仓量', axis=y_axis_oi, scale=alt.Scale(zero=False))
    )
    
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(height=height) # 使用动态高度
    return chart

def render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False, list_type=""):
    raw_df = bulk_data.get(symbol)
    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
    title_color = "black"
    chart = None
    info_html = ""
    
    # 动态设定高度：如果是 Top 榜单，高度设为 100 (约为原来的1/5)，否则 450
    chart_height = 100 if is_top_mover else 450
    is_sparkline = is_top_mover
    
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
                f'<span style="font-size: 13px; margin-left: 5px; color: #666;">'
                f'强度:<span style="color: {int_color}; font-weight: bold;">{int_val:.1f}%</span>'
                f'<span style="margin: 0 4px;">|</span>'
                f'+${growth_str}'
                f'</span>'
            )

        chart_df = downsample_data(raw_df, target_points=400)
        chart = create_dual_axis_chart(chart_df, symbol, height=chart_height, sparkline=is_sparkline)

    fire_icon = "🔥" if list_type == "strength" else ("🐳" if list_type == "whale" else "")
    
    # Top 榜单字体稍小
    font_size = "18px" if is_top_mover else "22px"
    
    expander_title_html = (
        f'<div style="text-align: center; margin-bottom: 2px;">'
        f'{fire_icon} '
        f'<a href="{coinglass_url}" target="_blank" '
        f'style="text-decoration:none; color:{title_color}; font-weight:bold; font-size:{font_size};">'
        f' {symbol} </a>'
        f'{info_html}'
        f'</div>'
    )
    
    label = f"{fire_icon} {symbol}" if is_top_mover else f"#{rank} {symbol}"

    with st.expander(label, expanded=True):
        st.markdown(expander_title_html, unsafe_allow_html=True)
        if chart:
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("暂无数据")

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("⚡ OI 双塔监控")
    
    with st.spinner("读取数据..."):
        supply_data = fetch_circulating_supply()
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        if not sorted_symbols: st.stop()
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data: st.warning("暂无数据"); st.stop()

    # --- 计算统计 ---
    ranking_data = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        token_info = supply_data.get(sym)
        current_price = df['标记价格 (USDC)'].iloc[-1]
        
        min_oi = df['未平仓量'].min()
        current_oi = df['未平仓量'].iloc[-1]
        oi_growth_usd = (current_oi - min_oi) * current_price
        
        intensity = 0
        market_cap = 0
        if token_info and token_info.get('market_cap'):
            market_cap = token_info['market_cap']
            intensity = oi_growth_usd / market_cap
        elif token_info and token_info.get('circulating_supply'):
            intensity = (current_oi - min_oi) / token_info['circulating_supply']
        else:
            if min_oi > 0: intensity = ((current_oi - min_oi) / min_oi) * 0.1

        ranking_data.append({
            "symbol": sym, "intensity": intensity, "oi_growth_usd": oi_growth_usd, "market_cap": market_cap
        })

    # ==========================
    # 1. 指标区 (紧凑版)
    # ==========================
    col_left, col_right = st.columns(2)
    
    top_intensity = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:10] if ranking_data else []
    top_whales = sorted(ranking_data, key=lambda x: x['oi_growth_usd'], reverse=True)[:10] if ranking_data else []

    # --- 左侧：Top 10 强度 (紧凑排列) ---
    with col_left:
        st.subheader("🔥 Top 10 强度 (Relative)")
        # 使用两行，每行 5 个，取代原来的 10 行
        if top_intensity:
            # 第一行 1-5
            cols1 = st.columns(5)
            for i in range(5):
                item = top_intensity[i]
                cols1[i].metric(f"#{i+1} {item['symbol']}", f"{item['intensity']*100:.1f}%", f"${format_number(item['market_cap'])} MC", delta_color="off")
            
            # 第二行 6-10
            cols2 = st.columns(5)
            for i in range(5, 10):
                item = top_intensity[i]
                cols2[i-5].metric(f"#{i+1} {item['symbol']}", f"{item['intensity']*100:.1f}%", f"${format_number(item['market_cap'])} MC", delta_color="off")
    
    # --- 右侧：Top 10 巨鲸 (紧凑排列) ---
    with col_right:
        st.subheader("🐳 Top 10 巨鲸 (Absolute)")
        if top_whales:
            # 第一行 1-5
            cols1 = st.columns(5)
            for i in range(5):
                item = top_whales[i]
                cols1[i].metric(f"#{i+1} {item['symbol']}", f"+${format_number(item['oi_growth_usd'])}", "Inflow")
            
            # 第二行 6-10
            cols2 = st.columns(5)
            for i in range(5, 10):
                item = top_whales[i]
                cols2[i-5].metric(f"#{i+1} {item['symbol']}", f"+${format_number(item['oi_growth_usd'])}", "Inflow")
    
    st.markdown("---")
    
    # ==========================
    # 2. 图表区 (迷你版)
    # ==========================
    chart_col_left, chart_col_right = st.columns(2)
    
    with chart_col_left:
        st.caption("📈 强度 Top 10 走势 (1/5高度迷你图)")
        if top_intensity:
            for i, item in enumerate(top_intensity, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True, list_type="strength")

    with chart_col_right:
        st.caption("📈 巨鲸 Top 10 走势 (1/5高度迷你图)")
        if top_whales:
            for i, item in enumerate(top_whales, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True, list_type="whale")
    
    st.markdown("---")
    st.subheader("📋 其他合约列表 (完整版)")

    # --- 底部：剩余列表 ---
    shown_symbols = set([i['symbol'] for i in top_intensity] + [i['symbol'] for i in top_whales])
    remaining_symbols = [s for s in target_symbols if s not in shown_symbols]

    for rank, symbol in enumerate(remaining_symbols, 1):
        render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False)

if __name__ == '__main__':
    main_app()



