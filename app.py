import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库配置 ----
# 请确保您已在 Streamlit Secrets 或环境变量中配置了这些信息
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD")
DB_CHARSET = 'utf8mb4'

DB_NAME_OI = 'open_interest_db'
DB_NAME_SUPPLY = 'circulating_supply'

# --- B. 数据库功能 ---

@st.cache_resource
def get_db_connection_params(db_name):
    if not DB_PASSWORD or not DB_HOST or not DB_USER:
        st.error("❌ 数据库连接信息未配置，请检查 Secrets 或环境变量。")
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
    try:
        conn = pymysql.connect(**params)
        yield conn
    except pymysql.MySQLError as e:
        st.error(f"❌ 数据库连接失败: {e}")
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

@st.cache_data(ttl=300)
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
def fetch_bulk_data_one_shot(symbol_list, interval_sql="24 HOUR"):
    if not symbol_list: return {}
    
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    sql_query = f"""
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM `hyperliquid`
    WHERE symbol IN ({placeholders})
      AND `time` >= NOW() - INTERVAL {interval_sql}
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        with get_connection(DB_NAME_OI) as conn:
            df_all = pd.read_sql(sql_query, conn, params=tuple(symbol_list))
        
        if df_all.empty: return {}

        result = {}
        grouped = df_all.groupby('symbol')
        for sym, group in grouped:
            # 简单降采样，保留首尾，中间均匀取点
            if len(group) > 150:
                step = len(group) // 150
                sampled = group.iloc[::step].copy()
                if group.index[-1] not in sampled.index:
                    sampled = pd.concat([sampled, group.iloc[[-1]]])
                result[sym] = sampled
            else:
                result[sym] = group
            
        return result
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 辅助逻辑 ---

def format_number(num):
    if num is None: return "$0"
    if abs(num) >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000: return f"{num / 1_000:.1f}K"
    else: return f"{num:.0f}"

def get_signal_info(price_chg, oi_chg):
    """根据价格和OI涨跌返回信号标签文本、背景色和文字色"""
    # 使用极小的阈值来判断涨跌，避免浮点数精度问题
    epsilon = 1e-9
    if price_chg > epsilon and oi_chg > epsilon:
        return "🟢多头增仓", "#e6fffa", "#009900" # 强势看多
    elif price_chg > epsilon and oi_chg < -epsilon:
        return "🟡空头平仓", "#fffbe6", "#d48806" # 反弹/诱多
    elif price_chg < -epsilon and oi_chg > epsilon:
        return "🔴空头增仓", "#fff1f0", "#cf1322" # 强势看空
    elif price_chg < -epsilon and oi_chg < -epsilon:
        return "🟠多头平仓", "#fff7e6", "#d46b08" # 回调/踩踏
    else:
        return "⚪震荡/无信号", "#f5f5f5", "#8c8c8c"

def create_mini_chart(df):
    """创建极简迷你图 (Sparkline) - 高度 35px"""
    if df.empty: return None
    
    # 确保时间列是 datetime 类型
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df = df.copy()
        df['time'] = pd.to_datetime(df['time'])
    
    df = df.reset_index(drop=True)
    df['index'] = df.index
    
    tooltip_fields = [
        alt.Tooltip('time', title='时间', format="%H:%M"),
        alt.Tooltip('标记价格 (USDC)', title='价格', format='$.4f'),
        alt.Tooltip('未平仓量', title='OI', format=',.0f') 
    ]
    
    base = alt.Chart(df).encode(alt.X('index', axis=None))
    
    line_price = base.mark_line(color='#d62728', strokeWidth=1.5).encode(
        alt.Y('标记价格 (USDC)', axis=None, scale=alt.Scale(zero=False))
    )
    line_oi = base.mark_line(color='purple', strokeWidth=1.5).encode(
        alt.Y('未平仓量', axis=None, scale=alt.Scale(zero=False))
    )
    
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(height=35, width='container').configure_view(strokeWidth=0)
    return chart

def render_chart_component(rank, symbol, bulk_data, ranking_data, list_type=""):
    """渲染单个列表项 - 修复 HTML 渲染问题"""
    raw_df = bulk_data.get(symbol)
    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
    
    chart = None
    main_value_str = "0%"
    sub_tag_str = "MC: $0"
    signal_span = "" # 初始化为空字符串
    
    if raw_df is not None and not raw_df.empty:
        item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)
        
        if item_stats:
            # 获取信号数据
            sig_text, sig_bg, sig_color = item_stats['signal']
            # 构建信号标签的 HTML 字符串，注意边框颜色使用文字颜色
            if sig_text and "无信号" not in sig_text:
                signal_span = f'<span style="background-color: {sig_bg}; color: {sig_color}; padding: 1px 4px; border-radius: 3px; font-weight: 600; font-size: 10px; margin-right: 4px; border: 1px solid {sig_color};">{sig_text}</span>'

            # 根据榜单类型设置显示内容
            if list_type == "strength":
                val = item_stats['intensity'] * 100
                main_value_str = f"{val:.2f}%"
                mc = format_number(item_stats['market_cap'])
                sub_tag_str = f"MC: ${mc}"
            elif list_type == "whale":
                val = item_stats['oi_growth_usd']
                main_value_str = f"+${format_number(val)}"
                sub_tag_str = "资金净流入"
            else:
                val = item_stats['price_change_pct'] * 100
                main_value_str = f"{val:+.2f}%"
                sub_tag_str = f"MC: ${format_number(item_stats['market_cap'])}"

        # 创建图表
        chart = create_mini_chart(raw_df)

    # 构建最终的 HTML，将 signal_span 嵌入
    html_content = f"""
    <a href="{coinglass_url}" target="_blank" style="text-decoration:none; display: block; color: inherit;">
        <div style="font-family: -apple-system, BlinkMacSystemFont, sans-serif;">
            <div style="font-size: 11px; color: #888; margin-bottom: -2px;">
                No.{rank} <span style="color: #333; font-weight: 500;">{symbol}</span>
            </div>
            <div style="font-size: 22px; font-weight: 600; color: #333; letter-spacing: -0.5px; line-height: 1.2;">
                {main_value_str}
            </div>
            <div style="margin-top: 2px; display: flex; align-items: center;">
                {signal_span}
                <div style="background-color: #f0f2f6; padding: 1px 6px; border-radius: 3px; font-size: 10px; color: #666;">
                    {sub_tag_str}
                </div>
            </div>
        </div>
    </a>
    """

    st.markdown(html_content, unsafe_allow_html=True)
    if chart:
        st.altair_chart(chart, use_container_width=True)
    st.markdown("""<hr style="margin: 4px 0; border: 0; border-top: 1px solid #f0f0f0;">""", unsafe_allow_html=True)

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="HL OI Dashboard")
    
    # CSS 样式调整
    st.markdown("""
        <style>
        .block-container { padding-top: 1rem; padding-bottom: 2rem; }
        .element-container { margin-bottom: 0px !important; }
        .stMarkdown { margin-bottom: -5px !important; }
        div[data-testid="stAltairChart"] { height: 35px !important; min-height: 35px !important; }
        canvas { height: 35px !important; }
        h5 { padding-top: 0px; margin-bottom: 10px; }
        /* 修复侧边栏标题和信息框的间距 */
        [data-testid="stSidebar"] h1 { margin-bottom: 0.5rem; }
        [data-testid="stSidebar"] .stAlert { margin-top: 1rem; }
        </style>
    """, unsafe_allow_html=True)

    # 侧边栏
    with st.sidebar:
        st.title("⚙️ 监控设置")
        time_period = st.selectbox(
            "时间周期 (Timeframe)", 
            ["1H (突发)", "4H (趋势)", "24H (日线)", "7D (周线)"], 
            index=2
        )
        sql_mapping = {
            "1H (突发)": "1 HOUR",
            "4H (趋势)": "4 HOUR",
            "24H (日线)": "24 HOUR",
            "7D (周线)": "7 DAY"
        }
        interval_sql = sql_mapping[time_period]
        st.info(f"正在显示过去 {time_period} 的 OI 变化")

    st.title(f"⚡ OI 极简看板 ({time_period})")
    
    with st.spinner(f"🚀 正在分析过去 {time_period} 的市场数据..."):
        supply_data = fetch_circulating_supply()
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        
        if not sorted_symbols:
            st.error("❌ 无法获取币种列表，请检查数据库连接。")
            st.stop()
        
        # 取前 100 个活跃币种进行分析
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols, interval_sql=interval_sql)

    if not bulk_data:
        st.warning(f"⚠️ 过去 {time_period} 内无数据更新，请尝试切换其他周期。"); st.stop()

    # --- 计算统计数据 ---
    ranking_data = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        
        token_info = supply_data.get(sym, {})
        
        # 价格数据
        start_p = df['标记价格 (USDC)'].iloc[0]
        current_p = df['标记价格 (USDC)'].iloc[-1]
        # 避免价格为0的除零错误
        price_change_pct = (current_p - start_p) / start_p if start_p > 0 else 0
        
        # OI 数据
        min_oi = df['未平仓量'].min()
        start_oi = df['未平仓量'].iloc[0]
        current_oi = df['未平仓量'].iloc[-1]
        
        # 强度榜逻辑 (基于区间内最大增长量)
        oi_growth_tokens = current_oi - min_oi 
        oi_growth_usd = oi_growth_tokens * current_p
        
        # 信号逻辑 (基于区间首尾变化率)
        oi_change_pct = (current_oi - start_oi) / start_oi if start_oi > 0 else 0
        signal_tuple = get_signal_info(price_change_pct, oi_change_pct)
        
        # 计算强度和市值
        intensity = 0
        market_cap = token_info.get('market_cap', 0)
        supply = token_info.get('circulating_supply', 0)

        if market_cap > 0:
            intensity = oi_growth_usd / market_cap
        elif supply > 0:
            intensity = oi_growth_tokens / supply
        else:
            # 如果没有市值和流通量信息，用一个备用算法
            if min_oi > 0: intensity = (oi_growth_tokens / min_oi) * 0.1

        ranking_data.append({
            "symbol": sym,
            "intensity": intensity, 
            "oi_growth_usd": oi_growth_usd,
            "market_cap": market_cap,
            "price_change_pct": price_change_pct,
            "signal": signal_tuple
        })

    # 排序
    top_intensity = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:10]
    top_whales = sorted(ranking_data, key=lambda x: x['oi_growth_usd'], reverse=True)[:10]
    
    # 列表展示
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown(f"##### 🔥 强度榜 ({time_period})") 
        st.markdown("---")
        if top_intensity:
            for i, item in enumerate(top_intensity, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, list_type="strength")
        else:
            st.info("暂无数据")

    with col_right:
        st.markdown(f"##### 🐳 巨鲸榜 ({time_period})")
        st.markdown("---")
        if top_whales:
            for i, item in enumerate(top_whales, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, list_type="whale")
        else:
            st.info("暂无数据")
    
    st.markdown("##### 📋 其他异动")
    shown_symbols = set([x['symbol'] for x in top_intensity] + [x['symbol'] for x in top_whales])
    remaining = [s for s in target_symbols if s not in shown_symbols]
    
    if remaining:
        cols = st.columns(4)
        for idx, symbol in enumerate(remaining):
            with cols[idx % 4]:
                render_chart_component(idx+1, symbol, bulk_data, ranking_data, list_type="normal")

if __name__ == '__main__':
    main_app()
