import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- A. 数据库连接配置 ---
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'hyperliquid' 
DATA_LIMIT = 4000 

# --- B. 核心数据功能 ---

@st.cache_resource
def get_db_connection_params():
    """返回数据库连接参数"""
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。")
        st.stop()
    return {
        'host': DB_HOST,
        'port': DB_PORT,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'db': NEW_DB_NAME,
        'charset': DB_CHARSET,
        'autocommit': True,
        'connect_timeout': 5 
    }

def get_connection():
    """获取单个数据库连接（非缓存，用于多线程）"""
    params = get_db_connection_params()
    try:
        return pymysql.connect(**params)
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    """获取按 OI 排序的合约列表"""
    params = get_db_connection_params()
    conn = None
    try:
        conn = pymysql.connect(**params)
        # 使用 MAX(oi_usd) 近似排序，速度最快
        sql_query = f"""
        SELECT symbol 
        FROM `{TABLE_NAME}`
        GROUP BY symbol
        ORDER BY MAX(oi_usd) DESC;
        """
        df = pd.read_sql(sql_query, conn)
        return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 获取合约列表失败: {e}")
        return []
    finally:
        if conn: conn.close()

def fetch_single_symbol_data(symbol):
    """单个合约的数据抓取函数（供线程池调用）"""
    conn = get_connection()
    if not conn:
        return symbol, pd.DataFrame()
    
    try:
        sql_query = f"""
        SELECT `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
        FROM `{TABLE_NAME}`
        WHERE `symbol` = %s
        ORDER BY `time` DESC
        LIMIT %s
        """
        df = pd.read_sql(sql_query, conn, params=(symbol, DATA_LIMIT))
        df = df.sort_values('time', ascending=True)
        return symbol, df
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return symbol, pd.DataFrame()
    finally:
        conn.close()

@st.cache_data(ttl=60, show_spinner=False)
def fetch_batch_data_concurrently(symbol_list):
    """
    【核心优化】多线程并发抓取数据。
    """
    results = {}
    # 增加线程数以加快 100 个币种的下载速度，但不要过大以免爆数据库连接
    max_workers = min(len(symbol_list), 20) 
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_single_symbol_data, sym): sym for sym in symbol_list}
        
        for future in as_completed(future_to_symbol):
            sym, df = future.result()
            if not df.empty:
                results[sym] = df
                
    return results

# --- C. 绘图函数 ---

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

    # 简化 tooltip
    tooltip_fields = [
        alt.Tooltip('time', title='时间', format="%m-%d %H:%M"),
        alt.Tooltip('标记价格 (USDC)', title='价格', format='$,.4f'),
        alt.Tooltip('未平仓量', title='OI', format=',.0f') 
    ]
    
    base = alt.Chart(df).encode(
        alt.X('index', title=None, axis=alt.Axis(labels=False))
    )
    
    line_price = base.mark_line(color='#d62728', strokeWidth=2).encode(
        alt.Y('标记价格 (USDC)', axis=alt.Axis(title='', titleColor='#d62728', orient='right'), scale=alt.Scale(zero=False))
    )

    line_oi = base.mark_line(color='purple', strokeWidth=2).encode(
        alt.Y('未平仓量', 
              axis=alt.Axis(title='OI', titleColor='purple', orient='right', offset=45, labelExpr=axis_format_logic),
              scale=alt.Scale(zero=False)
        )
    )
    
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(height=350)

    return chart

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    
    st.title("⚡ Hyperliquid OI 极速监控 (Top 100)")
    st.markdown("---") 
    
    # 1. 获取排名 (缓存)
    with st.spinner("正在加载市场排名..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
    
    if not sorted_symbols:
        st.stop()

    # --- UI 控制区 ---
    col1, col2 = st.columns([1, 3])
    with col1:
        # 【修改点 1】默认值设为 100，满足您的需求
        top_n = st.slider("显示合约数量 (按 OI 排名)", min_value=1, max_value=100, value=100, step=10)
    
    target_symbols = sorted_symbols[:top_n]

    # 2. 并发获取数据 (缓存)
    with st.spinner(f"正在并发获取 Top {top_n} 合约数据（数据量较大，请稍候）..."):
        bulk_data = fetch_batch_data_concurrently(target_symbols)

    # 3. 渲染界面
    # 注意：渲染 100 个 Altair 图表对浏览器压力较大
    
    for rank, symbol in enumerate(target_symbols, 1):
        data_df = bulk_data.get(symbol)
        
        coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
        color = "black"
        if data_df is not None and not data_df.empty:
            price_change = data_df['标记价格 (USDC)'].iloc[-1] - data_df['标记价格 (USDC)'].iloc[0]
            color = "#009900" if price_change >= 0 else "#D10000"

        expander_title_html = (
            f'<div style="text-align: center; margin-bottom: 5px;">'
            f'<a href="{coinglass_url}" target="_blank" '
            f'style="text-decoration:none; color:{color}; font-weight:bold; font-size:22px;">'
            f'#{rank} {symbol} </a>'
            f'</div>'
        )
        
        # 【修改点 2】强制展开前 100 个 (或者全部)
        with st.expander(f"#{rank} {symbol}", expanded=True):
            st.markdown(expander_title_html, unsafe_allow_html=True)
            
            if data_df is not None and not data_df.empty:
                chart = create_dual_axis_chart(data_df, symbol)
                if chart:
                    st.altair_chart(chart, use_container_width=True)
            else:
                st.warning("暂无数据")

if __name__ == '__main__':
    main_app()



