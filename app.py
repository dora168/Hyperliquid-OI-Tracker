import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库连接配置 ---
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'hyperliquid' 
DATA_LIMIT = 4000 

# --- B. 数据库核心功能 ---

@st.cache_resource
def get_db_connection_params():
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
        'connect_timeout': 10
    }

@contextmanager
def get_connection():
    """上下文管理器，确保连接自动关闭"""
    params = get_db_connection_params()
    conn = pymysql.connect(**params)
    try:
        yield conn
    finally:
        conn.close()

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    """获取按 OI 排序的合约列表 (只连1次库)"""
    try:
        with get_connection() as conn:
            # 获取所有币种的最新 OI_USD 进行排序
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

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    """
    【核心优化 - V3】
    只使用 1 次数据库连接，查询所有选定币种的数据。
    使用 MySQL 8.0+ 的窗口函数 ROW_NUMBER() 来高效筛选每个币种的前 N 条。
    """
    if not symbol_list:
        return {}

    # 安全地构建 IN 查询的占位符
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    # 构造超级 SQL：
    # 1. 找出这批 symbol 的数据
    # 2. 对每个 symbol 内部按时间倒序打上排名 (rn)
    # 3. 取出 rn <= DATA_LIMIT 的数据
    sql_query = f"""
    WITH RankedData AS (
        SELECT 
            symbol, `time`, `price`, `oi`,
            ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `{TABLE_NAME}`
        WHERE symbol IN ({placeholders})
    )
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM RankedData
    WHERE rn <= %s
    ORDER BY symbol, `time` ASC;
    """
    
    # 参数组合：所有 symbol 列表 + 最后的 limit 参数
    query_params = tuple(symbol_list) + (DATA_LIMIT,)

    try:
        with get_connection() as conn:
            # 执行这一次超级查询
            df_all = pd.read_sql(sql_query, conn, params=query_params)
            
        if df_all.empty:
            return {}

        # 【内存处理】将大表拆分为字典：{'BTC': df_btc, 'ETH': df_eth...}
        # 这一步在 Python 内存中进行，速度极快
        result_dict = {sym: group for sym, group in df_all.groupby('symbol')}
        return result_dict

    except Exception as e:
        # 如果你的数据库版本低于 MySQL 8.0，不支持窗口函数，会报错。
        # 这种情况下，请告知我，我会换成基于时间的过滤方式。
        st.error(f"⚠️ 批量查询失败 (可能是数据库版本不支持窗口函数): {e}")
        return {}

# --- C. 绘图函数 (保持不变) ---

axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
format(datum.value, ',.0f')
"""

def create_dual_axis_chart(df, symbol):
    if df.empty: return None
    
    # 确保没有 SettingWithCopyWarning
    df = df.copy()
    
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'])
    
    df = df.reset_index(drop=True)
    df['index'] = df.index

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
    
    st.title("⚡ Hyperliquid OI 极速监控 (单次连接版)")
    st.markdown("---") 
    
    # 1. 获取排名
    with st.spinner("正在分析市场排名..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
    
    if not sorted_symbols:
        st.stop()

    # --- UI 控制区 ---
    col1, col2 = st.columns([1, 3])
    with col1:
        top_n = st.slider("显示合约数量", min_value=1, max_value=100, value=100, step=10)
    
    target_symbols = sorted_symbols[:top_n]

    # 2. 批量获取数据 (仅 1 次 SQL)
    with st.spinner(f"正在打包下载 Top {top_n} 合约数据 (One-Shot Query)..."):
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    # 3. 渲染界面
    # 提示：渲染 100 张图表是浏览器端的压力瓶颈，与数据库无关了
    if not bulk_data:
         st.warning("未获取到数据，请检查数据库连接或表结构。")
    
    for rank, symbol in enumerate(target_symbols, 1):
        # 从字典中直接取数据，不再查库
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
        
        with st.expander(f"#{rank} {symbol}", expanded=True):
            st.markdown(expander_title_html, unsafe_allow_html=True)
            
            if data_df is not None and not data_df.empty:
                chart = create_dual_axis_chart(data_df, symbol)
                if chart:
                    st.altair_chart(chart, use_container_width=True)
            else:
                st.info("该合约暂无数据")

if __name__ == '__main__':
    main_app()




