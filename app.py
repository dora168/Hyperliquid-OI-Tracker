import streamlit as st
import pandas as pd
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库连接配置 (保持不变) ---
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'hyperliquid' 
DATA_LIMIT = 4000 # 获取足够的数据计算涨跌幅，但绘图时我们会抽样

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
    params = get_db_connection_params()
    conn = pymysql.connect(**params)
    try:
        yield conn
    finally:
        conn.close()

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    """获取按 OI 排序的合约列表"""
    try:
        with get_connection() as conn:
            sql_query = f"SELECT symbol FROM `{TABLE_NAME}` GROUP BY symbol ORDER BY MAX(oi_usd) DESC;"
            df = pd.read_sql(sql_query, conn)
            return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 列表获取失败: {e}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    """单次连接获取所有数据"""
    if not symbol_list: return {}
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    # 获取数据
    sql_query = f"""
    WITH RankedData AS (
        SELECT symbol, `time`, `price`, `oi`,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `{TABLE_NAME}`
        WHERE symbol IN ({placeholders})
    )
    SELECT symbol, `time`, `price`, `oi`
    FROM RankedData
    WHERE rn <= %s
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        with get_connection() as conn:
            df_all = pd.read_sql(sql_query, conn, params=tuple(symbol_list) + (DATA_LIMIT,))
        if df_all.empty: return {}
        return {sym: group for sym, group in df_all.groupby('symbol')}
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 数据处理与表格生成 ---

def prepare_dashboard_data(target_symbols, bulk_data):
    """将原始数据转换为适合表格展示的摘要格式"""
    dashboard_rows = []
    
    for symbol in target_symbols:
        df = bulk_data.get(symbol)
        if df is None or df.empty:
            continue
            
        # 1. 获取当前值
        current_price = df['price'].iloc[-1]
        current_oi = df['oi'].iloc[-1]
        
        # 2. 计算 24H 涨跌幅 (假设数据够多，取最早和最晚对比)
        # 这里简单用第一条数据做对比，实际应按时间计算
        start_price = df['price'].iloc[0]
        price_change_pct = ((current_price - start_price) / start_price) 
        
        # 3. 生成迷你图数据 (Downsampling)
        # 浏览器渲染 4000 个点很慢，我们每隔 40 个点取一个，保留 100 个点，形状是一样的
        # 这对性能提升至关重要！
        step = max(1, len(df) // 100) 
        mini_chart_data = df['price'].iloc[::step].tolist()
        
        dashboard_rows.append({
            "合约": symbol,
            "价格 (USDC)": current_price,
            "24H 涨跌幅": price_change_pct,
            "未平仓量 (OI)": current_oi,
            "价格走势 (7D)": mini_chart_data, # 列表数据，Streamlit 会自动画成线
            "链接": f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
        })
        
    return pd.DataFrame(dashboard_rows)

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    
    st.title("⚡ Hyperliquid OI 极速看板")
    st.caption("高性能表格模式 | 包含迷你走势图 | 滚动无卡顿")
    st.markdown("---") 
    
    # 1. 获取排名
    with st.spinner("读取市场数据..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        if not sorted_symbols: st.stop()
        
        # 默认直接取前 100
        target_symbols = sorted_symbols[:100]
        
        # 一次性获取数据
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    # 2. 转换为表格数据
    if bulk_data:
        df_display = prepare_dashboard_data(target_symbols, bulk_data)
        
        # 3. 渲染高性能表格
        st.dataframe(
            df_display,
            column_config={
                "合约": st.column_config.TextColumn("合约", help="点击表头排序", width="small"),
                "价格 (USDC)": st.column_config.NumberColumn("价格", format="$%.4f"),
                "24H 涨跌幅": st.column_config.NumberColumn(
                    "涨跌幅", 
                    format="%.2f%%", 
                    help="基于当前获取数据区间的变化",
                ),
                "未平仓量 (OI)": st.column_config.NumberColumn(
                    "持仓量 (OI)", 
                    format="%.0f",
                    help="未平仓合约数量"
                ),
                "价格走势 (7D)": st.column_config.LineChartColumn(
                    "近期走势",
                    width="medium",
                    y_min=None, y_max=None # 自动缩放
                ),
                "链接": st.column_config.LinkColumn(
                    "详情",
                    display_text="Coinglass"
                )
            },
            use_container_width=True,
            hide_index=True,
            height=800 # 固定高度，允许表格内部滚动
        )
    else:
        st.warning("暂无数据显示")

if __name__ == '__main__':
    main_app()
