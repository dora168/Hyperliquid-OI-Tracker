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
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'hyperliquid' 
DATA_LIMIT = 4000 

# --- B. 数据库功能 (单次连接极速版) ---

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
    """获取排名列表"""
    try:
        with get_connection() as conn:
            sql = f"SELECT symbol FROM `{TABLE_NAME}` GROUP BY symbol ORDER BY MAX(oi_usd) DESC;"
            df = pd.read_sql(sql, conn)
            return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 列表获取失败: {e}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    """单次查询所有数据 (One-Shot)"""
    if not symbol_list: return {}
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    # 使用窗口函数一次性提取 Top N 数据
    sql_query = f"""
    WITH RankedData AS (
        SELECT symbol, `time`, `price`, `oi`,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `{TABLE_NAME}`
        WHERE symbol IN ({placeholders})
    )
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM RankedData
    WHERE rn <= %s
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        with get_connection() as conn:
            df_all = pd.read_sql(sql_query, conn, params=tuple(symbol_list) + (DATA_LIMIT,))
        
        if df_all.empty: return {}
        # 内存分组
        return {sym: group for sym, group in df_all.groupby('symbol')}
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 降采样逻辑 (核心优化) ---

def downsample_data(df, target_points=150):
    """
    【核心优化函数】
    将 4000 个点的数据压缩到 target_points (默认150个)，
    极大减轻浏览器绘图压力。
    """
    if len(df) <= target_points:
        return df
    
    # 计算步长，例如 4000 / 150 ≈ 26，每 26 个点取 1 个
    step = len(df) // target_points
    
    # 简单的切片采样 (Slicing)
    # 这比聚合计算(mean/max)要快得多，对于展示趋势完全足够
    df_sampled = df.iloc[::step].copy()
    
    # 确保最后一个点（最新数据）被包含进去，否则可能会漏掉最新价格
    if df.index[-1] not in df_sampled.index:
        df_sampled = pd.concat([df_sampled, df.iloc[[-1]]])
        
    return df_sampled

# --- D. 绘图函数 ---

axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
format(datum.value, ',.0f')
"""

def create_dual_axis_chart(df, symbol):
    if df.empty: return None
    
    # 确保时间类型
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'])
    
    # 重置索引用于 X 轴绘制
    df = df.reset_index(drop=True)
    df['index'] = df.index

    # Tooltip 简化
    tooltip_fields = [
        alt.Tooltip('time', title='时间', format="%m-%d %H:%M"),
        alt.Tooltip('标记价格 (USDC)', title='价格', format='$,.4f'),
        alt.Tooltip('未平仓量', title='OI', format=',.0f') 
    ]
    
    # 基础图层
    base = alt.Chart(df).encode(
        alt.X('index', title=None, axis=alt.Axis(labels=False))
    )
    
    # 价格线 (红)
    line_price = base.mark_line(color='#d62728', strokeWidth=2).encode(
        alt.Y('标记价格 (USDC)', 
              axis=alt.Axis(title='', titleColor='#d62728', orient='right'), 
              scale=alt.Scale(zero=False))
    )

    # OI 线 (紫)
    line_oi = base.mark_line(color='purple', strokeWidth=2).encode(
        alt.Y('未平仓量', 
              axis=alt.Axis(title='OI', titleColor='purple', orient='right', offset=45, labelExpr=axis_format_logic),
              scale=alt.Scale(zero=False))
    )
    
    # 组合
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(
        height=300 # 稍微降低高度，一屏能看更多
    )

    return chart

# --- E. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    
    st.title("⚡ Hyperliquid OI 极速监控 (降采样优化版)")
    st.markdown("---") 
    
    # 1. 获取排名
    with st.spinner("正在加载排名..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
    
    if not sorted_symbols: st.stop()

    # --- UI 控制 ---
    col1, col2 = st.columns([1, 3])
    with col1:
        # 默认 100 个
        top_n = st.slider("显示合约数量", min_value=1, max_value=100, value=100, step=10)
    
    target_symbols = sorted_symbols[:top_n]

    # 2. 批量获取数据
    with st.spinner(f"正在获取数据..."):
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据")
        st.stop()
        
    st.success(f"✅ 数据加载完成。当前开启【降采样模式】，只渲染关键点，滚动更流畅。")

    # 3. 循环渲染
    for rank, symbol in enumerate(target_symbols, 1):
        raw_df = bulk_data.get(symbol)
        
        coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
        color = "black"
        
        if raw_df is not None and not raw_df.empty:
            # 计算涨跌色
            start_p = raw_df['标记价格 (USDC)'].iloc[0]
            end_p = raw_df['标记价格 (USDC)'].iloc[-1]
            color = "#009900" if end_p >= start_p else "#D10000"
            
            # 【关键步骤】在这里进行降采样！
            # 原始数据可能有4000条，我们只传150条给绘图引擎
            chart_df = downsample_data(raw_df, target_points=150)
            
            # 画图
            chart = create_dual_axis_chart(chart_df, symbol)
        else:
            chart = None

        expander_title_html = (
            f'<div style="text-align: center; margin-bottom: 5px;">'
            f'<a href="{coinglass_url}" target="_blank" '
            f'style="text-decoration:none; color:{color}; font-weight:bold; font-size:20px;">'
            f'#{rank} {symbol} </a>'
            f'</div>'
        )
        
        # 保持展开
        with st.expander(f"#{rank} {symbol}", expanded=True):
            st.markdown(expander_title_html, unsafe_allow_html=True)
            if chart:
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("暂无数据")

if __name__ == '__main__':
    main_app()
