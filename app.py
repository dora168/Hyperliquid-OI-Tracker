import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
import time

# --- A. 数据库连接配置 (用于 Streamlit Cloud 部署) ---
# 确保在 Streamlit Cloud 的 Secrets 中设置了 DB_HOST, DB_PORT, DB_USER, DB_PASSWORD
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 

DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'Hyperliquid'
DATA_LIMIT = 4000 # 读取每个合约历史记录的行数限制

# --- B. 数据读取和排序函数 ---

# 1. 缓存数据库连接资源
@st.cache_resource(ttl=3600) 
def get_db_connection():
    """建立并缓存数据库连接，如果连接失败则在页面上显示错误并停止应用"""
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。请检查 Streamlit Secrets 或本地 secrets.toml 文件。")
        st.stop()
        return None
    try:
        return pymysql.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
            db=NEW_DB_NAME, charset=DB_CHARSET
        )
    except Exception as e:
        st.error(f"❌ 数据库连接失败: {e}。请检查您的网络和腾讯云配置。")
        st.stop()
        return None

# 2. 获取所有合约及其最新 OI 的函数，用于排名
@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi():
    """
    获取所有合约的最新 OI 值，并返回一个按 OI/OI_USD 降序排列的合约列表。
    *** 如果您的数据库中存在 oi_usd 字段，请将 SQL 语句中的 t1.oi 改为 t1.oi_usd ***
    """
    conn = get_db_connection()
    if conn is None: return []

    try:
        # SQL 查询：获取每个合约的最新记录的 oi 值（作为排名依据），并按降序排列
        sql_query = f"""
        SELECT 
            t1.symbol, 
            t1.oi  
        FROM `{TABLE_NAME}` t1
        INNER JOIN (
            SELECT symbol, MAX(time) as max_time
            FROM `{TABLE_NAME}`
            GROUP BY symbol
        ) t2 
        ON t1.symbol = t2.symbol AND t1.time = t2.max_time
        ORDER BY t1.oi DESC;
        """
        
        df_oi_rank = pd.read_sql(sql_query, conn)
        
        if df_oi_rank.empty:
            st.error("数据库中没有找到任何合约的最新数据。")
            return []

        # 返回按 oi 降序排列的 symbol 列表
        return df_oi_rank['symbol'].tolist()
        
    except Exception as e:
        st.error(f"❌ 无法获取和排序合约列表: {e}")
        return []

# 3. 读取指定合约数据 (用于绘图)
@st.cache_data(ttl=60)
def fetch_data_for_symbol(symbol, limit=DATA_LIMIT):
    """从数据库中读取指定 symbol 的最新数据"""
    conn = get_db_connection()
    if conn is None: return pd.DataFrame()

    try:
        # 注意：这里读取的仍然是 oi，如果数据库中有 oi_usd，建议修改为：
        # SELECT `time`, `price` AS `标记价格 (USDC)`, `oi_usd` AS `未平仓量`
        sql_query = f"""
        SELECT `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
        FROM `{TABLE_NAME}`
        WHERE `symbol` = %s
        ORDER BY `time` DESC
        LIMIT %s
        """
        df = pd.read_sql(sql_query, conn, params=(symbol, limit))
        df = df.sort_values('time', ascending=True)
        return df

    except Exception as e:
        st.warning(f"⚠️ 查询 {symbol} 数据失败: {e}")
        return pd.DataFrame()


# --- C. 核心绘图函数 ---

# Y 轴自定义格式逻辑 (Vega Expression)，用于 OI (未平仓量)
# 假设 oi 字段对应的是美元值，才使用 K/M/B 格式
axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
datum.value
"""

def create_dual_axis_chart(df, symbol):
    """生成一个双轴 Altair 图表，X轴使用时间，Y轴使用价格和未平仓量"""
    
    # 确保时间列是日期时间类型，才能正确在X轴显示
    df['time'] = pd.to_datetime(df['time'])
    
    base = alt.Chart(df).encode(
        alt.X('time', title='时间', axis=alt.Axis(format="%m-%d %H:%M"))
    )

    # 标记价格 (右轴，红色)
    line_price = base.mark_line(color='#d62728', strokeWidth=2).encode(
        alt.Y('标记价格 (USDC)',
              axis=alt.Axis(
                  title='标记价格 (USDC)',
                  titleColor='#d62728',
                  orient='right',
                  offset=0
              ),
              scale=alt.Scale(zero=False, padding=10)
        )
    )

    # 未平仓量 (右轴偏移，紫色，K/M/B 格式)
    line_oi = base.mark_line(color='purple', strokeWidth=2).encode(
        alt.Y('未平仓量',
              axis=alt.Axis(
                  title='未平仓量',
                  titleColor='purple',
                  orient='right',
                  offset=30,
                  labelExpr=axis_format_logic
              ),
              scale=alt.Scale(zero=False, padding=10)
        )
    )

    chart = alt.layer(line_price, line_oi).resolve_scale(
        y='independent'
    ).properties(
        title=alt.Title(f"{symbol} 价格与未平仓量", anchor='middle'),
        height=400 # 优化高度以容纳多图
    )

    st.altair_chart(chart, use_container_width=True)


# --- D. UI 渲染：主应用逻辑 ---

def main_app():
    # 页面配置和标题
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("✅ Hyperliquid 合约未平仓量实时监控")
    st.markdown("---") 
    
    # 1. 获取并排序所有合约列表
    st.header("📉 合约热度排名 (按最新未平仓量降序)")
    sorted_symbols = get_sorted_symbols_by_oi()
    
    if not sorted_symbols:
        st.error("无法获取合约列表。请检查数据库连接和 Hyperliquid 表中是否有数据。")
        st.stop()

    # 2. 循环遍历并绘制所有合约的图表
    for rank, symbol in enumerate(sorted_symbols, 1):
        
        # 默认展开前 100 名的图表
        with st.expander(f"**#{rank}： {symbol}**", expanded=(rank <= 100)): 
            
            # 2a. 读取数据
            data_df = fetch_data_for_symbol(symbol)
            
            if not data_df.empty:
                # 2b. 绘制图表
                create_dual_axis_chart(data_df, symbol)
                
                # 仅保留分隔线
                st.markdown("---") 
            else:
                st.warning(f"⚠️ 警告：合约 {symbol} 尚未采集到数据或查询失败。")


if __name__ == '__main__':
    main_app()
