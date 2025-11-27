import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
import time

# --- A. 数据库连接配置 ---
# 注意：在 Streamlit Cloud 中运行时，这些值将自动从 Secrets 中读取
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", "您的本地密码") 

DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'Hyperliquid'
DATA_LIMIT = 4000 # 读取行数限制

# --- B. 数据读取和动态列表函数 ---

# 1. 缓存数据库连接资源
@st.cache_resource(ttl=3600)
def get_db_connection():
    """建立并缓存数据库连接，如果失败则返回 None"""
    try:
        return pymysql.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
            db=NEW_DB_NAME, charset=DB_CHARSET
        )
    except Exception as e:
        # 在 Streamlit 界面显示详细错误信息
        st.error(f"❌ 数据库连接失败，请检查 Secrets 和腾讯云安全组: {e}")
        st.stop()
        return None


# 2. 动态获取所有合约列表 (不再接受 'conn' 参数)
@st.cache_data(ttl=60)
def fetch_all_symbols():
    """动态从数据库中获取所有不重复的合约名称 (184 个)"""
    conn = get_db_connection()  # 内部调用缓存的连接
    if conn is None: return []

    try:
        sql_query = f"SELECT DISTINCT `symbol` FROM `{TABLE_NAME}` ORDER BY `symbol` ASC"
        df = pd.read_sql(sql_query, conn)
        return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 无法从数据库获取合约列表: {e}")
        return []
    finally:
        # 注意：使用 cache_resource 缓存的连接不应该在这里关闭
        pass

    # 3. 读取指定合约数据 (不再接受 'conn' 参数)


@st.cache_data(ttl=60)  # 缓存数据 60 秒
def fetch_data_for_symbol(symbol, limit=DATA_LIMIT):
    """从数据库中读取指定 symbol 的最新数据"""
    conn = get_db_connection()  # 内部调用缓存的连接
    if conn is None: return pd.DataFrame()

    try:
        sql_query = f"""
        SELECT `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
        FROM `{TABLE_NAME}`
        WHERE `symbol` = %s
        ORDER BY `time` DESC
        LIMIT %s
        """

        # 使用 pandas 读取 SQL 查询结果
        df = pd.read_sql(sql_query, conn, params=(symbol, limit))
        df = df.sort_values('time', ascending=True)
        return df

    except Exception as e:
        st.warning(f"⚠️ 查询 {symbol} 数据失败: {e}")
        return pd.DataFrame()


# --- C. 核心绘图函数 (省略，与前一个版本保持一致) ---
# ... (保持不变)

# Y 轴自定义格式逻辑 (Vega Expression)，用于 OI (未平仓量)
axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
datum.value
"""


def create_dual_axis_chart(df, symbol):
    """生成一个双轴 Altair 图表，X轴使用时间，Y轴使用价格和未平仓量"""
    base = alt.Chart(df).encode(
        alt.X('time', title='时间', axis=alt.Axis(format="%m-%d %H:%M"))
    )

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
        height=500
    )

    st.altair_chart(chart, use_container_width=True)


# --- D. UI 渲染：主应用逻辑 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("✅ Hyperliquid 合约未平仓量实时监控")
    st.markdown("---")

    # 1. 动态获取所有合约列表 (不再需要传入 conn)
    all_symbols = fetch_all_symbols()

    if not all_symbols:
        st.error("无法获取合约列表。请检查数据库连接和 Hyperliquid 表中是否有数据。")
        st.stop()

    # 2. 侧边栏选择器
    selected_symbol = st.sidebar.selectbox(
        "选择合约：",
        all_symbols,
        index=all_symbols.index("BTC-USD") if "BTC-USD" in all_symbols else 0
    )

    # 3. 数据读取和绘图 (不再需要传入 conn)
    if selected_symbol:
        st.subheader(f"合约: {selected_symbol}")

        # 读取数据
        data_df = fetch_data_for_symbol(selected_symbol)

        if not data_df.empty:
            # 绘制图表
            create_dual_axis_chart(data_df, selected_symbol)

            # 显示数据表格
            st.markdown("### 最新数据预览")
            data_df['time'] = pd.to_datetime(data_df['time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            st.dataframe(data_df.tail(10), use_container_width=True)
        else:
            st.warning(f"⚠️ 警告：合约 {selected_symbol} 尚未采集到数据或查询失败。")


if __name__ == '__main__':
    main_app()
