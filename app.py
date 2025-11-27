import streamlit as st
import pymysql
import pymysql.cursors
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
# 强制重新部署 20251127
# -------------------------------------------------------------------------
#                          A. 数据库连接配置 (请确保与采集代码一致)
# -------------------------------------------------------------------------
DB_HOST = 'cd-cdb-p6vea42o.sql.tencentcdb.com'
DB_PORT = 24197
DB_USER = 'root'
DB_PASSWORD = 'CZQ168txy..'  # 替换为您的真实密码
DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'Hyperliquid'
DATA_LIMIT = 4000 # 读取行数限制

# -------------------------------------------------------------------------
#                          B. 数据库和绘图函数
# -------------------------------------------------------------------------

@st.cache_data(ttl=60) # 缓存数据 60 秒，避免每次刷新都查询数据库
def fetch_data_for_visualization(symbol, limit=DATA_LIMIT):
    """从数据库中读取指定 symbol 的最新数据，用于绘图。"""
    conn = None
    try:
        conn = pymysql.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
            db=NEW_DB_NAME, charset=DB_CHARSET
        )
        sql_query = f"""
        SELECT `time`, `price`, `oi`, `oi_usd`
        FROM `{TABLE_NAME}`
        WHERE `symbol` = %s
        ORDER BY `time` DESC
        LIMIT %s
        """
        df = pd.read_sql(sql_query, conn, params=(symbol, limit))
        df = df.sort_values('time', ascending=True)
        return df

    except Exception as e:
        st.error(f"❌ 数据库连接或查询失败: {e}")
        return pd.DataFrame()
    finally:
        if conn and conn.open:
            conn.close()

def create_dual_axis_chart(df, symbol):
    """创建左右双轴折线图"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # 左轴：OI (未平仓量)
    fig.add_trace(
        go.Scatter(x=df['time'], y=df['oi'], name="OI (Left Axis)", line=dict(color='purple')),
        secondary_y=False,
    )
    # 右轴：Price (价格)
    fig.add_trace(
        go.Scatter(x=df['time'], y=df['price'], name="Price (Right Axis)", line=dict(color='red')),
        secondary_y=True,
    )

    # 布局配置
    fig.update_layout(
        title_text=f"Hyperliquid OI & Price: {symbol}",
        height=500,
        margin=dict(t=50, b=50),
        legend=dict(y=1.1, x=0.1, orientation="h")
    )
    fig.update_yaxes(title_text="Open Interest (OI)", secondary_y=False, title_font=dict(color='purple'))
    fig.update_yaxes(title_text="Price (USDC)", secondary_y=True, title_font=dict(color='red'))
    
    return fig

# -------------------------------------------------------------------------
#                          C. Streamlit 应用主逻辑
# -------------------------------------------------------------------------

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("📈 Hyperliquid 合约未平仓量实时监控")

    # 1. 获取所有独特的 symbol 列表 (假设 BTC-USD 是一个默认值)
    # 理想情况下，您应该查询数据库获取所有 symbol，这里简化为手动列表
    all_symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "SEI-USD", "TIA-USD", "DOGE-USD", "WIF-USD"] 
    
    # 可以在这里添加查询以获取所有 symbol
    # try:
    #     conn = pymysql.connect(...)
    #     with conn.cursor() as cursor:
    #         cursor.execute(f"SELECT DISTINCT `symbol` FROM {TABLE_NAME}")
    #         all_symbols = [row['symbol'] for row in cursor.fetchall()]
    # except:
    #     pass

    # 2. 侧边栏选择器
    selected_symbol = st.sidebar.selectbox(
        "选择合约：",
        all_symbols
    )

    # 3. 实时查询和绘图
    if selected_symbol:
        st.subheader(f"合约: {selected_symbol}")
        
        # 3a. 读取数据
        data_df = fetch_data_for_visualization(selected_symbol)
        
        if not data_df.empty:
            # 3b. 绘制图表
            chart = create_dual_axis_chart(data_df, selected_symbol)
            st.plotly_chart(chart, use_container_width=True)
            
            # 3c. 显示最新数据
            st.markdown(f"**最新时间：** {data_df['time'].iloc[-1]}")
            st.dataframe(data_df.tail(5), use_container_width=True)
        else:
            st.warning(f"合约 {selected_symbol} 尚未采集到数据。")

# 运行应用
if __name__ == '__main__':

    main_app()
