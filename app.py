import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
import time

# --- A. 数据库连接配置 (用于 Streamlit Cloud 部署) ---
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 

DB_CHARSET = 'utf8mb4'
NEW_DB_NAME = 'open_interest_db'
TABLE_NAME = 'hyperliquid' 
DATA_LIMIT = 4000 

# --- B. 数据读取和排序函数 (保持不变) ---

@st.cache_resource(ttl=3600)
def get_db_connection_params():
    """返回数据库连接所需的参数字典。"""
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。请检查 Streamlit Secrets 或本地 secrets.toml 文件。")
        st.stop()
        return None
    return {
        'host': DB_HOST,
        'port': DB_PORT,
        'user': DB_USER,
        'password': DB_PASSWORD,
        'db': NEW_DB_NAME,
        'charset': DB_CHARSET,
        'autocommit': True 
    }

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    """获取所有合约的最新 OI_USD 值，并返回一个按 OI_USD 降序排列的合约列表。"""
    params = get_db_connection_params()
    if params is None: return []

    conn = None
    try:
        conn = pymysql.connect(**params)
        
        sql_query = f"""
        SELECT 
            t1.symbol, 
            t1.oi_usd  
        FROM `{TABLE_NAME}` t1
        INNER JOIN (
            SELECT symbol, MAX(time) as max_time
            FROM `{TABLE_NAME}`
            GROUP BY symbol
        ) t2 
        ON t1.symbol = t2.symbol AND t1.time = t2.max_time
        ORDER BY t1.oi_usd DESC;
        """
        
        df_oi_rank = pd.read_sql(sql_query, conn)
        
        if df_oi_rank.empty:
            st.error("数据库中没有找到任何合约的最新数据。")
            return []

        return df_oi_rank['symbol'].tolist()
        
    except Exception as e:
        st.error(f"❌ 无法获取和排序合约列表: {e}")
        return []
    finally:
        if conn:
            conn.close()

@st.cache_data(ttl=60)
def fetch_data_for_symbol(symbol, limit=DATA_LIMIT):
    """从数据库中读取指定 symbol 的最新数据，使用 oi 字段。"""
    params = get_db_connection_params()
    if params is None: return pd.DataFrame()

    conn = None
    try:
        conn = pymysql.connect(**params)
        
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
    finally:
        if conn:
            conn.close()


# --- C. 核心绘图函数 (X 轴按等距索引显示) ---

# Y 轴自定义格式逻辑 (Vega Expression)
axis_format_logic = """
datum.value >= 1000000000 ? format(datum.value / 1000000000, ',.2f') + 'B' : 
datum.value >= 1000000 ? format(datum.value / 1000000, ',.2f') + 'M' : 
datum.value >= 1000 ? format(datum.value / 1000, ',.1f') + 'K' : 
format(datum.value, ',.0f')
"""

# 定义 Y 轴标签样式常量
LABEL_FONT_SIZE = 12
LABEL_FONT_WEIGHT = 'bold'

def create_dual_axis_chart(df, symbol):
    """生成一个双轴 Altair 图表，X 轴按等距索引显示数据点。"""
    
    if not df.empty:
        df['index'] = range(len(df))
    
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])

    tooltip_fields = [
        alt.Tooltip('time', title='时间', format="%Y-%m-%d %H:%M:%S"),
        alt.Tooltip('标记价格 (USDC)', title='标记价格', format='$,.4f'),
        alt.Tooltip('未平仓量', title='OI', format=',.0f') 
    ]
    
    # 1. 定义基础图表
    base = alt.Chart(df).encode(
        alt.X('index', title=None, axis=alt.Axis(labels=False))
    )
    
    # 2. 标记价格 (右轴，红色)
    line_price = base.mark_line(color='#d62728', strokeWidth=2).encode(
        alt.Y('标记价格 (USDC)',
              axis=alt.Axis(
                  title='标记价格 (USDC)',
                  titleColor='#d62728',
                  orient='right',
                  offset=0,
                  labelFontWeight=LABEL_FONT_WEIGHT,
                  labelFontSize=LABEL_FONT_SIZE
              ),
              scale=alt.Scale(zero=False, padding=10)
        ),
        tooltip=tooltip_fields
    )

    # 3. 未平仓量 (OI) (右轴偏移，紫色)
    line_oi = base.mark_line(color='purple', strokeWidth=2).encode(
        alt.Y('未平仓量',
              axis=alt.Axis(
                  title='未平仓量', 
                  titleColor='purple',
                  orient='right',
                  offset=30, 
                  labelExpr=axis_format_logic,
                  labelFontWeight=LABEL_FONT_WEIGHT,
                  labelFontSize=LABEL_FONT_SIZE
              ),
              scale=alt.Scale(zero=False, padding=10)
        ),
        tooltip=tooltip_fields
    )
    
    # 4. 组合图表
    chart = alt.layer(
        line_price, 
        line_oi
    ).resolve_scale(
        y='independent'
    ).properties(
        # *** 关键修改：移除 Altair 标题，由 Streamlit Markdown 替代 ***
        title=None, 
        height=400 
    )

    st.altair_chart(chart, use_container_width=True)


# --- D. UI 渲染：主应用逻辑 (修改为使用 Markdown + 超链接) ---

def main_app():
    # 页面配置和标题
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("✅ Hyperliquid 合约未平仓量实时监控")
    st.markdown("---") 
    
    # 1. 获取并排序所有合约列表
    st.header("📈 合约热度排名")
    sorted_symbols = get_sorted_symbols_by_oi_usd()
    
    if not sorted_symbols:
        st.error("无法获取合约列表。请检查数据库连接和 Hyperliquid 表中是否有数据。")
        st.stop()

    # 2. 循环遍历并绘制所有合约的图表
    for rank, symbol in enumerate(sorted_symbols, 1):
        
        # 默认展开前 100 名的图表
        # 创建可点击的 Expander 标题，并添加 OI/价格图表的链接
        coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
        # 使用 markdown 和 HTML <a> 标签创建大号、粗体、可点击的标题
        # 字体大小使用 style="font-size:24px;" 
        expander_title_html = (
            f'<a href="{coinglass_url}" target="_blank" '
            f'style="text-decoration:none; color:inherit; font-weight:bold; font-size:24px;">'
            f'#{rank}： {symbol} 价格与未平仓量 (点击查看 Coinglass)</a>'
        )
        
        # 使用 Markdown 配合 unsafe_allow_html=True 来渲染 HTML 标题
        st.markdown(expander_title_html, unsafe_allow_html=True)
        
        with st.expander("点击展开/折叠图表", expanded=(rank <= 100)): 
            
            # 2a. 读取数据
            data_df = fetch_data_for_symbol(symbol)
            
            if not data_df.empty:
                # 2b. 绘制图表
                create_dual_axis_chart(data_df, symbol)
                
                # 仅保留分隔线
                st.markdown("---") 
            else:
                st.warning(f"⚠️ 警告：合约 {symbol} 尚未采集到数据或查询失败。")
                st.markdown("---")


if __name__ == '__main__':
    main_app()









