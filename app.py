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
        return {sym: group for sym, group in df_all.groupby('symbol')}
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 降采样逻辑 (400点) ---

def downsample_data(df, target_points=400):
    if len(df) <= target_points:
        return df
    
    step = len(df) // target_points
    df_sampled = df.iloc[::step].copy()
    
    if df.index[-1] not in df_sampled.index:
        df_sampled = pd.concat([df_sampled, df.iloc[[-1]]])
        
    return df_sampled

# --- D. 绘图函数 (高度 450px) ---

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
        alt.Y('未平仓量', axis=alt.Axis(title='OI', titleColor='purple', orient='right', offset=45, labelExpr=axis_format_logic), scale=alt.Scale(zero=False))
    )
    
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(height=450)

    return chart

# --- E. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    
    st.title("⚡ Hyperliquid OI 极速监控")
    
    # 1. 获取排名
    with st.spinner("正在加载排名..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
    
    if not sorted_symbols: st.stop()

    # Top 100 设置
    top_n = 100
    target_symbols = sorted_symbols[:top_n]

    # 2. 批量获取数据
    with st.spinner(f"正在获取 Top {top_n} 数据..."):
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据，请检查网络或白名单设置")
        st.stop()

    # --- 【新增功能 1】全局概览：OI 飙升榜 (Top 5 Movers) ---
    st.markdown("### 🔥 OI 24H 飙升榜 (Top 5)")
    
    # 计算所有已加载币种的 OI 变化率
    oi_metrics = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        start_oi = df['未平仓量'].iloc[0]
        end_oi = df['未平仓量'].iloc[-1]
        
        if start_oi == 0: continue
        
        oi_change = (end_oi - start_oi) / start_oi
        oi_metrics.append({"symbol": sym, "change": oi_change})
    
    # 排序并取前 5
    if oi_metrics:
        top_movers = sorted(oi_metrics, key=lambda x: x['change'], reverse=True)[:5]
        
        cols = st.columns(5)
        for i, mover in enumerate(top_movers):
            # 使用 Metric 组件展示
            cols[i].metric(
                label=f"Top {i+1} {mover['symbol']}",
                value=f"{mover['change']:.2%}",
                delta="OI 激增",
                delta_color="normal" # 默认为绿色
            )
    
    st.markdown("---") 

    # 3. 循环渲染列表
    for rank, symbol in enumerate(target_symbols, 1):
        raw_df = bulk_data.get(symbol)
        
        coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
        title_color = "black"
        chart = None
        oi_info_html = "" # 用于存放 OI 信息的 HTML
        
        if raw_df is not None and not raw_df.empty:
            # A. 计算价格涨跌色
            start_p = raw_df['标记价格 (USDC)'].iloc[0]
            end_p = raw_df['标记价格 (USDC)'].iloc[-1]
            title_color = "#009900" if end_p >= start_p else "#D10000"
            
            # B. 【新增功能 2】计算 OI 变化率并生成标题信息
            start_oi = raw_df['未平仓量'].iloc[0]
            end_oi = raw_df['未平仓量'].iloc[-1]
            
            if start_oi > 0:
                oi_pct = (end_oi - start_oi) / start_oi * 100
                oi_color = "#009900" if oi_pct >= 0 else "#D10000"
                # 添加 emoji 增强视觉
                oi_icon = "🔥" if oi_pct > 5 else ("❄️" if oi_pct < -5 else "")
                
                oi_info_html = (
                    f'<span style="font-size: 16px; color: #555; margin-left: 15px;">'
                    f'OI 变化: <span style="color: {oi_color}; font-weight: bold;">{oi_pct:+.2f}%</span> {oi_icon}'
                    f'</span>'
                )

            # C. 采样并绘图
            chart_df = downsample_data(raw_df, target_points=400)
            chart = create_dual_axis_chart(chart_df, symbol)

        # 组合标题 HTML
        expander_title_html = (
            f'<div style="text-align: center; margin-bottom: 5px;">'
            f'<a href="{coinglass_url}" target="_blank" '
            f'style="text-decoration:none; color:{title_color}; font-weight:bold; font-size:22px;">'
            f'#{rank} {symbol} </a>'
            f'{oi_info_html}'  # 把 OI 信息插在这里
            f'</div>'
        )
        
        with st.expander(f"#{rank} {symbol}", expanded=True):
            st.markdown(expander_title_html, unsafe_allow_html=True)
            if chart:
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("暂无数据")

if __name__ == '__main__':
    main_app()
