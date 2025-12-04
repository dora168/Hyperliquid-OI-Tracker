import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库配置 ----
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'

DB_NAME_OI = 'open_interest_db'
DB_NAME_SUPPLY = 'circulating_supply'

# --- B. 数据库功能 ---

@st.cache_resource
def get_db_connection_params(db_name):
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。")
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
    conn = pymysql.connect(**params)
    try:
        yield conn
    finally:
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
def fetch_bulk_data_one_shot(symbol_list):
    if not symbol_list: return {}
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    # 获取最近24小时数据，避免全表扫描
    sql_query = f"""
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM `hyperliquid`
    WHERE symbol IN ({placeholders})
      AND `time` >= NOW() - INTERVAL 24 HOUR
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        with get_connection(DB_NAME_OI) as conn:
            df_all = pd.read_sql(sql_query, conn, params=tuple(symbol_list))
        
        if df_all.empty: return {}
        
        # 简单降采样
        result = {}
        grouped = df_all.groupby('symbol')
        for sym, group in grouped:
            if len(group) > 200:
                step = len(group) // 200
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

# --- C. 辅助与绘图 ---

def format_number(num):
    if abs(num) >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000: return f"{num / 1_000:.1f}K"
    else: return f"{num:.0f}"

# 信号判断逻辑
def get_signal_info(price_chg, oi_chg):
    epsilon = 1e-9
    if price_chg > epsilon and oi_chg > epsilon:
        return "🟢多头增仓", "#009900" 
    elif price_chg > epsilon and oi_chg < -epsilon:
        return "🟡空头平仓", "#d48806" 
    elif price_chg < -epsilon and oi_chg > epsilon:
        return "🔴空头增仓", "#cf1322" 
    elif price_chg < -epsilon and oi_chg < -epsilon:
        return "🟠多头平仓", "#d46b08" 
    else:
        return "⚪震荡", "#8c8c8c"

def downsample_data(df, target_points=400):
    if len(df) <= target_points: return df
    step = len(df) // target_points
    df_sampled = df.iloc[::step].copy()
    if df.index[-1] not in df_sampled.index:
        df_sampled = pd.concat([df_sampled, df.iloc[[-1]]])
    return df_sampled

def create_mini_chart(df, symbol):
    """创建迷你图 (Sparkline)"""
    if df.empty: return None
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
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
    ).properties(height=60, width='container').configure_view(strokeWidth=0)
    return chart

def render_chart_component(rank, symbol, bulk_data, ranking_data, list_type=""):
    """
    渲染单个组件：改为可折叠样式
    """
    raw_df = bulk_data.get(symbol)
    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
    
    # 1. 准备数据变量
    main_value_str = ""
    signal_html = ""
    info_html = ""
    chart = None
    
    if raw_df is not None and not raw_df.empty:
        item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)
        
        if item_stats:
            # 信号标签
            sig_text, sig_color = item_stats['signal']
            signal_html = f'<span style="color: {sig_color}; border: 1px solid {sig_color}; padding: 1px 4px; border-radius: 3px; font-size: 12px; margin-right: 8px;">{sig_text}</span>'
            
            # 根据榜单类型决定显示什么核心数据
            if list_type == "strength":
                val = item_stats['intensity'] * 100
                main_value_str = f"{val:.2f}%"
                
                int_color = "#d62728" if val > 5 else "#009900"
                info_html = f"""
                <div style='margin-bottom: 5px;'>
                    <span style='font-size: 24px; font-weight: bold; color: {int_color};'>{main_value_str}</span>
                    <span style='color: #666; font-size: 12px; margin-left: 10px;'>MC: ${format_number(item_stats['market_cap'])}</span>
                </div>
                """
                
            elif list_type == "whale":
                val = item_stats['oi_growth_usd']
                main_value_str = f"+${format_number(val)}"
                
                info_html = f"""
                <div style='margin-bottom: 5px;'>
                    <span style='font-size: 24px; font-weight: bold; color: #009900;'>{main_value_str}</span>
                    <span style='color: #666; font-size: 12px; margin-left: 10px;'>资金净流入</span>
                </div>
                """

        # 2. 决定是否画图 (Top 10 画图, 11-20 不画)
        should_draw_chart = True
        if list_type == "whale" and rank > 10:
            should_draw_chart = False
            
        if should_draw_chart:
            chart_df = downsample_data(raw_df, target_points=200)
            chart = create_mini_chart(chart_df, symbol)

    # 3. 生成折叠框的标题 (Expander Label)
    # 因为 expander label 不支持 HTML，只能用纯文本，所以我们尽量把关键信息拼进去
    label_text = f"No.{rank}  {symbol}   |   {main_value_str}"

    # 4. 渲染折叠框
    with st.expander(label_text, expanded=False):
        # 内部显示详细 HTML
        st.markdown(f"""
            <div style='display: flex; align-items: center; margin-bottom: 10px;'>
                <a href="{coinglass_url}" target="_blank" style="text-decoration:none; font-weight:bold; font-size:18px; margin-right: 10px;">{symbol} 🔗</a>
                {signal_html}
            </div>
            {info_html}
        """, unsafe_allow_html=True)
        
        # 显示图表 (如果满足条件)
        if chart:
            st.altair_chart(chart, use_container_width=True)
        elif list_type == "whale" and rank > 10:
            st.caption("📉 (Top 11-20 仅显示数据，不绘制图表)")
        else:
            st.info("暂无图表数据")

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("⚡ OI 双塔监控 (Top 20 巨鲸)")
    
    with st.spinner("正在读取流通量数据库..."):
        supply_data = fetch_circulating_supply()
        
    with st.spinner("正在加载市场数据..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        if not sorted_symbols: st.stop()
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据"); st.stop()

    # --- 计算统计数据 ---
    ranking_data = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        
        token_info = supply_data.get(sym)
        
        # 数据准备
        start_p = df['标记价格 (USDC)'].iloc[0]
        current_p = df['标记价格 (USDC)'].iloc[-1]
        min_oi = df['未平仓量'].min()
        start_oi = df['未平仓量'].iloc[0]
        current_oi = df['未平仓量'].iloc[-1]
        
        # 指标计算
        oi_growth_tokens = current_oi - min_oi
        oi_growth_usd = oi_growth_tokens * current_p
        
        price_change_pct = (current_p - start_p) / start_p if start_p > 0 else 0
        oi_change_pct = (current_oi - start_oi) / start_oi if start_oi > 0 else 0
        signal_tuple = get_signal_info(price_change_pct, oi_change_pct)
        
        intensity = 0
        market_cap = 0
        if token_info and token_info.get('market_cap') and token_info['market_cap'] > 0:
            market_cap = token_info['market_cap']
            intensity = oi_growth_usd / market_cap
        elif token_info and token_info.get('circulating_supply') and token_info['circulating_supply'] > 0:
            supply = token_info['circulating_supply']
            intensity = oi_growth_tokens / supply
        else:
            if min_oi > 0: intensity = (oi_growth_tokens / min_oi) * 0.1

        ranking_data.append({
            "symbol": sym,
            "intensity": intensity, 
            "oi_growth_usd": oi_growth_usd,
            "market_cap": market_cap,
            "signal": signal_tuple
        })

    # ==========================
    # 榜单生成
    # ==========================
    
    # 强度榜维持 Top 10
    top_intensity = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:10]
    
    # 🔥 修改点：巨鲸榜扩大到 Top 20
    top_whales = sorted(ranking_data, key=lambda x: x['oi_growth_usd'], reverse=True)[:20]

    # 左右双栏布局
    col_left, col_right = st.columns(2)
    
    # --- 左塔：强度 Top 10 (可折叠) ---
    with col_left:
        st.subheader("🔥 强度 Top 10")
        st.caption("Collapsible Strength List")
        if top_intensity:
            for i, item in enumerate(top_intensity, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, list_type="strength")
        else:
            st.info("暂无数据")

    # --- 右塔：巨鲸 Top 20 (可折叠，仅前10画图) ---
    with col_right:
        st.subheader("🐳 巨鲸 Top 20")
        st.caption("Collapsible Whale List (Charts for Top 10 only)")
        if top_whales:
            for i, item in enumerate(top_whales, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, list_type="whale")
        else:
            st.info("暂无数据")
            
    st.markdown("---")
    st.subheader("📋 其他活跃合约")

    # --- 底部：剩余列表 ---
    shown_symbols = set()
    for item in top_intensity: shown_symbols.add(item['symbol'])
    for item in top_whales: shown_symbols.add(item['symbol'])
    
    remaining_symbols = [s for s in target_symbols if s not in shown_symbols]

    if remaining_symbols:
        # 底部简单列表展示，节省资源
        cols = st.columns(4)
        for idx, symbol in enumerate(remaining_symbols):
             with cols[idx % 4]:
                 st.markdown(f"**{symbol}**")

if __name__ == '__main__':
    main_app()

