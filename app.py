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

# --- B. 数据库功能 (已优化性能) ---

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

@st.cache_data(ttl=300) # 流通量数据缓存久一点
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
            # 获取OI金额最大的币种排序
            sql = f"SELECT symbol FROM `hyperliquid` GROUP BY symbol ORDER BY MAX(oi_usd) DESC;"
            df = pd.read_sql(sql, conn)
            return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 列表获取失败: {e}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    """
    🚀 性能优化版：
    1. 仅获取过去 24 小时的数据 (WHERE time > NOW() - INTERVAL 24 HOUR)
    2. 移除 ROW_NUMBER() 窗口函数，大幅降低数据库CPU负载
    3. Python端进行降采样
    """
    if not symbol_list: return {}
    
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    # SQL 仅筛选最近 24 小时，利用 (symbol, time) 索引加速
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

        # Python 端降采样：确保每个币种只保留约 150 个点，减少前端渲染压力
        result = {}
        grouped = df_all.groupby('symbol')
        for sym, group in grouped:
            if len(group) > 150:
                step = len(group) // 150
                # 必须保留最后一条数据以显示最新价格
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

# --- C. 辅助与绘图 (UI 紧凑化) ---

def format_number(num):
    if abs(num) >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000: return f"{num / 1_000:.1f}K"
    else: return f"{num:.0f}"

def create_mini_chart(df, symbol):
    """
    创建极简迷你图 (Sparkline) - 高度压缩版 (35px)
    """
    if df.empty: return None
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'])
    
    # 只需要 Reset index 供 Altair 画图
    df = df.reset_index(drop=True)
    df['index'] = df.index
    
    tooltip_fields = [
        alt.Tooltip('time', title='时间', format="%H:%M"),
        alt.Tooltip('标记价格 (USDC)', title='价格', format='$.4f'),
        alt.Tooltip('未平仓量', title='OI', format=',.0f') 
    ]
    
    # 隐藏 X 轴
    base = alt.Chart(df).encode(alt.X('index', axis=None))
    
    # 价格线 (红色)
    line_price = base.mark_line(color='#d62728', strokeWidth=1.5).encode(
        alt.Y('标记价格 (USDC)', axis=None, scale=alt.Scale(zero=False))
    )
    
    # OI线 (紫色)
    line_oi = base.mark_line(color='purple', strokeWidth=1.5).encode(
        alt.Y('未平仓量', axis=None, scale=alt.Scale(zero=False))
    )
    
    # 组合图表
    chart = alt.layer(line_price, line_oi).resolve_scale(y='independent').encode(
        tooltip=tooltip_fields
    ).properties(
        height=35,  # 🔥 关键：高度压缩至 35px
        width='container'
    ).configure_view(
        strokeWidth=0 # 去除边框
    )
    return chart

def render_chart_component(rank, symbol, bulk_data, ranking_data, list_type=""):
    """
    渲染单个列表项 - CSS 极致紧凑版
    """
    raw_df = bulk_data.get(symbol)
    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
    
    chart = None
    main_value_str = "0%"
    sub_tag_str = "MC: $0"
    
    if raw_df is not None and not raw_df.empty:
        # 获取统计信息
        item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)
        
        if item_stats:
            # 根据榜单类型决定显示内容
            if list_type == "strength":
                val = item_stats['intensity'] * 100
                main_value_str = f"{val:.2f}%"
                mc = format_number(item_stats['market_cap'])
                sub_tag_str = f"MC: ${mc}"
            elif list_type == "whale":
                val = item_stats['oi_growth_usd']
                main_value_str = f"+${format_number(val)}"
                sub_tag_str = "资金净流入"
            else:
                # 默认显示价格涨幅
                start_p = raw_df['标记价格 (USDC)'].iloc[0]
                end_p = raw_df['标记价格 (USDC)'].iloc[-1]
                pct = (end_p - start_p) / start_p * 100
                main_value_str = f"{pct:+.2f}%"
                sub_tag_str = f"MC: ${format_number(item_stats['market_cap'])}"

        # 生成图表 (直接使用 Python 降采样后的数据)
        chart = create_mini_chart(raw_df, symbol)

    # 🔥 HTML 样式：缩小字体、减小间距
    html_content = f"""
    <a href="{coinglass_url}" target="_blank" style="text-decoration:none; display: block; color: inherit;">
        <div style="font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin-bottom: 0px;">
            <div style="font-size: 11px; color: #888; margin-bottom: -2px;">
                No.{rank} <span style="color: #333; font-weight: 500;">{symbol}</span>
            </div>
            <div style="font-size: 22px; font-weight: 600; color: #333; letter-spacing: -0.5px; line-height: 1.2;">
                {main_value_str}
            </div>
            <div style="display: inline-block; background-color: #f0f2f6; padding: 1px 6px; border-radius: 3px; font-size: 10px; color: #666; margin-top: 1px;">
                ↑ {sub_tag_str}
            </div>
        </div>
    </a>
    """

    st.markdown(html_content, unsafe_allow_html=True)
    if chart:
        st.altair_chart(chart, use_container_width=True)
    
    # 极窄的分割线
    st.markdown("""<hr style="margin: 4px 0; border: 0; border-top: 1px solid #f0f0f0;">""", unsafe_allow_html=True)

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="HL OI Dashboard")
    
    # 🔥 全局 CSS：强制压缩 Altair 图表高度，移除默认边距
    st.markdown("""
        <style>
        /* 移除元素间的默认大间距 */
        .block-container { padding-top: 1rem; padding-bottom: 2rem; }
        .element-container { margin-bottom: 0px !important; }
        .stMarkdown { margin-bottom: -5px !important; }
        
        /* 强制设定 Altair 图表容器高度为 35px，防止 Streamlit 预留空白 */
        div[data-testid="stAltairChart"] { height: 35px !important; min-height: 35px !important; }
        canvas { height: 35px !important; }
        
        /* 调整标题间距 */
        h5 { padding-top: 0px; margin-bottom: 10px; }
        </style>
    """, unsafe_allow_html=True)

    st.title("⚡ OI 极简看板")
    
    with st.spinner("🚀 正在极速加载数据..."):
        supply_data = fetch_circulating_supply()
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        
        if not sorted_symbols: st.stop()
        
        # 获取前 100 个币种
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据 (请检查数据库连接或表数据)"); st.stop()

    # --- 计算统计数据 ---
    ranking_data = []
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        
        token_info = supply_data.get(sym)
        current_price = df['标记价格 (USDC)'].iloc[-1]
        
        min_oi = df['未平仓量'].min()
        current_oi = df['未平仓量'].iloc[-1]
        
        # 计算核心指标
        oi_growth_tokens = current_oi - min_oi
        oi_growth_usd = oi_growth_tokens * current_price
        
        intensity = 0
        market_cap = 0
        
        # 强度计算逻辑
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
            "market_cap": market_cap
        })

    # 排序
    top_intensity = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:10]
    top_whales = sorted(ranking_data, key=lambda x: x['oi_growth_usd'], reverse=True)[:10]
    
    # ==========================
    # 列表展示 (左右双栏)
    # ==========================
    
    col_left, col_right = st.columns(2)
    
    # --- 左栏：强度榜 ---
    with col_left:
        st.markdown("##### 🔥 强度榜 (Intensity)") 
        st.markdown("---")
        if top_intensity:
            for i, item in enumerate(top_intensity, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, list_type="strength")
        else:
            st.info("暂无数据")

    # --- 右栏：巨鲸榜 ---
    with col_right:
        st.markdown("##### 🐳 巨鲸榜 (Net Inflow)")
        st.markdown("---")
        if top_whales:
            for i, item in enumerate(top_whales, 1):
                render_chart_component(i, item['symbol'], bulk_data, ranking_data, list_type="whale")
        else:
            st.info("暂无数据")
    
    # --- 底部：剩余列表 (4列网格) ---
    st.markdown("##### 📋 其他异动")
    shown_symbols = set([x['symbol'] for x in top_intensity] + [x['symbol'] for x in top_whales])
    remaining = [s for s in target_symbols if s not in shown_symbols]
    
    if remaining:
        # 使用 4 列布局来节省底部空间
        cols = st.columns(4)
        for idx, symbol in enumerate(remaining):
            with cols[idx % 4]:
                render_chart_component(idx+1, symbol, bulk_data, ranking_data, list_type="normal")

if __name__ == '__main__':
    main_app()
