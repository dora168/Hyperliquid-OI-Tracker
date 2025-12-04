import streamlit as st
import pandas as pd
import altair as alt
import pymysql
import os
from contextlib import contextmanager

# --- A. 数据库配置 ---
# ⚠️ 请确认密码正确
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'

# 两个数据库名
DB_NAME_OI = 'open_interest_db'
DB_NAME_SUPPLY = 'circulating_supply'

DATA_LIMIT = 4000 

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
    """通用连接管理器"""
    params = get_db_connection_params(db_name)
    conn = pymysql.connect(**params)
    try:
        yield conn
    finally:
        conn.close()

@st.cache_data(ttl=3600) # 流通量数据不常变，缓存 1 小时
def fetch_circulating_supply():
    """从 circulating_supply 数据库读取流通量数据"""
    try:
        with get_connection(DB_NAME_SUPPLY) as conn:
            # 读取 symbol, circulating_supply, market_cap
            sql = f"SELECT symbol, circulating_supply, market_cap FROM `{DB_NAME_SUPPLY}`"
            df = pd.read_sql(sql, conn)
            # 转为字典以便快速查找: {'BTC': {'supply': 19000000, 'mcap': ...}}
            return df.set_index('symbol').to_dict('index')
    except Exception as e:
        # 如果读取失败（比如表还没建好），不报错，只打印警告并返回空
        print(f"⚠️ 流通量数据读取失败: {e}")
        return {}

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    """获取 OI 排名"""
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
    """批量获取行情数据"""
    if not symbol_list: return {}
    placeholders = ', '.join(['%s'] * len(symbol_list))
    
    sql_query = f"""
    WITH RankedData AS (
        SELECT symbol, `time`, `price`, `oi`,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `hyperliquid`
        WHERE symbol IN ({placeholders})
    )
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM RankedData
    WHERE rn <= %s
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        with get_connection(DB_NAME_OI) as conn:
            df_all = pd.read_sql(sql_query, conn, params=tuple(symbol_list) + (DATA_LIMIT,))
        
        if df_all.empty: return {}
        return {sym: group for sym, group in df_all.groupby('symbol')}
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 辅助与绘图 ---

def format_number(num):
    if abs(num) >= 1_000_000_000: return f"{num / 1_000_000_000:.2f}B"
    elif abs(num) >= 1_000_000: return f"{num / 1_000_000:.2f}M"
    elif abs(num) >= 1_000: return f"{num / 1_000:.1f}K"
    else: return f"{num:.0f}"

def downsample_data(df, target_points=400):
    if len(df) <= target_points: return df
    step = len(df) // target_points
    df_sampled = df.iloc[::step].copy()
    if df.index[-1] not in df_sampled.index:
        df_sampled = pd.concat([df_sampled, df.iloc[[-1]]])
    return df_sampled

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
    
    base = alt.Chart(df).encode(alt.X('index', title=None, axis=alt.Axis(labels=False)))
    
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

# --- D. 主程序 ---

def main_app():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")
    st.title("⚡ OI 强度监控 (OI Growth vs Supply)")
    
    # 1. 准备数据
    with st.spinner("正在读取流通量数据库..."):
        supply_data = fetch_circulating_supply()
        
    with st.spinner("正在加载市场数据..."):
        sorted_symbols = get_sorted_symbols_by_oi_usd()
        if not sorted_symbols: st.stop()
        
        # 默认前 100
        target_symbols = sorted_symbols[:100]
        bulk_data = fetch_bulk_data_one_shot(target_symbols)

    if not bulk_data:
        st.warning("暂无数据")
        st.stop()

    # --- 【核心算法】计算 OI 强度 ---
    # 强度指标 = (OI增加的Token数量) / (流通量)
    # 或者等价于 = (OI增加的USD价值) / (流通市值)
    
    ranking_data = []
    
    for sym, df in bulk_data.items():
        if df.empty or len(df) < 2: continue
        
        # 获取该币种的流通数据
        token_info = supply_data.get(sym)
        
        # 计算 OI 变化
        start_oi = df['未平仓量'].iloc[0] # 单位通常是 Token 数量
        end_oi = df['未平仓量'].iloc[-1]
        price = df['标记价格 (USDC)'].iloc[-1]
        
        oi_change_tokens = end_oi - start_oi
        oi_change_usd = oi_change_tokens * price
        
        # 计算强度 (Intensity)
        intensity = 0
        mc = 0
        
        if token_info and token_info.get('market_cap') and token_info['market_cap'] > 0:
            # 如果有市值数据，直接用 (OI变动金额 / 市值)
            mc = token_info['market_cap']
            intensity = oi_change_usd / mc
        elif token_info and token_info.get('circulating_supply') and token_info['circulating_supply'] > 0:
            # 如果只有流通量，用 (OI变动数量 / 流通量)
            supply = token_info['circulating_supply']
            intensity = oi_change_tokens / supply
        else:
            # 如果都没有，暂时给个低权重，或者只看 OI 变动比例作为保底
            if start_oi > 0:
                intensity = (oi_change_tokens / start_oi) * 0.1 # 降权处理
        
        ranking_data.append({
            "symbol": sym,
            "intensity": intensity, # 这是一个比例，比如 0.05 代表 OI 增加了流通盘的 5%
            "oi_change_usd": oi_change_usd,
            "market_cap": mc
        })

    # --- 【顶部展示】 Top 5 强度榜单 ---
    st.markdown("### 🔥 OI 强度榜 (OI 增量 / 流通市值)")
    st.caption("该榜单显示 **OI 净增长占流通盘的比例**。比例越高，说明主力资金相对于该币种体量介入得越深。")

    if ranking_data:
        # 按强度绝对值排序 (关注暴涨和暴跌) -> 这里我们先只看正向流入 (暴涨潜力)
        # 如果想看双向，可以用 key=lambda x: abs(x['intensity'])
        top_movers = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:5]
        
        cols = st.columns(5)
        for i, item in enumerate(top_movers):
            sym = item['symbol']
            intensity_pct = item['intensity'] * 100
            
            # 显示格式
            icon = "🔥" if intensity_pct > 2 else "📈"
            mc_str = format_number(item['market_cap']) if item['market_cap'] > 0 else "N/A"
            
            cols[i].metric(
                label=f"No.{i+1} {sym} {icon}",
                value=f"{intensity_pct:.2f}%", # 显示 5.20% (即 OI 占了流通盘的 5.2%)
                delta=f"MC: ${mc_str}", # 显示市值作为参考
                delta_color="off" # 灰色显示市值
            )
            
        # 顺便把这 Top 5 的图表直接画出来？用户说"图标一并列出"
        # 我们可以在下面直接展示这 5 个图表，或者只在下面大列表中高亮
    
    st.markdown("---")

    # --- 大列表渲染 ---
    # 按照 OI 美元总量排序 (默认逻辑)
    for rank, symbol in enumerate(target_symbols, 1):
        raw_df = bulk_data.get(symbol)
        
        coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"
        title_color = "black"
        chart = None
        info_html = ""
        
        if raw_df is not None and not raw_df.empty:
            start_p = raw_df['标记价格 (USDC)'].iloc[0]
            end_p = raw_df['标记价格 (USDC)'].iloc[-1]
            title_color = "#009900" if end_p >= start_p else "#D10000"
            
            # 获取强度信息
            item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)
            
            if item_stats:
                # 强度显示
                int_val = item_stats['intensity'] * 100
                int_color = "#d62728" if int_val > 5 else ("#009900" if int_val > 1 else "#555")
                # OI 增量显示
                inflow = item_stats['oi_change_usd']
                inflow_str = format_number(inflow)
                prefix = "+" if inflow > 0 else ""
                
                info_html = (
                    f'<span style="font-size: 16px; margin-left: 15px; color: #666;">'
                    f'强度: <span style="color: {int_color}; font-weight: bold;">{int_val:.2f}%</span>'
                    f'<span style="margin: 0 8px;">|</span>'
                    f'净流入: <span style="color: {"green" if inflow>0 else "red"};">{prefix}${inflow_str}</span>'
                    f'</span>'
                )

            chart_df = downsample_data(raw_df, target_points=400)
            chart = create_dual_axis_chart(chart_df, symbol)

        expander_title_html = (
            f'<div style="text-align: center; margin-bottom: 5px;">'
            f'<a href="{coinglass_url}" target="_blank" '
            f'style="text-decoration:none; color:{title_color}; font-weight:bold; font-size:22px;">'
            f'#{rank} {symbol} </a>'
            f'{info_html}'
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

