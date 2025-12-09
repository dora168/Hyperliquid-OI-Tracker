import streamlit as st
import pandas as pd
import altair as alt
import os
import connectorx as cx  # <--- 引入 Rust 编写的高性能加载器
from urllib.parse import quote_plus

# --- A. 数据库配置 (保持不变) ----
DB_HOST = os.getenv("DB_HOST") or st.secrets.get("DB_HOST", "cd-cdb-p6vea42o.sql.tencentcdb.com")
DB_PORT = int(os.getenv("DB_PORT") or st.secrets.get("DB_PORT", 24197))
DB_USER = os.getenv("DB_USER") or st.secrets.get("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD") or st.secrets.get("DB_PASSWORD", None) 
DB_CHARSET = 'utf8mb4'
DB_NAME_OI = 'open_interest_db'
DB_NAME_SUPPLY = 'circulating_supply'

# 优化策略：虽然只取400点绘图，但为了计算准确的 min/max，我们可以在 SQL 里做某种程度的预聚合，
# 或者只取必要的点。这里我们采用 "间隔采样" 策略。
DATA_LIMIT_RAW = 4000 
SAMPLE_STEP = 4  # SQL层面每10行取1行，将数据量直接减少90%

# --- B. 数据库功能 (Rust 加速版) ---

@st.cache_resource
def get_db_uri(db_name):
    """构建 connectorx 需要的连接字符串 (mysql://...)"""
    if not DB_PASSWORD:
        st.error("❌ 数据库密码未配置。")
        st.stop()
    # 对密码进行 URL 编码，防止特殊字符导致连接失败
    safe_pwd = quote_plus(DB_PASSWORD)
    return f"mysql://{DB_USER}:{safe_pwd}@{DB_HOST}:{DB_PORT}/{db_name}?charset={DB_CHARSET}"

@st.cache_data(ttl=300) # 流通量不常变，缓存久一点
def fetch_circulating_supply():
    try:
        uri = get_db_uri(DB_NAME_SUPPLY)
        query = f"SELECT symbol, circulating_supply, market_cap FROM `{DB_NAME_SUPPLY}`"
        # 使用 Rust 引擎读取，速度极快
        df = cx.read_sql(uri, query)
        return df.set_index('symbol').to_dict('index')
    except Exception as e:
        print(f"⚠️ 流通量数据读取失败: {e}")
        return {}

@st.cache_data(ttl=60)
def get_sorted_symbols_by_oi_usd():
    try:
        uri = get_db_uri(DB_NAME_OI)
        # 获取列表只需极少数据，非常快
        query = "SELECT symbol FROM `hyperliquid` GROUP BY symbol ORDER BY MAX(oi_usd) DESC"
        df = cx.read_sql(uri, query)
        return df['symbol'].tolist()
    except Exception as e:
        st.error(f"❌ 列表获取失败: {e}")
        return []

@st.cache_data(ttl=60, show_spinner=False)
def fetch_bulk_data_one_shot(symbol_list):
    if not symbol_list: return {}
    
    # 构造 SQL IN 子句的字符串
    symbols_str = "', '".join(symbol_list)
    
    # 🌟 核心优化 SQL 🌟
    # 1. 使用 MOD(rn, 10) = 1 在数据库端直接过滤 90% 的数据
    # 2. 这样传输到 Python 的数据只有 4000/10 = 400 行左右，完美适配绘图，无需再做 downsample
    sql_query = f"""
    WITH RankedData AS (
        SELECT symbol, `time`, `price`, `oi`,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY `time` DESC) as rn
        FROM `hyperliquid`
        WHERE symbol IN ('{symbols_str}')
    )
    SELECT symbol, `time`, `price` AS `标记价格 (USDC)`, `oi` AS `未平仓量`
    FROM RankedData
    WHERE rn <= {DATA_LIMIT_RAW} 
    AND (rn = 1 OR rn % {SAMPLE_STEP} = 0) -- 保留最新一条(rn=1)和每隔N条的数据
    ORDER BY symbol, `time` ASC;
    """
    
    try:
        uri = get_db_uri(DB_NAME_OI)
        # ConnectorX (Rust) 直接将 SQL 结果写入 Pandas 内存，零拷贝，极快
        df_all = cx.read_sql(uri, sql_query)
        
        if df_all.empty: return {}
        
        # 转换时间列 (ConnectorX 有时返回 str 有时返回 datetime，确保统一)
        if not pd.api.types.is_datetime64_any_dtype(df_all['time']):
            df_all['time'] = pd.to_datetime(df_all['time'])

        return {sym: group for sym, group in df_all.groupby('symbol')}
    except Exception as e:
        st.error(f"⚠️ 数据查询失败: {e}")
        return {}

# --- C. 辅助与绘图 (微调) ---

# 注意：由于我们在 SQL 里已经做了降采样，Python 里的 downsample_data 函数可以简化或移除
# 为了兼容性，我们可以保留它做一个简单的检查

def downsample_data(df, target_points=400):
    # 如果数据量已经很小（因为 SQL 过滤过了），直接返回
    if len(df) <= target_points * 1.5: 
        return df
    return df.iloc[::len(df)//target_points]

# ... (其余 C 和 D 部分的代码保持不变，因为绘图逻辑不需要动) ...

# 将你的 main_app 等其余代码粘贴在下面即可



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

    ).properties(height=450) # 保持高清高度

    return chart



def render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False, list_type=""):

    """

    渲染单个图表组件

    list_type: 用于区分 'strength' 或 'whale'，方便生成唯一的 key

    """

    raw_df = bulk_data.get(symbol)

    coinglass_url = f"https://www.coinglass.com/tv/zh/Hyperliquid_{symbol}-USD"

    title_color = "black"

    chart = None

    info_html = ""

    

    if raw_df is not None and not raw_df.empty:

        start_p = raw_df['标记价格 (USDC)'].iloc[0]

        end_p = raw_df['标记价格 (USDC)'].iloc[-1]

        title_color = "#009900" if end_p >= start_p else "#D10000"

        

        # 获取统计信息

        item_stats = next((item for item in ranking_data if item["symbol"] == symbol), None)

        if item_stats:

            int_val = item_stats['intensity'] * 100

            int_color = "#d62728" if int_val > 5 else ("#009900" if int_val > 1 else "#555")

            growth_usd = item_stats['oi_growth_usd']

            growth_str = format_number(growth_usd)

            

            info_html = (

                f'<span style="font-size: 14px; margin-left: 10px; color: #666;">' # 字体稍微调小适应分栏

                f'强度:<span style="color: {int_color}; font-weight: bold;">{int_val:.1f}%</span>'

                f'<span style="margin: 0 4px;">|</span>'

                f'增量:<span style="color: #009900; font-weight: bold;">+${growth_str}</span>'

                f'</span>'

            )



        chart_df = downsample_data(raw_df, target_points=400)

        chart = create_dual_axis_chart(chart_df, symbol)



    # 标题生成

    fire_icon = "🔥" if list_type == "strength" else ("🐳" if list_type == "whale" else "")

    expander_title_html = (

        f'<div style="text-align: center; margin-bottom: 5px;">'

        f'{fire_icon} '

        f'<a href="{coinglass_url}" target="_blank" '

        f'style="text-decoration:none; color:{title_color}; font-weight:bold; font-size:20px;">' # 字体稍微调小

        f' {symbol} </a>'

        f'{info_html}'

        f'</div>'

    )

    

    if is_top_mover:

        label = f"{fire_icon} {symbol}"

    else:

        label = f"#{rank} {symbol}"



    # 这里的 expanded=True 配合 use_container_width=True 会自动适应左右分栏的宽度（变窄）

    with st.expander(label, expanded=True):

        st.markdown(expander_title_html, unsafe_allow_html=True)

        if chart:

            st.altair_chart(chart, use_container_width=True)

        else:

            st.info("暂无数据")



# --- D. 主程序 ---



def main_app():

    st.set_page_config(layout="wide", page_title="Hyperliquid OI Dashboard")

    st.title("⚡ OI 双塔监控 (强度 vs 巨鲸)")

    

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

        current_price = df['标记价格 (USDC)'].iloc[-1]

        

        min_oi = df['未平仓量'].min()

        current_oi = df['未平仓量'].iloc[-1]

        oi_growth_tokens = current_oi - min_oi

        oi_growth_usd = oi_growth_tokens * current_price

        

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

            "market_cap": market_cap

        })



    # ==========================

    # 榜单指标区 (Metric Lists)

    # ==========================

    col_left, col_right = st.columns(2)

    

    # 准备数据

    top_intensity = []

    top_whales = []

    if ranking_data:

        top_intensity = sorted(ranking_data, key=lambda x: x['intensity'], reverse=True)[:10]

        top_whales = sorted(ranking_data, key=lambda x: x['oi_growth_usd'], reverse=True)[:10]


# --- 左侧指标：Top 10 强度 ---

    with col_left:

        st.subheader("🔥 Top 10 强度榜 (相对比例)")

        st.caption("逻辑：(当前OI - 最低OI) / 市值。")

        st.markdown("---")

        for i, item in enumerate(top_intensity):

            st.metric(

                label=f"No.{i+1} {item['symbol']}",

                value=f"{item['intensity']*100:.2f}%",

                delta=f"MC: ${format_number(item['market_cap'])}",

                delta_color="off"

            )

            st.markdown("""<hr style="margin: 5px 0; border-top: 1px dashed #eee;">""", unsafe_allow_html=True)

    

    # --- 右侧指标：Top 10 巨鲸 ---

    with col_right:

        st.subheader("🐳 Top 10 巨鲸榜 (绝对金额)")

        st.caption("逻辑：(当前OI - 最低OI) * 价格。")

        st.markdown("---")

        for i, item in enumerate(top_whales):

            st.metric(

                label=f"No.{i+1} {item['symbol']}",

                value=f"+${format_number(item['oi_growth_usd'])}",

                delta="资金净流入",

                delta_color="normal"

            )

            st.markdown("""<hr style="margin: 5px 0; border-top: 1px dashed #eee;">""", unsafe_allow_html=True)

    

    st.markdown("---")
    

    # ==========================

    # 双塔图表区 (Charts) - 左右并列

    # ==========================

    

    chart_col_left, chart_col_right = st.columns(2)

    

    # --- 左塔：Top 10 强度图表 ---

    with chart_col_left:

        st.subheader("📈 强度 Top 10 走势")

        if top_intensity:

            for i, item in enumerate(top_intensity, 1):

                # 放在半宽的 column 里，Streamlit 会自动缩小图表宽度

                render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True, list_type="strength")

        else:

            st.info("暂无数据")



    # --- 右塔：Top 10 巨鲸图表 ---

    with chart_col_right:

        st.subheader("📈 巨鲸 Top 10 走势")

        if top_whales:

            for i, item in enumerate(top_whales, 1):

                render_chart_component(i, item['symbol'], bulk_data, ranking_data, is_top_mover=True, list_type="whale")

        else:

            st.info("暂无数据")

    

    st.markdown("---")

    st.subheader("📋 其他合约列表 (已去重)")



    # --- 底部：剩余列表 (去重) ---

    # 收集已经在上面两个榜单里展示过的 symbol

    shown_symbols = set()

    for item in top_intensity: shown_symbols.add(item['symbol'])

    for item in top_whales: shown_symbols.add(item['symbol'])

    

    # 过滤

    remaining_symbols = [s for s in target_symbols if s not in shown_symbols]



    # 全宽展示剩余的

    for rank, symbol in enumerate(remaining_symbols, 1):

        render_chart_component(rank, symbol, bulk_data, ranking_data, is_top_mover=False)



if __name__ == '__main__':

    main_app()



