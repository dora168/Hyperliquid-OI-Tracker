import streamlit as st
import streamlit.components.v1 as components
import requests
import pandas as pd

# --- 核心配置 ---
# 如果 API 失败，使用这份静态的 Hyperliquid 热门币种列表作为保底
FALLBACK_SYMBOLS = [
    "BTC", "ETH", "SOL", "HYPE", "PURR", "WIF", "PEPE", "DOGE", "AVAX", "SUI",
    "ARB", "OP", "TIA", "INJ", "LINK", "ORDI", "RNDR", "NEAR", "STX", "GMX",
    "ATOM", "DYDX", "APT", "SEI", "BLUR", "PYTH", "JUP", "STRK", "ENA", "PENDLE"
]

# --- 组件：渲染 TradingView Widget ---
def render_tradingview_widget(symbol, height=400):
    """
    渲染嵌入 Open Interest (OI) 指标的 TradingView Widget
    """
    # 清洗数据
    clean_symbol = symbol.upper().strip()
    
    # Hyperliquid 的 symbol 通常不带 USDT，但 TradingView 需要。
    # 大部分 Hyperliquid 的资产在币安都有，所以我们要构建 BINANCE 格式
    # 特殊处理：如果是 kPEPE 等转换过的名字，或者只有 HL 才有的币，
    # TradingView 可能找不到。这里默认尝试构建为币安永续。
    
    # 移除可能的 "-USD" 或 "USDT" 后缀
    base_symbol = clean_symbol.replace("-USD", "").replace("USDT", "")
    
    # 构造 TradingView 能够识别的代码
    # 注意：如果 TradingView 找不到 BINANCE:{base_symbol}USDT.P，图表会显示 Invalid Symbol
    tv_symbol = f"BINANCE:{base_symbol}USDT.P"
    
    container_id = f"tv_{base_symbol}"

    html_code = f"""
    <div class="tradingview-widget-container" style="height: {height}px; width: 100%;">
      <div id="{container_id}" style="height: 100%; width: 100%;"></div>
      <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
      <script type="text/javascript">
      new TradingView.widget(
      {{
        "autosize": true,
        "symbol": "{tv_symbol}",
        "interval": "60",
        "timezone": "Asia/Shanghai",
        "theme": "light",
        "style": "1",
        "locale": "zh_CN",
        "enable_publishing": false,
        "hide_top_toolbar": true,
        "hide_legend": false,
        "save_image": false,
        "container_id": "{container_id}",
        "studies": [
            "MASimple@tv-basicstudies",     
            "STD;Fund_crypto_open_interest"
        ],
        "disabled_features": [
            "header_symbol_search", "header_compare", "use_localstorage_for_settings", 
            "display_market_status", "timeframes_toolbar", "volume_force_overlay",
            "header_chart_type", "header_settings", "header_indicators"
        ]
      }}
      );
      </script>
    </div>
    """
    components.html(html_code, height=height, scrolling=False)

# --- 数据获取：Hyperliquid API ---
@st.cache_data(ttl=60) # 缓存1分钟，Hyperliquid 数据更新很快
def get_hyperliquid_top_volume(limit=80):
    """
    从 Hyperliquid API 获取 24小时成交量排名的资产
    """
    url = "https://api.hyperliquid.xyz/info"
    headers = {"Content-Type": "application/json"}
    
    # Hyperliquid 的 API 需要通过 POST 请求获取 meta info
    payload = {"type": "metaAndAssetCtxs"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            universe = data[0]['universe']   # 资产列表 (symbol 信息)
            asset_ctxs = data[1]             # 资产上下文 (包含 volume, price 等)

            # 将两个列表组合起来
            combined_data = []
            for i, asset_info in enumerate(universe):
                if i < len(asset_ctxs):
                    ctx = asset_ctxs[i]
                    # 获取 24h Volume (dayNtlVlm)
                    # 注意：Hyperliquid API 返回的 volume 是名义价值 (Notional Volume)
                    volume = float(ctx.get('dayNtlVlm', 0))
                    symbol = asset_info['name']
                    combined_data.append({"symbol": symbol, "volume": volume})
            
            # 按成交量降序排序
            sorted_data = sorted(combined_data, key=lambda x: x['volume'], reverse=True)
            
            # 提取前 N 名的 symbol
            top_symbols = [item['symbol'] for item in sorted_data[:limit]]
            
            return top_symbols, "Hyperliquid API"
            
    except Exception as e:
        print(f"Hyperliquid API Error: {e}")
        pass

    # 如果 API 失败，返回保底列表
    return FALLBACK_SYMBOLS, "离线保底列表 (API 连接受限)"

# --- 主程序逻辑 ---
def main():
    st.set_page_config(layout="wide", page_title="Hyperliquid OI Wall")
    
    st.title("💧 Hyperliquid 成交量 Top 80 - OI 监控墙")

    # 1. 获取数据
    with st.spinner("正在从 Hyperliquid 链上获取实时数据..."):
        symbols, source_type = get_hyperliquid_top_volume(80)

    # 2. 侧边栏控制
    with st.sidebar:
        st.header("⚙️ 控制面板")
        
        # 显示数据源状态
        if "离线" in source_type:
            st.error(f"⚠️ 数据源：{source_type}")
            st.caption("无法连接 Hyperliquid API，显示预设热门币种。")
        else:
            st.success(f"✅ 数据源：{source_type}")
            st.caption(f"已按 24H 成交量排序获取前 {len(symbols)} 名")

        if st.button("强制刷新数据"):
            st.cache_data.clear()
            st.rerun()
            
        st.markdown("---")
        
        # 分页设置
        total_items = len(symbols)
        # 默认每页 20 个，80 个需要 4 页，体验较好
        items_per_page = st.select_slider("每页显示数量", options=[10, 20, 40, 80], value=20)
        
        # 计算页数
        total_pages = (total_items + items_per_page - 1) // items_per_page
        current_page = st.number_input(f"页码 (共 {total_pages} 页)", min_value=1, max_value=total_pages, value=1)

    # 3. 数据切片
    start_idx = (current_page - 1) * items_per_page
    end_idx = min(start_idx + items_per_page, total_items)
    current_batch = symbols[start_idx:end_idx]

    # 4. 页面显示
    st.markdown(f"**当前显示：按成交量排名第 {start_idx + 1} - {end_idx} 名**")
    
    # 渲染图表 Grid
    cols = st.columns(2) # 两列布局
    for i, sym in enumerate(current_batch):
        with cols[i % 2]:
            # Hyperliquid 交易链接
            hl_url = f"https://app.hyperliquid.xyz/trade/{sym}"
            
            # 标题栏：显示排名和币种，点击跳转到 Hyperliquid 交易界面
            st.markdown(f"#### #{start_idx + i + 1} [{sym}]({hl_url})")
            
            # 渲染 TradingView
            # 提示：有些 Hyperliquid 独有的币（如 HYPE）可能在 TradingView 只有现货图表或没有图表
            render_tradingview_widget(sym)
            st.markdown("---")

    if end_idx >= total_items:
        st.success("🎉 已显示全部加载的币种。")

if __name__ == "__main__":
    main()






