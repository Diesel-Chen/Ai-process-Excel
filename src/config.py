"""
配置文件，存储所有设置和常量
"""

# OpenAI配置
OPENAI_API_KEY = "111"  # 替换为您的API密钥
OPENAI_MODEL = "gpt-4"
OPENAI_TEMPERATURE = 0.7

# 市场数据URL配置
CURRENCY_PAIRS = {
    'USD CNY': 'https://cn.investing.com/currencies/usd-cny-historical-data',
    'EUR CNY': 'https://cn.investing.com/currencies/eur-cny-historical-data',
    'EUR USD': 'https://cn.investing.com/currencies/eur-usd-historical-data',
    'HKD CNY': 'https://cn.investing.com/currencies/hkd-cny-historical-data',
    'CNY HKD': 'https://cn.investing.com/currencies/cny-hkd-historical-data',
    'JPY USD': 'https://cn.investing.com/currencies/jpy-usd-historical-data',
    'USD 10Y': 'https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data',
}

# 系统提示配置
SYSTEM_PROMPT = "你是一个专业的金融市场分析师，擅长分析货币市场数据。"

# 分析提示模板
ANALYSIS_PROMPT_TEMPLATE = """
请分析以下金融市场数据URL的内容: {url}
我需要获取以下信息:
1. 当前汇率
2. 今日涨跌幅
3. 近期趋势分析
4. 重要的市场影响因素

请以结构化的方式提供这些信息。
"""

# 日志配置
LOG_LEVEL = "INFO"

# Excel配置
EXCEL_OUTPUT_PATH = "data/market_index.xlsx"

