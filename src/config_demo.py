"""
配置文件，存储所有设置和常量
"""

# OpenAI配置
OPENAI_API_KEY = "111"  # 替换为您的API密钥
OPENAI_MODEL = "gpt-4"
OPENAI_TEMPERATURE = 0.7

# 原有的汇率数据对保持不变
CURRENCY_PAIRS = {
    'USD CNY': 'https://cn.investing.com/currencies/usd-cny-historical-data',
    'EUR CNY': 'https://cn.investing.com/currencies/eur-cny-historical-data',
    'EUR USD': 'https://cn.investing.com/currencies/eur-usd-historical-data',
    'HKD CNY': 'https://cn.investing.com/currencies/hkd-cny-historical-data',
    'CNY HKD': 'https://cn.investing.com/currencies/cny-hkd-historical-data',
    'JPY USD': 'https://cn.investing.com/currencies/usd-jpy-historical-data',
    'USD 10Y': 'https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data'
}

# 日频数据配对，每个数据源都包含URL和对应的爬虫方法
DAILY_DATA_PAIRS = {
    'Steel price': {
        'url': 'https://index.mysteel.com/xpic/detail.html?tabName=kuangsi',
        'crawler': 'crawl_steel_price'
    },
    'Shibor': {
        'url': 'https://www.shibor.org/shibor/index.html',
        'crawler': 'crawl_shibor_rate'
    },
    'LPR': {
        'url': 'https://www.shibor.org/shibor/index.html',
        'crawler': 'crawl_lpr'
    },
    'SOFR': {
        'url': 'https://www.newyorkfed.org/markets/reference-rates/sofr',
        'crawler': 'crawl_sofr'
    },
    'ESTER': {
        'url': 'https://www.euribor-rates.eu/en/ester/',
        'crawler': 'crawl_ester'
    },
    'JPY rate': {
        'url': 'https://www.global-rates.com/en/interest-rates/central-banks/9/japanese-boj-overnight-call-rate/',
        'crawler': 'crawl_jpy_rate'
    }
}

# 月度数据配对，每个数据源都包含URL和对应的爬虫方法
MONTHLY_DATA_PAIRS = {
    'US Interest Rate': {
        'url': 'https://data.eastmoney.com/cjsj/foreign_0_22.html',
        'crawler': 'crawl_us_interest_rate'
    },
    'Import and Export': {
        'url': 'https://data.eastmoney.com/cjsj/hgjck.html',
        'crawler': 'crawl_import_export'
    },
    'Money Supply': {
        'url': 'https://data.eastmoney.com/cjsj/hbgyl.html',
        'crawler': 'crawl_money_supply'
    },
    'PPI': {
        'url': 'https://data.eastmoney.com/cjsj/ppi.html',
        'crawler': 'crawl_ppi'
    },
    'CPI': {
        'url': 'https://data.eastmoney.com/cjsj/cpi.html',
        'crawler': 'crawl_cpi'
    },
    'PMI': {
        'url': 'https://data.eastmoney.com/cjsj/pmi.html',
        'crawler': 'crawl_pmi'
    },
    'New Bank Loan Addition': {
        'url': 'https://data.eastmoney.com/cjsj/xzxd.html',
        'crawler': 'crawl_new_bank_loan_addition'
    }
}

# Excel表格中各数据类型的列定义
COLUMN_DEFINITIONS = {
    # 汇率和美债数据列
    'CURRENCY': ['日期', '收盘', '开盘', '高', '低', '涨跌幅'],

    # 日频数据列定义
    'Steel price': ['日期', '本日', '昨日', '日环比', '上周', '周环比', '上月度', '与上月比', '去年同期', '与去年比'],
    'Shibor': ['日期', 'O/N', '1W', '2W', '1M', '3M', '6M', '9M', '1Y'],
    'LPR': ['日期', '1Y', '5Y'],
    'SOFR': ['日期', 'RATE(%)', '1ST PERCENTILE(%)', '25TH PERCENTILE(%)', '75TH PERCENTILE(%)', '99TH PERCENTILE(%)', 'VOLUME ($Billions)'],
    'ESTER': ['日期', 'value'],
    'JPY rate': ['日期', 'value'],

    # 月度数据列定义
    'US Interest Rate': ['日期', '前值', '现值', '发布日期'],
    'Import and Export': ['日期', '当月出口额金额', '当月出口额同比增长', '当月出口额环比增长',
                         '当月进口额金额', '当月进口额同比增长', '当月进口额环比增长',
                         '累计出口额金额', '累计出口额同比增长', '累计进口额金额', '累计进口额同比增长'],
    'Money Supply': ['日期', 'M2数量', 'M2同比增长', 'M2环比增长',
                    'M1数量', 'M1同比增长', 'M1环比增长',
                    'M0数量', 'M0同比增长', 'M0环比增长'],
    'PPI': ['日期', '当月', '当月同比增长', '累计'],
    'CPI': ['日期', '全国当月', '全国同比增长', '全国环比增长', '全国累计',
            '城市当月', '城市同比增长', '城市环比增长', '城市累计',
            '农村当月', '农村同比增长', '农村环比增长', '农村累计'],
    'PMI': ['日期', '制造业指数', '制造业同比增长', '非制造业指数', '非制造业同比增长'],
    'New Bank Loan Addition': ['日期', '当月', '同比增长', '环比增长', '累计', '同比增长']
}

# 日志级别设置
LOG_LEVEL = 'INFO'



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

