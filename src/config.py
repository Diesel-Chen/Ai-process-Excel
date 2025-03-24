"""
配置文件，存储所有设置和常量
"""
import os
import sys
import platform

# 获取项目根目录路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # 适用于src目录结构

# 日志级别设置
LOG_LEVEL = 'INFO'

# 获取资源文件路径的函数
def resource_path(relative_path):
    """获取资源的绝对路径，适用于开发环境和PyInstaller打包后的环境，支持Windows和macOS"""
    # 标准化路径分隔符，确保跨平台兼容
    relative_path = relative_path.replace('/', os.sep).replace('\\', os.sep)

    # 如果是相对路径中包含目录，提取文件名
    file_name = os.path.basename(relative_path)

    # 获取可执行文件所在目录
    if getattr(sys, 'frozen', False):
        # 如果是打包后的应用程序
        application_path = os.path.dirname(sys.executable)
        print(f"应用程序路径: {application_path}")

        # 首先尝试在可执行文件所在目录直接查找文件
        exec_dir_path = os.path.join(application_path, file_name)
        if os.path.exists(exec_dir_path):
            print(f"在可执行文件目录找到文件: {exec_dir_path}")
            return exec_dir_path

        # 如果没找到，尝试原始相对路径
        original_path = os.path.join(application_path, relative_path)
        if os.path.exists(original_path):
            print(f"在可执行文件目录的原始路径找到文件: {original_path}")
            return original_path

        # Windows特有：检查程序目录下的data子目录
        if platform.system() == "Windows":
            win_data_path = os.path.join(application_path, "data", file_name)
            if os.path.exists(win_data_path):
                print(f"在Windows应用程序的data目录找到文件: {win_data_path}")
                return win_data_path

    # 首先尝试在当前工作目录直接查找文件
    current_dir_file = os.path.join(os.getcwd(), file_name)
    if os.path.exists(current_dir_file):
        print(f"在当前工作目录找到文件: {current_dir_file}")
        return current_dir_file

    # 如果没找到，尝试原始相对路径
    current_dir_path = os.path.join(os.getcwd(), relative_path)
    if os.path.exists(current_dir_path):
        print(f"在当前工作目录的原始路径找到文件: {current_dir_path}")
        return current_dir_path

    # Windows特有：检查当前目录下的data子目录
    if platform.system() == "Windows":
        win_cwd_data_path = os.path.join(os.getcwd(), "data", file_name)
        if os.path.exists(win_cwd_data_path):
            print(f"在Windows当前目录的data子目录找到文件: {win_cwd_data_path}")
            return win_cwd_data_path

    # 然后尝试在项目根目录查找文件
    base_dir_file = os.path.join(BASE_DIR, file_name)
    if os.path.exists(base_dir_file):
        print(f"在项目根目录找到文件: {base_dir_file}")
        return base_dir_file

    base_dir_path = os.path.join(BASE_DIR, relative_path)
    if os.path.exists(base_dir_path):
        print(f"在项目根目录的原始路径找到文件: {base_dir_path}")
        return base_dir_path

    # Windows特有：检查项目根目录下的data子目录
    if platform.system() == "Windows":
        win_base_data_path = os.path.join(BASE_DIR, "data", file_name)
        if os.path.exists(win_base_data_path):
            print(f"在Windows项目根目录的data子目录找到文件: {win_base_data_path}")
            return win_base_data_path

    # 如果是打包环境，尝试在打包目录查找
    try:
        if hasattr(sys, '_MEIPASS'):
            # PyInstaller创建临时文件夹，将路径存储在_MEIPASS中
            base_path = sys._MEIPASS

            # 尝试直接在临时目录根目录查找文件名
            root_path = os.path.join(base_path, file_name)
            if os.path.exists(root_path):
                print(f"在PyInstaller临时目录根目录找到文件: {root_path}")
                return root_path

            # 尝试原始相对路径
            full_path = os.path.join(base_path, relative_path)
            if os.path.exists(full_path):
                print(f"在PyInstaller临时目录的原始路径找到文件: {full_path}")
                return full_path

            # Windows特有：检查临时目录下的data子目录
            if platform.system() == "Windows":
                win_temp_data_path = os.path.join(base_path, "data", file_name)
                if os.path.exists(win_temp_data_path):
                    print(f"在Windows临时目录的data子目录找到文件: {win_temp_data_path}")
                    return win_temp_data_path
    except Exception as e:
        print(f"在打包环境中查找文件时出错: {e}")

    # 如果所有尝试都失败，抛出错误
    raise FileNotFoundError(f"找不到文件: {file_name}。请确保文件存在于正确的位置。")

# 原有的汇率数据对保持不变
CURRENCY_PAIRS = {
    'USD CNY': 'https://cn.investing.com/currencies/usd-cny-historical-data',
    'EUR CNY': 'https://cn.investing.com/currencies/eur-cny-historical-data',
    'EUR USD': 'https://cn.investing.com/currencies/eur-usd-historical-data',
    'HKD CNY': 'https://cn.investing.com/currencies/hkd-cny-historical-data',
    'CNY HKD': 'https://cn.investing.com/currencies/cny-hkd-historical-data',
    'JPY USD': 'https://cn.investing.com/currencies/jpy-usd-historical-data',
    'USD 10Y': 'https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data'
}

# 日频数据配对，每个数据源都包含URL和对应的爬虫方法
DAILY_DATA_PAIRS = {
    'Steel price': {
        'url': 'https://index.mysteel.com/xpic/detail.html?tabName=kuangsi',
        'crawler': 'crawl_steel_price'
    },
    'Shibor': {
        'url': 'https://www.shibor.org/shibor/web/html/shibor.html',
        'crawler': 'crawl_shibor_rate'
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
    },
    'Shibor': {
        'url': 'https://www.shibor.org/shibor/shiborquote/',
        'crawler': 'crawl_shibor_rate'
    },
    'LPR': {
        'url': 'https://www.shibor.org/shibor/lprquote/',
        'crawler': 'crawl_lpr'
    },
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
    'CURRENCY': ['日期', '收盘', '开盘', '高', '低','交易量', '涨跌幅'],
    'USD 10Y': ['日期', '收盘', '开盘', '高', '低', '涨跌幅'],

    # 日频数据列定义
    'Steel price': ['日期', '本日', '昨日', '日环比', '上周', '周环比', '上月度', '与上月比', '去年同期', '与去年比'],
    'Shibor': ['日期', 'O/N', '1W', '2W', '1M', '3M', '6M', '9M', '1Y'],
    'LPR': ['日期', '1Y', '5Y','PBOC_(6M-1Y)', 'rowPBOC_(>5Y)'],
    'SOFR': ['日期', 'Rate Type','RATE(%)', '1ST PERCENTILE(%)', '25TH PERCENTILE(%)', '75TH PERCENTILE(%)', '99TH PERCENTILE(%)', 'VOLUME ($Billions)'],
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
    'New Bank Loan Addition': ['日期', '当月', '同比增长', '环比增长', '累计', '累计同比增长']
}

# Excel配置（使用适用于打包环境的路径）
# 直接使用文件名，不包含data目录
EXCEL_OUTPUT_PATH = resource_path("Market Index.xlsx")

