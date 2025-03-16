import requests
import pandas as pd
import logging
from datetime import datetime
from openai import OpenAI
import config
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import time
import random
from openpyxl import load_workbook
from openpyxl.styles import Alignment

import platform
from datetime import datetime
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException, WebDriverException

import time
import concurrent.futures

# 在脚本开头导入并配置连接池
from urllib3.poolmanager import PoolManager
import urllib3

# 增加连接池大小
urllib3.connection_from_url = lambda url, **kw: PoolManager(num_pools=10, maxsize=10, **kw).connection_from_url(url)

# 设置日志
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

def log_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"{func.__name__} 执行时间: {elapsed_time:.2f} 秒")
        return result
    return wrapper

def retry_on_timeout(func):
    """重试装饰器，用于处理超时情况"""
    def wrapper(*args, **kwargs):
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                return func(*args, **kwargs)
            except TimeoutException:
                retry_count += 1
                logger.warning(f"第{retry_count}次尝试超时，正在重试...")
                if retry_count >= max_retries:
                    logger.error(f"已达到最大重试次数({max_retries})，放弃尝试")
                    return None
                # 每次重试增加等待时间
                time.sleep(2 * retry_count)
            except Exception as e:
                logger.error(f"发生非超时错误: {str(e)}")
                return None
    return wrapper

class MarketDataAnalyzer:
    _driver = None
    _driver_lock = False  # 简单锁，防止并发初始化

    def __init__(self):
        print("初始化市场数据分析器...")
        # 初始化User-Agent生成器
        self.ua = UserAgent()
        # 预先初始化WebDriver
        self._init_driver()

    def _init_driver(self):
        """
        优化的WebDriver初始化方法
        """
        if MarketDataAnalyzer._driver is not None:
            return

        if MarketDataAnalyzer._driver_lock:
            # 等待其他线程初始化完成
            wait_count = 0
            while MarketDataAnalyzer._driver is None and wait_count < 30:
                time.sleep(0.5)
                wait_count += 1
            if MarketDataAnalyzer._driver is not None:
                return

        MarketDataAnalyzer._driver_lock = True

        try:
            # 检测操作系统
            system = platform.system()
            logger.info(f"检测到操作系统: {system}")

            # 设置通用选项
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument(f'user-agent={UserAgent().random}')

            # 设置页面加载策略为eager，加快加载速度
            options.page_load_strategy = 'eager'

            # 禁用图片加载，提高性能
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2
            }
            options.add_experimental_option("prefs", prefs)

            # 根据操作系统选择合适的驱动
            try:
                # 首先尝试Chrome
                driver_path = ChromeDriverManager().install()  # 移除 cache_valid_range 参数
                service = Service(executable_path=driver_path)
                MarketDataAnalyzer._driver = webdriver.Chrome(service=service, options=options)
                logger.info("成功初始化 Chrome WebDriver")
            except Exception as e:
                logger.warning(f"Chrome WebDriver 初始化失败: {str(e)}")

                try:
                    # 尝试Edge
                    edge_options = webdriver.EdgeOptions()
                    for arg in options.arguments:
                        edge_options.add_argument(arg)
                    edge_options.page_load_strategy = 'eager'

                    driver_path = EdgeChromiumDriverManager().install()  # 移除 cache_valid_range 参数
                    service = Service(executable_path=driver_path)
                    MarketDataAnalyzer._driver = webdriver.Edge(service=service, options=edge_options)
                    logger.info("成功初始化 Edge WebDriver")
                except Exception as e:
                    logger.warning(f"Edge WebDriver 初始化失败: {str(e)}")

                    try:
                        # 最后尝试Firefox
                        firefox_options = webdriver.FirefoxOptions()
                        for arg in options.arguments:
                            if not arg.startswith('--disable-dev-shm-usage') and not arg.startswith('--no-sandbox'):
                                firefox_options.add_argument(arg)
                        firefox_options.page_load_strategy = 'eager'

                        driver_path = GeckoDriverManager().install()  # 移除 cache_valid_range 参数
                        service = Service(executable_path=driver_path)
                        MarketDataAnalyzer._driver = webdriver.Firefox(service=service, options=firefox_options)
                        logger.info("成功初始化 Firefox WebDriver")
                    except Exception as e:
                        logger.error(f"所有WebDriver初始化失败: {str(e)}")
                        raise Exception("无法初始化任何WebDriver")

        except Exception as e:
            logger.error(f"WebDriver初始化失败: {str(e)}")
            MarketDataAnalyzer._driver_lock = False
            raise

        MarketDataAnalyzer._driver_lock = False

    @classmethod
    def get_driver(cls):
        """
        获取WebDriver实例，如果不存在则初始化
        """
        if cls._driver is None:
            instance = cls()

        # 检查驱动是否仍然有效
        try:
            if cls._driver is not None:
                cls._driver.current_url  # 尝试访问属性以检查驱动是否仍然有效
        except (WebDriverException, Exception) as e:
            logger.warning(f"WebDriver已失效，重新初始化: {str(e)}")
            cls._driver = None
            instance = cls()

        return cls._driver

    def close_driver(self):
        """
        关闭WebDriver实例
        """
        if self.__class__._driver:
            try:
                self.__class__._driver.quit()
            except Exception as e:
                logger.warning(f"关闭WebDriver时出错: {str(e)}")
            finally:
                self.__class__._driver = None

    def format_exchange_rate_date(self,raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%m月 %d, %Y")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def format_stee_price_date(self,raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%Y/%m/%d")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def format_shibor_rate_date(self,raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%Y-%m-%d")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def format_sofr_date(self, raw_date):
        # 获取当前年份
        current_year = datetime.now().year
        # 拼接年份、月份和日期
        full_date_str = f"{current_year}/{raw_date}"

        try:
            # 解析日期字符串为 datetime 对象
            dt = datetime.strptime(full_date_str, "%Y/%m/%d")
            # 判断操作系统
            if platform.system() == "Windows":
                return dt.strftime("%Y/%#m/%d")
            else:  # Linux/macOS
                return dt.strftime("%Y/%-m/%d")
        except ValueError:
            print(f"日期解析失败，输入的日期 {raw_date} 格式可能不正确。")
            return None

    def format_ester_date(self, raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%m/%d/%Y")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def format_jpy_rate_date(self, raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%m-%d-%Y")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def format_lpr_date(self, raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%Y-%m-%d")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def format_us_interest_rate_date(self, raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%Y-%m-%d")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    @log_execution_time
    @retry_on_timeout
    def crawl_exchange_rate(self, url):
        """
        使用爬虫直接获取汇率数据
        """
        try:
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
                'Referer': 'https://cn.investing.com/',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0',
            }

            # 添加随机延时
            time.sleep(1 + random.random() * 2)  # 减少等待时间

            # 发送请求
            logger.info(f"正在请求URL: {url}")
            response = requests.get(url, headers=headers, timeout=10)  # 减少超时时间
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # 选择表格的前两行数据
            rows = soup.select('tr.historical-data-v2_price__atUfP')[:2]

            if len(rows) < 2:
                logger.error("未找到足够的数据行，请检查HTML结构或反爬机制")
                return None

            results = []
            for row in rows:
                date = row.find('time').text.strip()
                cells = row.find_all('td')

                if url == 'https://cn.investing.com/rates-bonds/u.s.-10-year-bond-yield-historical-data':
                    # 10年期美债数据
                    result = {
                        "日期": self.format_exchange_rate_date(date),
                        "收盘": cells[1].text.strip(),
                        "开盘": cells[2].text.strip(),
                        "高": cells[3].text.strip(),
                        "低": cells[4].text.strip(),
                        "涨跌幅": cells[5].text.strip() if len(cells) > 5 else "N/A"
                    }
                else:
                    # 构造返回结果
                    result = {
                        "日期": self.format_exchange_rate_date(date),
                        "收盘": cells[1].text.strip(),
                        "开盘": cells[2].text.strip(),
                        "高": cells[3].text.strip(),
                        "低": cells[4].text.strip(),
                        "涨跌幅": cells[6].text.strip() if len(cells) > 6 else "N/A"
                    }
                results.append(result)

            logger.info(f"成功爬取数据: {results}")
            return results

        except requests.RequestException as e:
            logger.error(f"网络请求失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"爬取过程出错: {str(e)}", exc_info=True)
            return None

    def find_last_row(self, sheet):
        """
        改进的查找最后一行方法：逆向查找第一个非空行
        """
        for row in reversed(range(1, sheet.max_row + 1)):
            if any(cell.value for cell in sheet[row]):
                return row
        return 1  # 如果全为空，从第一行开始

    def write_monthly_data(self, worksheet, data, row):
        """
        写入月度数据到Excel

        Args:
            worksheet: Excel工作表对象
            data: 包含数据的字典
            row: 要写入的行号
        """
        # 获取工作表名称
        sheet_name = worksheet.title

        # 获取该工作表对应的列定义
        if sheet_name in config.COLUMN_DEFINITIONS:
            columns = config.COLUMN_DEFINITIONS[sheet_name]
        else:
            logger.warning(f"未找到 {sheet_name} 的列定义，使用默认列")
            columns = ['日期']

        # 写入数据
        for col_idx, col_name in enumerate(columns, 1):
            value = data.get(col_name, '')
            cell = worksheet.cell(row=row, column=col_idx, value=value)
            if sheet_name == 'Import and Export' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'Money Supply' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'PPI' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'CPI' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'PMI' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'New Bank Loan Addition' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            else:
                cell.alignment = Alignment(horizontal='right')

        logger.info(f"已在 {sheet_name} 的第 {row} 行写入月度数据")

    def write_daily_data(self, worksheet, data, last_row, sheet_name):
        """
        写入日频数据到Excel

        Args:
            worksheet: Excel工作表对象
            data: 包含数据的列表（通常有两行）
            last_row: 最后一行的行号
            sheet_name: 工作表名称
        """
        if not data or len(data) < 2:
            logger.error(f"{sheet_name} 数据不足，无法写入")
            return

        new_date_str1 = data[0].get("日期", "")
        new_date_str2 = data[1].get("日期", "")
        if not new_date_str1 or not new_date_str2:
            logger.error(f"{sheet_name} 数据中缺少日期字段，跳过")
            return  # 这里需要 return，否则代码继续执行会导致错误

        # 解析新日期
        try:
            year1, month1, day1 = map(int, new_date_str1.split('/'))
            new_date1 = datetime(year1, month1, day1)
            year2, month2, day2 = map(int, new_date_str2.split('/'))
            new_date2 = datetime(year2, month2, day2)
        except Exception as e:
            logger.error(f"解析新日期 '{new_date_str1}' 或 '{new_date_str2}' 失败: {str(e)}")
            return  # 解析失败就退出，避免后续错误

        # **初始化 last_date**
        last_date = None

        # 获取最后一行的日期值
        last_date_value = worksheet.cell(row=last_row, column=1).value


        # 解析现有日期
        if isinstance(last_date_value, datetime):
            last_date = last_date_value
        else:
            try:
                if last_date_value:
                    if sheet_name == 'SOFR':
                        month, day, year = map(int, str(last_date_value).split('/'))
                        last_date = datetime(year, month, day)
                    elif sheet_name == 'Shibor':
                        year, month, day = map(int, str(last_date_value).split('-'))
                        last_date = datetime(year, month, day)
                    else:
                        year, month, day = map(int, str(last_date_value).split('/'))
                        last_date = datetime(year, month, day)
            except Exception as e:
                logger.warning(f"解析现有日期 '{last_date_value}' 失败: {str(e)}")

        # **确保 last_date 被正确初始化**
        if last_date is None:
            logger.warning(f"未找到 {sheet_name} 的有效日期，跳过")
            return  # 这里要 return，否则 last_date 仍然可能是 None

        # **比较日期并决定写入策略**
        if new_date1.date() == last_date.date():
            logger.info(f"{sheet_name} 数据已是最新，无需更新")
            return
        elif new_date2.date() == last_date.date():
            # 添加两行数据
            self.write_single_daily_row(worksheet, data[1], last_row, sheet_name)
            self.write_single_daily_row(worksheet, data[0], last_row + 1, sheet_name)
            logger.info(f"已在 {sheet_name} 的第 {last_row} 和 {last_row+1} 行添加新数据")
        else:
            # 只需要添加第一行数据
            target_row = last_row + 1
            self.write_single_daily_row(worksheet, data[0], target_row, sheet_name)
            logger.info(f"已在 {sheet_name} 的第 {target_row} 行添加新数据")

    def write_single_daily_row(self, worksheet, row_data, row_num, sheet_name):
        """
        写入单行日频数据

        Args:
            worksheet: Excel工作表对象
            row_data: 单行数据字典
            row_num: 要写入的行号
            sheet_name: 工作表名称
        """
        # 获取该工作表对应的列定义
        if sheet_name in config.COLUMN_DEFINITIONS:
            columns = config.COLUMN_DEFINITIONS[sheet_name]
        elif sheet_name in config.CURRENCY_PAIRS:
            # 汇率数据使用通用列定义
            if sheet_name == 'USD 10Y':
                columns = config.COLUMN_DEFINITIONS['USD 10Y']
            else:
                columns = config.COLUMN_DEFINITIONS['CURRENCY']
        else:
            logger.warning(f"未找到 {sheet_name} 的列定义，使用默认列")
            columns = ['日期']

        # 写入数据
           # 写入数据
        for col_idx, col_name in enumerate(columns, 1):
            value = row_data.get(col_name, '')
            if sheet_name == 'Shibor' and col_idx == 1:
                value_dt = datetime.strptime(value, '%Y/%m/%d')
                if isinstance(value_dt, datetime):
                    value = value_dt.strftime('%Y-%m-%d')
            if sheet_name == 'SOFR' and col_idx == 1:
                value_dt = datetime.strptime(value, '%Y/%m/%d')
                if isinstance(value_dt, datetime):
                    value = value_dt.strftime('%m/%d/%Y')
                    # 去掉月份和日期的前导零
                    month, day, year = value.split('/')
                    month = month.lstrip('0') if month.startswith('0') and len(month) > 1 else month
                    day = day.lstrip('0') if day.startswith('0') and len(day) > 1 else day
                    value = f"{month}/{day}/{year}"
            cell = worksheet.cell(row=row_num, column=col_idx, value=value)
            if sheet_name == 'Shibor':
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'SOFR' and col_idx == 1:
                cell.alignment = Alignment(horizontal='left')
            elif sheet_name == 'SOFR' and col_idx == 2:
                cell.alignment = Alignment(horizontal='left')
            else:
                cell.alignment = Alignment(horizontal='right')

    def update_excel(self):
        """
        更新现有Excel文件，追加数据到对应sheet的最后一行（优化版）
        """
        MAX_RETRIES = 2  # 最大重试次数
        try:
            results = {}

            # 处理汇率数据
            for pair, url in config.CURRENCY_PAIRS.items():
                print(f"\n正在分析 {pair} 的数据...")
                data = {}
                crawler_data = None
                retries = 0
                while retries < MAX_RETRIES:
                    try:
                        crawler_data = self.crawl_exchange_rate(url)
                        if crawler_data:
                            data = crawler_data
                            print(f"成功获取 {pair} 的爬虫数据")
                            break
                    except requests.RequestException as e:
                        logger.warning(f"第 {retries + 1} 次请求 {url} 失败: {str(e)}，正在重试...")
                        retries += 1
                        time.sleep(2)  # 等待2秒后重试

                if not crawler_data:
                    logger.error(f"多次尝试后仍无法获取 {pair} 的爬虫数据，跳过")

                results[pair] = data

            # 使用线程池并行处理日频数据爬取
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                # 创建任务字典
                future_to_sheet = {}

                # 提交日频数据爬取任务
                for sheet_name, info in config.DAILY_DATA_PAIRS.items():
                    print(f"\n正在分析日频数据 {sheet_name}...")
                    crawler_method = getattr(self, info['crawler'])
                    future = executor.submit(crawler_method, info['url'])
                    future_to_sheet[future] = sheet_name

                # 处理完成的任务
                for future in concurrent.futures.as_completed(future_to_sheet):
                    sheet_name = future_to_sheet[future]
                    try:
                        data = future.result()
                        if data:
                            results[sheet_name] = data
                            print(f"成功获取日频数据 {sheet_name}")
                        else:
                            logger.warning(f"获取 {sheet_name} 数据失败")
                    except Exception as e:
                        logger.error(f"处理 {sheet_name} 数据时出错: {str(e)}")

            # 处理月度数据
            for sheet_name, info in config.MONTHLY_DATA_PAIRS.items():
                print(f"\n正在分析月度数据 {sheet_name}...")
                crawler_method = getattr(self, info['crawler'])
                data = crawler_method(info['url'])
                if data:
                    # 只保留第一行数据
                    if isinstance(data, list) and len(data) > 0:
                        results[sheet_name] = data[0]
                    else:
                        results[sheet_name] = data
                    print(f"成功获取月度数据 {sheet_name}")

            # 加载现有Excel文件
            wb = load_workbook(config.EXCEL_OUTPUT_PATH)

            # 更新各个sheet
            for sheet_name, data in results.items():
                if not data:
                    logger.warning(f"{sheet_name} 数据为空，跳过...")
                    continue

                if sheet_name not in wb.sheetnames:
                    logger.warning(f"工作表 {sheet_name} 不存在，跳过...")
                    continue

                ws = wb[sheet_name]

                # 查找最后一行数据
                last_row = self.find_last_row(ws)

                # 根据数据类型选择不同的处理方法
                if sheet_name in config.MONTHLY_DATA_PAIRS:
                    # 月度数据处理
                    new_date = data.get("日期", "")
                    if not new_date:
                        logger.error(f"{sheet_name} 数据中缺少日期字段，跳过")
                        continue

                    # 获取最后一行的日期值
                    last_date_value = ws.cell(row=last_row, column=1).value

                    # 比较日期，如果不同则更新
                    if str(last_date_value) != str(new_date):
                        self.write_monthly_data(ws, data, last_row + 1)
                    else:
                        logger.info(f"{sheet_name} 数据已是最新，无需更新")
                else:
                    # 日频数据处理（包括汇率数据）
                    self.write_daily_data(ws, data, last_row, sheet_name)

            # 保存Excel文件
            wb.save(config.EXCEL_OUTPUT_PATH)
            logger.info(f"数据已成功保存到 {config.EXCEL_OUTPUT_PATH}")

            return results

        except Exception as e:
            logger.error(f"更新Excel过程中出错: {str(e)}", exc_info=True)
            return None
        finally:
            self.close_driver()

    @log_execution_time
    @retry_on_timeout
    def crawl_steel_price(self, url):
        """
        爬取钢铁价格数据（优化版）
        """
        logger.info(f"正在请求URL: {url}")
        driver = self.get_driver()

        try:
            # 针对特定站点增加超时时间
            if "mysteel.com" in url:  # Steel price站点
                driver.set_page_load_timeout(60)  # 增加到60秒
                wait = WebDriverWait(driver, 30)  # 增加等待时间
            elif "euribor-rates.eu" in url:  # ESTER站点
                driver.set_page_load_timeout(60)
                wait = WebDriverWait(driver, 30)
            else:
                driver.set_page_load_timeout(20)
                wait = WebDriverWait(driver, 10)

            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait.until(EC.element_to_be_clickable((By.XPATH, '//span[text()="相对价格指数走势图"]'))).click()

            # 等待数据完全加载
            wait.until(EC.presence_of_element_located((By.XPATH, '//td[contains(text(),"/") and string-length(text())>8]')))

            # 获取表格引用
            table = driver.find_element(By.XPATH, '//table[contains(@class,"detailTab")]')

            # 单次获取所有需要的数据
            rows = table.find_elements(By.XPATH, './/tbody/tr[position()<=2]')
            data = []

            for row in rows:
                try:
                    # 实时获取当前行元素
                    cells = row.find_elements(By.XPATH, './/td[not(contains(@style,"none"))]')

                    # 过滤无效行
                    if len(cells) < 10:
                        logger.warning(f"跳过无效行，列数：{len(cells)}")
                        continue

                    # 立即提取文本内容
                    cell_texts = [cell.text for cell in cells]

                    # 动态映射字段
                    item = {
                        "日期": self.format_stee_price_date(cell_texts[0]),
                        "本日": cell_texts[1],
                        "昨日": cell_texts[2],
                        "日环比": cell_texts[3],
                        "上周": cell_texts[4],
                        "周环比": cell_texts[5],
                        "上月度": cell_texts[6],
                        "与上月比": cell_texts[7],
                        "去年同期": cell_texts[8],
                        "与去年比": cell_texts[9]
                    }
                    data.append(item)

                except StaleElementReferenceException:
                    logger.warning("检测到元素过期，重新获取表格数据...")
                    # 重新获取表格和行
                    table = driver.find_element(By.XPATH, '//table[contains(@class,"detailTab")]')
                    rows = table.find_elements(By.XPATH, './/tbody/tr[position()<=2]')
                    continue

            logger.info(f"成功抓取 Steel price 数据: {len(data)} 条记录")
            return data

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"爬取钢铁价格数据失败: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    def crawl_shibor_rate(self, url):
        """
        爬取Shibor利率数据（优化版）
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.ID, 'shibor-tendays-show-data')))

            # 初始化结果数组
            result_list = []
            row_count = 0

            for row in table.find_elements(By.CSS_SELECTOR, "tr:has(td)"):
                if row_count >= 2:
                    break  # 只取前两行数据

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 9:
                    continue

                # 解析数据
                current_record = {}
                current_record['日期'] = self.format_shibor_rate_date(cells[0].text.strip())
                terms = ['O/N', '1W', '2W', '1M', '3M', '6M', '9M', '1Y']

                for i, term in enumerate(terms):
                    current_record[term] = cells[i + 1].text.strip()

                result_list.append(current_record)
                row_count += 1

            logger.info(f"成功抓取 Shibor 数据: {result_list}")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取失败: {str(e)}")
            return None

    @log_execution_time
    def crawl_lpr(self, url):
        """
        爬取LPR数据（优化版）
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.ID, 'lpr-ten-days-table')))

            # 提取关键数据
            rows = table.find_elements(By.CSS_SELECTOR, "tr")
            # 跳过表头行
            data_rows = rows[3:]

            # 初始化结果数组
            result_list = []
            row_index = 0

            for row in data_rows:
                if row_index > 2:
                    break

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue

                date = self.format_lpr_date(cells[0].text.strip())
                term_1y = cells[1].text.strip()
                term_5y = cells[2].text.strip()

                current_record = {
                    "日期": date,
                    "1Y": term_1y,
                    "5Y": term_5y,
                    "PBOC_(6M-1Y)": 4.35,
                    "rowPBOC_(>5Y)": 4.9
                }
                result_list.append(current_record)
                row_index += 1

            logger.info(f"成功抓取 LPR 数据: {result_list}")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取失败: {str(e)}")
            return None

    @log_execution_time
    def crawl_sofr(self, url):
        """
        爬取SOFR数据（优化版）
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.ID, 'pr_id_1-table')))

            # 获取所有数据行
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 确保列数足够
                if len(cells) < 7:
                    logger.warning(f"检测到不完整行，实际列数：{len(cells)}")
                    continue

                # 按顺序提取字段
                record = {
                    "日期": self.format_sofr_date(cells[0].text.strip()),
                    "Rate Type": 'SOFR',
                    "RATE(%)": cells[1].text.strip(),
                    "1ST PERCENTILE(%)": cells[2].text.strip(),
                    "25TH PERCENTILE(%)": cells[3].text.strip(),
                    "75TH PERCENTILE(%)": cells[4].text.strip(),
                    "99TH PERCENTILE(%)": cells[5].text.strip(),
                    "VOLUME ($Billions)": cells[6].text.strip()
                }
                result_list.append(record)

            logger.info(f"成功抓取 SOFR 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取失败: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    def crawl_ester(self, url):
        """
        爬取ESTER数据（优化版）
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待，增加超时时间
            wait = WebDriverWait(driver, 15)

            # 使用更精确的选择器
            tables = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.table-striped")))
            if not tables:
                logger.error("未找到目标表格")
                return None

            table = tables[0]  # 取第一个表格

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            logger.info(f"找到数据行数：{len(rows)}")

            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 2:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": self.format_ester_date(cells[0].get_attribute('textContent').strip()),
                    "value": cells[1].get_attribute('textContent').strip().replace(' %', '')
                }
                result_list.append(record)

            logger.info(f"成功抓取 ESTER 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    def crawl_jpy_rate(self, url):
        """
        爬取JPY利率数据（优化版）
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)

            # 使用更精确的选择器
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table[class='table ']")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 2:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": self.format_jpy_rate_date(cells[0].get_attribute('textContent').strip()),
                    "value": cells[1].get_attribute('textContent').strip().replace(' %', '')
                }
                result_list.append(record)

            logger.info(f"成功抓取 JPY rate 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_us_interest_rate(self, url):
        """
        爬取美国利率数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 4:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": cells[0].text.strip(),
                    "前值": cells[1].text.strip(),
                    "现值": cells[2].text.strip(),
                    "发布日期": self.format_us_interest_rate_date(cells[3].text.strip()),
                }
                result_list.append(record)

            logger.info(f"成功抓取 US Interest Rate 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_import_export(self, url):
        """
        爬取进出口贸易数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 11:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": cells[0].text.strip(),
                    "当月出口额金额": cells[1].text.strip(),
                    "当月出口额同比增长": cells[2].text.strip(),
                    "当月出口额环比增长": cells[3].text.strip(),
                    "当月进口额金额": cells[4].text.strip(),
                    "当月进口额同比增长": cells[5].text.strip(),
                    "当月进口额环比增长": cells[6].text.strip(),
                    "累计出口额金额": cells[7].text.strip(),
                    "累计出口额同比增长": cells[8].text.strip(),
                    "累计进口额金额": cells[9].text.strip(),
                    "累计进口额同比增长": cells[10].text.strip(),
                }
                result_list.append(record)

            logger.info(f"成功抓取 Import and Export 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_money_supply(self, url):
        """
        爬取货币供应数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 10:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": cells[0].text.strip(),
                    "M2数量": cells[1].text.strip(),
                    "M2同比增长": cells[2].text.strip(),
                    "M2环比增长": cells[3].text.strip(),
                    "M1数量": cells[4].text.strip(),
                    "M1同比增长": cells[5].text.strip(),
                    "M1环比增长": cells[6].text.strip(),
                    "M0数量": cells[7].text.strip(),
                    "M0同比增长": cells[8].text.strip(),
                    "M0环比增长": cells[9].text.strip(),
                }
                result_list.append(record)

            logger.info(f"成功抓取 Money Supply 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_ppi(self, url):
        """
        爬取ppi数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 4:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": cells[0].text.strip(),
                    "当月": cells[1].text.strip(),
                    "当月同比增长": cells[2].text.strip(),
                    "累计": cells[3].text.strip(),
                }
                result_list.append(record)

            logger.info(f"成功抓取 PPI 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_cpi(self, url):
        """
        爬取cpi数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 13:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": cells[0].text.strip(),
                    "全国当月": cells[1].text.strip(),
                    "全国同比增长": cells[2].text.strip(),
                    "全国环比增长": cells[3].text.strip(),
                    "全国累计": cells[4].text.strip(),
                    "城市当月": cells[5].text.strip(),
                    "城市同比增长": cells[6].text.strip(),
                    "城市环比增长": cells[7].text.strip(),
                    "城市累计": cells[8].text.strip(),
                    "农村当月": cells[9].text.strip(),
                    "农村同比增长": cells[10].text.strip(),
                    "农村环比增长": cells[11].text.strip(),
                    "农村累计": cells[12].text.strip(),
                }
                result_list.append(record)

            logger.info(f"成功抓取 CPI 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_pmi(self, url):
        """
        爬取pmi数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 5:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录
                record = {
                    "日期": cells[0].text.strip(),
                    "制造业指数": cells[1].text.strip(),
                    "制造业同比增长": cells[2].text.strip(),
                    "非制造业指数": cells[3].text.strip(),
                    "非制造业同比增长": cells[4].text.strip(),
                }
                result_list.append(record)

            logger.info(f"成功抓取 PMI 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

    @log_execution_time
    @retry_on_timeout
    def crawl_new_bank_loan_addition(self, url):
        """
        爬取 中国 新增信贷数据
        """
        driver = self.get_driver()
        logger.info(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待替代固定等待
            wait = WebDriverWait(driver, 15)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-model")))

            # 获取有效数据行（跳过表头）
            rows = table.find_elements(By.CSS_SELECTOR, "tr:has(td)")
            result_list = []

            # 处理前两行数据
            for row in rows[:2]:
                cells = row.find_elements(By.TAG_NAME, "td")

                # 验证数据完整性
                if len(cells) != 6:
                    logger.warning(f"异常行数据，跳过。实际列数：{len(cells)}")
                    continue

                # 创建格式化记录 - 修复字段名称，避免重复的"同比增长"
                record = {
                    "日期": cells[0].text.strip(),
                    "当月": cells[1].text.strip(),
                    "同比增长": cells[2].text.strip(),
                    "环比增长": cells[3].text.strip(),
                    "累计": cells[4].text.strip(),
                    "累计同比增长": cells[5].text.strip(),  # 修改为"累计同比增长"以区分
                }
                result_list.append(record)

            logger.info(f"成功抓取 New Bank Loan Addition 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return None

if __name__ == "__main__":
    # 初始化分析器
    analyzer = MarketDataAnalyzer()
    print("更新所有数据...")
    analyzer.update_excel()
    # analyzer.crawl_cpi('https://data.eastmoney.com/cjsj/cpi.html')
    print("\n程序运行结束")