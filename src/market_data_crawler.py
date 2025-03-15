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


# 设置日志
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class MarketDataAnalyzer:
    def __init__(self):
        print("初始化市场数据分析器...")
        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=config.OPENAI_API_KEY
        )
        # 初始化User-Agent生成器
        self.ua = UserAgent()

    def format_cn_date(self,raw_date):
        # 解析中文月份
        dt = datetime.strptime(raw_date, "%m月 %d, %Y")

        # 判断操作系统
        if platform.system() == "Windows":
            return dt.strftime("%Y/%#m/%d")
        else:  # Linux/macOS
            return dt.strftime("%Y/%-m/%d")

    def get_data_by_openai(self, url):
        """
        使用OpenAI分析市场数据URL并返回结构化数据

        Args:
            url (str): 需要分析的URL

        Returns:
            dict: 包含分析结果的字典
        """
        try:
            # 使用配置文件中的提示模板
            prompt = config.ANALYSIS_PROMPT_TEMPLATE.format(url=url)

            # 调用OpenAI API
            response = self.client.chat.completions.create(
                model=config.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": config.SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=config.OPENAI_TEMPERATURE
            )

            # 获取分析结果
            analysis = response.choices[0].message.content
            logger.info("OpenAI分析完成")

            return {
                'source': 'openai',
                'timestamp': datetime.now().isoformat(),
                'analysis': analysis
            }
        except Exception as e:
            logger.error(f"OpenAI分析过程中出错: {str(e)}")
            return None

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
            time.sleep(2 + random.random() * 3)

            # 发送请求
            logger.info(f"正在请求URL: {url}")
            response = requests.get(url, headers=headers, timeout=15)
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
                        "日期": self.format_cn_date(date),
                        "收盘": cells[1].text.strip(),
                        "开盘": cells[2].text.strip(),
                        "高": cells[3].text.strip(),
                        "低": cells[4].text.strip(),
                        "涨跌幅": cells[5].text.strip() if len(cells) > 5 else "N/A"
                    }
                else:
                    # 构造返回结果
                    result = {
                        "日期": self.format_cn_date(date),
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

    def update_excel(self, method='both'):
        """
        更新现有Excel文件，追加数据到对应sheet的最后一行（检查日期是否重复）

        Args:
            method (str): 数据获取方法，可选值为'crawler'、'openai'或'both'
        """
        MAX_RETRIES = 3  # 最大重试次数
        try:
            results = {}
            for pair, url in config.CURRENCY_PAIRS.items():
                print(f"\n正在分析 {pair} 的数据...")
                data = {}
                if method in ['crawler', 'both']:
                    crawler_data = None
                    retries = 0
                    while retries < MAX_RETRIES:
                        try:
                            # 对于需要使用Selenium的特殊URL，使用相应的爬虫方法
                            if 'mysteel.com' in url:
                                crawler_data = self.crawl_steel_price(url)
                            elif 'shibor.org' in url:
                                crawler_data = self.crawl_shibor_rate(url)
                            else:
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

                if method in ['openai', 'both']:
                    openai_data = self.get_data_by_openai(url)
                    if openai_data:
                        data['openai_analysis'] = openai_data['analysis']
                        print(f"成功获取 {pair} 的OpenAI分析数据")

                results[pair] = data

            # 加载现有Excel文件
            wb = load_workbook(config.EXCEL_OUTPUT_PATH)

            for sheet_name, data in results.items():
                if sheet_name not in wb.sheetnames:
                    logger.warning(f"工作表 {sheet_name} 不存在，跳过...")
                    continue

                ws = wb[sheet_name]

                # 检查数据有效性
                if not data or len(data) < 2:
                    logger.error(f"{sheet_name} 数据不足，跳过")
                    continue

                new_date_str1 = data[0].get("日期", "")
                new_date_str2 = data[1].get("日期", "")
                if not new_date_str1 or not new_date_str2:
                    logger.error(f"{sheet_name} 数据中缺少日期字段，跳过")
                    continue

                # 解析新日期
                try:
                    year1, month1, day1 = map(int, new_date_str1.split('/'))
                    new_date1 = datetime(year1, month1, day1)
                    year2, month2, day2 = map(int, new_date_str2.split('/'))
                    new_date2 = datetime(year2, month2, day2)
                except Exception as e:
                    logger.error(f"解析新日期 '{new_date_str1}' 或 '{new_date_str2}' 失败: {str(e)}")
                    continue

                # 查找最后一行数据
                last_row = self.find_last_row(ws)

                # 获取最后一行的日期值
                last_date_value = ws.cell(row=last_row, column=1).value

                # 解析现有日期
                if isinstance(last_date_value, datetime):
                    last_date = last_date_value
                else:
                    try:
                        if last_date_value:
                            # 处理可能存在的字符串格式日期
                            parts = list(map(int, str(last_date_value).split('/')))
                            if len(parts) == 3:
                                last_date = datetime(*parts)
                            else:
                                raise ValueError
                    except Exception as e:
                        logger.warning(f"解析现有日期 '{last_date_value}' 失败: {str(e)}")
                        last_date = None

                # 定义列顺序和行数据
                if sheet_name == "USD 10Y":
                    row_data1 = [
                        data[0].get("日期", ""),
                        data[0].get("收盘", ""),
                        data[0].get("开盘", ""),
                        data[0].get("高", ""),
                        data[0].get("低", ""),
                        data[0].get("涨跌幅", "")
                    ]
                    row_data2 = [
                        data[1].get("日期", ""),
                        data[1].get("收盘", ""),
                        data[1].get("开盘", ""),
                        data[1].get("高", ""),
                        data[1].get("低", ""),
                        data[1].get("涨跌幅", "")
                    ]
                else:
                    row_data1 = [
                        data[0].get("日期", ""),
                        data[0].get("收盘", ""),
                        data[0].get("开盘", ""),
                        data[0].get("高", ""),
                        data[0].get("低", ""),
                        data[0].get("交易量", ""),
                        data[0].get("涨跌幅", "")
                    ]
                    row_data2 = [
                        data[1].get("日期", ""),
                        data[1].get("收盘", ""),
                        data[1].get("开盘", ""),
                        data[1].get("高", ""),
                        data[1].get("低", ""),
                        data[1].get("交易量", ""),
                        data[1].get("涨跌幅", "")
                    ]

                # 确定写入位置
                target_row = last_row

                # 比较日期并决定是否填充数据
                if last_date and (new_date1.date() == last_date.date()):
                    logger.info(f"{sheet_name} 最新数据日期 {new_date_str1} 已存在，跳过更新")
                elif last_date and (new_date2.date() == last_date.date()):
                    # 最后一行日期等于第二行数据日期，填充两行数据
                    # 写入第二行数据
                    for col, value in enumerate(row_data2, start=1):
                        ws.cell(row=target_row, column=col, value=value)

                    # 设置对齐格式
                    right_align = Alignment(horizontal='right')
                    for col in range(1, len(row_data2)+1):
                        ws.cell(row=target_row, column=col).alignment = right_align

                    # 写入第一行数据
                    target_row = last_row + 1
                    for col, value in enumerate(row_data1, start=1):
                        ws.cell(row=target_row, column=col, value=value)

                    for col in range(1, len(row_data1)+1):
                        ws.cell(row=target_row, column=col).alignment = right_align

                    logger.info(f"已为 {sheet_name} 在第 {target_row - 1} 和 {target_row} 行追加新数据")
                else:
                    target_row = last_row + 1
                    # 最后一行日期不等于第二行数据日期，只填充第一行数据
                    for col, value in enumerate(row_data1, start=1):
                        ws.cell(row=target_row, column=col, value=value)

                    right_align = Alignment(horizontal='right')
                    for col in range(1, len(row_data1)+1):
                        ws.cell(row=target_row, column=col).alignment = right_align

                    logger.info(f"已为 {sheet_name} 在第 {target_row} 行追加新数据")

            # 保存修改
            wb.save(config.EXCEL_OUTPUT_PATH)
            logger.info(f"数据已成功保存到 {config.EXCEL_OUTPUT_PATH}")
            return results

        except Exception as e:
            logger.error(f"更新Excel过程中出错: {str(e)}", exc_info=True)
            return None

    def find_last_row(self, sheet):
        """
        改进的查找最后一行方法：逆向查找第一个非空行
        """
        for row in reversed(range(1, sheet.max_row + 1)):
            if any(cell.value for cell in sheet[row]):
                return row
        return 1  # 如果全为空，从第一行开始

    def get_webdriver(self):
        """
        获取WebDriver实例，支持多种浏览器，使用无头模式

        Returns:
            WebDriver: 浏览器驱动实例
        """
        try:
            # 首先尝试Chrome
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument(f'user-agent={self.ua.random}')

            # 使用webdriver_manager自动下载和管理驱动
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            logger.info("成功初始化Chrome WebDriver")
            return driver
        except Exception as e:
            logger.warning(f"Chrome WebDriver初始化失败: {str(e)}")

            try:
                # 尝试Firefox
                from selenium.webdriver.firefox.options import Options as FirefoxOptions
                options = FirefoxOptions()
                options.add_argument('--headless')

                service = Service(GeckoDriverManager().install())
                driver = webdriver.Firefox(service=service, options=options)
                logger.info("成功初始化Firefox WebDriver")
                return driver
            except Exception as e:
                logger.warning(f"Firefox WebDriver初始化失败: {str(e)}")

                try:
                    # 最后尝试Edge
                    from selenium.webdriver.edge.options import Options as EdgeOptions
                    options = EdgeOptions()
                    options.add_argument('--headless')

                    service = Service(EdgeChromiumDriverManager().install())
                    driver = webdriver.Edge(service=service, options=options)
                    logger.info("成功初始化Edge WebDriver")
                    return driver
                except Exception as e:
                    logger.error(f"所有WebDriver初始化都失败: {str(e)}")
                    raise Exception("无法初始化任何WebDriver，请确保至少安装了Chrome、Firefox或Edge浏览器之一")

    def crawl_steel_price(self, url):
        """
        爬取钢铁价格数据（修复StaleElement异常版）

        Args:
            url (str): 数据URL
        """
        from selenium.common.exceptions import StaleElementReferenceException
        logger.info(f"正在请求URL: {url}")
        driver = self.get_webdriver()
        driver.get(url)

        try:
            # 点击"相对价格指数走势图"
            WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//span[text()="相对价格指数走势图"]'))
            ).click()

            # 等待数据完全加载（关键修复点）
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.XPATH, '//td[contains(text(),"/") and string-length(text())>8]'))  # 匹配日期格式数据
            )

            # 获取表格引用（每次重新获取元素）
            table = driver.find_element(By.XPATH, '//table[contains(@class,"detailTab")]')

            # 单次获取所有需要的数据（避免重复查询DOM）
            rows = table.find_elements(By.XPATH, './/tbody/tr[position()<=2]')
            data = []

            for row in rows:
                try:
                    # 实时获取当前行元素（防止状态过期）
                    cells = row.find_elements(By.XPATH, './/td[not(contains(@style,"none"))]')

                    # 过滤无效行（关键修复点）
                    if len(cells) < 10:  # 根据调试结果调整阈值
                        print(f"跳过无效行，列数：{len(cells)}")
                        continue

                    # 立即提取文本内容（防止元素失效）
                    cell_texts = [cell.text for cell in cells]

                    # 动态映射字段（根据实际列顺序调整）
                    item = {
                        "时间": cell_texts[0],
                        "本日": cell_texts[1],
                        "昨日": cell_texts[2],
                        "日环比": cell_texts[3].strip("%"),
                        "上周": cell_texts[4],
                        "周环比": cell_texts[5].strip("%"),
                        "上月度": cell_texts[6],
                        "与上月比": cell_texts[7].strip("%"),
                        "去年同期": cell_texts[8],
                        "与去年比": cell_texts[9].strip("%")
                    }
                    data.append(item)

                except StaleElementReferenceException:
                    print("检测到元素过期，重新获取表格数据...")
                    table = driver.find_element(By.XPATH, '//table[contains(@class,"detailTab")]')
                    rows = table.find_elements(By.XPATH, './/tbody/tr[position()<=2]')
                    continue

            print("最终有效数据:", data)
            return data

        finally:
            driver.quit()

    def crawl_shibor_rate(self, url):
        """
        爬取Shibor利率数据

        Args:
            url (str): 数据URL
        """
        # 获取WebDriver实例
        driver = self.get_webdriver()
        driver.get(url)
        logger.info(f"正在请求URL: {url}")

        try:

            time.sleep(3)  # 等待页面加载
            # 新式定位方法（Selenium 4.x+语法）
            table = driver.find_element(By.ID, 'shiborData')

           # 提取关键数据
            date_str = driver.find_element(By.ID, "home-shibor-date")\
                        .text.strip()\
                        .replace("\xa0", " ")

            # 初始化结果数组
            result_list = []

            # 处理数据表格
            current_record = {"日期": date_str}

            for row in table.find_elements(By.CSS_SELECTOR, "tr:has(td)"):  # 只处理有td的行
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue

                term = cells[1].text.strip()
                rate = cells[2].text.strip()

                # 只收集目标期限类型
                if term in ['O/N', '1W', '2W', '1M', '3M', '6M', '9M', '1Y']:
                    current_record[term] = rate

            # 验证数据完整性后添加到数组
            if len(current_record) >= 9:  # 日期+8个期限
                result_list.append(current_record)
            else:
                print("数据不完整，已丢弃当前记录")

            print(result_list)
            return result_list

        except Exception as e:
            print(f"数据抓取失败: {str(e)}")
            return []
        finally:
            if driver:
                driver.quit()

    def crawl_lpr(self, url):
        """
        爬取LPR数据

        Args:
            url (str): 数据URL
        """
        # 获取WebDriver实例
        driver = self.get_webdriver()
        driver.get(url)
        logger.info(f"正在请求URL: {url}")

        try:

            time.sleep(3)  # 等待页面加载
            # 新式定位方法（Selenium 4.x+语法）
            table = driver.find_element(By.ID, 'lpr-table')

           # 提取关键数据
            date_str = driver.find_element(By.ID, "home-lpr-date")\
                        .text.strip()\
                        .replace("\xa0", " ")

            # 初始化结果数组
            result_list = []

            # 处理数据表格
            current_record = {"日期": date_str}

            for row in table.find_elements(By.CSS_SELECTOR, "tr:has(td)"):  # 只处理有td的行
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue

                term = cells[1].text.strip()
                rate = cells[2].text.strip()

                # 只收集目标期限类型
                if term in ['1Y', '5Y']:
                    current_record[term] = rate

            # 验证数据完整性后添加到数组
            if len(current_record) >= 3:  # 日期+3个期限
                result_list.append(current_record)
            else:
                print("数据不完整，已丢弃当前记录")

            print(result_list)
            return result_list

        except Exception as e:
            print(f"数据抓取失败: {str(e)}")
            return []
        finally:
            if driver:
                driver.quit()

    def crawl_sofr(self, url):
        """
        爬取SOFR数据并按指定顺序返回前两行数据

        Args:
            url (str): 数据URL

        Returns:
            list: 包含前两行数据的字典列表，按指定字段顺序排列
        """
        driver = self.get_webdriver()
        driver.get(url)
        logger.info(f"正在请求URL: {url}")

        try:
            time.sleep(3)  # 等待页面加载
            table = driver.find_element(By.ID, 'pr_id_1-table')

            # 获取所有数据行（跳过可能存在的表头）
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
                    "日期": cells[0].text.strip(),
                    "RATE(%)": cells[1].text.strip(),
                    "1ST PERCENTILE(%)": cells[2].text.strip(),
                    "25TH PERCENTILE(%)": cells[3].text.strip(),
                    "75TH PERCENTILE(%)": cells[4].text.strip(),
                    "99TH PERCENTILE(%)": cells[5].text.strip(),
                    "VOLUME ($Billions)": cells[6].text.strip()
                }
                result_list.append(record)

            logger.info(f"成功抓取 {len(result_list)} 条记录")
            print(result_list)
            return result_list

        except Exception as e:
            logger.error(f"数据抓取失败: {str(e)}", exc_info=True)
            return []
        finally:
            if driver:
                driver.quit()

    def crawl_ester(self, url):
        """
        爬取页面中第一个ESTER表格数据

        Args:
            url (str): 数据URL
        """
        driver = self.get_webdriver()
        driver.get(url)
        logger.info(f"正在请求URL: {url}")

        try:
            time.sleep(3)
            # 定位第一个表格（两种方式任选其一）
            # 方式1：通过CSS选择器列表索引
            tables = driver.find_elements(By.CSS_SELECTOR, "table.table-striped")
            if not tables:
                logger.error("未找到目标表格")
                return []
            table = tables[0]  # 取第一个表格

            # 方式2：直接使用:nth-of-type选择器
            # table = driver.find_element(By.CSS_SELECTOR, "table.table-striped:nth-of-type(1)")

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
                    "日期": cells[0].text.strip(),
                    "value": cells[1].text.strip().replace(' %', '')
                }
                result_list.append(record)

            logger.info(f"成功抓取 {len(result_list)} 条记录")
            print(f"DEBUG - 抓取结果: {result_list}")  # 调试输出
            return result_list

        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return []
        finally:
            if driver:
                driver.quit()

    def crawl_jpy_rate(self, url):
        """
        爬取页面jpy_rate数据

        Args:
            url (str): 数据URL
        """
        driver = self.get_webdriver()
        driver.get(url)
        logger.info(f"正在请求URL: {url}")

        try:
            time.sleep(3)
            # 定位第一个表格（两种方式任选其一）
            # 方式1：通过CSS选择器列表索引
            table = driver.find_element(By.CSS_SELECTOR, "table.table[class='table ']")
            if not table:
                logger.error("未找到目标表格")
                return []



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
                    "日期": cells[0].text.strip(),
                    "value": cells[1].text.strip().replace(' %', '')
                }
                result_list.append(record)

            logger.info(f"成功抓取 {len(result_list)} 条记录")
            print(f"DEBUG - 抓取结果: {result_list}")  # 调试输出
            return result_list

        except Exception as e:
            logger.error(f"数据抓取异常: {str(e)}", exc_info=True)
            return []
        finally:
            if driver:
                driver.quit()


if __name__ == "__main__":
    # 初始化分析器
    analyzer = MarketDataAnalyzer()

    print("更新所有数据...")
    # results = analyzer.update_excel('crawler')
    # analyzer.crawl_steel_price('https://index.mysteel.com/xpic/detail.html?tabName=kuangsi')
    # analyzer.crawl_shibor_rate('https://www.shibor.org/shibor/index.html')
    # analyzer.crawl_lpr('https://www.shibor.org/shibor/index.html')
    # analyzer.crawl_sofr('https://www.newyorkfed.org/markets/reference-rates/sofr')
    # analyzer.crawl_ester('https://www.euribor-rates.eu/en/ester/')
    analyzer.crawl_jpy_rate('https://www.global-rates.com/en/interest-rates/central-banks/9/japanese-boj-overnight-call-rate/')




    print("\n程序运行结束")