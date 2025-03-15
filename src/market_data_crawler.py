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

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


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
        使用爬虫直接获取市场数据
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

    def crawl_steel_price(self):
        """
        爬取钢铁价格数据（修复StaleElement异常版）
        """
        from selenium.common.exceptions import StaleElementReferenceException
        driver = webdriver.Chrome()
        driver.get("https://index.mysteel.com/xpic/detail.html?tabName=kuangsi")

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

if __name__ == "__main__":
    analyzer = MarketDataAnalyzer()
    # 只使用爬虫方式获取数据
    # results = analyzer.update_excel('crawler')
    # analyzer.crawl_steel_price()

    print("\n程序运行结束")