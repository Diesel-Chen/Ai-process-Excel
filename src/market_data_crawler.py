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

    def get_data_by_crawler(self, url):
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

            # 更精准的选择器：通过类名定位第一行数据
            first_row = soup.select_one('tr.historical-data-v2_price__atUfP')
            if not first_row:
                logger.error("未找到数据行，请检查HTML结构或反爬机制")
                return None

            # 提取数据
            date = first_row.find('time').text.strip()
            cells = first_row.find_all('td')

            # 构造返回结果
            result = {
                "日期": self.format_cn_date(date),
                "收盘": cells[1].text.strip(),
                "开盘": cells[2].text.strip(),
                "高": cells[3].text.strip(),
                "低": cells[4].text.strip(),
                "涨跌幅": cells[6].text.strip() if len(cells) > 6 else "N/A"
            }


            logger.info(f"成功爬取数据: {result}")
            return result

        except requests.RequestException as e:
            logger.error(f"网络请求失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"爬取过程出错: {str(e)}", exc_info=True)
            return None



    def update_excel(self, method='both'):
        """
        更新现有Excel文件，追加数据到对应sheet的最后一行
        """
        try:
            results = {}
            for pair, url in config.CURRENCY_PAIRS.items():
                print(f"\n正在分析 {pair} 的数据...")
                data = {}
                if method in ['crawler', 'both']:
                    crawler_data = self.get_data_by_crawler(url)
                    if crawler_data:
                        data = crawler_data
                        print(f"成功获取 {pair} 的爬虫数据")

                if method in ['openai', 'both']:
                    openai_data = self.get_data_by_openai(url)
                    if openai_data:
                        data['openai_analysis'] = openai_data['analysis']
                        print(f"成功获取 {pair} 的OpenAI分析数据")

                results[pair] = data

            # 加载现有Excel文件
            wb = load_workbook(config.EXCEL_OUTPUT_PATH)
            print(wb.sheetnames)

            for sheet_name, data in results.items():
                if sheet_name not in wb.sheetnames:
                    logger.warning(f"工作表 {sheet_name} 不存在，跳过...")
                    continue

                ws = wb[sheet_name]

                # 手动定义列顺序（根据你的Excel实际列名调整）
                expected_headers = ["日期", "收盘", "开盘", "高", "低", "交易量", "涨跌幅"]

                # 按固定顺序填充数据
                row_data = [
                    data.get("日期", ""),
                    data.get("收盘", ""),
                    data.get("开盘", ""),
                    data.get("高", ""),
                    data.get("低", ""),
                    data.get("交易量", ""),
                    data.get("涨跌幅", "")
                ]

                  # 方法1：直接追加到末尾（默认行为）
                # ws.append(row_data)

                # 方法2：查找实际最后一行（解决空白行问题）
                def find_last_row(sheet):
                    # 逆向查找第一个非空行
                    for row in reversed(range(1, sheet.max_row + 1)):
                        if any(cell.value for cell in sheet[row]):
                            return row
                    return 1  # 如果全为空，从第一行开始

                last_row = find_last_row(ws)
                target_row = last_row + 1

                # 写入数据到目标行
                for col, value in enumerate(row_data, start=1):
                    ws.cell(row=target_row, column=col, value=value)

                # 设置对齐和格式
                right_align = Alignment(horizontal='right')
                for col in range(1, len(row_data) + 1):
                    ws.cell(row=target_row, column=col).alignment = right_align

                logger.info(f"已为 {sheet_name} 在第 {target_row} 行追加新数据")

            # 保存修改
            wb.save(config.EXCEL_OUTPUT_PATH)
            logger.info(f"数据已成功保存到 {config.EXCEL_OUTPUT_PATH}")
            return results

        except Exception as e:
            logger.error(f"更新Excel过程中出错: {str(e)}", exc_info=True)
            return None

if __name__ == "__main__":
    analyzer = MarketDataAnalyzer()
    # 只使用爬虫方式获取数据
    results = analyzer.update_excel('crawler')
    print("\n程序运行结束")
