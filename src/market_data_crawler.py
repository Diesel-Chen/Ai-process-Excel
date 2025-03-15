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
                            crawler_data = self.get_data_by_crawler(url)
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

    def crawl_steel_url(self):
        """
        爬取钢铁价格数据
        """
        # 请求URL
        url = 'https://index.mysteel.com/zs/newxpic/getReport.ms?typeName=%25E7%259F%25BF%25E7%259F%25B3%25E7%25BB%25BC%25E5%2590%2588&tabName=KUANGSHIZONGHE&dateType=day&startTime=&endTime=&returnType=&callback=json&v=1742044813486'

        # 生成当前时间戳
        timestamp = str(int(time.time() * 1000))

        # 请求头
        headers = {
            'accept': '*/*',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'appkey': '47EE3F12CF0C443F8FD51EFDA73AC815',
            'connection': 'keep-alive',
            'cookie': 'buriedDistinctId=a82a0697b234487a829f0842abe6857c; uuid_5d36a9e0-919c-11e9-903c-ab24dbab411b=b7f3242f-11e3-4e68-afc2-568004f3d1f2; WM_NI=%2FqB4owX6QzNuqOmRY56IXT7%2FlCFAQer9lH0LwBhBqidbzQCLNtuCVDipLbaALCt6F0HoyhCZA4qvh1Mt7gSMxXzUbakbZz6mRs8vWXIGakzWPqZkelgKI%2Bry%2FenAr1l2Y2s%3D; WM_NIKE=9ca17ae2e6ffcda170e2e6eeb3f17e93b09bb1b43ef4eb8ba7d54e978f9ab1d642b8bca793c53cabaa9caae12af0fea7c3b92aedaf8ed5ee7d9cea9f9bcd7fb391a3b3dc3ff69798b5b625b09d8aa2d46fb2e7a4a9b348b59ea8a3f463a8beb6b2c2488eacbba4d550abb08483e44b909dbcd7c53c8cb1b789c25283ecbf97eb3b8facf988d247bc88b6b6e16ba9be969bb56792adf897d6548b99af8cf95d94af968ce868a59587ccf173edbba2a2f13bb0ee97d3f237e2a3; WM_TID=PkWDfO34CF1FEAUVFUbWd5Jn6V1nuHYP; href=https%3A%2F%2Findex.mysteel.com%2Fxpic%2Fdetail.html%3FtabName%3Dkuangsi; accessId=5d36a9e0-919c-11e9-903c-ab24dbab411b; Hm_lvt_1c4432afacfa2301369a5625795031b8=1741976396; HMACCOUNT=78E18EFE467443D4; qimo_xstKeywords_5d36a9e0-919c-11e9-903c-ab24dbab411b=; gdxidpyhxdE=xruAjcMxoXdpMKD9lp4bL1jfyZrglCAsK35%2FadlpW3i9fv2iMnZA5CT5bgjuqUhIEtqV4IPDLDyxc%2BJU1Py%5CjQn7h%2F%2BDux7lKbnq3%2FddLbKHpCowV8TsECcHuUuAmY932gQWYhOonjME9OYVI%5CJ7m%2FLteWjft5iPEcg8iOGBw54hq245%3A1742044941304; qimo_seosource_0=%E7%AB%99%E5%86%85; qimo_seokeywords_0=; qimo_seosource_5d36a9e0-919c-11e9-903c-ab24dbab411b=%E7%AB%99%E5%86%85; qimo_seokeywords_5d36a9e0-919c-11e9-903c-ab24dbab411b=; BURIED_STARTUP=eyJTVEFSVFVQIjp0cnVlLCJTVEFSVFVQVElNRSI6IjIwMjUtMDMtMTUgMjE6MTg6MDAuMzIxIn0%3D; pageViewNum=17; MYSTEEL_GLOBAL_BURIED_IDENTITY=ce68b00e4af388de91542dfdf8139bf7; Hm_lpvt_1c4432afacfa2301369a5625795031b8=1742044804; BURIED_COMMON_SPM=107.index_mysteel_com.main.0.0.1742044813467',
            'host': 'index.mysteel.com',
            'referer': 'https://index.mysteel.com/xpic/detail.html?tabName=kuangsi',
            'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'sign': 'BEBEA042BA896730CA4FC5F0590E4F89',
            'timestamp': timestamp,
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'version': '1.0.0',
            'x-requested-with': 'XMLHttpRequest'
        }

        try:
            # 发送请求
            response = requests.get(url, headers=headers)
            # 检查响应状态码
            response.raise_for_status()
            # 打印响应内容
            print(response.json())
        except requests.exceptions.HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

if __name__ == "__main__":
    analyzer = MarketDataAnalyzer()
    # 只使用爬虫方式获取数据
    # results = analyzer.update_excel('crawler')
    analyzer.crawl_steel_url()

    print("\n程序运行结束")