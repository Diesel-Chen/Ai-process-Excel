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

        Args:
            url (str): 需要爬取的URL

        Returns:
            dict: 包含爬取数据的字典
        """
        try:
            headers = {
                'User-Agent': self.ua.random,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
                'Connection': 'keep-alive',
            }

            # 添加随机延时避免被封
            time.sleep(2 + random.random() * 3)

            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # 获取汇率数据（以investing.com为例）
            try:
                current_price = soup.find('span', {'data-test': 'instrument-price-last'}).text.strip()
                change_percent = soup.find('span', {'data-test': 'instrument-price-change-percent'}).text.strip()
            except AttributeError:
                logger.warning("无法找到特定元素，尝试备用选择器")
                current_price = soup.find('span', class_='text-2xl').text.strip()
                change_percent = soup.find('span', class_='pill').text.strip()

            return {
                'source': 'crawler',
                'timestamp': datetime.now().isoformat(),
                'current_price': current_price,
                'change_percent': change_percent,
                'raw_html': response.text  # 保存原始HTML以备后续分析
            }

        except requests.RequestException as e:
            logger.error(f"爬虫获取数据失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"解析数据时出错: {str(e)}")
            return None

    def update_excel(self, method='both'):
        """
        更新Excel文件，可以选择数据获取方式

        Args:
            method (str): 'openai', 'crawler', 或 'both'
        """
        try:
            results = {}
            for pair, url in config.CURRENCY_PAIRS.items():
                print(f"\n正在分析 {pair} 的数据...")

                data = {}
                if method in ['openai', 'both']:
                    openai_data = self.get_data_by_openai(url)
                    if openai_data:
                        data['openai_analysis'] = openai_data['analysis']

                if method in ['crawler', 'both']:
                    crawler_data = self.get_data_by_crawler(url)
                    if crawler_data:
                        # 直接将爬虫获取的数据赋值给data，保留原始HTML
                        data = crawler_data
                        # 打印关键信息用于调试
                        print(f"货币对: {pair}")
                        print(f"当前价格: {crawler_data.get('current_price', 'N/A')}")
                        print(f"变化百分比: {crawler_data.get('change_percent', 'N/A')}")
                        print(f"原始HTML长度: {len(crawler_data.get('raw_html', ''))}")

                        # 可选：保存原始HTML到文件进行检查
                        if 'raw_html' in crawler_data and crawler_data['raw_html']:
                            html_filename = f"{pair}_raw_html.html"
                            with open(html_filename, 'w', encoding='utf-8') as f:
                                f.write(crawler_data['raw_html'])
                            print(f"原始HTML已保存到文件: {html_filename}")

                results[pair] = data

            # 调试模式：不执行Excel逻辑，只返回结果
            print("\n爬虫测试结果:")
            for pair, data in results.items():
                print(f"{pair}: {data.keys()}")

            return results  # 返回结果以便进一步调试

            # 注释掉Excel相关逻辑
            # df = pd.DataFrame.from_dict(results, orient='index')
            # df.to_excel(config.EXCEL_OUTPUT_PATH)
            # logger.info(f"分析结果已保存到 {config.EXCEL_OUTPUT_PATH}")

        except Exception as e:
            logger.error(f"爬虫测试过程中出错: {str(e)}")
            return None

if __name__ == "__main__":
    analyzer = MarketDataAnalyzer()
    # 只使用爬虫方式获取数据
    results = analyzer.update_excel('crawler')
    print("\n程序运行结束")