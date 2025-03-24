import requests
import logging
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup
from .base_crawler import BaseCrawler, log_execution_time, retry_on_timeout

# 配置日志
logger = logging.getLogger(__name__)

class ExchangeRateCrawler(BaseCrawler):
    """专门处理汇率数据的爬虫类"""
    
    def __init__(self):
        super().__init__()
        logger.info("初始化汇率数据爬虫...")
    
    def format_exchange_rate_date(self, raw_date):
        """格式化汇率数据日期"""
        try:
            # 处理不同格式的日期
            if isinstance(raw_date, datetime):
                return raw_date.strftime('%Y-%m-%d')
            
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试不同的日期格式
            date_formats = [
                '%Y年%m月%d日',  # 例如: 2023年01月01日
                '%Y-%m-%d',      # 例如: 2023-01-01
                '%Y/%m/%d',      # 例如: 2023/01/01
                '%b %d, %Y',     # 例如: Jan 01, 2023
                '%d %b %Y',      # 例如: 01 Jan 2023
                '%d-%m-%Y',      # 例如: 01-01-2023
                '%m/%d/%Y',      # 例如: 01/01/2023
                '%Y.%m.%d',      # 例如: 2023.01.01
            ]
            
            parsed_date = None
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(raw_date, fmt)
                    break
                except ValueError:
                    continue
            
            if parsed_date:
                return parsed_date.strftime('%Y-%m-%d')
            else:
                logger.warning(f"无法解析日期格式: {raw_date}")
                return raw_date
        except Exception as e:
            logger.error(f"日期格式化错误: {str(e)}")
            return raw_date
    
    @log_execution_time
    @retry_on_timeout
    def crawl_exchange_rate(self, url, pair_name=None):
        """
        优化后的汇率数据爬取方法（带详细调试日志）
        
        Args:
            url: 汇率数据的URL
            pair_name: 汇率对名称，可选
        """
        logger.debug(f"开始爬取汇率数据，URL: {url}")
        
        try:
            # 设置请求头，模拟浏览器
            headers = {
                'User-Agent': self.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Cache-Control': 'max-age=0'
            }
            
            # 添加随机延迟，避免被检测为爬虫
            time.sleep(random.uniform(1, 3))
            
            # 发送请求
            logger.debug("发送HTTP请求...")
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()  # 如果响应状态码不是200，将引发HTTPError异常
            
            # 解析HTML
            logger.debug("解析HTML内容...")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找汇率数据表格
            table = soup.find('table', class_='historicalRateTable')
            if not table:
                logger.warning("未找到汇率数据表格")
                return None
            
            # 提取表格行
            rows = table.find_all('tr')
            if len(rows) <= 1:  # 只有表头，没有数据
                logger.warning("汇率表格中没有数据行")
                return None
            
            # 提取数据（跳过表头）
            data = []
            for row in rows[1:11]:  # 只取前10行数据
                cells = row.find_all('td')
                if len(cells) >= 2:
                    date_text = cells[0].get_text(strip=True)
                    rate_text = cells[1].get_text(strip=True)
                    
                    # 格式化日期
                    formatted_date = self.format_exchange_rate_date(date_text)
                    
                    # 处理汇率值
                    try:
                        rate_value = float(rate_text.replace(',', ''))
                    except ValueError:
                        logger.warning(f"无法将汇率值转换为浮点数: {rate_text}")
                        rate_value = rate_text
                    
                    data.append({
                        "日期": formatted_date,
                        "汇率": rate_value
                    })
            
            logger.debug(f"成功爬取汇率数据: {len(data)} 条记录")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"请求异常: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"爬取汇率数据失败: {str(e)}")
            return None
