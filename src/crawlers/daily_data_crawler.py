import logging
import random
import time
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException, TimeoutException

from .base_crawler import BaseCrawler, log_execution_time, retry_on_timeout

# 配置日志
logger = logging.getLogger(__name__)

class DailyDataCrawler(BaseCrawler):
    """专门处理日频数据的爬虫类"""
    
    def __init__(self):
        super().__init__()
        logger.info("初始化日频数据爬虫...")
    
    def format_stee_price_date(self, raw_date):
        """格式化钢铁价格日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期
            date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"钢铁价格日期格式化错误: {str(e)}")
            return raw_date
    
    def format_shibor_rate_date(self, raw_date):
        """格式化Shibor利率日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期
            date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"Shibor利率日期格式化错误: {str(e)}")
            return raw_date
    
    def format_sofr_date(self, raw_date):
        """格式化SOFR日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期 (例如: "30 Jun 2023")
            date_obj = datetime.strptime(raw_date, '%d %b %Y')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"SOFR日期格式化错误: {str(e)}")
            return raw_date
    
    def format_ester_date(self, raw_date):
        """格式化ESTER日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期 (例如: "30/06/2023")
            date_obj = datetime.strptime(raw_date, '%d/%m/%Y')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"ESTER日期格式化错误: {str(e)}")
            return raw_date
    
    def format_jpy_rate_date(self, raw_date):
        """格式化JPY利率日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期 (例如: "June 30, 2023")
            date_obj = datetime.strptime(raw_date, '%B %d, %Y')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"JPY利率日期格式化错误: {str(e)}")
            return raw_date
    
    def format_lpr_date(self, raw_date):
        """格式化LPR日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期
            date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"LPR日期格式化错误: {str(e)}")
            return raw_date
    
    def format_us_interest_rate_date(self, raw_date):
        """格式化美国利率日期"""
        try:
            # 移除可能的额外空格
            raw_date = raw_date.strip()
            
            # 尝试解析日期 (例如: "Jun 30, 2023")
            date_obj = datetime.strptime(raw_date, '%b %d, %Y')
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            logger.error(f"美国利率日期格式化错误: {str(e)}")
            return raw_date
    
    @log_execution_time
    @retry_on_timeout
    def crawl_steel_price(self, url):
        """
        爬取钢铁价格数据（优化版）
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 针对特定站点增加超时时间
            if "mysteel.com" in url:  # Steel price站点
                driver.set_page_load_timeout(60)  # 增加到60秒
                wait = WebDriverWait(driver, 30)  # 增加等待时间
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

            # 单次获取所有需要的数据 - 修改为获取前10行
            rows = table.find_elements(By.XPATH, './/tbody/tr[position()<=10]')
            data = []

            for row in rows:
                try:
                    # 实时获取当前行元素
                    cells = row.find_elements(By.XPATH, './/td[not(contains(@style,"none"))]')

                    # 数据校验
                    if len(cells) < 10:
                        logger.debug(f"Steel price: 跳过无效行，列数：{len(cells)}")
                        continue

                    # 立即提取文本内容
                    cell_texts = [cell.text for cell in cells]

                    # 动态映射字段
                    item = {
                        "日期": self.format_stee_price_date(cells[0].get_attribute('textContent').strip()),
                        "本日": cells[1].text.strip(),
                        "昨日": cells[2].text.strip(),
                        "日环比": cells[3].text.strip(),
                        "上周": cells[4].text.strip(),
                        "周环比": cells[5].text.strip(),
                        "上月度": cells[6].text.strip(),
                        "与上月比": cells[7].text.strip(),
                        "去年同期": cells[8].text.strip(),
                        "与去年比": cells[9].text.strip(),
                    }
                    data.append(item)

                except StaleElementReferenceException:
                    logger.debug("Steel price: 检测到元素过期，重新获取表格数据...")
                    # 重新获取表格和行
                    table = driver.find_element(By.XPATH, '//table[contains(@class,"detailTab")]')
                    rows = table.find_elements(By.XPATH, './/tbody/tr[position()<=10]')
                    continue
                except Exception as e:
                    logger.debug(f"Steel price: 行解析异常：{str(e)}")
                    continue

            logger.debug(f"成功抓取 Steel price 数据: {len(data)} 条记录")
            return data

        except TimeoutException:
            logger.error("Steel price: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"Steel price: 爬取数据失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_shibor_rate(self, url):
        """
        爬取Shibor利率数据（优化版）
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.ID, 'shibor-tendays-show-data')))

            # 初始化结果数组
            result_list = []
            row_count = 0

            for row in table.find_elements(By.CSS_SELECTOR, "tr:has(td)"):
                if row_count >= 10:  # 修改为获取前10行数据
                    break  # 只取前10行数据

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

            logger.debug(f"成功抓取 Shibor 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("Shibor: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"Shibor: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_lpr(self, url):
        """
        爬取LPR数据（优化版）
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
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
                if row_index >= 10:  # 修改为获取前10行数据
                    break  # 只取前10行数据

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 3:
                    continue

                # 解析数据
                current_record = {}
                current_record['日期'] = self.format_lpr_date(cells[0].text.strip())
                current_record['1年期LPR'] = cells[1].text.strip()
                current_record['5年期以上LPR'] = cells[2].text.strip()

                result_list.append(current_record)
                row_index += 1

            logger.debug(f"成功抓取 LPR 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("LPR: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"LPR: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_sofr(self, url):
        """
        爬取SOFR数据（优化版）
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table-hover')))

            # 初始化结果数组
            result_list = []
            row_count = 0

            for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                if row_count >= 10:  # 修改为获取前10行数据
                    break  # 只取前10行数据

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2:
                    continue

                # 解析数据
                current_record = {}
                current_record['日期'] = self.format_sofr_date(cells[0].text.strip())
                current_record['SOFR'] = cells[1].text.strip()

                result_list.append(current_record)
                row_count += 1

            logger.debug(f"成功抓取 SOFR 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("SOFR: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"SOFR: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_ester(self, url):
        """
        爬取ESTER数据（优化版）
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 针对特定站点增加超时时间
            if "euribor-rates.eu" in url:  # ESTER站点
                driver.set_page_load_timeout(60)
                wait = WebDriverWait(driver, 30)
            else:
                driver.set_page_load_timeout(20)
                wait = WebDriverWait(driver, 10)

            driver.get(url)

            # 使用显式等待，减少固定等待时间
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table-hover')))

            # 初始化结果数组
            result_list = []
            row_count = 0

            for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                if row_count >= 10:  # 修改为获取前10行数据
                    break  # 只取前10行数据

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2:
                    continue

                # 解析数据
                current_record = {}
                current_record['日期'] = self.format_ester_date(cells[0].text.strip())
                current_record['ESTER'] = cells[1].text.strip()

                result_list.append(current_record)
                row_count += 1

            logger.debug(f"成功抓取 ESTER 数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("ESTER: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"ESTER: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_jpy_rate(self, url):
        """
        爬取JPY利率数据（优化版）
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.data-table')))

            # 初始化结果数组
            result_list = []
            row_count = 0

            for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                if row_count >= 10:  # 修改为获取前10行数据
                    break  # 只取前10行数据

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2:
                    continue

                # 解析数据
                current_record = {}
                current_record['日期'] = self.format_jpy_rate_date(cells[0].text.strip())
                current_record['JPY'] = cells[1].text.strip()

                result_list.append(current_record)
                row_count += 1

            logger.debug(f"成功抓取 JPY 利率数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("JPY: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"JPY: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_us_interest_rate(self, url):
        """
        爬取美国利率数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(20)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 10)
            table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.data-table')))

            # 初始化结果数组
            result_list = []
            row_count = 0

            for row in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
                if row_count >= 10:  # 修改为获取前10行数据
                    break  # 只取前10行数据

                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) < 2:
                    continue

                # 解析数据
                current_record = {}
                current_record['日期'] = self.format_us_interest_rate_date(cells[0].text.strip())
                current_record['美联储基准利率'] = cells[1].text.strip()

                result_list.append(current_record)
                row_count += 1

            logger.debug(f"成功抓取美国利率数据: {len(result_list)} 条记录")
            return result_list

        except TimeoutException:
            logger.error("美国利率: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"美国利率: 数据抓取失败: {str(e)}")
            return None
