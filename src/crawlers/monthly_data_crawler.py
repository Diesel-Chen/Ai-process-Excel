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

class MonthlyDataCrawler(BaseCrawler):
    """专门处理月度数据的爬虫类"""
    
    def __init__(self):
        super().__init__()
        logger.info("初始化月度数据爬虫...")
    
    @log_execution_time
    @retry_on_timeout
    def crawl_import_export(self, url):
        """
        爬取进出口贸易数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 15)
            
            # 等待页面加载完成
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table')))
            
            # 找到最新的月度数据表格
            table = driver.find_element(By.CSS_SELECTOR, 'table.table')
            
            # 获取表格中的所有行
            rows = table.find_elements(By.TAG_NAME, 'tr')
            
            # 跳过表头行，获取最新的一行数据
            if len(rows) > 1:
                latest_row = rows[1]  # 第二行通常是最新数据
                cells = latest_row.find_elements(By.TAG_NAME, 'td')
                
                # 确保有足够的单元格
                if len(cells) >= 5:
                    # 提取数据
                    data = {
                        "日期": cells[0].text.strip(),
                        "出口额": cells[1].text.strip(),
                        "同比增长": cells[2].text.strip(),
                        "进口额": cells[3].text.strip(),
                        "同比增长.1": cells[4].text.strip()
                    }
                    
                    logger.debug(f"成功抓取进出口贸易数据: {data}")
                    return [data]  # 返回列表，保持与其他爬虫一致的返回格式
                else:
                    logger.warning("进出口贸易数据表格格式不符合预期")
            else:
                logger.warning("进出口贸易数据表格为空或只有表头")
            
            return None

        except TimeoutException:
            logger.error("进出口贸易: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"进出口贸易: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_money_supply(self, url):
        """
        爬取货币供应数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 15)
            
            # 等待页面加载完成
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table')))
            
            # 找到最新的月度数据表格
            table = driver.find_element(By.CSS_SELECTOR, 'table.table')
            
            # 获取表格中的所有行
            rows = table.find_elements(By.TAG_NAME, 'tr')
            
            # 跳过表头行，获取最新的一行数据
            if len(rows) > 1:
                latest_row = rows[1]  # 第二行通常是最新数据
                cells = latest_row.find_elements(By.TAG_NAME, 'td')
                
                # 确保有足够的单元格
                if len(cells) >= 7:
                    # 提取数据
                    data = {
                        "日期": cells[0].text.strip(),
                        "M0": cells[1].text.strip(),
                        "同比增长": cells[2].text.strip(),
                        "M1": cells[3].text.strip(),
                        "同比增长.1": cells[4].text.strip(),
                        "M2": cells[5].text.strip(),
                        "同比增长.2": cells[6].text.strip()
                    }
                    
                    logger.debug(f"成功抓取货币供应数据: {data}")
                    return [data]  # 返回列表，保持与其他爬虫一致的返回格式
                else:
                    logger.warning("货币供应数据表格格式不符合预期")
            else:
                logger.warning("货币供应数据表格为空或只有表头")
            
            return None

        except TimeoutException:
            logger.error("货币供应: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"货币供应: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_ppi(self, url):
        """
        爬取ppi数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 15)
            
            # 等待页面加载完成
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table')))
            
            # 找到最新的月度数据表格
            table = driver.find_element(By.CSS_SELECTOR, 'table.table')
            
            # 获取表格中的所有行
            rows = table.find_elements(By.TAG_NAME, 'tr')
            
            # 跳过表头行，获取最新的一行数据
            if len(rows) > 1:
                latest_row = rows[1]  # 第二行通常是最新数据
                cells = latest_row.find_elements(By.TAG_NAME, 'td')
                
                # 确保有足够的单元格
                if len(cells) >= 3:
                    # 提取数据
                    data = {
                        "日期": cells[0].text.strip(),
                        "当月": cells[1].text.strip(),
                        "累计": cells[2].text.strip()
                    }
                    
                    logger.debug(f"成功抓取PPI数据: {data}")
                    return [data]  # 返回列表，保持与其他爬虫一致的返回格式
                else:
                    logger.warning("PPI数据表格格式不符合预期")
            else:
                logger.warning("PPI数据表格为空或只有表头")
            
            return None

        except TimeoutException:
            logger.error("PPI: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"PPI: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_cpi(self, url):
        """
        爬取cpi数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 15)
            
            # 等待页面加载完成
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table')))
            
            # 找到最新的月度数据表格
            table = driver.find_element(By.CSS_SELECTOR, 'table.table')
            
            # 获取表格中的所有行
            rows = table.find_elements(By.TAG_NAME, 'tr')
            
            # 跳过表头行，获取最新的一行数据
            if len(rows) > 1:
                latest_row = rows[1]  # 第二行通常是最新数据
                cells = latest_row.find_elements(By.TAG_NAME, 'td')
                
                # 确保有足够的单元格
                if len(cells) >= 3:
                    # 提取数据
                    data = {
                        "日期": cells[0].text.strip(),
                        "当月": cells[1].text.strip(),
                        "累计": cells[2].text.strip()
                    }
                    
                    logger.debug(f"成功抓取CPI数据: {data}")
                    return [data]  # 返回列表，保持与其他爬虫一致的返回格式
                else:
                    logger.warning("CPI数据表格格式不符合预期")
            else:
                logger.warning("CPI数据表格为空或只有表头")
            
            return None

        except TimeoutException:
            logger.error("CPI: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"CPI: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_pmi(self, url):
        """
        爬取pmi数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 15)
            
            # 等待页面加载完成
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table')))
            
            # 找到最新的月度数据表格
            table = driver.find_element(By.CSS_SELECTOR, 'table.table')
            
            # 获取表格中的所有行
            rows = table.find_elements(By.TAG_NAME, 'tr')
            
            # 跳过表头行，获取最新的一行数据
            if len(rows) > 1:
                latest_row = rows[1]  # 第二行通常是最新数据
                cells = latest_row.find_elements(By.TAG_NAME, 'td')
                
                # 确保有足够的单元格
                if len(cells) >= 3:
                    # 提取数据
                    data = {
                        "日期": cells[0].text.strip(),
                        "制造业PMI": cells[1].text.strip(),
                        "非制造业PMI": cells[2].text.strip()
                    }
                    
                    logger.debug(f"成功抓取PMI数据: {data}")
                    return [data]  # 返回列表，保持与其他爬虫一致的返回格式
                else:
                    logger.warning("PMI数据表格格式不符合预期")
            else:
                logger.warning("PMI数据表格为空或只有表头")
            
            return None

        except TimeoutException:
            logger.error("PMI: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"PMI: 数据抓取失败: {str(e)}")
            return None
    
    @log_execution_time
    @retry_on_timeout
    def crawl_new_bank_loan_addition(self, url):
        """
        爬取中国新增信贷数据
        """
        driver = self.get_driver()
        logger.debug(f"正在请求URL: {url}")

        try:
            # 设置页面加载超时
            driver.set_page_load_timeout(30)
            driver.get(url)

            # 使用显式等待，减少固定等待时间
            wait = WebDriverWait(driver, 15)
            
            # 等待页面加载完成
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'table.table')))
            
            # 找到最新的月度数据表格
            table = driver.find_element(By.CSS_SELECTOR, 'table.table')
            
            # 获取表格中的所有行
            rows = table.find_elements(By.TAG_NAME, 'tr')
            
            # 跳过表头行，获取最新的一行数据
            if len(rows) > 1:
                latest_row = rows[1]  # 第二行通常是最新数据
                cells = latest_row.find_elements(By.TAG_NAME, 'td')
                
                # 确保有足够的单元格
                if len(cells) >= 2:
                    # 提取数据
                    data = {
                        "日期": cells[0].text.strip(),
                        "新增信贷": cells[1].text.strip()
                    }
                    
                    logger.debug(f"成功抓取新增信贷数据: {data}")
                    return [data]  # 返回列表，保持与其他爬虫一致的返回格式
                else:
                    logger.warning("新增信贷数据表格格式不符合预期")
            else:
                logger.warning("新增信贷数据表格为空或只有表头")
            
            return None

        except TimeoutException:
            logger.error("新增信贷: 页面加载超时，请检查网络连接或URL是否正确")
            return None
        except Exception as e:
            logger.error(f"新增信贷: 数据抓取失败: {str(e)}")
            return None
