import os
import sys
import logging
import argparse
import time
import random
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入爬虫模块
from crawlers.base_crawler import BaseCrawler
from crawlers.exchange_rate_crawler import ExchangeRateCrawler
from crawlers.daily_data_crawler import DailyDataCrawler
from crawlers.monthly_data_crawler import MonthlyDataCrawler
from crawlers.excel_updater import ExcelUpdater

# 导入配置
import config

# 配置日志
def setup_logging(log_level=logging.INFO):
    """设置日志配置"""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../logs")
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"crawler_{timestamp}.log")

    # 配置根日志记录器
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    # 减少第三方库的日志级别
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("selenium").setLevel(logging.WARNING)
    logging.getLogger("webdriver_manager").setLevel(logging.WARNING)

    return log_file

def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="市场数据爬虫")
    parser.add_argument("--excel", type=str, help="Excel文件路径")
    parser.add_argument("--headless", action="store_true", help="使用无头模式运行浏览器")
    parser.add_argument("--debug", action="store_true", help="启用调试日志")
    parser.add_argument("--exchange-only", action="store_true", help="仅更新汇率数据")
    parser.add_argument("--daily-only", action="store_true", help="仅更新日频数据")
    parser.add_argument("--monthly-only", action="store_true", help="仅更新月度数据")

    return parser.parse_args()

def main():
    """主函数"""
    # 解析命令行参数
    args = parse_arguments()

    # 设置日志级别
    log_level = logging.DEBUG if args.debug else logging.INFO
    log_file = setup_logging(log_level)

    logger = logging.getLogger(__name__)
    logger.info("=" * 80)
    logger.info("市场数据爬虫启动")
    logger.info(f"日志文件: {log_file}")

    # 确定Excel文件路径
    excel_path = args.excel if args.excel else config.EXCEL_OUTPUT_PATH
    if not os.path.exists(excel_path):
        logger.error(f"Excel文件不存在: {excel_path}")
        return 1

    logger.info(f"目标Excel文件: {excel_path}")

    # 初始化基础爬虫
    base_crawler = BaseCrawler(headless=args.headless)

    try:
        # 初始化结果字典
        results = {}

        # 根据参数决定要运行的爬虫类型
        run_exchange = not (args.daily_only or args.monthly_only) or args.exchange_only
        run_daily = not (args.exchange_only or args.monthly_only) or args.daily_only
        run_monthly = not (args.exchange_only or args.daily_only) or args.monthly_only

        # 1. 爬取汇率数据
        if run_exchange:
            logger.info("开始爬取汇率数据...")
            exchange_crawler = ExchangeRateCrawler()

            # 爬取各种汇率数据
            for pair_name, url in config.CURRENCY_PAIRS.items():
                logger.info(f"爬取 {pair_name} 汇率数据...")
                data = exchange_crawler.crawl_exchange_rate(url, pair_name)
                if data:
                    results[pair_name] = data
                    logger.info(f"成功获取 {pair_name} 汇率数据: {len(data)} 条记录")
                else:
                    logger.warning(f"未能获取 {pair_name} 汇率数据")

        # 2. 爬取日频数据
        if run_daily:
            logger.info("开始爬取日频数据...")
            daily_crawler = DailyDataCrawler()

            # 初始化日频数据类型到爬虫方法的映射
            daily_crawler_methods = {}

            # 从 config.DAILY_DATA_PAIRS 中获取所有映射
            for sheet_name, info in config.DAILY_DATA_PAIRS.items():
                # 获取爬虫方法名和URL
                crawler_method_name = info['crawler']
                url = info['url']

                # 检查方法是否存在于daily_crawler对象中
                if hasattr(daily_crawler, crawler_method_name):
                    method = getattr(daily_crawler, crawler_method_name)
                    daily_crawler_methods[sheet_name] = {
                        'method': method,
                        'url': url
                    }
                else:
                    logger.warning(f"爬虫方法 {crawler_method_name} 不存在于DailyDataCrawler类中")

            # 使用映射爬取所有日频数据
            for sheet_name, info in daily_crawler_methods.items():
                logger.info(f"爬取 {sheet_name} 数据...")
                try:
                    # 获取爬虫方法和URL
                    crawler_method = info['method']
                    url = info['url']

                    # 执行爬取
                    data = crawler_method(url)

                    if data:
                        results[sheet_name] = data
                        logger.info(f"成功获取 {sheet_name} 数据: {len(data)} 条记录")
                    else:
                        logger.warning(f"未能获取 {sheet_name} 数据")
                except Exception as e:
                    logger.error(f"爬取 {sheet_name} 数据时出错: {str(e)}")

                # 添加随机延迟，避免请求过于频繁
                time.sleep(random.uniform(1, 3))

        # 3. 爬取月度数据
        if run_monthly:
            logger.info("开始爬取月度数据...")
            monthly_crawler = MonthlyDataCrawler()

            # 定义月度数据类型到爬虫方法的映射
            monthly_crawler_methods = {
                'Import and Export': monthly_crawler.crawl_import_export,
                'Money Supply': monthly_crawler.crawl_money_supply,
                'PPI': monthly_crawler.crawl_ppi,
                'CPI': monthly_crawler.crawl_cpi,
                'PMI': monthly_crawler.crawl_pmi,
                'New Bank Loan Addition': monthly_crawler.crawl_new_bank_loan_addition
            }

            # 爬取各种月度数据
            for pair_name, url in config.MONTHLY_DATA_PAIRS.items():
                logger.info(f"爬取 {pair_name} 数据...")

                try:
                    # 检查是否有对应的爬虫方法
                    if pair_name in monthly_crawler_methods:
                        # 获取对应的爬虫方法并执行
                        crawler_method = monthly_crawler_methods[pair_name]
                        data = crawler_method(url)

                        if data:
                            # 月度数据通常只有一条记录，取第一条
                            results[pair_name] = data[0] if isinstance(data, list) else data
                            logger.info(f"成功获取 {pair_name} 数据")
                        else:
                            logger.warning(f"未能获取 {pair_name} 数据")
                    else:
                        logger.warning(f"未知的月度数据类型: {pair_name}，没有对应的爬虫方法")
                except Exception as e:
                    logger.error(f"爬取 {pair_name} 数据时出错: {str(e)}")

                # 添加随机延迟，避免请求过于频繁
                time.sleep(random.uniform(2, 5))

        # 4. 更新Excel文件
        if results:
            logger.info("开始更新Excel文件...")
            excel_updater = ExcelUpdater(base_crawler)
            update_result = excel_updater.update_excel(results, excel_path)

            if update_result:
                logger.info("Excel文件更新成功")
            else:
                logger.error("Excel文件更新失败")
        else:
            logger.warning("没有获取到任何数据，Excel文件未更新")

        logger.info("市场数据爬虫运行完成")
        return 0

    except Exception as e:
        logger.error(f"运行过程中出错: {str(e)}", exc_info=True)
        return 1

    finally:
        # 确保资源被正确释放
        if base_crawler:
            base_crawler.cleanup()

if __name__ == "__main__":
    start_time = time.time()
    exit_code = main()
    elapsed_time = time.time() - start_time

    logger = logging.getLogger(__name__)
    logger.info(f"总运行时间: {elapsed_time:.2f} 秒")
    logger.info("=" * 80)

    sys.exit(exit_code)
