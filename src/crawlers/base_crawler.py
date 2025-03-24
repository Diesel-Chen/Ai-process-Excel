import logging
import threading
import signal
import time
import random
import os
from functools import wraps
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
from selenium.webdriver import ActionChains

# 配置日志
logger = logging.getLogger(__name__)

# 添加彩色日志输出
class ColoredFormatter(logging.Formatter):
    """自定义彩色日志格式化器"""

    COLORS = {
        'DEBUG': '\033[94m',     # 蓝色
        'INFO': '\033[92m',      # 绿色
        'WARNING': '\033[93m',   # 黄色
        'ERROR': '\033[91m',     # 红色
        'CRITICAL': '\033[91m\033[1m',  # 红色加粗
        'RESET': '\033[0m'       # 重置颜色
    }

    def format(self, record):
        log_message = super().format(record)
        level_name = record.levelname
        if level_name in self.COLORS:
            return f"{self.COLORS[level_name]}{log_message}{self.COLORS['RESET']}"
        return log_message

def setup_logging(debug=False):
    """设置日志配置"""
    level = logging.DEBUG if debug else logging.INFO

    # 清除现有的处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 设置日志级别
    logger.setLevel(level)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)

    # 创建格式化器
    if os.name == 'posix':  # 在类Unix系统上启用彩色输出
        formatter = ColoredFormatter('%(message)s')
    else:
        formatter = logging.Formatter('%(message)s')

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件处理器 - 详细日志保存到文件
    file_handler = logging.FileHandler('market_data_crawler.log')
    file_handler.setLevel(level)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

def log_execution_time(func):
    """记录函数执行时间的装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        # 只在DEBUG级别记录执行时间，或者在失败时记录
        if result is None:
            logger.warning(f"{func.__name__} 执行失败，耗时: {elapsed_time:.2f} 秒")
        else:
            logger.debug(f"{func.__name__} 执行时间: {elapsed_time:.2f} 秒")
        return result
    return wrapper

def format_error_message(error):
    """格式化错误信息，提取关键部分"""
    error_str = str(error)

    # 如果是Selenium错误，提取主要信息
    if "Session info" in error_str:
        # 提取主要错误信息，去除堆栈跟踪
        main_error = error_str.split('Stacktrace:')[0].strip()
        return main_error

    # 对于其他错误，直接返回错误信息
    return error_str

def log_error(message, error=None, show_traceback=False):
    """统一的错误日志记录函数"""
    if error:
        error_msg = format_error_message(error)
        logger.error(f"{message}: {error_msg}")
        # 只在调试模式下记录完整堆栈
        if show_traceback:
            logger.debug(f"详细错误信息:", exc_info=True)
    else:
        logger.error(message)

def retry_on_timeout(func):
    """重试装饰器，用于处理超时情况"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        max_retries = 3
        retry_count = 0
        while retry_count < max_retries:
            try:
                return func(*args, **kwargs)
            except TimeoutException:
                retry_count += 1
                logger.warning(f"{func.__name__} 第{retry_count}次尝试超时，正在重试...")
                if retry_count >= max_retries:
                    logger.error(f"{func.__name__} 已达到最大重试次数({max_retries})，放弃尝试")
                    return None
                # 每次重试增加等待时间
                time.sleep(2 * retry_count)
            except Exception as e:
                log_error(f"{func.__name__} 发生错误", e, show_traceback=False)
                return None
    return wrapper

# 创建一个统计对象来跟踪成功和失败的爬取
class CrawlStats:
    """爬取统计信息类，用于记录爬取成功、失败和跳过的数据"""

    def __init__(self):
        self.success = []
        self.failure = {}
        self.skipped = {}

    def add_success(self, name):
        self.success.append(name)

    def add_failure(self, name, reason):
        self.failure[name] = reason

    def add_skipped(self, name, reason):
        self.skipped[name] = reason

    def print_summary(self):
        """打印统计摘要"""
        logger.info("📊 爬取统计摘要")
        logger.info("=" * 50)

        # 成功数据
        if self.success:
            logger.info(f"✅ 成功: {len(self.success)} 项")
            # 每行最多显示4个项目
            for i in range(0, len(self.success), 4):
                chunk = self.success[i:i+4]
                logger.info(f"   {', '.join(chunk)}")
        else:
            logger.info("✅ 成功: 0 项")

        # 失败数据
        if self.failure:
            logger.info(f"❌ 失败: {len(self.failure)} 项")
            for name, reason in self.failure.items():
                logger.info(f"   {name}: {reason}")
        else:
            logger.info("❌ 失败: 0 项")

        # 跳过数据
        if self.skipped:
            logger.info(f"⏭️ 跳过: {len(self.skipped)} 项")
            for name, reason in self.skipped.items():
                logger.info(f"   {name}: {reason}")
        else:
            logger.info("⏭️ 跳过: 0 项")

        logger.info("=" * 50)

class BaseCrawler:
    """基础爬虫类，提供WebDriver管理和通用爬虫功能"""

    _driver = None
    _driver_lock = threading.RLock()
    _instance = None

    def __init__(self, headless=True):
        print("初始化基础爬虫...")
        # 不再预先初始化WebDriver，而是在需要时按需创建
        # 设置信号处理器，确保在程序被终止时关闭WebDriver
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # 设置是否使用无头模式
        self.headless = headless

        # 单例模式，保存实例引用
        BaseCrawler._instance = self

    def _signal_handler(self, sig, frame):
        """处理程序终止信号，确保关闭WebDriver"""
        print("\n检测到终止信号，正在关闭WebDriver...")
        self.close_driver()
        sys.exit(0)

    def _init_driver(self):
        """
        优化的WebDriver初始化方法
        """
        logger.debug("初始化WebDriver...")

        # 尝试不同的浏览器，按优先级顺序
        browsers = [
            ('chrome', self._init_chrome),
            ('firefox', self._init_firefox),
            ('edge', self._init_edge)
        ]

        for browser_name, init_func in browsers:
            try:
                logger.debug(f"尝试初始化 {browser_name} 浏览器...")
                driver = init_func()
                if driver:
                    logger.debug(f"成功初始化 {browser_name} 浏览器")
                    return driver
            except Exception as e:
                logger.warning(f"{browser_name} 浏览器初始化失败: {str(e)}")

        logger.error("所有浏览器初始化失败")
        return None

    def _init_chrome(self):
        """初始化Chrome WebDriver"""
        options = Options()

        # 基本配置
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-infobars')
        options.add_argument('--mute-audio')
        options.add_argument('--disable-browser-side-navigation')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')

        # 资源优化配置 - 针对低配置服务器(2核2G)的优化
        options.add_argument('--blink-settings=imagesEnabled=false')  # 禁用图片加载
        options.add_argument('--single-process')  # 单进程模式，减少内存占用
        options.add_argument('--disable-javascript')  # 禁用JavaScript，这可能会影响某些网站功能
        options.add_argument('--disk-cache-size=33554432')  # 限制磁盘缓存大小为32MB
        options.add_argument('--disable-application-cache')  # 禁用应用缓存
        options.add_argument('--disable-notifications')  # 禁用通知
        options.add_argument('--disable-popup-blocking')  # 禁用弹窗拦截
        options.add_argument('--disable-hang-monitor')  # 禁用挂起监控
        options.add_argument('--disable-component-update')  # 禁用组件更新
        options.add_argument('--disable-default-apps')  # 禁用默认应用
        options.add_argument('--disable-breakpad')  # 禁用崩溃报告
        options.add_argument('--disable-domain-reliability')  # 禁用域可靠性服务

        # 无头模式配置
        if self.headless:
            logger.debug("启用无头模式")
            options.add_argument('--headless=new')
        else:
            logger.debug("使用有界面模式")
        options.add_argument('--window-size=1920,1080')

        # 随机用户代理
        user_agent = self.get_random_user_agent()
        options.add_argument(f'--user-agent={user_agent}')

        # 添加实验性选项
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)

        # 创建WebDriver实例
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # 设置超时参数 - 针对低配置服务器进行优化
        driver.set_page_load_timeout(40)  # 页面加载超时时间增加到40秒
        driver.set_script_timeout(30)  # 脚本执行超时时间设置为30秒

        # 执行JavaScript来修改navigator.webdriver属性
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def _init_firefox(self):
        """初始化Firefox WebDriver"""
        from selenium.webdriver.firefox.options import Options as FirefoxOptions

        options = FirefoxOptions()
        if self.headless:
            logger.debug("启用Firefox无头模式")
            options.add_argument('--headless')
        else:
            logger.debug("使用Firefox有界面模式")
        options.add_argument('--width=1920')
        options.add_argument('--height=1080')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        # 随机用户代理
        user_agent = self.get_random_user_agent()
        options.set_preference("general.useragent.override", user_agent)

        # 禁用图片加载
        options.set_preference("permissions.default.image", 2)

        driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
        driver.set_page_load_timeout(30)

        return driver

    def _init_edge(self):
        """初始化Edge WebDriver"""
        from selenium.webdriver.edge.options import Options as EdgeOptions

        options = EdgeOptions()
        if self.headless:
            logger.debug("启用Edge无头模式")
            options.add_argument('--headless')
        else:
            logger.debug("使用Edge有界面模式")
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')

        # 随机用户代理
        user_agent = self.get_random_user_agent()
        options.add_argument(f'--user-agent={user_agent}')

        # 添加实验性选项
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()), options=options)
        driver.set_page_load_timeout(30)

        # 执行JavaScript来修改navigator.webdriver属性
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver

    def get_driver(self):
        """
        获取WebDriver实例，如果不存在则初始化

        Returns:
            WebDriver实例
        """
        with self._driver_lock:
            if self._driver is None:
                self._driver = self._init_driver()
            return self._driver

    def close_driver(self):
        """
        关闭WebDriver实例
        """
        with self._driver_lock:
            if self._driver is not None:
                try:
                    self._driver.quit()
                except Exception as e:
                    logger.warning(f"关闭WebDriver时出错: {str(e)}")
                finally:
                    self._driver = None

    def get_random_user_agent(self):
        """获取随机用户代理"""
        user_agents = [
            # Chrome
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
            # Firefox
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:90.0) Gecko/20100101 Firefox/90.0",
            # Edge
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
            # Safari
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15",
        ]
        return random.choice(user_agents)

    def find_last_row(self, sheet):
        """
        改进的查找最后一行方法：逆向查找第一个非空行
        """
        for row in range(sheet.max_row, 0, -1):
            if any(sheet.cell(row=row, column=col).value for col in range(1, sheet.max_column + 1)):
                return row
        return 1  # 如果工作表为空，返回第一行

    def cleanup(self):
        """
        清理资源，关闭WebDriver实例
        """
        logger.info("正在清理资源...")
        try:
            self.close_driver()
            logger.info("资源清理完成")
        except Exception as e:
            logger.error(f"清理资源时出错: {str(e)}")
