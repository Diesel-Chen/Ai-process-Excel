from flask import Flask, jsonify, send_file, Response, stream_with_context
from flask_cors import CORS
import os
import io
import sys
import logging
import threading
import queue
import time
from datetime import datetime

# 导入爬虫模块
import market_data_crawler
import config

app = Flask(__name__, static_folder='../static', static_url_path='')
CORS(app, resources={r"/api/*": {"origins": "*"}})  # 配置CORS以允许前端访问API

# 创建一个队列用于存储日志消息
log_queue = queue.Queue()
# 标记是否有数据更新
data_updated = False
# 存储爬虫执行结果
crawl_results = None
# 标记爬虫是否正在运行
crawler_running = False

# 自定义日志处理器，将日志放入队列
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put({
                'level': record.levelname,
                'message': msg,
                'timestamp': datetime.now().strftime('%H:%M:%S')
            })
        except Exception:
            self.handleError(record)

# 配置日志
def setup_logging():
    root_logger = logging.getLogger()
    # 删除现有的处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 设置根日志级别
    root_logger.setLevel(logging.DEBUG)

    # 添加队列处理器
    queue_handler = QueueHandler(log_queue)
    queue_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%H:%M:%S'))
    root_logger.addHandler(queue_handler)

    # 添加控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%H:%M:%S'))
    root_logger.addHandler(console_handler)

    # 设置第三方库的日志级别较高，减少干扰
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('webdriver_manager').setLevel(logging.WARNING)

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 爬虫线程函数
def crawler_thread_func():
    global data_updated, crawl_results, crawler_running

    logger.info("开始市场数据爬取...")

    try:
        # 初始化分析器
        analyzer = market_data_crawler.MarketDataAnalyzer()

        try:
            # 执行更新
            results = analyzer.update_excel()
            crawl_results = results

            # 检查是否有更新Excel
            # 通过日志消息来确定是否有更新
            log_list = list(log_queue.queue)
            for log_item in log_list:
                if "已更新以下工作表" in log_item['message'] or "已在第" in log_item['message'] and "行插入新数据" in log_item['message']:
                    data_updated = True
                    break

            if data_updated:
                logger.info("检测到数据更新，Excel文件已更新")
            else:
                logger.info("所有数据均已是最新，无需更新Excel")

        except Exception as e:
            logger.error(f"更新过程出错: {str(e)}")
        finally:
            # 确保关闭WebDriver
            try:
                analyzer.close_driver()
            except:
                pass
    except Exception as e:
        logger.error(f"爬虫线程异常: {str(e)}")
    finally:
        crawler_running = False
        logger.info("数据爬取完成")

# API路由
@app.route('/api/update', methods=['GET'])
def update_data():
    global crawler_running, data_updated, log_queue

    # 如果爬虫已经在运行，返回提示
    if crawler_running:
        return jsonify({'status': 'running', 'message': '数据更新正在进行中，请稍后'})

    # 重置状态
    data_updated = False

    # 清空日志队列
    while not log_queue.empty():
        log_queue.get()

    # 设置标志并启动爬虫线程
    crawler_running = True
    thread = threading.Thread(target=crawler_thread_func)
    thread.daemon = True
    thread.start()

    return jsonify({'status': 'started', 'message': '数据更新已启动'})

@app.route('/api/status', methods=['GET'])
def check_status():
    global crawler_running, data_updated

    if crawler_running:
        return jsonify({
            'status': 'running',
            'message': '数据更新正在进行中'
        })
    else:
        return jsonify({
            'status': 'completed',
            'updated': data_updated,
            'message': '数据已更新' if data_updated else '所有数据均已是最新，无需更新'
        })

@app.route('/api/logs', methods=['GET'])
def get_logs():
    def generate():
        # 发送队列中现有的所有日志
        logs = list(log_queue.queue)
        yield f"data: {logs}\n\n"

        # 定期发送新日志
        last_size = len(logs)
        while True:
            time.sleep(0.5)  # 每0.5秒检查一次
            current_logs = list(log_queue.queue)
            if len(current_logs) > last_size:
                yield f"data: {current_logs[last_size:]}\n\n"
                last_size = len(current_logs)

            # 如果爬虫不再运行且没有新日志，结束流
            if not crawler_running and len(current_logs) == last_size:
                yield f"data: [{{\"level\":\"INFO\",\"message\":\"日志流结束\",\"timestamp\":\"{datetime.now().strftime('%H:%M:%S')}\"}}]\n\n"
                break

    return Response(stream_with_context(generate()), content_type='text/event-stream')

@app.route('/api/download', methods=['GET'])
def download_excel():
    try:
        excel_path = config.EXCEL_OUTPUT_PATH

        if not os.path.exists(excel_path):
            return jsonify({'error': 'Excel文件不存在'}), 404

        # 获取文件名（不包含路径）
        filename = os.path.basename(excel_path)

        return send_file(
            excel_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logger.error(f"下载Excel文件出错: {str(e)}")
        return jsonify({'error': f'下载出错: {str(e)}'}), 500

# 前端路由
@app.route('/')
def index():
    return app.send_static_file('index.html')

# 设置Nginx代理后使用的URL前缀
@app.route('/market-data')
def market_data_index():
    return app.send_static_file('index.html')

# 生产环境配置
def create_app():
    return app

if __name__ == '__main__':
    # 开发环境配置
    app.run(debug=True, host='0.0.0.0', port=5000)