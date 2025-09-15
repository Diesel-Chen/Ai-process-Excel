def is_system_log(message: str) -> bool:
    """判断是否为系统级访问/噪声日志，前端无需展示。"""
    if not message:
        return False
    patterns = [
        r"\b127\.0\.0\.1\b\s*-\s*-\s*\[",  # Flask/Werkzeug 访问日志
        r"\bGET\s+/api/",                        # API 访问
        r"======\s*WebDriver manager\s*======",  # webdriver_manager 噪声
        r"found in cache",                        # 驱动缓存提示
    ]
    for p in patterns:
        if re.search(p, message, re.IGNORECASE):
            return True
    return False

from flask import Flask, jsonify, send_file, Response, stream_with_context, request
import json
from flask_cors import CORS
import os
import io
import sys
import logging
import threading
import queue
import time
from datetime import datetime
import uuid
import re

# 导入爬虫模块
try:
    # 当从src目录直接运行时
    import market_data_crawler
    import config
except ImportError:
    # 当从项目根目录运行时
    from src import market_data_crawler
    from src import config

app = Flask(__name__, static_folder='../static', static_url_path='')

# 配置CORS，允许所有来源，所有方法，所有头部
CORS(app, resources={
    r"/api/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "Accept", "Origin", "Referer"],
        "expose_headers": ["Content-Type", "Authorization"],
        "supports_credentials": True
    }
})

# 添加CORS预检请求的处理
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,Accept,Origin,Referer')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    return response

# 创建一个队列用于存储日志消息（SSE 实时日志）
log_queue = queue.Queue()

# 全局运行状态（用于 /api/status 兼容）
data_updated = False
crawl_results = None
crawler_running = False

# 简单任务队列与任务表（仅在单进程/开发模式下使用）
job_queue = queue.Queue()
jobs_lock = threading.RLock()
jobs = {}
current_job_id = None
job_log_buffers = {}  # {job_id: [log_entry, ...]}

# 全局日志序号（用于前端精确去重）
_log_seq = 0
_log_seq_lock = threading.RLock()

def next_log_seq() -> int:
    global _log_seq
    with _log_seq_lock:
        _log_seq += 1
        return _log_seq

# 自定义日志处理器，将日志放入队列
class QueueHandler(logging.Handler):
    def __init__(self, log_queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            # 仅取原始消息体，避免把时间/级别再次拼进 message，防止前端出现
            # "14:xx:xx - INFO - 14:xx:xx - INFO - ..." 的重复
            msg = record.getMessage()
            # 过滤系统访问日志/噪声，避免污染任务日志
            if is_system_log(msg):
                return
            # 绑定当前 job_id（若有）
            try:
                jid = current_job_id
            except Exception:
                jid = None

            log_entry = {
                'level': record.levelname,
                'message': msg,
                'timestamp': datetime.now().strftime('%H:%M:%S'),
                'job_id': jid,
                'seq': next_log_seq(),
            }

            # 全局日志队列（兼容）
            self.log_queue.put(log_entry)

            # 写入该任务的专属日志缓冲区
            if jid:
                with jobs_lock:
                    buf = job_log_buffers.setdefault(jid, [])
                    buf.append(log_entry)
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

def execute_crawl_job(job_id: str):
    """在队列worker线程中串行执行的任务。"""
    global data_updated, crawl_results, crawler_running, current_job_id

    with jobs_lock:
        job = jobs.get(job_id)
        if job:
            job["status"] = "running"
            job["started_at"] = time.time()
    current_job_id = job_id

    # 清空日志队列，确保前端只看到本任务的日志
    while not log_queue.empty():
        try:
            log_queue.get_nowait()
        except Exception:
            break

    # 运行状态置为 True
    crawler_running = True
    data_updated = False
    crawl_results = None

    logger.info(f"开始市场数据爬取... (job_id={job_id})")

    try:
        analyzer = market_data_crawler.MarketDataAnalyzer()
        try:
            results = analyzer.update_excel()
            crawl_results = results

            # 如果 update_excel 显式返回 False，认为任务失败
            if results is False:
                with jobs_lock:
                    job = jobs.get(job_id)
                    if job:
                        job["status"] = "failed"
                        job["error"] = job.get("error") or "Excel更新失败"
                raise Exception("Excel 更新失败")

            # 粗略判断是否更新：扫描日志关键字
            log_list = list(log_queue.queue)
            for log_item in log_list:
                if ("已更新以下工作表" in log_item['message']) or ("已在第" in log_item['message'] and "行插入新数据" in log_item['message']):
                    data_updated = True
                    break

            if data_updated:
                logger.info("检测到数据更新，Excel文件已更新")
            else:
                logger.info("所有数据均已是最新，无需更新Excel")

            # 不在此处标记 completed，等待最终日志与结束消息写入后再完成
            with jobs_lock:
                job = jobs.get(job_id)
                if job:
                    job["updated"] = data_updated

        except Exception as e:
            logger.error(f"更新过程出错: {str(e)}")
            with jobs_lock:
                job = jobs.get(job_id)
                if job:
                    job["status"] = "failed"
                    job["finished_at"] = time.time()
                    job["error"] = str(e)
        finally:
            try:
                analyzer.close_driver()
            except Exception:
                pass
    except Exception as e:
        logger.error(f"爬虫执行异常: {str(e)}")
        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["status"] = "failed"
                job["finished_at"] = time.time()
                job["error"] = str(e)
    finally:
        # 等待日志全部写入队列
        time.sleep(1)
        crawler_running = False
        # 追加结束消息到该 job 的缓冲与全局队列，确保前端能接收到
        end_msgs = []
        end_msgs.append({
            "level": "INFO",
            "message": "=== 数据更新完成 ===",
            "timestamp": datetime.now().strftime('%H:%M:%S'),
            "job_id": job_id,
            "seq": next_log_seq(),
        })
        end_msgs.append({
            "level": "INFO",
            "message": "SHOW_SUMMARY",
            "timestamp": datetime.now().strftime('%H:%M:%S'),
            "job_id": job_id,
            "seq": next_log_seq(),
        })
        with jobs_lock:
            buf = job_log_buffers.setdefault(job_id, [])
            buf.extend(end_msgs)
        for m in end_msgs:
            log_queue.put(m)

        logger.info("数据爬取完成")
        # 再等待片刻，确保SSE已发送结束消息
        time.sleep(0.5)
        with jobs_lock:
            job = jobs.get(job_id)
            if job:
                job["status"] = "completed" if job.get("status") != "failed" else job["status"]
                job["finished_at"] = time.time()
        current_job_id = None


def queue_worker():
    """单实例队列worker，保证一次只执行一个任务。"""
    while True:
        job_id = job_queue.get()  # 阻塞等待
        try:
            execute_crawl_job(job_id)
        finally:
            job_queue.task_done()

# 模块导入即启动队列 worker（在开发模式/Flask 内置服务器下也生效）
try:
    _worker_started
except NameError:
    _worker_started = False

if not _worker_started:
    worker = threading.Thread(target=queue_worker, daemon=True)
    worker.start()
    _worker_started = True
    logging.getLogger(__name__).info("任务队列Worker已启动")

# API路由
@app.route('/api/update', methods=['GET'])
def update_data():
    """创建一个新任务并入队，返回 job_id 与队列位置。"""
    global jobs

    with jobs_lock:
        job_id = uuid.uuid4().hex
        # 队列位置 = 当前队列长度 + （若当前有运行任务则+1，否则0）
        position = job_queue.qsize() + (1 if crawler_running else 0) + 1  # 包含本任务
        jobs[job_id] = {
            'id': job_id,
            'status': 'queued',
            'enqueued_at': time.time(),
        }

    job_queue.put(job_id)

    return jsonify({
        'status': 'queued',
        'job_id': job_id,
        'position': position,
        'message': '任务已入队，等待执行'
    })

@app.route('/api/status', methods=['GET'])
def check_status():
    global crawler_running, data_updated
    jid = request.args.get('job_id')

    # 若带 job_id：返回该任务的独立状态
    if jid:
        with jobs_lock:
            job = jobs.get(jid)
            # 计算队列位置
            queued = [j for j in jobs.values() if j['status'] == 'queued']
            queued_sorted = sorted(queued, key=lambda x: x['enqueued_at'])
            position_map = {j['id']: ((1 if current_job_id else 0) + idx) for idx, j in enumerate(queued_sorted, start=1)}

        if not job:
            return jsonify({'status': 'unknown', 'message': '未找到该任务'}), 404

        status = job.get('status')
        if status == 'queued':
            return jsonify({
                'status': 'queued',
                'position': position_map.get(jid, None),
                'message': '任务排队中'
            })
        if status == 'running':
            return jsonify({'status': 'running', 'message': '任务运行中'})
        if status == 'completed':
            return jsonify({
                'status': 'completed',
                'updated': job.get('updated', False),
                'message': '数据已更新' if job.get('updated') else '所有数据均已是最新，无需更新'
            })
        if status == 'failed':
            return jsonify({
                'status': 'failed',
                'error': job.get('error', '未知错误')
            })
        return jsonify({'status': status or 'unknown'})

    # 兼容：全局状态
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
    job_id = request.args.get('job_id')

    def generate_for_job(jid: str):
        # 初次发送该任务现有的所有日志
        with jobs_lock:
            buf = [entry for entry in job_log_buffers.get(jid, []) if not is_system_log(entry.get('message',''))]
        if buf:
            json_str = json.dumps(buf, ensure_ascii=False)
            yield f"data: {json_str}\n\n"

        last_size = len(buf)
        finished_sent = False

        while True:
            time.sleep(0.5)
            with jobs_lock:
                buf_now = [entry for entry in job_log_buffers.get(jid, []) if not is_system_log(entry.get('message',''))]
                job_state = jobs.get(jid, {})
                is_running_this_job = current_job_id == jid and crawler_running

            if len(buf_now) > last_size:
                new_logs = buf_now[last_size:]
                json_str = json.dumps(new_logs, ensure_ascii=False)
                yield f"data: {json_str}\n\n"
                last_size = len(buf_now)

            # 若任务已完成，仅在同时包含 SUMMARY_END 与 EXCEL_UNLOCKED 且无新增日志时才结束流
            if not is_running_this_job and job_state.get('status') in ('completed', 'failed'):
                has_summary_end = any((entry.get('message') == 'SUMMARY_END') for entry in buf_now)
                has_excel_unlocked = any((entry.get('message') == 'EXCEL_UNLOCKED') for entry in buf_now)
                if has_summary_end and has_excel_unlocked and len(buf_now) == last_size:
                    break

    def generate_global():
        # 旧行为：推送全局日志（可能包含其他任务日志）
        logs = [entry for entry in list(log_queue.queue) if not is_system_log(entry.get('message',''))]
        if logs:
            json_str = json.dumps(logs, ensure_ascii=False)
            yield f"data: {json_str}\n\n"

        last_size = len(logs)
        while True:
            time.sleep(0.5)
            current_logs = [entry for entry in list(log_queue.queue) if not is_system_log(entry.get('message',''))]
            if len(current_logs) > last_size:
                new_logs = current_logs[last_size:]
                json_str = json.dumps(new_logs, ensure_ascii=False)
                yield f"data: {json_str}\n\n"
                last_size = len(current_logs)
            if not crawler_running:
                # 发送结束消息
                end_message = [
                    {"level": "INFO", "message": "=== 数据更新完成 ===", "timestamp": datetime.now().strftime('%H:%M:%S')},
                    {"level": "INFO", "message": "SHOW_SUMMARY", "timestamp": datetime.now().strftime('%H:%M:%S')}
                ]
                json_str = str(end_message).replace("'", '"')
                yield f"data: {json_str}\n\n"
                break

    generator = generate_for_job(job_id) if job_id else generate_global()
    return Response(
        stream_with_context(generator),
        content_type='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no'
        }
    )

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

@app.route('/api/queue', methods=['GET'])
def queue_info():
    """返回队列看板信息。"""
    with jobs_lock:
        # 计算排队中的任务（非 running/completed/failed）
        queued = [j for j in jobs.values() if j['status'] == 'queued']
        queued_sorted = sorted(queued, key=lambda x: x['enqueued_at'])

        running = None
        if current_job_id and current_job_id in jobs:
            running = jobs[current_job_id]

        completed_recent = [j for j in jobs.values() if j['status'] in ('completed', 'failed')]
        # 只返回最近的最多10条历史
        completed_recent = sorted(completed_recent, key=lambda x: x.get('finished_at', 0), reverse=True)[:10]

        # 计算队列内位置
        for idx, j in enumerate(queued_sorted, start=1):
            j['position'] = (1 if running else 0) + idx

        return jsonify({
            'running': running,
            'queued': queued_sorted,
            'history': completed_recent,
            'queue_size': len(queued_sorted),
            'running_flag': crawler_running
        })

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
    # 启动单例队列worker
    try:
        _worker_started
    except NameError:
        _worker_started = False

    if not _worker_started:
        worker = threading.Thread(target=queue_worker, daemon=True)
        worker.start()
        _worker_started = True
        logger.info("任务队列Worker已启动")

    # 开发环境配置
    app.run(debug=True, host='0.0.0.0', port=5000)