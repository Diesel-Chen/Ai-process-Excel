let eventSource = null;
let statusCheckInterval = null;
let queueInterval = null;
let currentJobId = null;
let jobSummaryReceived = false; // 本次任务是否已收到 SHOW_SUMMARY
let excelUnlocked = false; // 本次任务是否已释放 Excel 文件锁
let firstCompletedAt = null; // 第一次检测到 status=completed 的时间戳
let apiBaseUrl = "/api"; // API基础路径，可根据部署环境修改
let modalEventSource = null; // 任务日志弹窗的 SSE 连接
let modalSeenLogKeys = null; // 当前弹窗中已渲染日志的去重集合

// 添加日志过滤状态
let logFilters = {
  DEBUG: true,
  INFO: true,
  WARNING: true,
  ERROR: true,
};

// 前端忽略的日志模式（不渲染到页面）
// 1) 本地访问日志 127.0.0.1
// 2) webdriver_manager 缓存提示
const IGNORED_LOG_PATTERNS = [
  /127\.0\.0\.1\b/,
  /found in cache/i,
];

function shouldIgnoreLog(log) {
  try {
    const text = `${log.timestamp} ${log.level} ${log.message}`;
    return IGNORED_LOG_PATTERNS.some((re) => re.test(text));
  } catch (e) {
    return false;
  }
}

// 添加过滤器UI
function createFilterUI() {
  const filterContainer = document.createElement("div");
  filterContainer.className = "filter-container";

  ["DEBUG", "INFO", "WARNING", "ERROR"].forEach((level) => {
    const label = document.createElement("label");
    label.className = "filter-label";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = logFilters[level];
    checkbox.className = "filter-checkbox";
    checkbox.addEventListener("change", (e) => {
      logFilters[level] = e.target.checked;
      updateLogVisibility();
    });

    label.appendChild(checkbox);
    label.appendChild(document.createTextNode(` ${level}`));
    filterContainer.appendChild(label);
  });

  // 插入到日志容器之前
  const logContainer = document.getElementById("logContainer");
  logContainer.parentNode.insertBefore(filterContainer, logContainer);
}

// 更新日志可见性
function updateLogVisibility() {
  const logEntries = document.querySelectorAll(".log-entry");
  logEntries.forEach((entry) => {
    const level = entry.className.split(" ")[1].toUpperCase();
    entry.style.display = logFilters[level] ? "block" : "none";
  });
}

// 启动数据更新
function startUpdate() {
  const updateBtn = document.getElementById("updateBtn");
  const downloadBtn = document.getElementById("downloadBtn");
  const statusMessage = document.getElementById("statusMessage");
  const logContainer = document.getElementById("logContainer");
  const logSummary = document.getElementById("logSummary");

  // 清空日志容器并隐藏摘要
  logContainer.innerHTML = "";
  logSummary.style.display = "none";
  jobSummaryReceived = false;
  excelUnlocked = false;
  firstCompletedAt = null;

  // 更新按钮状态
  updateBtn.disabled = true;
  updateBtn.innerHTML =
    '<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> 正在更新...';
  downloadBtn.style.display = "none";
  downloadBtn.disabled = true;

  // 更新状态消息
  statusMessage.innerHTML =
    '<span class="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></span> 正在爬取最新数据，请稍候...';

  // 发送更新请求
  fetch(`${apiBaseUrl}/update`)
    .then((response) => response.json())
    .then((data) => {
      console.log(data);

      // 显示队列信息
      if (data.job_id) {
        currentJobId = data.job_id;
      }
      if (data.position && data.position > 1) {
        statusMessage.innerHTML = `
          <span class="spinner-border spinner-border-sm text-warning" role="status" aria-hidden="true"></span>
          已加入队列，当前排队位置第 <b>${data.position}</b> 位，请稍候...`;
      }

      // 开始监听日志流（即使在队列中，后端会在任务启动时清空并写入日志）
      connectLogStream(currentJobId);

      // 开始定期检查状态
      statusCheckInterval = setInterval(checkStatus, 2000);

      // 开始轮询队列看板
      if (queueInterval) clearInterval(queueInterval);
      queueInterval = setInterval(pollQueue, 2000);
      // 立即拉取一次看板
      pollQueue();
    })
    .catch((error) => {
      console.error("Error:", error);
      statusMessage.textContent = "启动更新失败，请刷新页面重试。";
      updateBtn.disabled = false;
      updateBtn.innerHTML = '<span class="button-text">更新数据</span>';
    });
}

// 连接日志流
function connectLogStream(jobId = null) {
  // 如果已经有连接，先关闭
  if (eventSource) {
    eventSource.close();
  }

  console.log("开始连接日志流...");
  const url = jobId ? `${apiBaseUrl}/logs?job_id=${encodeURIComponent(jobId)}` : `${apiBaseUrl}/logs`;
  eventSource = new EventSource(url);

  // 用于收集摘要信息的变量
  let collectingSummary = false;
  let summaryText = "";

  eventSource.onmessage = function (event) {
    console.log("收到日志数据:", event.data);
    const logContainer = document.getElementById("logContainer");
    const logSummary = document.getElementById("logSummary");
    const logSummaryContent = document.getElementById("logSummaryContent");

    if (!logContainer) {
      console.error("找不到日志容器元素");
      return;
    }

    try {
      const logs = JSON.parse(event.data);

      // 确保logs是数组
      if (!Array.isArray(logs)) {
        console.error("收到非数组格式的日志数据:", logs);
        return;
      }

      logs.forEach((log) => {
        // 检查日志对象的完整性
        if (!log || !log.level || !log.message || !log.timestamp) {
          console.error("无效的日志条目:", log);
          return;
        }

        // 检查是否是摘要信息的开始或结束标记
        console.log(`检查日志消息: "${log.message}"`); // 打印每条日志消息以进行调试

        if (log.message.trim() === "SUMMARY_START") {
          console.log("检测到摘要开始标记");
          collectingSummary = true;
          summaryText = "";
          return;
        } else if (log.message.trim() === "SUMMARY_END") {
          console.log("检测到摘要结束标记");
          collectingSummary = false;
          console.log("摘要文本已收集完成:", summaryText);
          // 将 SUMMARY_END 也视为摘要就绪的信号（与 SHOW_SUMMARY 等效）
          if (summaryText && logSummaryContent && logSummary) {
            logSummaryContent.textContent = summaryText;
            logSummary.style.display = "block";
            jobSummaryReceived = true;
          }
          return;
        } else if (log.message.trim() === "SHOW_SUMMARY") {
          console.log("检测到显示摘要消息");
          // 显示摘要信息
          console.log("摘要文本:", summaryText);
          console.log("摘要元素:", logSummaryContent ? "存在" : "不存在", logSummary ? "存在" : "不存在");

          if (summaryText && logSummaryContent && logSummary) {
            logSummaryContent.textContent = summaryText;
            logSummary.style.display = "block";
            logContainer.scrollTop = logContainer.scrollHeight;
            console.log("摘要显示已设置");
            jobSummaryReceived = true; // 标记已收到任务结束摘要
          } else {
            console.error("无法显示摘要: ", {
              summaryText: Boolean(summaryText),
              logSummaryContent: Boolean(logSummaryContent),
              logSummary: Boolean(logSummary)
            });
          }
          return;
        } else if (log.message.trim() === "EXCEL_UNLOCKED") {
          // 后端在释放 Excel 文件锁后打点，告知可以进行下载
          excelUnlocked = true;
          return;
        }

        // 如果正在收集摘要信息，则将日志消息添加到摘要文本中
        if (collectingSummary) {
          console.log(`添加摘要行: "${log.message}"`);
          summaryText += log.message + "\n";
          return;
        }

        // 只显示DEBUG级别以上的日志，且过滤掉本地访问类/缓存提示类“系统日志”
        if (["DEBUG", "INFO", "WARNING", "ERROR"].includes(log.level)) {
          if (!shouldIgnoreLog(log)) {
            appendLog(log);
          }
        }
      });
    } catch (error) {
      console.error("解析日志数据时出错:", error);
      console.error("原始数据:", event.data);
    }
  };

  eventSource.onerror = function (error) {
    console.error("日志流连接错误:", error);
    const logContainer = document.getElementById("logContainer");

    // if (logContainer) {
    //   const errorEntry = document.createElement("div");
    //   errorEntry.className = "log-entry log-error";
    //   errorEntry.innerHTML = `
    //     <span class="log-timestamp">${new Date().toLocaleTimeString()}</span>
    //     <span class="log-icon">❌</span>
    //     <span class="log-level">ERROR</span>
    //     <span class="log-message">日志流连接断开，正在尝试重新连接...</span>
    //   `;
    //   logContainer.appendChild(errorEntry);
    // }

    // 如果连接关闭，尝试重新连接
    if (eventSource.readyState === EventSource.CLOSED) {
      console.log("连接已关闭，3秒后尝试重新连接...");
      setTimeout(connectLogStream, 3000);
    }
  };

  eventSource.onopen = function () {
    console.log("日志流连接已建立");
    const logContainer = document.getElementById("logContainer");

    if (logContainer) {
      const connectEntry = document.createElement("div");
      connectEntry.className = "log-entry log-info";
      connectEntry.innerHTML = `
        <span class="log-timestamp">${new Date().toLocaleTimeString()}</span>
        <span class="log-icon">ℹ️</span>
        <span class="log-level">INFO</span>
        <span class="log-message">日志流连接已建立</span>
      `;
      logContainer.appendChild(connectEntry);
    }
  };
}

// 检查爬虫状态
function checkStatus() {
  const statusUrl = currentJobId
    ? `${apiBaseUrl}/status?job_id=${encodeURIComponent(currentJobId)}`
    : `${apiBaseUrl}/status`;
  fetch(statusUrl)
    .then((response) => response.json())
    .then((data) => {
      const updateBtn = document.getElementById("updateBtn");
      const downloadBtn = document.getElementById("downloadBtn");
      const statusMessage = document.getElementById("statusMessage");

      // 如果该 job 已完成（不依赖全局）
      if (data.status === "completed") {
        // 若还未收到 SHOW_SUMMARY，继续保持连接，等待日志收尾
        if (!jobSummaryReceived || !excelUnlocked) {
          // 记录第一次完成时间
          if (!firstCompletedAt) firstCompletedAt = Date.now();
          const elapsed = Date.now() - firstCompletedAt;
          // 超过 3s 仍未收到标记，则容错放行，避免卡死
          if (elapsed > 3000) {
            console.warn("Grace finishing: summary or excel unlock not received in time");
          } else {
            statusMessage.innerHTML = '<span class="text-info">ℹ</span> 正在收尾并生成摘要，请稍候...';
            return;
          }
        }
        // 清除定时器
        clearInterval(statusCheckInterval);
        statusCheckInterval = null;

        // 关闭日志流
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }

        // 队列看板保持轮询，但降低频率
        if (queueInterval) clearInterval(queueInterval);
        queueInterval = setInterval(pollQueue, 5000);

        // 更新UI
        updateBtn.disabled = false;
        updateBtn.innerHTML = '<span class="button-text">更新数据</span>';

        if (data.updated) {
          statusMessage.innerHTML =
            '<span class="text-success">✓</span> 数据已成功更新，可以下载最新的Excel文件。';
          downloadBtn.style.display = "inline-block";
          downloadBtn.disabled = false;
        } else {
          statusMessage.innerHTML =
            '<span class="text-info">ℹ</span> 所有数据均已是最新，无需更新。';
          downloadBtn.style.display = "inline-block";
          downloadBtn.disabled = false;
        }
      } else if (data.status === "failed") {
        // 任务失败
        clearInterval(statusCheckInterval);
        statusCheckInterval = null;
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }
        if (queueInterval) clearInterval(queueInterval);
        queueInterval = setInterval(pollQueue, 5000);

        updateBtn.disabled = false;
        updateBtn.innerHTML = '<span class="button-text">更新数据</span>';
        statusMessage.innerHTML = `<span class="text-danger">✗</span> 任务失败：${data.error || '未知错误'}`;
        downloadBtn.style.display = "inline-block";
        downloadBtn.disabled = false;
      } else if (data.status === "queued" && data.position) {
        // 若仍在排队，刷新提示位置
        statusMessage.innerHTML = `
          <span class="spinner-border spinner-border-sm text-warning" role="status" aria-hidden="true"></span>
          已加入队列，当前排队位置第 <b>${data.position}</b> 位，请稍候...`;
      }
    })
    .catch((error) => {
      console.error("Error:", error);
    });
}

// 下载Excel文件
function downloadExcel() {
  window.location.href = `${apiBaseUrl}/download`;
}

// 显示统计摘要面板
function showSummaryPanel(summaryData) {
  const logSummary = document.getElementById("logSummary");
  const logSummaryContent = document.getElementById("logSummaryContent");

  logSummaryContent.textContent = summaryData;
  logSummary.style.display = "block";
  logContainer.scrollTop = logContainer.scrollHeight;
}

// 下载统计报告
function downloadLogSummary() {
  const content = document.getElementById("logSummaryContent").textContent;
  const blob = new Blob([content], { type: "text/plain" });
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `数据统计报告_${new Date().toLocaleDateString().replace(/\//g, "-")}.txt`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  window.URL.revokeObjectURL(url);
}

// 当文档加载完成时，检查URL中是否有API基础路径参数
document.addEventListener("DOMContentLoaded", function () {
  // 从URL参数中获取API基础路径
  const urlParams = new URLSearchParams(window.location.search);
  const apiUrl = urlParams.get("apiUrl");

  if (apiUrl) {
    apiBaseUrl = apiUrl;
    console.log(`从URL参数设置API基础路径: ${apiBaseUrl}`);
  }

  createFilterUI();
  // 初始拉取一次队列看板（页面刷新时也能看到状态）
  pollQueue();
  queueInterval = setInterval(pollQueue, 5000);
});

function appendLog(log) {
  const logContainer = document.getElementById("logContainer");
  if (!logContainer) {
    console.error("找不到日志容器元素");
    return;
  }

  // 创建新的日志条目
  const logEntry = document.createElement("div");
  logEntry.className = `log-entry ${log.level.toLowerCase()}`;

  // 处理消息格式
  let message = log.message;

  // 特殊处理统计摘要
  if (message.includes("===== 爬取统计摘要 =====")) {
    logEntry.className += " summary-header";
    if (message.includes("===== 爬取统计摘要 =====")) {
    showSummaryPanel(log.message); // 显示统计摘要面板
    return; // 跳过常规日志显示
  }
  message = "\n" + message; // 添加额外的换行
  } else if (message.startsWith("  ")) {
    logEntry.className += " summary-item";
    // 保持缩进
    message = message.trim();
  } else if (
    message.startsWith("成功:") ||
    message.startsWith("失败:") ||
    message.startsWith("跳过:")
  ) {
    logEntry.className += " summary-item";
    message = "  " + message; // 添加缩进
  }

  // 设置日志内容
  logEntry.textContent = `${log.timestamp} - ${log.level} - ${message}`;

  // 添加到容器
  logContainer.appendChild(logEntry);

  // 自动滚动到底部
  logContainer.scrollTop = logContainer.scrollHeight;
}

// 队列看板：轮询后端 /api/queue
function pollQueue() {
  fetch(`${apiBaseUrl}/queue`)
    .then((res) => res.json())
    .then(renderQueue)
    .catch((err) => console.error("获取队列信息失败:", err));
}

function renderQueue(data) {
  const runningEl = document.getElementById("queueRunning");
  const queuedEl = document.getElementById("queueQueued");
  const historyEl = document.getElementById("queueHistory");
  if (!runningEl || !queuedEl || !historyEl) return;

  // 时间格式化工具（显示“加入队列时间”等）
  function formatTime(ts) {
    if (!ts) return "-";
    const d = new Date(ts * 1000);
    const pad = (n) => (n < 10 ? "0" + n : "" + n);
    const yyyy = d.getFullYear();
    const mm = pad(d.getMonth() + 1);
    const dd = pad(d.getDate());
    const hh = pad(d.getHours());
    const mi = pad(d.getMinutes());
    const ss = pad(d.getSeconds());
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
  }

  // Running
  if (data.running) {
    runningEl.innerHTML = `
      <div class="list-group-item d-flex justify-content-between align-items-center">
        <div>
          <div>Job ID: <a href="#" class="job-log-link" data-job-id="${data.running.id}" data-enqueued-at="${data.running.enqueued_at || ''}"><code>${data.running.id}</code></a></div>
          <div class="small text-muted">状态：${data.running.status}</div>
          <div class="small text-muted">加入队列：${formatTime(data.running.enqueued_at)}</div>
        </div>
        <span class="badge bg-primary">运行中</span>
      </div>`;
  } else if (data.running_flag) {
    runningEl.innerHTML = `<div class="list-group-item">任务运行中...</div>`;
  } else {
    runningEl.innerHTML = `<div class="list-group-item">暂无运行中的任务</div>`;
  }

  // Queued
  if (data.queued && data.queued.length > 0) {
    queuedEl.innerHTML = data.queued
      .map(
        (j) => `
        <div class="list-group-item d-flex justify-content-between align-items-center">
          <div>
            <div>Job ID: <a href="#" class="job-log-link" data-job-id="${j.id}" data-enqueued-at="${j.enqueued_at || ''}"><code>${j.id}</code></a></div>
            <div class="small text-muted">排队位置：第 ${j.position} 位</div>
            <div class="small text-muted">加入队列：${formatTime(j.enqueued_at)}</div>
          </div>
          <span class="badge bg-warning text-dark">排队中</span>
        </div>`
      )
      .join("");
  } else {
    queuedEl.innerHTML = `<div class="list-group-item">队列为空</div>`;
  }

  // History
  if (data.history && data.history.length > 0) {
    historyEl.innerHTML = data.history
      .map((j) => {
        const ok = j.status === "completed" && j.updated;
        const badge = j.status === "failed" ?
          '<span class="badge bg-danger">失败</span>' :
          `<span class="badge ${ok ? 'bg-success' : 'bg-secondary'}">${ok ? '已更新' : '无变动'}</span>`;
        return `
        <div class="list-group-item d-flex justify-content-between align-items-center">
          <div>
            <div>Job ID: <a href="#" class="job-log-link" data-job-id="${j.id}" data-enqueued-at="${j.enqueued_at || ''}"><code>${j.id}</code></a></div>
            <div class="small text-muted">状态：${j.status}${j.error ? '，错误：' + j.error : ''}</div>
            <div class="small text-muted">加入队列：${formatTime(j.enqueued_at)}</div>
          </div>
          ${badge}
        </div>`;
      })
      .join("");
  } else {
    historyEl.innerHTML = `<div class="list-group-item">暂无历史</div>`;
  }

  // 绑定 Job 日志弹窗事件
  bindJobLogLinks();
}

// 绑定“Job ID”点击事件，打开日志弹窗并接入该 job 的日志 SSE
function bindJobLogLinks() {
  document.querySelectorAll('.job-log-link').forEach((a) => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      const jobId = a.getAttribute('data-job-id');
      const enq = a.getAttribute('data-enqueued-at');
      openJobLogModal(jobId, enq);
    });
  });
}

function openJobLogModal(jobId, enqueuedAt) {
  // 元信息
  const metaEl = document.getElementById('jobLogMeta');
  const container = document.getElementById('jobLogContainer');
  if (!metaEl || !container) return;

  metaEl.textContent = `Job ID: ${jobId}  |  加入队列：${enqueuedAt ? new Date(enqueuedAt * 1000).toLocaleString() : '-'}`;
  container.innerHTML = '';
  // 重置去重集合
  modalSeenLogKeys = new Set();

  // 关闭旧的 SSE 连接
  if (modalEventSource) {
    try { modalEventSource.close(); } catch (_) {}
    modalEventSource = null;
  }

  // 打开 SSE 读取该 job 的日志
  const url = `${apiBaseUrl}/logs?job_id=${encodeURIComponent(jobId)}`;
  modalEventSource = new EventSource(url);

  modalEventSource.onmessage = (event) => {
    try {
      const logs = JSON.parse(event.data);
      if (!Array.isArray(logs)) return;
      const autoChk = document.getElementById('jobLogAutoScroll');
      const nearBottom = () => {
        const delta = container.scrollHeight - (container.scrollTop + container.clientHeight);
        return delta < 40; // 40px 内认为靠近底部
      };
      logs.forEach((log) => {
        if (!log || !log.level || !log.message || !log.timestamp) return;
        if (!shouldIgnoreLog(log)) {
          // 优先使用后端提供的全局递增 seq 做严格去重；若无 seq 再回退到 复合键
          const key = (log.seq !== undefined && log.seq !== null)
            ? `seq:${log.seq}`
            : `${log.timestamp}|${log.level}|${log.message}`;
          if (modalSeenLogKeys && modalSeenLogKeys.has(key)) {
            return; // 已渲染，跳过
          }
          const div = document.createElement('div');
          div.className = `log-entry ${log.level.toLowerCase()}`;
          div.textContent = `${log.timestamp} - ${log.level} - ${log.message}`;
          container.appendChild(div);
          if (modalSeenLogKeys) modalSeenLogKeys.add(key);
          // 仅在勾选“自动跟随最新”且用户当前接近底部时自动滚动，避免阅读历史时跳动
          if (autoChk && autoChk.checked && nearBottom()) {
            container.scrollTop = container.scrollHeight;
          }
        }
      });
    } catch (_) {}
  };

  modalEventSource.onerror = () => {
    // 弹窗内的 SSE 出错时静默处理，可能是流结束
  };

  // 打开 Bootstrap 弹窗
  try {
    const modalEl = document.getElementById('jobLogModal');
    if (!modalEl) return;
    const modal = new bootstrap.Modal(modalEl);
    modal.show();

    // 弹窗关闭时断开 SSE
    modalEl.addEventListener('hidden.bs.modal', () => {
      if (modalEventSource) {
        try { modalEventSource.close(); } catch (_) {}
        modalEventSource = null;
      }
    }, { once: true });
  } catch (e) {
    console.error('打开日志弹窗失败，是否已引入 Bootstrap JS?', e);
  }
}

// 更新 CSS 样式
const style = document.createElement("style");
style.textContent = `
    .filter-container {
        margin-bottom: 15px;
        padding: 10px;
        background: #f8f9fa;
        border-radius: 8px;
        display: flex;
        gap: 20px;
        align-items: center;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    }

    .filter-label {
        display: inline-flex;
        align-items: center;
        padding: 6px 12px;
        background: #fff;
        border-radius: 6px;
        border: 1px solid #e1e4e8;
        cursor: pointer;
        user-select: none;
        transition: all 0.2s ease;
        font-size: 13px;
        color: #444;
    }

    .filter-label:hover {
        border-color: #0066ff;
        color: #0066ff;
    }

    .filter-checkbox {
        appearance: none;
        -webkit-appearance: none;
        -moz-appearance: none;
        width: 16px;
        height: 16px;
        border: 1px solid #d1d5db;
        border-radius: 4px;
        margin-right: 8px;
        position: relative;
        cursor: pointer;
        transition: all 0.2s ease;
        background-color: white;
    }

    .filter-checkbox:checked {
        background-color: #4a90e2;
        border-color: #4a90e2;
    }

    .filter-checkbox:checked::after {
        content: "✓";
        position: absolute;
        color: white;
        font-size: 11px;
        left: 2px;
        top: -1px;
        font-weight: bold;
    }

    .filter-checkbox:hover {
        border-color: #4a90e2;
    }

    /* 队列看板固定高度，仅显示约两条，超出滚动 */
    #queueRunning, #queueQueued, #queueHistory {
        max-height: 120px; /* 约两条 list-group-item 的高度 */
        overflow-y: auto;
        border: 1px solid #e1e4e8;
        border-radius: 6px;
    }

    #logContainer {
        font-family: Menlo, Monaco, 'Courier New', monospace;
        height: 600px;
        overflow-y: auto;
        margin: 0;
        padding: 15px;
        font-size: 12px;
        line-height: 1.5;
        background: #fff;
        border: 1px solid #e1e4e8;
        border-radius: 8px;
    }

    .log-entry {
        white-space: pre;
        margin: 1px 0;
        padding: 1px 0;
    }

    .log-entry.debug {
        color: #666;
    }

    .log-entry.info {
        color: #0066ff;
    }

    .log-entry.warning {
        color: #997700;
    }

    .log-entry.error {
        color: #cc0000;
    }

    .log-entry.summary-header {
        margin-top: 15px;
        margin-bottom: 10px;
        font-weight: 600;
        color: #24292e;
        border-top: 1px solid #e1e4e8;
        padding-top: 10px;
    }

    .log-entry.summary-item {
        color: #24292e;
        padding-left: 20px;
        margin-bottom: 4px;
    }

    /* 滚动条样式 */
    #logContainer::-webkit-scrollbar {
        width: 8px;
    }

    #logContainer::-webkit-scrollbar-track {
        background: #f8f9fa;
        border-radius: 4px;
    }

    #logContainer::-webkit-scrollbar-thumb {
        background: #dfe2e5;
        border-radius: 4px;
        border: 2px solid #f8f9fa;
    }

    #logContainer::-webkit-scrollbar-thumb:hover {
        background: #c8ccd0;
    }
`;
document.head.appendChild(style);
