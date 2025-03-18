let eventSource = null;
let statusCheckInterval = null;
let apiBaseUrl = "/api"; // API基础路径，可根据部署环境修改

// 添加日志过滤状态
let logFilters = {
  DEBUG: true,
  INFO: true,
  WARNING: true,
  ERROR: true,
};

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

  // 清空日志容器
  logContainer.innerHTML = "";

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

      // 开始监听日志流
      connectLogStream();

      // 开始定期检查状态
      statusCheckInterval = setInterval(checkStatus, 2000);
    })
    .catch((error) => {
      console.error("Error:", error);
      statusMessage.textContent = "启动更新失败，请刷新页面重试。";
      updateBtn.disabled = false;
      updateBtn.innerHTML = '<span class="button-text">更新数据</span>';
    });
}

// 连接日志流
function connectLogStream() {
  // 如果已经有连接，先关闭
  if (eventSource) {
    eventSource.close();
  }

  console.log("开始连接日志流...");
  eventSource = new EventSource(`${apiBaseUrl}/logs`);

  eventSource.onmessage = function (event) {
    console.log("收到日志数据:", event.data);
    const logContainer = document.getElementById("logContainer");

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

        // 只显示DEBUG级别以上的日志
        if (["DEBUG", "INFO", "WARNING", "ERROR"].includes(log.level)) {
          appendLog(log);
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

    if (logContainer) {
      const errorEntry = document.createElement("div");
      errorEntry.className = "log-entry log-error";
      errorEntry.innerHTML = `
        <span class="log-timestamp">${new Date().toLocaleTimeString()}</span>
        <span class="log-icon">❌</span>
        <span class="log-level">ERROR</span>
        <span class="log-message">日志流连接断开，正在尝试重新连接...</span>
      `;
      logContainer.appendChild(errorEntry);
    }

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
  fetch(`${apiBaseUrl}/status`)
    .then((response) => response.json())
    .then((data) => {
      const updateBtn = document.getElementById("updateBtn");
      const downloadBtn = document.getElementById("downloadBtn");
      const statusMessage = document.getElementById("statusMessage");

      // 如果爬虫已完成
      if (data.status === "completed") {
        // 清除定时器
        clearInterval(statusCheckInterval);
        statusCheckInterval = null;

        // 关闭日志流
        if (eventSource) {
          eventSource.close();
          eventSource = null;
        }

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
