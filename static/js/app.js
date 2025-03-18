let eventSource = null;
let statusCheckInterval = null;
let apiBaseUrl = "/api"; // API基础路径，可根据部署环境修改

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

  eventSource = new EventSource(`${apiBaseUrl}/logs`);

  eventSource.onmessage = function (event) {
    const logContainer = document.getElementById("logContainer");
    const logs = JSON.parse(event.data);

    logs.forEach((log) => {
      const logEntry = document.createElement("div");
      logEntry.className = `log-entry log-${log.level}`;
      logEntry.textContent = `${log.timestamp} - ${log.level} - ${log.message}`;
      logContainer.appendChild(logEntry);
    });

    // 自动滚动到底部
    logContainer.scrollTop = logContainer.scrollHeight;
  };

  eventSource.onerror = function () {
    console.error("日志流连接错误");
    // 尝试重新连接
    setTimeout(connectLogStream, 3000);
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
});
