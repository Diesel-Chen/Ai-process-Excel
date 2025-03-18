FROM python:3.9-slim

# 安装依赖包
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# 安装Chrome浏览器
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制必要文件
COPY requirements.txt .

# 创建虚拟环境
RUN python -m venv .venv && \
    . .venv/bin/activate && \
    pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY src/ src/
COPY static/ static/
COPY wsgi.py .
COPY start.py .
COPY "Market Index.xlsx" .

# 设置环境变量
ENV PYTHONPATH=/app:$PYTHONPATH
ENV DISPLAY=:99

# 启动虚拟显示和应用
CMD Xvfb :99 -screen 0 1280x1024x24 -ac &> /dev/null & \
    python start.py --mode prod --host 0.0.0.0 --port 8080 --no-browser

# 暴露端口
EXPOSE 8080