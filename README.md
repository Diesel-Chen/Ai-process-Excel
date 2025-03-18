# 市场数据自动获取工具 (前后端版本)

这是一个自动从各种网站获取最新市场数据（汇率、钢铁价格、利率等）并更新到 Excel 文件的系统，采用前后端分离架构。

## 功能特点

- 支持多个货币对的数据获取
- 自动更新 Excel 文件中的对应 sheet
- 支持 Windows、Mac 和 Linux 平台
- 包含错误处理和重试机制
- 使用随机 User-Agent 避免被封禁
- 使用无头模式（不显示浏览器窗口）
- 自动下载和管理 WebDriver，无需手动安装
- 支持多种浏览器（Chrome、Firefox、Edge）
- 提供 Web 界面，方便用户操作
- 通过 API 接口获取数据，支持远程访问
- 实时日志显示，可查看爬取进度
- 根据更新结果提供 Excel 下载服务
- 支持独立部署前端和后端
- 提供完整的生产环境部署支持
- 自动检测和激活虚拟环境

## 支持的数据类型

### 汇率数据

- USD/CNY (美元/人民币)
- EUR/CNY (欧元/人民币)
- EUR/USD (欧元/美元)
- HKD/CNY (港币/人民币)
- CNY/HKD (人民币/港币)
- JPY/USD (日元/美元)
- USD 10Y (美国 10 年期国债收益率)

### 日频数据

- Steel price (钢铁价格)
- SOFR (担保隔夜融资利率)
- ESTER (欧元短期利率)
- JPY rate (日元利率)
- Shibor (上海银行间同业拆放利率)
- LPR (贷款市场报价利率)

### 月度数据

- US Interest Rate (美国利率)
- Import and Export (进出口数据)
- Money Supply (货币供应量)
- PPI (生产者价格指数)
- CPI (消费者价格指数)
- PMI (采购经理指数)
- New Bank Loan Addition (新增信贷)

## 安装要求

- Python 3.8+
- pip 包管理器
- 至少安装了以下浏览器之一：Chrome、Firefox 或 Edge
- 网络连接（用于获取数据）

## 安装步骤

1. 克隆或下载本项目
2. 创建并激活虚拟环境（推荐）:

```bash
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

3. 安装依赖包：

```bash
pip install -r requirements.txt
```

## 使用方法

### 开发环境

启动开发服务器：

```bash
python start.py --mode dev
```

服务器将在 http://localhost:8080 上运行，并自动打开浏览器。

> **注意**：启动脚本会自动检测项目目录中的 `.venv` 虚拟环境并尝试激活它。如果不在虚拟环境中运行，脚本会自动尝试激活虚拟环境后再运行应用程序。

### 生产环境

启动生产服务器：

```bash
python start.py --mode prod
```

或者使用更专业的 WSGI 服务器（如 uwsgi、gunicorn）：

```bash
# 使用uwsgi
uwsgi --ini uwsgi.ini

# 使用gunicorn
gunicorn -w 4 -b 0.0.0.0:8080 wsgi:application
```

### 命令行参数

`start.py` 脚本支持以下命令行参数：

- `--mode`: 运行模式，可选 `dev`（开发）或 `prod`（生产），默认为 `dev`
- `--port`: 服务器端口，默认为 8080
- `--host`: 服务器主机，默认为 0.0.0.0
- `--no-browser`: 添加此参数不会自动打开浏览器

例如：

```bash
python start.py --mode prod --port 9000 --no-browser
```

### 访问 Web 界面

1. 开发环境: http://localhost:8080
2. 生产环境（通过 Nginx）: http://your-domain.com/market-data/

### API 接口

- **GET /api/update**: 启动数据更新过程
- **GET /api/status**: 获取当前爬取状态
- **GET /api/logs**: 获取实时日志流（使用 Server-Sent Events）
- **GET /api/download**: 下载最新的 Excel 文件

## 项目结构

```
.
├── src/                    # 源代码目录
│   ├── app.py              # Flask应用服务器
│   ├── market_data_crawler.py  # 爬虫核心代码
│   └── config.py           # 配置文件
├── static/                 # 静态文件
│   ├── css/                # 样式表
│   │   └── style.css       # 主样式表
│   ├── js/                 # JavaScript文件
│   │   └── app.js          # 主脚本文件
│   ├── img/                # 图片资源
│   └── index.html          # 前端HTML页面
├── .venv/                  # 虚拟环境目录
├── Market Index.xlsx       # 数据Excel文件
├── start.py                # 启动脚本
├── wsgi.py                 # WSGI入口点
├── nginx_config.conf       # Nginx配置示例
├── requirements.txt        # 依赖包列表
└── README.md               # 项目说明文档
```

## 生产环境部署

### 1. 使用 Waitress（简单方式）

直接使用内置生产模式启动：

```bash
python start.py --mode prod
```

### 2. 使用 Nginx 和 uWSGI/Gunicorn（推荐方式）

步骤：

1. 安装必要的软件：

   ```bash
   pip install uwsgi  # 或 pip install gunicorn
   ```

2. 配置 Nginx：

   - 编辑 nginx_config.conf 文件，替换其中的域名和路径
   - 将配置文件复制到 /etc/nginx/sites-available/
   - 创建符号链接到 /etc/nginx/sites-enabled/
   - 重启 Nginx

3. 启动 WSGI 服务器：

   ```bash
   # uwsgi
   uwsgi --http :8080 --wsgi-file wsgi.py --callable application --processes 4 --threads 2

   # 或 gunicorn
   gunicorn -w 4 -b :8080 wsgi:application
   ```

4. 设置自动启动（使用 systemd）：
   - 创建服务文件 `/etc/systemd/system/market-data.service`
   - 启用并启动服务：
     ```bash
     systemctl enable market-data
     systemctl start market-data
     ```

### 3. 使用 Docker（容器化方式）

项目可以轻松容器化，创建一个 Dockerfile 并构建镜像：

```bash
docker build -t market-data-app .
docker run -p 8080:8080 market-data-app
```

## 跨域配置

如果前端和后端分别部署在不同的域或端口，需要注意跨域问题：

1. 后端已配置 CORS 支持，允许 API 跨域访问
2. 可以通过 URL 参数配置前端 API 基础路径：
   ```
   http://your-frontend-domain.com/index.html?apiUrl=http://your-api-domain.com/api
   ```

## 注意事项

- 脚本包含适当的延时以避免频繁请求
- 建议不要过于频繁地运行爬取，以免被网站封禁
- 确保网络连接正常
- 首次运行时，WebDriver Manager 会自动下载适合您系统的 WebDriver
- 如果您的系统没有安装任何受支持的浏览器，程序会提示错误
- 在 macOS 上，端口 5000 被 AirPlay Receiver 占用，所以我们默认使用 8080 端口
- 启动脚本会自动检测并激活项目目录中的 `.venv` 虚拟环境

## 跨平台支持

- **Windows**: 完全支持，自动下载适合 Windows 的 WebDriver
- **macOS**: 完全支持，自动下载适合 macOS 的 WebDriver
- **Linux**: 完全支持，自动下载适合 Linux 的 WebDriver

## 故障排除

如果遇到问题，请尝试以下步骤：

1. 确保已安装最新版本的浏览器
2. 检查网络连接
3. 查看控制台和日志输出
4. 更新依赖包：`pip install -r requirements.txt --upgrade`
5. 在生产环境中，检查 Nginx 错误日志：`/var/log/nginx/error.log`
6. 检查应用日志：`journalctl -u market-data.service`
7. 如果端口被占用，尝试使用不同的端口：`python start.py --port 9000`
8. 如果虚拟环境路径不是 `.venv`，您可以手动激活后再运行：`source 您的虚拟环境路径/bin/activate && python start.py`
