# 汇率数据自动获取工具

这是一个自动从 investing.com 获取最新汇率数据并更新到 Excel 文件的 Python 脚本。

## 功能特点

- 支持多个货币对的数据获取
- 自动更新 Excel 文件中的对应 sheet
- 支持 Windows、Mac 和 Linux 平台
- 包含错误处理和重试机制
- 使用随机 User-Agent 避免被封禁
- 使用无头模式（不显示浏览器窗口）
- 自动下载和管理 WebDriver，无需手动安装
- 支持多种浏览器（Chrome、Firefox、Edge）

## 支持的货币对

- USD/CNY (美元/人民币)
- EUR/CNY (欧元/人民币)
- EUR/USD (欧元/美元)
- HKD/CNY (港币/人民币)
- CNY/HKD (人民币/港币)
- JPY/USD (日元/美元)

## 安装要求

- Python 3.8+
- pip 包管理器
- 至少安装了以下浏览器之一：Chrome、Firefox 或 Edge

## 安装步骤

1. 克隆或下载本项目
2. 安装依赖包：

```bash
pip install -r requirements.txt
```

## 使用方法

1. 确保已安装所有依赖
2. 运行脚本：

```bash
python src/market_data_crawler.py
```

程序会自动在无头模式下运行，不会显示浏览器窗口，并使用爬虫方式获取所有数据。

## 输出文件

脚本会在`data`目录下创建或更新`market_index.xlsx`文件，每个货币对的数据会保存在对应的 sheet 中。

## 注意事项

- 脚本包含适当的延时以避免频繁请求
- 建议不要过于频繁地运行脚本，以免被网站封禁
- 确保网络连接正常
- 首次运行时，WebDriver Manager 会自动下载适合您系统的 WebDriver
- 如果您的系统没有安装任何受支持的浏览器，程序会提示错误

## 跨平台支持

- **Windows**: 完全支持，自动下载适合 Windows 的 WebDriver
- **macOS**: 完全支持，自动下载适合 macOS 的 WebDriver
- **Linux**: 完全支持，自动下载适合 Linux 的 WebDriver

## 故障排除

如果遇到问题，请尝试以下步骤：

1. 确保已安装最新版本的浏览器
2. 检查网络连接
3. 更新依赖包：`pip install -r requirements.txt --upgrade`
