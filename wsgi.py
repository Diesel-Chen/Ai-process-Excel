#!/usr/bin/env python3
"""
WSGI入口点 - 用于生产环境部署
"""
import os
import sys

# 添加项目路径到Python路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, 'src'))

# 导入应用
from app import create_app

# 创建应用实例
application = create_app()

if __name__ == "__main__":
    application.run()