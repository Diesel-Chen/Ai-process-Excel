#!/usr/bin/env python3
"""
市场数据获取工具启动脚本
支持开发环境和生产环境
"""
import os
import sys
import webbrowser
import time
import threading
import argparse
import subprocess
import platform

def open_browser(url):
    """在浏览器中打开应用"""
    # 等待服务器启动
    time.sleep(2)
    webbrowser.open(url)

def activate_venv():
    """检测并激活虚拟环境"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    venv_path = os.path.join(script_dir, '.venv')

    # 检查虚拟环境是否存在
    if not os.path.exists(venv_path):
        print("警告: 虚拟环境(.venv)不存在，将使用系统Python环境")
        return False

    # 检查是否已经在虚拟环境中
    if sys.prefix == sys.base_prefix:
        # 不在虚拟环境中，尝试激活
        print("检测到虚拟环境，尝试激活...")

        # 确定激活脚本路径
        if platform.system() == "Windows":
            activate_script = os.path.join(venv_path, 'Scripts', 'activate.bat')
            if not os.path.exists(activate_script):
                activate_script = os.path.join(venv_path, 'Scripts', 'activate')
        else:  # Linux/macOS
            activate_script = os.path.join(venv_path, 'bin', 'activate')

        if not os.path.exists(activate_script):
            print(f"警告: 激活脚本不存在: {activate_script}")
            return False

        # 创建激活命令
        if platform.system() == "Windows":
            cmd = f'call "{activate_script}" && python "{__file__}" {" ".join(sys.argv[1:])}'
            shell = True
        else:
            cmd = f'source "{activate_script}" && python "{__file__}" {" ".join(sys.argv[1:])}'
            shell = True

        print(f"执行: {cmd}")

        # 执行激活命令并启动脚本
        try:
            subprocess.run(cmd, shell=shell, check=True)
            sys.exit(0)  # 成功执行后退出当前进程
        except subprocess.CalledProcessError as e:
            print(f"激活虚拟环境失败: {e}")
            return False
    else:
        # 已经在虚拟环境中
        print(f"已在虚拟环境中: {sys.prefix}")
        return True

def main():
    """主函数"""
    # 检查并激活虚拟环境
    # in_venv = activate_venv()

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='市场数据自动获取工具')
    parser.add_argument('--mode', default='dev', choices=['dev', 'prod'], help='运行模式：开发(dev)或生产(prod)')
    parser.add_argument('--port', type=int, default=8080, help='服务器端口，默认为8080')
    parser.add_argument('--host', default='0.0.0.0', help='服务器主机')
    parser.add_argument('--no-browser', action='store_true', help='不自动打开浏览器')
    args = parser.parse_args()

    # 确保工作目录正确
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    # 打印欢迎信息
    print("=" * 50)
    print(f"市场数据自动获取工具 (前后端版本) - {args.mode}模式")
    # if in_venv:
    #     print(f"Python环境: 虚拟环境 ({sys.prefix})")
    # else:
    #     print(f"Python环境: 系统环境 ({sys.prefix})")
    print("=" * 50)

    # 设置Python路径
    sys.path.append(os.path.join(script_dir, 'src'))

    # 默认浏览器URL
    browser_url = f'http://localhost:{args.port}'

    # 在开发模式下且未禁用浏览器，在线程中打开浏览器
    # if args.mode == 'dev' and not args.no_browser:
    #     print(f"服务器即将启动，将自动打开浏览器: {browser_url}")
    #     browser_thread = threading.Thread(target=open_browser, args=(browser_url,))
    #     browser_thread.daemon = True
    #     browser_thread.start()

    # 启动应用
    try:
        if args.mode == 'dev':
            # 开发模式：使用Flask内置服务器
            from app import app
            app.run(host=args.host, port=args.port, debug=True)
        else:
            # 生产模式：使用产品级WSGI服务器（如果安装）
            try:
                from waitress import serve
                from app import create_app

                app = create_app()
                print(f"使用Waitress服务器在{args.host}:{args.port}上运行")
                serve(app, host=args.host, port=args.port)
            except ImportError:
                # 如果没有安装Waitress，回退到Flask开发服务器
                print("警告: Waitress未安装，回退到Flask开发服务器")
                print("建议安装Waitress：pip install waitress")
                from app import app
                app.run(host=args.host, port=args.port, debug=False)

    except ImportError as e:
        print(f"错误: 无法导入应用 ({e})")
        print("请确保已经安装了所有依赖: pip install -r requirements.txt")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n用户中断，程序退出")
    except Exception as e:
        print(f"错误: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()