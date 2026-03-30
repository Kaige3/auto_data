# 义选电商数算中心

自动化处理多平台（抖音小店、视频号、拼多多等）的原始订单导出数据，通过内存级 Pandas 数据引擎进行聚合收敛，并生成符合国内业务习惯的复杂嵌套报表（支持一键导出带样式的 Excel）。

## 核心功能与特色

- **多平台适配与极强容错**：统一处理抖音小店、视频号、拼多多的非标表头。内置强大的数据清洗机制，能自动剥离拼多多导出文件中恶心的制表符、换行符和异常引号。
- **智能数据收敛**：对于长期订单，当某一周期的未完成率低于 1% 时，自动在内存中将多天数据合并为一行，极大缩短长尾报表长度（拼多多则执行特供的“半月折叠”策略）。
- **昨日数据聚合仪表盘**：首页顶置“昨日发前退款率聚合报表”，直观展示各平台、各店铺、各货号在昨天的核心指标，并高亮标识“发前退款率 <= 10%”的达标状态。
- **全平台数据快照**：历史上传数据以 append-only 的方式安全存储于本地 CSV 数据库，每次分析时提取最新快照，同时保留所有历史记录供追溯。
- **中国式复杂报表导出**：前端使用原生 HTML Table 配合 `rowspan` 渲染嵌套分组报表，并能一键导出为原生保留样式的 `.xls` 格式。

## 1. 本地运行指南

### 方式一：使用 Docker 部署（推荐，支持一键启动）

1. 确保服务器已安装 `docker` 和 `docker-compose`。
2. 在项目根目录下执行：
   ```bash
   docker-compose up -d
   ```
3. 容器启动后，访问 `http://服务器IP:5000` 即可。
   > **注意**: 所有的数据库文件、历史快照都会通过 Volume 自动挂载到当前目录下的 `database`, `snapshots` 等文件夹中，保证数据不会丢失。

### 方式二：原生 Python 部署

1. 安装依赖：
   ```bash
   pip install flask pandas openpyxl gunicorn
   ```
2. 启动服务：
   ```bash
   python app.py
   ```
3. 访问系统：打开浏览器访问 `http://127.0.0.1:5000`

## 2. 目录结构说明

- `/app.py`：Flask 后端主程序路由控制器。
- `/utils/`：**核心模块目录**
  - `config.py`：平台映射配置中心。
  - `file_parser.py`：文件解析与强力脏数据清洗模块。
  - `calculator.py`：多平台指标（退款率、发货量）计算引擎。
  - `convergence.py`：智能数据收敛折叠算法。
- `/snapshots/`：按 `平台/店铺/` 结构存储的 `daily_snapshot.csv` 核心持久化数据库。
- `/history_data/`：保存每次上传的原始解析 JSON 备份，作为安全冗余。
- `/templates/`：前端 HTML 视图（单页面应用）。
- `/static/js/`：包含 `main.js` (入口), `api.js` (网络请求), `ui.js` (复杂报表渲染引擎)。

## 3. 统计规则说明

**核心逻辑 (针对货号 6050, 6301):**
- **当日订单总数 (Paid Orders)**:
  - 统计逻辑：按 `订单下单时间` 归类为同一天。
  - 商品筛选：仅统计商品名称中包含 `6301` 或 `6050` 的订单行。
  - 计数规则：每行计为 1 个订单。

- **发前退款订单 (Pre-ship Refund)**:
  - 判定条件：
    1. `订单下单时间` 不为空 (≠ '-')
    2. `订单发货时间` 为空 (= '-')
    3. `商品售后` 状态为 `退款完成`

- **发货量 (Shipped Volume)**:
  - 计算公式：`当日订单总数` - `发前退款订单`
  - *注：此逻辑假设所有非发前退款的订单最终都会发货，或暂时视为发货量。*

- **已完成 (Completed)**:
  - 判定条件：
    1. `支付时间` 不为空
    2. `订单状态` 包含 `已完成`
    3. `商品售后` 为无售后或售后已关闭（如：`-`, `无`, `售后关闭`, `用户取消申请`, `无售后或售后取消`, `商家拒绝退款`, `平台处理完成`）

- **未完成 (Uncompleted)**:
  - 判定条件：
    1. `支付时间` 不为空
    2. `订单状态` 包含 `已发货` 或 `待发货`
    3. `商品售后` 为无售后或售后已关闭（同上）

- **发货退款 (Post-ship Refund)**:
  - 判定条件：
    1. `支付时间` 不为空
    2. `订单发货时间` 不为空
    3. `商品售后` **不为** `无` 且 **不为** `用户取消申请`

## 4. Linux 服务器后台静默运行 (生产环境推荐)

为了保证终端关闭后服务依然运行，我们使用 `gunicorn` 作为 WSGI 服务器，并配合 `nohup` 或 `systemd`。

### 方法 A: 使用 nohup (最简单)

1.  安装 gunicorn：
    ```bash
    pip install gunicorn
    ```
2.  后台启动：
    ```bash
    # -w 4 表示开启4个工作进程，-b 绑定地址和端口
    nohup gunicorn -w 4 -b 0.0.0.0:5000 app:app > server.log 2>&1 &
    ```
3.  停止服务：
    ```bash
    pkill gunicorn
    ```

### 方法 B: 使用 Systemd (更稳定，开机自启)

1.  创建服务文件 `/etc/systemd/system/order_calc.service`：
    ```ini
    [Unit]
    Description=Order Calc Web Service
    After=network.target

    [Service]
    User=root
    WorkingDirectory=/path/to/order_calc_web
    ExecStart=/usr/local/bin/gunicorn -w 4 -b 0.0.0.0:5000 app:app
    Restart=always

    [Install]
    WantedBy=multi-user.target
    ```
2.  启动并设置开机自启：
    ```bash
    systemctl start order_calc
    systemctl enable order_calc
    ```
