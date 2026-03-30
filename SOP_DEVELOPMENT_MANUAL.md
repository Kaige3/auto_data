# 义选电商数算中心 - SOP 开发与运维手册

## 一、 项目背景与架构概览

### 1.1 项目目标
本项目旨在解决电商运营团队每天需要从多平台（抖音小店、视频号、拼多多等）导出各种非标准化数据，进行繁琐的人工清洗、对齐、并计算退款率与发货指标的痛点。通过自动化的“数据收敛”算法，将长尾订单历史折叠，最终生成直观的“中国式复杂报表”。

### 1.2 技术栈
- **后端**：Python 3.10+, Flask (轻量级 Web 框架), Pandas (核心数据清洗与计算引擎)
- **前端**：Vanilla JavaScript (ES6 Modules), HTML5, CSS3 (无重型前端框架，追求极致轻量和快速渲染)
- **数据存储**：纯本地 CSV 数据库 (Append-only) + JSON 原始快照备份

### 1.1 目录结构与数据流
- **`app.py`**: 核心路由，处理文件上传、清洗并调用底层引擎。
- **`utils/data_engine.py`**: **核心数据中台引擎**（ETL）。基于 SQLite 实现了全量订单明细的幂等存储和指标聚合计算。
- **`utils/calculator.py` & `utils/convergence.py`**: 保留了部分前端所需的内存态收敛折叠逻辑。
- **`database/order_system.db`**: 唯一的数据持久化文件，包含 `raw_orders` 和 `daily_snapshots` 两个核心表。

### 1.2 数据模型设计 (双层数仓)
1. **ODS/DWD明细层 (`raw_orders`)**
   - 作用：记录每笔订单的**最新状态**。
   - 机制：以 `order_id` 为主键，执行 `INSERT OR REPLACE`。同一订单重复上传会自动覆盖旧状态（如从“已发货”变为“退款成功”）。
2. **DWS快照层 (`daily_snapshots`)**
   - 作用：记录每次计算产生的统计指标，形成历史趋势。
   - 机制：执行 `INSERT INTO` 追加写入，带有 `batch_id`（批次号）和 `stat_date`（精确到秒的时间戳），确保每次上传都能留下不可篡改的快照痕迹。

### 1.3 核心设计原则
1. **数据库收口计算**：所有的聚合统计（如退款率、发货量）必须在 `data_engine.py` 中通过标准 SQL 语句执行，确保多端逻辑统一。
2. **读写分离与不可变数据**：`daily_snapshots` 仅执行追加（Append）操作，绝对不使用覆盖去重。
3. **极简轻量**：废弃了之前往本地写入大量 JSON/CSV 文件的做法，所有数据单点保存在 SQLite 中，配合复合索引（`idx_raw_orders_shop_date`）保证千万级数据的查询性能。

---

## 二、 核心业务逻辑 SOP

### 2.1 平台数据清洗标准化 (Data Normalization)
当新增电商平台（如快手、淘宝）时，必须在 `utils/config.py` 中的 `PLATFORM_CONFIG` 注册映射规则：
```python
'platform_key': {
    'name': '平台名称',
    'required_columns': ['必须包含的校验列1', '必须包含的校验列2'],
    'time_column': '作为基准日期的列名',
    'rename_map': {
        '原始非标列名': '标准统一列名' # 必须向 '商家编码', '订单状态', '售后状态' 等核心字段对齐
    }
}
```

### 2.2 数据清洗与强力容错 (Robust Data Cleaning)
为了应对各平台导出的极不规范的数据文件，在 `app.py` 读取 DataFrame 后必须执行强力清洗：
1. **智能分隔符识别**：对于 CSV，如果正常的逗号分隔解析失败，尝试 `gb18030` 等其他编码。
2. **终极暴力清洗 (去制表符)**：必须遍历所有列名和字符串单元格，使用 `replace('\t', '').replace('\n', '').strip()` 剥离所有的隐藏换行符和制表符。**极其重要**，否则会导致 SQL 中的 `IN ('退款成功')` 精确匹配失败。
3. **Excel 浮点数日期修复**：处理形如 `46100.577025` 的 Excel 数字日期时，需基于 `1899-12-30` 基准转换为标准的 `YYYY-MM-DD`。

### 2.3 核心算法：自然周智能收敛 (Convergence Algorithm)
位于 `utils/convergence.py`。
**业务定义**：为了缩短报表长度同时保留趋势分析能力，当**某一自然周**（周一至周日）的整体订单“未完成率”（未完成数 / 支付总数）低于 1% 时，系统认为该周的数据已稳定，自动将其合并为一行展示。
**特殊平台处理**：对于拼多多，系统执行“半月折叠”特权策略，自动将每月 1号-14号 的数据强制聚合为一行，15号及之后的数据保持原样。
**实现规范**：
1. 数据传入前，必须按 `update_time` 倒序，并使用 `df.drop_duplicates(subset=['itemCode', 'date'], keep='first')` 取得每组最新快照。
2. **按自然周分组**：使用 `pd.to_datetime().isocalendar()` 获取 `年份-周数`，将每天的数据划分到对应的自然周包中。
3. **按周判定**：计算整周的 `(total_uncompleted / total_paid) < 0.01`。
4. 若满足条件，将这一整周的日期聚合为一行，`date` 字段变为区间格式（如 `2026-03-02 至 2026-03-08`），`is_converged` 标记为 `True`；若不满足，则该周内的每一天均保留不合并，`is_converged` 标记为 `False`。

### 2.3 前端复杂报表渲染 (Complex Table Rendering)
前端 `ui.js` 负责将后端传来的一维 JSON 渲染为多级嵌套表格。
**嵌套层级**：`Platform (平台)` -> `Shop (店铺)` -> `ItemCode (货号)` -> `Date (登记日期)` -> `Snapshots (历史快照)`。
**关键技术**：
- 使用原生的 HTML `rowspan` 属性进行行合并。
- 日期分组（`dateGroup`）内：
  - 如果是收敛行（`is_converged: true`），背景色标黄，无快照子项。
  - 如果是未收敛行，背景色循环分配，按 `update_time` 升序（旧在前，新在后）展示多条快照明细。

---

## 三、 本地开发与测试 SOP

### 3.1 环境初始化
1. 确保安装 Python 3.10+。
2. 建立虚拟环境并安装依赖：
   ```bash
   python -m venv .venv
   source .venv/Scripts/activate  # Windows
   pip install flask pandas openpyxl
   ```

### 3.2 模拟数据生成
在没有真实线上数据的情况下进行 UI 调整或算法测试时，必须使用模拟数据：
```bash
python generate_mock_data.py
```
这将在 `mock_data/` 目录下生成模拟的抖音 CSV 和视频号 XLSX 文件。

### 3.3 运行与联调
启动服务：
```bash
python app.py
```
打开浏览器访问 `http://127.0.0.1:5000`。上传生成的 mock 数据，观察终端的 Pandas 处理日志以及前端的网络请求。

---

## 四、 常见问题排查与运维 SOP

### 4.1 故障：上传后提示“缺少关键列”
- **原因**：平台导出的表头发生了变更，或者运营传错了平台。
- **SOP**：
  1. 打开用户上传的源文件，查看真实表头。
  2. 修改 `app.py` 中的 `PLATFORM_CONFIG`，更新 `required_columns` 或 `rename_map`。

### 4.2 故障：前端报 TypeError: Cannot read properties of null
- **原因**：前端 JS 试图操作一个已经被从 `index.html` 中删除的 DOM 元素。
- **SOP**：
  1. 检查 `main.js` 或 `ui.js` 中是否有废弃的 `document.getElementById` 调用。
  2. 清理相关冗余代码。

### 4.3 故障：导出的 Excel 没有样式，全挤在一起
- **原因**：前端使用了纯 CSV 导出，或 Excel 不识别 HTML 表格的默认流式布局。
- **SOP**：
  1. 确保 `exportToExcelWithStyles` 函数中注入了 `\uFEFF` (BOM) 防止中文乱码。
  2. 确保模板的 `<style>` 标签中包含 `table-layout: fixed; white-space: nowrap;` 等强制排版属性。

### 4.4 故障：历史数据“消失”
- **原因**：后端在向 `daily_snapshot.csv` 写入时，错误地使用了 `drop_duplicates` 或覆写模式（`mode='w'`）。
- **SOP**：
  1. 检查 `app.py` 的落库逻辑，必须先读取旧 CSV，与新 DF `concat` 后，直接全量写入（或追加写入）。
  2. 若已丢失，可从 `history_data/` 的 JSON 备份中编写脚本进行数据恢复。