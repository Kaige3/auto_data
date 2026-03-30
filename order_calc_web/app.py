from flask import Flask, request, jsonify, render_template
import pandas as pd
import io
import os
import time
import glob
import sqlite3
import logging
from logging.handlers import RotatingFileHandler
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta

# 导入提取出的模块
from utils.config import PLATFORM_CONFIG
from utils.convergence import apply_convergence_logic
from utils.file_parser import get_file_headers, validate_platform_file
from utils.data_engine import DataEngine

app = Flask(__name__)

# 初始化 DataEngine
engine = DataEngine()

# 配置历史记录存储目录
SNAPSHOT_DIR = os.path.join(app.root_path, 'snapshots')

if not os.path.exists(SNAPSHOT_DIR):
    os.makedirs(SNAPSHOT_DIR)

# --- 配置日志系统 ---
LOG_DIR = os.path.join(app.root_path, 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 设置日志格式
formatter = logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s (line %(lineno)d): %(message)s'
)

# 配置文件处理器：每个文件最大 10MB，最多保留 5 个备份文件
file_handler = RotatingFileHandler(
    os.path.join(LOG_DIR, 'app.log'), maxBytes=10 * 1024 * 1024, backupCount=5, encoding='utf-8'
)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.INFO)

# 将处理器添加到 Flask 实例的日志记录器
app.logger.addHandler(file_handler)
app.logger.setLevel(logging.INFO)

# 启动时记录一条信息
app.logger.info('Order Calc Web Server Started.')

@app.route('/')
def index():
    app.logger.info("Accessing index page")
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'msg': '没有上传文件'})
    
    file = request.files['file']
    platform = request.form.get('platform')
    shop = request.form.get('shop')

    if file.filename == '':
        return jsonify({'status': 'error', 'msg': '文件名为空'})

    try:
        # 1. 预读取表头进行校验 (只读 Header，不加载全量数据)
        headers = get_file_headers(file)
        if headers is None:
             return jsonify({'status': 'error', 'msg': '无法读取文件表头，请检查文件格式'})
             
        # 打印表头，方便调试拼多多等平台的数据格式
        if platform == 'pinduoduo':
            print(f"\n[{platform}] 原始表头: {headers}\n")

        # 2. 校验文件内容是否匹配所选平台
        is_valid, error_msg = validate_platform_file(headers, platform)
        if not is_valid:
            return jsonify({'status': 'error', 'msg': error_msg})

        # 3. 读取完整文件
        try:
            if file.filename.endswith('.csv'):
                try:
                    df = pd.read_csv(file, dtype=str, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    file.seek(0)
                    try:
                        df = pd.read_csv(file, dtype=str, encoding='gb18030')
                    except UnicodeDecodeError:
                        file.seek(0)
                        df = pd.read_csv(file, dtype=str, encoding='latin1')
            elif file.filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file, dtype=str)
            else:
                return jsonify({'status': 'error', 'msg': '仅支持 csv 或 xlsx 格式'})
        except Exception as e:
            return jsonify({'status': 'error', 'msg': f'读取文件失败: {str(e)}'})

        # [新增] 全局清洗表头和内容，去除 \t, \n, \r 等隐藏空白字符
        # 清洗表头
        df.columns = [str(col).strip().replace('\t', '').replace('\n', '').replace('\r', '') for col in df.columns]
        
        # 清洗所有字符串类型的数据列
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: str(x).strip().replace('\t', '').replace('\n', '').replace('\r', '') if pd.notna(x) else x)

        # 4. 统一表头 (基于配置)
        config = PLATFORM_CONFIG.get(platform, {})
        rename_map = config.get('rename_map', {})
        
        # 只重命名存在的列
        valid_rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
        if valid_rename_map:
            df.rename(columns=valid_rename_map, inplace=True)

        # [新增] 去除完全重复的行 (应用户要求保留原始所有数据，故注释掉)
        # df.drop_duplicates(inplace=True)

        # 修复 Pandas 2.0+ 警告: 不能直接对不同类型的列用空字符串 fillna
        for col in df.columns:
            if df[col].dtype == 'object' or pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].fillna('')
            else:
                # 数值列或其他类型暂不强填空字符串，避免不兼容
                pass

        # 4. 提取特征字段
        # 策略A: 优先从 '商家编码' 列提取 4 位数字
        if '商家编码' in df.columns:
            # 针对不同店铺提取货号规则
            if platform == 'douyin' and shop == 'JOJO':
                extracted_codes = df['商家编码'].astype(str).str.extract(r'(6050|6051|6052|6301)')[0]
                # 将 6051, 6052 统一映射为 6050
                df['货号'] = extracted_codes.replace({'6051': '6050', '6052': '6050'})
            else:
                df['货号'] = df['商家编码'].astype(str).str.extract(r'(6050|6301)')[0]
        
        # 策略B: 如果没提取到 (或没有编码列)，尝试从 '选购商品' 列提取
        # 注意：这里需要对 '货号' 列中仍然为空（NaN）的行进行填补，而不是只有当整列都不存在时才处理
        if '货号' not in df.columns:
            df['货号'] = None # 初始化
            
        if '选购商品' in df.columns:
            # 找出当前货号为空的行
            mask_na = df['货号'].isna()
            if mask_na.any():
                # 从商品名提取
                if platform == 'douyin' and shop == 'JOJO':
                    extracted = df.loc[mask_na, '选购商品'].astype(str).str.extract(r'(6050|6051|6052|6301)')[0]
                    # 将 6051, 6052 统一映射为 6050
                    df.loc[mask_na, '货号'] = extracted.replace({'6051': '6050', '6052': '6050'})
                else:
                    extracted = df.loc[mask_na, '选购商品'].astype(str).str.extract(r'(6050|6301)')[0]
                    df.loc[mask_na, '货号'] = extracted

        
        # 优化日期解析：统一转换为 YYYY-MM-DD 格式，确保以“订单提交时间”为准
        # errors='coerce' 会将无法解析的日期设为 NaT，随后会被 dropna 过滤
        # [修改] 根据平台配置获取时间字段
        time_col = config.get('time_column', '订单提交时间')
        
        # 对于拼多多，由于我们做了 rename，time_col 实际上已经被重命名为 '支付完成时间'，所以这里必须去 rename_map 里找
        if time_col not in df.columns:
            rename_target = config.get('rename_map', {}).get(time_col)
            if rename_target and rename_target in df.columns:
                time_col = rename_target
            elif '支付完成时间' in df.columns: time_col = '支付完成时间'
            elif '订单提交时间' in df.columns: time_col = '订单提交时间'
            elif '订单下单时间' in df.columns: time_col = '订单下单时间'

        
        if time_col in df.columns:
            # 增加空列拦截，防止错误的文件导致满屏 NaT
            if df[time_col].isna().all() or (df[time_col] == '').all():
                return jsonify({'status': 'error', 'msg': f'致命错误: 选定的时间列 "{time_col}" 内容全部为空，无法进行日期统计！'})

            # 兼容处理 Excel 导出的数字格式日期 (如 46100.577025)
            def parse_date(d):
                try:
                    # 提前转字符串并去掉首尾空格
                    d_str = str(d).strip()
                    if not d_str or d_str == 'nan' or d_str == 'None':
                        return pd.NaT
                        
                    # 如果是浮点数格式的字符串 (比如 '46100.577025')
                    if d_str.replace('.', '', 1).isdigit() and float(d_str) > 10000:
                        # 转换为 float 后用 timedelta 加到 1899-12-30 上
                        return pd.to_datetime('1899-12-30') + pd.to_timedelta(float(d_str), unit='D')
                    # 否则正常解析标准日期字符串
                    return pd.to_datetime(d_str)
                except:
                    return pd.NaT
                    
            df['日期'] = df[time_col].apply(parse_date).dt.strftime('%Y-%m-%d')
        else:
            df['日期'] = None # 会被后续 dropna 过滤



        # 过滤掉没有货号或没有日期的脏数据 (应用户要求不删除任何数据，改为填充'未知')
        df['货号'] = df['货号'].fillna('未知')
        df['货号'] = df['货号'].replace('', '未知')
        
        # 修复 NaT 无法使用 replace 的问题，先将 NaT 转换为 None，再填充
        df['日期'] = df['日期'].replace({pd.NaT: None})
        df['日期'] = df['日期'].fillna('未知')
        df['日期'] = df['日期'].replace('', '未知')

        # [修改] 移除对 ODS 层的强过滤。我们要把买家的所有购买记录（不管什么款）都存入数据库。
        # 统计时，底层的 data_engine 会在计算 daily_snapshots 快照时自动使用 WHERE sku_code IN ('6050', '6301') 来过滤报表。
        
        # [修改] 全平台强制按行统计 (每行计为1)
        # 无论是抖音还是视频号，都不再累加商品数量，而是统计订单行数
        df['qty'] = 1

        # 【新版ETL核心】使用 DataEngine 处理并同步到 SQLite
        # 获取原始文件名用于提取日期
        original_filename = file.filename
        engine.process_and_sync(df, platform, shop, original_filename)
        
        # 为了统一计算口径，完全废弃 calculator.py 的内存计算
        # 从新数据库中读取刚才由于 engine.process_and_sync 算好的数据返回给前端
        results = []
        with sqlite3.connect(engine.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # 根据本次上传的文件日期（stat_date）拉取最新计算的快照
            # 如果文件名没有日期，则默认抓取今天生成的全部快照
            # 这里为了保险，直接拉取当前店铺下各个日期的最新一条快照
            cursor.execute("""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY order_date, sku_code ORDER BY stat_date DESC) as rn
                    FROM daily_snapshots
                    WHERE shop_name = ?
                ) WHERE rn = 1
            """, (shop,))
            rows = cursor.fetchall()
            
            for row in rows:
                row_dict = dict(row)
                results.append({
                    'date': row_dict['order_date'],
                    'itemCode': row_dict['sku_code'],
                    'paidOrders': row_dict['paid_count'],
                    'shippedVolume': row_dict['shipped_vol'],
                    'preShipRefund': row_dict['pre_refund'],
                    'postShipRefund': row_dict['post_refund'],
                    'completed': row_dict['completed'],
                    'uncompleted': row_dict['uncompleted'],
                    'preRate': round(row_dict['pre_refund_rate'] * 100, 2) if row_dict['pre_refund_rate'] else 0.0,
                    'postRate': round(row_dict['post_refund_rate'] * 100, 2) if row_dict['post_refund_rate'] else 0.0,
                    'totalRate': round(row_dict['refund_rate'] * 100, 2) if row_dict['refund_rate'] else 0.0,
                    'update_time': row_dict['stat_date']
                })

        # 返回给前端的数据，动态执行内存态收敛逻辑
        converged_data = apply_convergence_logic(pd.DataFrame(results))
        return jsonify({'status': 'success', 'data': converged_data})

    except Exception as e:
        import traceback
        error_msg = str(e)
        app.logger.error(f"处理文件上传时出错: {error_msg}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'msg': f'处理文件时出错: {error_msg}'})

@app.route('/api/global_data', methods=['GET'])
def get_global_data():
    """获取全平台、全店铺的汇总数据（支持一键导出）及全局快照历史（重构为查数据库）"""
    try:
        # 新版 ETL: 直接从 SQLite 的 daily_snapshots 表读取数据
        # 连接数据库
        with sqlite3.connect(engine.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 获取最新快照的记录
            cursor.execute("""
                SELECT * FROM daily_snapshots 
                ORDER BY stat_date DESC, order_date DESC
            """)
            rows = cursor.fetchall()
            
            all_global_data = []
            all_snapshots = []
            
            for row in rows:
                row_dict = dict(row)
                # 为保持和前端的兼容，映射字段名
                mapped_row = {
                    'date': row_dict['order_date'],
                    'itemCode': row_dict.get('sku_code', '6050'), # 动态读取 sku_code，如果缺失则回退到 6050
                    'platform_name': row_dict.get('platform', '抖音小店'), # 尽量从配置或店铺名推断，这里为了前端渲染不为空，给个默认值
                    'shop_name': row_dict['shop_name'],
                    'paidOrders': row_dict['paid_count'],
                    'shippedVolume': row_dict['shipped_vol'],
                    'preShipRefund': row_dict['pre_refund'],
                    'postShipRefund': row_dict['post_refund'],
                    'completed': row_dict['completed'],
                    'uncompleted': row_dict['uncompleted'],
                    'preRate': round(row_dict['pre_refund_rate'] * 100, 2) if row_dict['pre_refund_rate'] else 0.0,
                    'postRate': round(row_dict['post_refund_rate'] * 100, 2) if row_dict['post_refund_rate'] else 0.0,
                    'totalRate': round(row_dict['refund_rate'] * 100, 2) if row_dict['refund_rate'] else 0.0,
                    'update_time': row_dict['stat_date']
                }
                all_snapshots.append(mapped_row)
                all_global_data.append(mapped_row)

            # 对历史大盘数据执行收敛折叠，避免前端表格过长卡死
            df_global = pd.DataFrame(all_global_data)
            if not df_global.empty:
                # 【关键修复】传入 convergence 之前必须去重，每个日期只保留最新一条快照
                df_global = df_global.sort_values('update_time', ascending=False).drop_duplicates(
                    subset=['platform_name', 'shop_name', 'itemCode', 'date'], keep='first'
                )
                # 按店铺拆分执行收敛（防止跨店聚合出错）
                # 理论上 global_data 已经是多店铺混合了，需要在 apply_convergence_logic 中支持按店分组，
                # 但目前 apply_convergence_logic 只支持按 itemCode 分组。
                # 简单修复：由于我们没有改动 convergence 里的分组逻辑，这里先把 global 按平台和店铺拆分执行再合并。
                converged_global_data = []
                for (pf, sp), group in df_global.groupby(['platform_name', 'shop_name']):
                    conv_res = apply_convergence_logic(group, pf)
                    for r in conv_res:
                        r['platform_name'] = pf
                        r['shop_name'] = sp
                        converged_global_data.append(r)
                all_global_data = converged_global_data

        return jsonify({'status': 'success', 'data': all_global_data, 'snapshots': all_snapshots})
    except Exception as e:
        import traceback
        error_msg = str(e)
        app.logger.error(f"获取全局数据时出错: {error_msg}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'msg': error_msg})

@app.route('/api/yesterday_data', methods=['GET'])
def get_yesterday_data():
    """获取全平台、全店铺【昨天】的统计数据"""
    try:
        # 计算昨天的日期字符串
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # 允许前端通过参数覆盖日期（方便测试）
        query_date = request.args.get('date', yesterday)

        yesterday_data = []
        
        with sqlite3.connect(engine.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # 从快照表中读取昨天的数据（按店铺和货号去重，只取最新的一条快照）
            cursor.execute("""
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER(PARTITION BY platform, shop_name, sku_code ORDER BY stat_date DESC) as rn
                    FROM daily_snapshots 
                    WHERE order_date = ?
                ) WHERE rn = 1
            """, (query_date,))
            rows = cursor.fetchall()

            for row in rows:
                row_dict = dict(row)
                paid_orders = row_dict['paid_count']
                shipped = row_dict['shipped_vol']
                pre_refund = row_dict['pre_refund']
                
                # 待发数 = 订单数 - 发货数 - 发前退款
                pending = paid_orders - shipped - pre_refund
                if pending < 0: pending = 0
                
                # 发前退款率
                pre_rate = round(row_dict['pre_refund_rate'] * 100, 2) if row_dict['pre_refund_rate'] else 0.0
                
                # 状态判定 (<= 10% 达标)
                status = '达标' if pre_rate <= 10 else '不达标'

                abnormal_orders = []
                if pre_rate > 10:
                    # 查询对应的异常订单号 (发前退款的订单)
                    cursor.execute("""
                        SELECT order_id 
                        FROM raw_orders 
                        WHERE shop_name = ? AND order_date = ? AND sku_code = ?
                        AND (pay_time IS NOT NULL AND trim(pay_time) != '' AND pay_time != '-')
                        AND (ship_time IS NULL OR trim(ship_time) = '' OR ship_time = '-')
                        AND refund_status IN ('退款成功', '售后成功', '退款完成', '待收退货', '待退货', '售后处理中')
                    """, (row_dict['shop_name'], query_date, row_dict['sku_code']))
                    abnormal_rows = cursor.fetchall()
                    abnormal_orders = [r['order_id'] for r in abnormal_rows if r['order_id'] and r['order_id'] != '-']
                
                yesterday_data.append({
                    'platform': row_dict.get('platform', '抖音小店'), 
                    'platform_name': row_dict.get('platform', '抖音小店'), # 直接使用存好的中文名称
                    'shop_name': row_dict['shop_name'],
                    'itemCode': row_dict.get('sku_code', '6050'), # 动态读取 sku_code
                    'date': query_date,
                    'paidOrders': paid_orders,
                    'pending': pending,
                    'shippedVolume': shipped,
                    'preShipRefund': pre_refund,
                    'preRate': round(pre_rate, 2),
                    'status': status,
                    'abnormalOrders': abnormal_orders
                })

        # 排序
        yesterday_data = sorted(yesterday_data, key=lambda x: (x['platform_name'], x['shop_name'], x['itemCode']))
        
        return jsonify({'status': 'success', 'data': yesterday_data})
    except Exception as e:
        import traceback
        error_msg = str(e)
        app.logger.error(f"获取昨日数据时出错: {error_msg}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'msg': error_msg})

@app.route('/api/batches', methods=['GET'])
def get_batches():
    """获取所有的上传批次记录"""
    try:
        with sqlite3.connect(engine.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT batch_id, shop_name, stat_date, COUNT(DISTINCT order_date) as date_count
                FROM daily_snapshots
                GROUP BY batch_id, shop_name, stat_date
                ORDER BY stat_date DESC
            """)
            rows = cursor.fetchall()
            batches = [dict(row) for row in rows]
        return jsonify({'status': 'success', 'data': batches})
    except Exception as e:
        app.logger.error(f"获取批次列表时出错: {str(e)}")
        return jsonify({'status': 'error', 'msg': str(e)})

@app.route('/api/batches', methods=['DELETE'])
def delete_batches():
    """批量删除指定的上传批次"""
    data = request.json
    batch_ids = data.get('batch_ids', [])
    if not batch_ids:
        return jsonify({'status': 'error', 'msg': '没有提供要删除的批次ID'})

    try:
        with sqlite3.connect(engine.db_path) as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['?'] * len(batch_ids))
            
            # 从快照表中删除
            cursor.execute(f"DELETE FROM daily_snapshots WHERE batch_id IN ({placeholders})", batch_ids)
            snapshots_deleted = cursor.rowcount
            
            # 从明细表中删除
            cursor.execute(f"DELETE FROM raw_orders WHERE batch_id IN ({placeholders})", batch_ids)
            raw_deleted = cursor.rowcount
            
            conn.commit()
            
        app.logger.info(f"成功删除批次: {batch_ids}. 删除了 {snapshots_deleted} 条快照，{raw_deleted} 条明细。")
        return jsonify({
            'status': 'success', 
            'msg': f'成功删除！清理了 {snapshots_deleted} 条快照数据，{raw_deleted} 条明细数据。'
        })
    except Exception as e:
        import traceback
        error_msg = str(e)
        app.logger.error(f"删除批次时出错: {error_msg}\n{traceback.format_exc()}")
        return jsonify({'status': 'error', 'msg': error_msg})

@app.route('/upload_qianchuan', methods=['POST'])
def upload_qianchuan():
    if 'file' not in request.files:
        return jsonify({'status': 'error', 'msg': '没有上传文件'})
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'status': 'error', 'msg': '文件名为空'})

    try:
        # 读取完整文件
        try:
            if file.filename.endswith('.csv'):
                try:
                    df = pd.read_csv(file, encoding='utf-8-sig')
                except UnicodeDecodeError:
                    file.seek(0)
                    try:
                        df = pd.read_csv(file, encoding='gb18030')
                    except UnicodeDecodeError:
                        file.seek(0)
                        df = pd.read_csv(file, encoding='latin1')
            elif file.filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file)
            else:
                return jsonify({'status': 'error', 'msg': '仅支持 csv 或 xlsx 格式'})
        except Exception as e:
            return jsonify({'status': 'error', 'msg': f'读取文件失败: {str(e)}'})

        # 清洗表头
        df.columns = [str(col).strip().replace('\t', '').replace('\n', '').replace('\r', '') for col in df.columns]
        
        # 校验关键列
        required_columns = ['素材ID', '素材名称', '整体消耗', '基础消耗', '追投调控消耗', '追投调控支付ROI', '整体支付ROI']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            return jsonify({'status': 'error', 'msg': f'文件缺少必要的列: {", ".join(missing_cols)}'})

        # 生成批次号并入库
        batch_id = f"qc_{int(time.time())}"
        engine.insert_material_report(df, batch_id, file.filename)

        return jsonify({
            'status': 'success',
            'msg': '千川数据上传成功',
            'batch_id': batch_id
        })
    except Exception as e:
        app.logger.error(f"千川数据上传失败: {str(e)}")
        return jsonify({'status': 'error', 'msg': f'处理失败: {str(e)}'})

@app.route('/api/qianchuan_diff', methods=['GET'])
def get_qianchuan_diff():
    search_kw = request.args.get('keyword', '')
    target_batch = request.args.get('batch_id', None)
    try:
        result = engine.get_material_diff(search_kw, target_batch)
        if not result:
            return jsonify({'status': 'success', 'data': [], 'latest_batch': None})
        return jsonify({'status': 'success', 'data': result['data'], 'latest_batch': result['latest_batch']})
    except Exception as e:
        app.logger.error(f"查询千川数据差异失败: {str(e)}")
        return jsonify({'status': 'error', 'msg': str(e)})

@app.route('/api/qianchuan_batches', methods=['GET'])
def get_qianchuan_batches():
    try:
        batches = engine.get_qianchuan_batches()
        return jsonify({'status': 'success', 'data': batches})
    except Exception as e:
        app.logger.error(f"获取千川批次列表失败: {str(e)}")
        return jsonify({'status': 'error', 'msg': str(e)})

@app.route('/api/qianchuan_batches/<batch_id>', methods=['DELETE'])
def delete_qianchuan_batch(batch_id):
    try:
        engine.delete_qianchuan_batch(batch_id)
        return jsonify({'status': 'success', 'msg': '删除成功'})
    except Exception as e:
        app.logger.error(f"删除千川批次失败: {str(e)}")
        return jsonify({'status': 'error', 'msg': str(e)})

# -------------------------------
# 素材采集中心 (VAMS) 轻量接口
# -------------------------------
import re

def _vams_db_path():
    db_dir = os.path.join(app.root_path, 'database')
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, 'vams_data.db')

def _ensure_vams_schema():
    with sqlite3.connect(_vams_db_path()) as conn:
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS video_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ukey TEXT UNIQUE,
                video_source TEXT,
                title TEXT,
                play_count INTEGER,
                digg_count INTEGER,
                comment_count INTEGER,
                share_count INTEGER,
                collect_count INTEGER,
                share_url TEXT,
                publish_time TEXT,
                sync_feishu INTEGER DEFAULT 0,
                feishu_record_id TEXT
            )
        """)
        # 尝试为现有的表添加 ukey 列（如果不存在的话）
        try:
            # 检查列是否已经存在
            c.execute("PRAGMA table_info(video_records)")
            columns = [col[1] for col in c.fetchall()]
            if 'ukey' not in columns:
                c.execute("ALTER TABLE video_records ADD COLUMN ukey TEXT UNIQUE")
        except Exception as e:
            app.logger.warning(f"添加 ukey 列时出现异常: {e}")

        c.execute("""
            CREATE TABLE IF NOT EXISTS video_attributes (
                video_id INTEGER,
                attr_type TEXT,
                attr_value TEXT
            )
        """)
        conn.commit()

def _to_play_count(s: str) -> int:
    if not s: return 0
    s = str(s).replace(',', '').strip()
    if '万' in s:
        try:
            return int(float(s.replace('万','')) * 10000)
        except:
            return 0
    try:
        return int(float(s))
    except:
        return 0

import hashlib

def generate_unique_key(text):
    # 方式 1：提取 URL ID (业务唯一标识)
    url_match = re.search(r'v\.douyin\.com/(\w+)/', text)
    if url_match:
        business_key = url_match.group(1)
    else:
        business_key = None

    # 方式 2：生成内容 Hash (数据唯一标识)
    content_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
    
    return business_key, content_hash

@app.route('/api/v1/video/upload', methods=['POST'])
def api_vams_upload():
    try:
        # 增加请求数据的日志打印，方便排查 RPA 发送过来的真实数据格式
        raw_payload = request.get_data(as_text=True)
        app.logger.info(f"VAMS 收到上传请求，原始 Payload: {raw_payload}")

        import json
        import ast

        data = {}
        # 尝试使用标准 json 解析
        try:
            data = request.get_json(force=True, silent=True) or {}
            if not data and raw_payload:
                data = json.loads(raw_payload)
        except Exception:
            pass
            
        # 如果标准 json 解析失败 (比如因为单引号)，尝试使用 ast.literal_eval
        if not data and raw_payload:
            try:
                # 很多 RPA 工具拼接 JSON 时会错误地使用单引号，ast.literal_eval 可以安全地解析 Python 字典格式的字符串
                parsed_ast = ast.literal_eval(raw_payload)
                if isinstance(parsed_ast, dict):
                    data = parsed_ast
            except Exception:
                pass

        raw = data.get('raw_data', [])
        
        # 记录解析后的 JSON
        app.logger.info(f"VAMS 解析出的 JSON: {data}, 提取出的 raw_data 长度: {len(raw) if isinstance(raw, list) else 'Not a list'}")

        if not isinstance(raw, list) or len(raw) < 5:
            return jsonify({"code": 400, "message": "raw_data 需要 5 项"}), 400

        _ensure_vams_schema()

        title_line = str(raw[0] or '')
        play_line = str(raw[1] or '0')
        col3 = str(raw[2] or '').strip()
        share_url = str(raw[3] or '')
        publish_time = str(raw[4] or '').replace('发布时间：','').strip()

        # 生成唯一主键 (优先使用 URL ID, 否则使用整个原始数组的 Hash)
        raw_text_for_hash = "".join([str(x) for x in raw])
        biz_key, c_hash = generate_unique_key(share_url if share_url else raw_text_for_hash)
        ukey = biz_key if biz_key else c_hash

        lines = [l.strip() for l in col3.split('\n') if l.strip()]
        is_stats = len(lines) > 0 and all(re.fullmatch(r'\d+', l) for l in lines)
        source_type = '主页视频' if is_stats else '本地视频'

        title = title_line.split('#')[0].strip()
        play_count = _to_play_count(play_line)

        with sqlite3.connect(_vams_db_path()) as conn:
            cur = conn.cursor()
            try:
                cur.execute("""
                    INSERT INTO video_records (ukey, video_source, title, play_count, share_url, publish_time)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (ukey, source_type, title, play_count, share_url, publish_time))
                vid = cur.lastrowid
            except sqlite3.IntegrityError:
                app.logger.warning(f'VAMS: 发现重复数据 ukey={ukey}, 跳过插入')
                return jsonify({"code": 200, "message": "success (duplicate skipped)", "data": {"id": None}})

            tags = re.findall(r'#([\u4e00-\u9fa5\w]+)', title_line)
            for t in tags:
                cur.execute("INSERT INTO video_attributes (video_id, attr_type, attr_value) VALUES (?, ?, ?)",
                            (vid, 'TOPIC', t))

            if is_stats:
                digg = int(lines[0]) if len(lines) > 0 else 0
                comment = int(lines[1]) if len(lines) > 1 else 0
                share = int(lines[2]) if len(lines) > 2 else 0
                collect = int(lines[3]) if len(lines) > 3 else 0
                cur.execute("""
                    UPDATE video_records SET digg_count=?, comment_count=?, share_count=?, collect_count=? WHERE id=?
                """, (digg, comment, share, collect, vid))
            else:
                biz_tags = re.split(r'[、\s]+', col3)
                for b in biz_tags:
                    if b:
                        cur.execute("INSERT INTO video_attributes (video_id, attr_type, attr_value) VALUES (?, ?, ?)",
                                    (vid, 'BIZ', b))
            conn.commit()

        app.logger.info(f'VAMS: 新增视频记录 id={vid}, source={source_type}, title="{title}"')
        return jsonify({"code": 200, "message": "success", "data": {"id": vid}})
    except Exception as e:
        import traceback
        app.logger.error(f"VAMS 上传出错: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"code": 500, "message": str(e)}), 500

@app.route('/api/v1/video/list', methods=['GET'])
def api_vams_list():
    try:
        _ensure_vams_schema()
        with sqlite3.connect(_vams_db_path()) as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("""
                SELECT id, video_source, title, play_count, digg_count, comment_count, share_count, collect_count, share_url, publish_time, sync_feishu
                FROM video_records
                ORDER BY publish_time DESC, play_count DESC, id DESC
                LIMIT 200
            """)
            rows = c.fetchall()
            
            data = []
            for r in rows:
                row_dict = dict(r)
                c.execute("SELECT attr_type, attr_value FROM video_attributes WHERE video_id = ?", (row_dict['id'],))
                attrs = c.fetchall()
                row_dict['topics'] = [a['attr_value'] for a in attrs if a['attr_type'] == 'TOPIC']
                row_dict['biz_tags'] = [a['attr_value'] for a in attrs if a['attr_type'] == 'BIZ']
                data.append(row_dict)
                
        return jsonify({"code": 200, "message": "success", "data": data})
    except Exception as e:
        import traceback
        app.logger.error(f"VAMS 获取列表出错: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"code": 500, "message": str(e)}), 500

@app.route('/api/v1/video/sync_feishu', methods=['POST'])
def api_vams_sync_feishu():
    try:
        data = request.get_json(force=True, silent=True) or {}
        ids = data.get('ids', [])
        
        if not ids or not isinstance(ids, list):
            return jsonify({"code": 400, "message": "需要提供有效的 ids 列表"}), 400

#   LARK_APP_ID: str({ default: 'cli_a923b21f66781bd9' }),
#   LARK_APP_SECRET: str({ default: 'u6H3FnoucrODnEerCw5NKdbi42F3XHGm' }),
#   LARK_BITABLE_APP_TOKEN: str({ default: 'Nd0jbdJY6aQK7isUU22cyvZPnge' }),
#   LARK_BITABLE_TABLE_ID: str({ default: 'cc' }),

        # TODO: 这里需要你填入飞书机器人的真实凭证和多维表格 ID
        FEISHU_APP_ID = "cli_a923b21f66781bd9" 
        FEISHU_APP_SECRET = "u6H3FnoucrODnEerCw5NKdbi42F3XHGm"
        FEISHU_APP_TOKEN = "Nd0jbdJY6aQK7isUU22cyvZPnge" # 多维表格所在应用的 token
        FEISHU_TABLE_ID = "tblsJlLnmZXPBnIo" # 具体的表格 ID

        success_count = 0
        fail_count = 0

        # 关闭 mock 模式，直接调用飞书真实接口
        is_mock_sync = False

        # 飞书 API 辅助函数
        def get_tenant_access_token():
            import requests
            url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
            req_body = {"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET}
            resp = requests.post(url, json=req_body).json()
            return resp.get("tenant_access_token")

        def add_bitable_record(token, fields):
            import requests
            url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{FEISHU_APP_TOKEN}/tables/{FEISHU_TABLE_ID}/records"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            payload = {"fields": fields}
            resp = requests.post(url, headers=headers, json=payload).json()
            if resp.get("code") != 0:
                raise Exception(f"Feishu API Error: {resp.get('msg')}")
            return resp

        access_token = None
        if not is_mock_sync:
            try:
                access_token = get_tenant_access_token()
                if not access_token:
                    return jsonify({"code": 500, "message": "无法获取飞书 tenant_access_token"}), 500
            except Exception as e:
                return jsonify({"code": 500, "message": f"飞书鉴权失败: {str(e)}"}), 500

        with sqlite3.connect(_vams_db_path()) as conn:
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            for vid in ids:
                try:
                    if is_mock_sync:
                        # 模拟同步成功，更新数据库状态
                        c.execute("UPDATE video_records SET sync_feishu = 1 WHERE id = ?", (vid,))
                        success_count += 1
                    else:
                        # 真实同步逻辑
                        c.execute("SELECT * FROM video_records WHERE id = ?", (vid,))
                        row = c.fetchone()
                        if not row:
                            fail_count += 1
                            continue
                        
                        # 获取标签
                        c.execute("SELECT attr_type, attr_value FROM video_attributes WHERE video_id = ?", (vid,))
                        attrs = c.fetchall()
                        topics = [a['attr_value'] for a in attrs if a['attr_type'] == 'TOPIC']
                        biz_tags = [a['attr_value'] for a in attrs if a['attr_type'] == 'BIZ']

                        # 组装飞书多维表格的字段 (这里字段名称必须和你在飞书表格里建立的一模一样)
                        # 根据你的示例，飞书表格只需要传一个文本字段 "视频链接"
                        fields = {}
                        if row['share_url']:
                            fields["视频链接"] = row['share_url']
                        
                        add_bitable_record(access_token, fields)
                        c.execute("UPDATE video_records SET sync_feishu = 1 WHERE id = ?", (vid,))
                        success_count += 1
                        
                except Exception as e:
                    app.logger.error(f"同步记录 {vid} 到飞书失败: {str(e)}")
                    fail_count += 1
            
            conn.commit()

        return jsonify({
            "code": 200, 
            "message": "success", 
            "data": {
                "success_count": success_count, 
                "fail_count": fail_count,
                "is_mock": is_mock_sync
            }
        })

    except Exception as e:
        import traceback
        app.logger.error(f"批量同步飞书出错: {str(e)}\n{traceback.format_exc()}")
        return jsonify({"code": 500, "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
