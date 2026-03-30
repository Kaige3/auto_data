import sqlite3
import pandas as pd
import re
import os
from datetime import datetime

class DataEngine:
    def __init__(self, db_path=None):
        if db_path is None:
            # 默认将数据库放在项目根目录下的 database 文件夹中
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.db_path = os.path.join(base_dir, "database", "order_system.db")
        else:
            self.db_path = db_path
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        """初始化 ODS(明细) 和 Report(快照) 表"""
        with sqlite3.connect(self.db_path) as conn:
            # 1. 原始订单明细表 (每次上传新增，取消 order_id 主键以允许重复行)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT,
                    platform TEXT,
                    shop_name TEXT,
                    order_date TEXT,      -- 格式化为 YYYY-MM-DD 或 MM-DD
                    sku_code TEXT,        -- 正则匹配出的4位数字
                    order_status TEXT,
                    refund_status TEXT,
                    pay_time TEXT,
                    ship_time TEXT,
                    update_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    batch_id TEXT         -- 上传批次号
                )
            """)
            
            # [新增] 动态字段检查与迁移逻辑 (处理 PyInstaller 打包或旧库升级时缺少字段的问题)
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(raw_orders)")
            columns = [col[1] for col in cursor.fetchall()]
            if 'batch_id' not in columns:
                conn.execute("ALTER TABLE raw_orders ADD COLUMN batch_id TEXT")
            if 'platform' not in columns:
                conn.execute("ALTER TABLE raw_orders ADD COLUMN platform TEXT")

            # 2. 每日业务指标快照 (增量存储，允许多次生成同一天的快照)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    stat_date TEXT,       -- 统计日期 (来自文件名，也是 update_time)
                    order_date TEXT,      -- 订单日期 (下单日)
                    platform TEXT,        -- 新增：平台标识
                    shop_name TEXT,
                    sku_code TEXT,        -- 货号 (新增字段，按 sku_code 分组)
                    paid_count INTEGER,   -- 1. 支付订单总数
                    pre_refund INTEGER,   -- 2. 发前退款订单
                    shipped_vol INTEGER,  -- 3. 发货量
                    post_refund INTEGER,  -- 4. 发后退款订单
                    completed INTEGER,    -- 5. 已完成
                    uncompleted INTEGER,  -- 6. 未完成
                    pre_refund_rate REAL, -- 7. 发前退货率 (发前退款/总数)
                    post_refund_rate REAL,-- 8. 发后退货率 (发后退款/总数)
                    refund_rate REAL,     -- 9. 退货率 (发前+发后)/总数
                    batch_id TEXT         -- 10. 上传批次号 (便于追踪和修改)
                )
            """)
            
            cursor.execute("PRAGMA table_info(daily_snapshots)")
            snap_columns = [col[1] for col in cursor.fetchall()]
            if 'platform' not in snap_columns:
                conn.execute("ALTER TABLE daily_snapshots ADD COLUMN platform TEXT")
            if 'batch_id' not in snap_columns:
                conn.execute("ALTER TABLE daily_snapshots ADD COLUMN batch_id TEXT")

            # 3. 创建性能优化索引 (防患于未然)
            # 针对 raw_orders 的 shop_name, order_date, sku_code 查询做联合索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_orders_shop_date_sku 
                ON raw_orders (shop_name, order_date, sku_code)
            """)
            
            # 针对 daily_snapshots 的排序和查询做索引
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_snapshots_date_shop_sku 
                ON daily_snapshots (order_date, shop_name, sku_code)
            """)

            # 4. 千川素材报表 (支持动态流水查询)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS material_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT,        -- 每次上传的唯一批次号
                    upload_time DATETIME, -- 上传入库时间，用于时间线对比
                    original_filename TEXT, -- 原始文件名
                    material_name TEXT,   -- 素材名称
                    material_id TEXT,     -- 素材ID (核心索引)
                    material_eval TEXT,   -- 素材评估
                    material_duration TEXT, -- 素材时长
                    material_create_time TEXT, -- 素材创建时间
                    material_source TEXT, -- 素材来源
                    tags TEXT,            -- 标签
                    total_cost REAL,      -- 整体消耗
                    basic_cost REAL,      -- 基础消耗
                    additional_cost REAL, -- 追投调控消耗
                    additional_roi REAL,  -- 追投调控支付ROI
                    total_roi REAL        -- 整体支付ROI
                )
            """)
            
            cursor.execute("PRAGMA table_info(material_reports)")
            mat_columns = [col[1] for col in cursor.fetchall()]
            if 'original_filename' not in mat_columns:
                conn.execute("ALTER TABLE material_reports ADD COLUMN original_filename TEXT")

            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_material_reports_id_time 
                ON material_reports (material_id, upload_time DESC)
            """)

    def process_and_sync(self, df, platform, shop_name, file_name, batch_id=None):
        """核心处理逻辑：清洗 -> 入库 -> 统计"""
        # 如果没有传入 batch_id，则生成一个基于时间戳的默认批次号
        if not batch_id:
            import time
            batch_id = f"batch_{int(time.time())}"
        # A. 统一平台映射配置
        # 注意：这里的配置必须和 app.py 中 PLATFORM_CONFIG 的 rename_map 重命名后的结果对齐！
        # 因为传入这里的 df 已经是经过 app.py 重命名后的 DataFrame 了。
        config = {
            'douyin': {'name': '抖音小店', 'id': '主订单编号', 'time': '订单提交时间', 'pay': '支付完成时间', 'status': '订单状态', 'refund': '售后状态', 'product': '选购商品', 'ship': '发货时间'},
            'channels': {'name': '视频号', 'id': '订单号', 'time': '订单下单时间', 'pay': '支付完成时间', 'status': '订单状态', 'refund': '售后状态', 'product': '选购商品', 'ship': '发货时间'},
            'pinduoduo': {'name': '拼多多', 'id': '订单号', 'time': '订单成交时间', 'pay': '支付完成时间', 'status': '订单状态', 'refund': '售后状态', 'product': '选购商品', 'ship': '发货时间'}
        }
        
        if platform not in config:
            # 如果平台不在配置中，尝试动态适配或抛出异常
            raise ValueError(f"不支持的平台: {platform}")
            
        cfg = config[platform]

        # B. 提取统计日期 (从文件名提取，例如 3-04 或 2026-03-04)
        # 如果从文件名提取不到日期，则使用当前完整时间作为更新时间
        stat_date_match = re.search(r'(\d{4}-)?\d{1,2}-\d{1,2}', file_name)
        # 统一使用当前精确时间作为快照的时间戳，以便前端按时间精确区分多次上传的快照
        stat_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 确保列存在，防止 KeyError
        for col_key, col_name in cfg.items():
             if col_key == 'name':
                 continue # 跳过中文名配置项的检查，因为它不是文件里的列名
                 
             if col_name not in df.columns:
                 # 尝试模糊匹配，因为有些列名可能有前后空格或略微不同
                 matched = False
                 for df_col in df.columns:
                     if col_name in str(df_col):
                         cfg[col_key] = df_col
                         matched = True
                         break
                 if not matched:
                      # 对于发货时间，有些文件可能真的没有
                      if col_key == 'ship':
                          df[col_name] = '-'
                          cfg[col_key] = col_name
                      else:
                          print(f"Warning: 找不到列 '{col_name}' in {df.columns}")

        # C. 核心清洗：支付时间不为空 (应用户要求保留全量原始数据，故注释掉过滤)
        # df = df[df[cfg['pay']].notna() & (df[cfg['pay']] != '-') & (df[cfg['pay']].str.strip() != '')]
        
        # 使用 app.py 中已经清洗和映射好的 '货号'，不再重新从商品名称提取
        if '货号' in df.columns:
            df['sku_code'] = df['货号']
        else:
            # 兼容性备用方案
            df['sku_code'] = df[cfg['product']].astype(str).str.extract(r'(6050|6301)')[0]
        
        df['sku_code'] = df['sku_code'].fillna('未知')

        # D. 转换并同步到 raw_orders (明细层)
        items = []
        for _, row in df.iterrows():
            try:
                # 尝试解析日期，统一格式
                time_val = row[cfg['time']]
                try:
                    dt = pd.to_datetime(time_val)
                    order_date = dt.strftime('%Y-%m-%d')
                except:
                    order_date = str(time_val)[:10] if pd.notna(time_val) else '-'

                items.append((
                    str(row.get(cfg['id'], '-')), 
                    cfg.get('name', platform), # 存入中文平台名称，如 '抖音小店'
                    shop_name,
                    order_date,
                    str(row.get('sku_code', '未知')), 
                    str(row.get(cfg['status'], '-')), 
                    str(row.get(cfg['refund'], '-')) if pd.notna(row.get(cfg['refund'])) else '-',
                    str(row.get(cfg['pay'], '-')), 
                    str(row.get(cfg['ship'], '-')) if pd.notna(row.get(cfg['ship'])) else '-',
                    batch_id
                ))
            except Exception as e:
                 print(f"Error processing row: {e}")
                 continue

        if not items:
            print("No valid items to sync after filtering.")
            return

        with sqlite3.connect(self.db_path) as conn:
            # 取消 UPSERT (INSERT OR REPLACE)，改为普通 INSERT，以保留所有明细行
            conn.executemany("""
                INSERT INTO raw_orders
                (order_id, platform, shop_name, order_date, sku_code, order_status, refund_status, pay_time, ship_time, batch_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, items)
            
            # E. 触发 7 大指标计算并拍入快照
            self._update_snapshots(stat_date, shop_name, conn, batch_id)

    def _update_snapshots(self, stat_date, shop_name, conn, batch_id):
        """执行 7 大指标聚合 SQL"""
        # 注意：这里的 SQL 逻辑需要根据实际业务含义微调
        # 例如：refund_status 的枚举值 ('退款成功', '待收退货', '待退货', '售后成功') 
        # 不同平台的值可能略有不同，建议在上面的映射或者这里的 CASE WHEN 里兼容
        
        sql = """
        INSERT INTO daily_snapshots
        (stat_date, order_date, platform, shop_name, sku_code, paid_count, pre_refund, shipped_vol, post_refund, completed, uncompleted, pre_refund_rate, post_refund_rate, refund_rate, batch_id)
        WITH BaseData AS (
            SELECT
                order_date,
                platform,
                shop_name,
                sku_code,
                CASE WHEN pay_time IS NOT NULL AND trim(pay_time) != '' AND trim(pay_time) != '-' AND lower(trim(pay_time)) != 'nan' THEN 1 ELSE 0 END as is_paid,
                CASE WHEN ship_time IS NULL OR trim(ship_time) = '' OR trim(ship_time) = '-' OR lower(trim(ship_time)) = 'nan' THEN 1 ELSE 0 END as is_unshipped,
                refund_status,
                order_status
            FROM raw_orders
            WHERE shop_name = ? AND batch_id = ? AND sku_code IN ('6050', '6301')
            AND order_date IS NOT NULL AND trim(order_date) != '' AND trim(order_date) != '-' AND order_date != '未知'
        )
        SELECT
            ? as stat_date,
            order_date,
            MAX(platform) as platform,
            shop_name,
            sku_code,
            -- 1. 支付订单总数 (仅统计已支付的)
            SUM(is_paid) as paid_count,
            
            -- 2. 发前退款订单
            SUM(CASE WHEN is_paid=1 AND is_unshipped=1 AND refund_status IN ('退款成功', '售后成功', '退款完成', '待收退货', '待退货', '售后处理中') THEN 1 ELSE 0 END) as pre_refund,
            
            -- 3. 发货量 = 支付订单总数 - 发前退款订单
            SUM(is_paid) - SUM(CASE WHEN is_paid=1 AND is_unshipped=1 AND refund_status IN ('退款成功', '售后成功', '退款完成', '待收退货', '待退货', '售后处理中') THEN 1 ELSE 0 END) as shipped_vol,
            
            -- 4. 发货后退款订单 (排除了正常状态，剩下的且发了货的，即为发后退款)
            SUM(CASE WHEN is_paid=1 AND is_unshipped=0 AND refund_status NOT IN ('-', '售后关闭', '无', '用户取消申请', '无售后或售后取消', '', '商家拒绝退款', '平台处理完成') THEN 1 ELSE 0 END) as post_refund,
            
            -- 5. 已完成 (售后状态无或关闭，且订单状态为 '已完成')
            SUM(CASE WHEN is_paid=1 AND refund_status IN ('-', '售后关闭', '无', '用户取消申请', '无售后或售后取消', '', '商家拒绝退款', '平台处理完成') AND (order_status = '已完成' OR order_status LIKE '%已完成%') THEN 1 ELSE 0 END) as completed,
            
            -- 6. 未完成 (售后状态无或关闭，且订单状态为 '已发货' 或 '待发货')
            -- (注: 抖店等平台的未完成状态一般对应 '已发货' 或 '待发货', 拼多多可能是 '已发货，待收货')
            SUM(CASE WHEN is_paid=1 AND refund_status IN ('-', '售后关闭', '无', '用户取消申请', '无售后或售后取消', '', '商家拒绝退款', '平台处理完成') AND (order_status LIKE '%已发货%' OR order_status LIKE '%待发货%') THEN 1 ELSE 0 END) as uncompleted,
            
            -- 7. 发前退款率 = 发前退款订单 / 支付总数
            CAST(SUM(CASE WHEN is_paid=1 AND is_unshipped=1 AND refund_status IN ('退款成功', '售后成功', '退款完成', '待收退货', '待退货', '售后处理中') THEN 1 ELSE 0 END) AS REAL) / NULLIF(SUM(is_paid), 0) as pre_refund_rate,
            
            -- 8. 发后退款率 = 发货后退款订单 / 支付总数
            CAST(SUM(CASE WHEN is_paid=1 AND is_unshipped=0 AND refund_status NOT IN ('-', '售后关闭', '无', '用户取消申请', '无售后或售后取消', '', '商家拒绝退款', '平台处理完成') THEN 1 ELSE 0 END) AS REAL) / NULLIF(SUM(is_paid), 0) as post_refund_rate,
            
            -- 9. 退货率 = 发前退款率 + 发后退款率
            (CAST(SUM(CASE WHEN is_paid=1 AND is_unshipped=1 AND refund_status IN ('退款成功', '售后成功', '退款完成', '待收退货', '待退货', '售后处理中') THEN 1 ELSE 0 END) AS REAL) / NULLIF(SUM(is_paid), 0)) + 
            (CAST(SUM(CASE WHEN is_paid=1 AND is_unshipped=0 AND refund_status NOT IN ('-', '售后关闭', '无', '用户取消申请', '无售后或售后取消', '', '商家拒绝退款', '平台处理完成') THEN 1 ELSE 0 END) AS REAL) / NULLIF(SUM(is_paid), 0)) as refund_rate,
            
            ? as batch_id
        FROM BaseData
        GROUP BY order_date, sku_code
        """
        conn.execute(sql, (shop_name, batch_id, stat_date, batch_id))

    def insert_material_report(self, df, batch_id, filename=""):
        """导入千川素材报表数据"""
        upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        def safe_float(val):
            if pd.isna(val) or val == '' or val is None:
                return 0.0
            # 移除千分位逗号后再转换
            try:
                return float(str(val).replace(',', ''))
            except ValueError:
                return 0.0

        records = []
        for _, row in df.iterrows():
            records.append((
                batch_id,
                upload_time,
                filename,
                str(row.get('素材名称', '')),
                str(row.get('素材ID', '')),
                str(row.get('素材评估', '')),
                str(row.get('素材时长', '')),
                str(row.get('素材创建时间', '')),
                str(row.get('素材来源', '')),
                str(row.get('标签', '')),
                safe_float(row.get('整体消耗', 0)),
                safe_float(row.get('基础消耗', 0)),
                safe_float(row.get('追投调控消耗', 0)),
                safe_float(row.get('追投调控支付ROI', 0)),
                safe_float(row.get('整体支付ROI', 0))
            ))

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany("""
                INSERT INTO material_reports (
                    batch_id, upload_time, original_filename, material_name, material_id, material_eval,
                    material_duration, material_create_time, material_source, tags,
                    total_cost, basic_cost, additional_cost, additional_roi, total_roi
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)

    def get_material_diff(self, search_kw=None, target_batch=None, explicit_prev_batch=None):
        """查询千川素材指定批次（或最新批次）与全局时间线上的前一个批次（或指定的旧批次）的差异"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # 1. 确定要查询的“当前”目标批次
            if not target_batch:
                cursor = conn.execute("SELECT batch_id, upload_time FROM material_reports ORDER BY upload_time DESC LIMIT 1")
                row = cursor.fetchone()
                if not row:
                    return []
                latest_batch = row['batch_id']
                target_time = row['upload_time']
            else:
                latest_batch = target_batch
                cursor = conn.execute("SELECT upload_time FROM material_reports WHERE batch_id = ? LIMIT 1", (latest_batch,))
                row = cursor.fetchone()
                if not row:
                    return []
                target_time = row['upload_time']

            # 2. 查询逻辑
            if explicit_prev_batch:
                # 如果明确指定了旧批次
                query = """
                    WITH CurrentData AS (
                        SELECT * FROM material_reports WHERE batch_id = ?
                    ),
                    PreviousData AS (
                        SELECT * FROM material_reports WHERE batch_id = ?
                    )
                    SELECT 
                        c.material_id,
                        c.material_name,
                        c.tags,
                        c.material_create_time,
                        c.upload_time,
                        c.total_cost as current_total_cost,
                        p.total_cost as prev_total_cost,
                        CASE WHEN p.id IS NOT NULL THEN (c.total_cost - p.total_cost) ELSE 0 END as diff_total_cost,
                        c.basic_cost as current_basic_cost,
                        p.basic_cost as prev_basic_cost,
                        CASE WHEN p.id IS NOT NULL THEN (c.basic_cost - p.basic_cost) ELSE 0 END as diff_basic_cost,
                        c.additional_cost as current_additional_cost,
                        p.additional_cost as prev_additional_cost,
                        CASE WHEN p.id IS NOT NULL THEN (c.additional_cost - p.additional_cost) ELSE 0 END as diff_additional_cost,
                        c.additional_roi as current_additional_roi,
                        p.additional_roi as prev_additional_roi,
                        CASE WHEN p.id IS NOT NULL THEN (c.additional_roi - p.additional_roi) ELSE 0 END as diff_additional_roi,
                        c.total_roi as current_total_roi,
                        p.total_roi as prev_total_roi,
                        CASE WHEN p.id IS NOT NULL THEN (c.total_roi - p.total_roi) ELSE 0 END as diff_total_roi
                    FROM CurrentData c
                    LEFT JOIN PreviousData p ON c.material_id = p.material_id
                    WHERE 1=1
                """
                params = [latest_batch, explicit_prev_batch]
            else:
                # 否则自动寻找上一次（无视日期限制，只看全局时间线上的前一次）
                query = """
                    WITH RankedData AS (
                        SELECT 
                            *,
                            ROW_NUMBER() OVER(PARTITION BY material_id ORDER BY upload_time DESC) as rn
                        FROM material_reports
                        WHERE upload_time <= ?
                    ),
                    CurrentData AS (
                        SELECT * FROM RankedData WHERE rn = 1 AND batch_id = ?
                    ),
                    PreviousData AS (
                        SELECT * FROM RankedData WHERE rn = 2
                    )
                    SELECT 
                        c.material_id,
                        c.material_name,
                        c.tags,
                        c.material_create_time,
                        c.upload_time,
                        c.total_cost as current_total_cost,
                        p.total_cost as prev_total_cost,
                        CASE WHEN p.id IS NOT NULL THEN (c.total_cost - p.total_cost) ELSE 0 END as diff_total_cost,
                        c.basic_cost as current_basic_cost,
                        p.basic_cost as prev_basic_cost,
                        CASE WHEN p.id IS NOT NULL THEN (c.basic_cost - p.basic_cost) ELSE 0 END as diff_basic_cost,
                        c.additional_cost as current_additional_cost,
                        p.additional_cost as prev_additional_cost,
                        CASE WHEN p.id IS NOT NULL THEN (c.additional_cost - p.additional_cost) ELSE 0 END as diff_additional_cost,
                        c.additional_roi as current_additional_roi,
                        p.additional_roi as prev_additional_roi,
                        CASE WHEN p.id IS NOT NULL THEN (c.additional_roi - p.additional_roi) ELSE 0 END as diff_additional_roi,
                        c.total_roi as current_total_roi,
                        p.total_roi as prev_total_roi,
                        CASE WHEN p.id IS NOT NULL THEN (c.total_roi - p.total_roi) ELSE 0 END as diff_total_roi
                    FROM CurrentData c
                    LEFT JOIN PreviousData p ON c.material_id = p.material_id
                    WHERE 1=1
                """
                params = [target_time, latest_batch]
            
            if search_kw:
                query += " AND (c.material_name LIKE ? OR c.material_id LIKE ?)"
                params.extend([f"%{search_kw}%", f"%{search_kw}%"])
                
            query += " ORDER BY c.total_cost DESC"
            
            cursor = conn.execute(query, params)
            results = []
            for r in cursor.fetchall():
                results.append({
                    'material_id': r['material_id'],
                    'material_name': r['material_name'],
                    'tags': r['tags'],
                    'material_create_time': r['material_create_time'],
                    'upload_time': r['upload_time'],
                    'current_total_cost': round(r['current_total_cost'] or 0, 2),
                    'diff_total_cost': round(r['diff_total_cost'] or 0, 2),
                    'current_basic_cost': round(r['current_basic_cost'] or 0, 2),
                    'diff_basic_cost': round(r['diff_basic_cost'] or 0, 2),
                    'current_additional_cost': round(r['current_additional_cost'] or 0, 2),
                    'diff_additional_cost': round(r['diff_additional_cost'] or 0, 2),
                    'current_additional_roi': round(r['current_additional_roi'] or 0, 2),
                    'diff_additional_roi': round(r['diff_additional_roi'] or 0, 2),
                    'current_total_roi': round(r['current_total_roi'] or 0, 2),
                    'diff_total_roi': round(r['diff_total_roi'] or 0, 2)
                })

            # 如果是自动模式，尝试找出它实际对比的那个 prev_batch_id 告诉前端
            actual_prev_batch = explicit_prev_batch
            if not explicit_prev_batch and results:
                # 简单查询一下刚刚选出的 prev_batch_id，这里为了简单直接查询最近的非本批次
                c_prev = conn.execute("SELECT batch_id FROM material_reports WHERE upload_time < ? ORDER BY upload_time DESC LIMIT 1", (target_time,))
                p_row = c_prev.fetchone()
                if p_row:
                    actual_prev_batch = p_row['batch_id']

            return {'latest_batch': latest_batch, 'prev_batch': actual_prev_batch, 'data': results}

    def get_qianchuan_batches(self):
        """获取所有千川上传批次列表"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT batch_id, MAX(upload_time) as upload_time, MAX(original_filename) as filename, COUNT(1) as record_count 
                FROM material_reports 
                GROUP BY batch_id 
                ORDER BY upload_time DESC
            """)
            return [dict(r) for r in cursor.fetchall()]

    def delete_qianchuan_batch(self, batch_id):
        """删除指定的千川批次"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM material_reports WHERE batch_id = ?", (batch_id,))
            conn.commit()

