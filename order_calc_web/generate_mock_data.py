import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

def generate_mock_data():
    """生成测试用的模拟电商订单数据"""
    
    # 定义基础配置
    start_date = datetime(2026, 3, 1)
    end_date = datetime(2026, 3, 15)
    
    # 模拟数据条数
    num_records = 500
    
    # 候选货号
    item_codes = ['6050', '6301']
    
    # 抖音售后状态池及概率
    douyin_refund_status = [
        ('无', 0.8),
        ('待收退货', 0.05),
        ('待退货', 0.05),
        ('退款成功', 0.05),
        ('退款关闭', 0.05)
    ]
    
    # 视频号售后状态池及概率
    channels_refund_status = [
        ('无', 0.75),
        ('待买家退货', 0.05),
        ('退款成功', 0.05),
        ('平台处理完成', 0.05),
        ('用户取消申请', 0.05),
        ('商家拒绝退款', 0.05)
    ]
    
    # 订单状态
    order_status = ['已发货', '已完成', '交易关闭', '待发货']
    
    # --- 生成抖音模拟数据 ---
    douyin_records = []
    for _ in range(num_records):
        order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        # 随机时间
        order_time = order_date.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59))
        
        status = random.choice(order_status)
        
        # 支付时间
        paid_time = (order_time + timedelta(minutes=random.randint(1, 60))).strftime('%Y-%m-%d %H:%M:%S') if status != '交易关闭' else '-'
        
        # 发货时间
        if status in ['已发货', '已完成']:
            ship_time = (order_time + timedelta(hours=random.randint(1, 48))).strftime('%Y-%m-%d %H:%M:%S')
        else:
            ship_time = '-'
            
        # 售后状态
        refund_choices, refund_weights = zip(*douyin_refund_status)
        refund_stat = random.choices(refund_choices, weights=refund_weights)[0]
        
        douyin_records.append({
            '订单号': f'DY{random.randint(1000000000, 9999999999)}',
            '订单提交时间': order_time.strftime('%Y-%m-%d %H:%M:%S'),
            '支付完成时间': paid_time,
            '发货时间': ship_time,
            '订单状态': status,
            '售后状态': refund_stat,
            '商家编码': f"DY-{random.choice(item_codes)}-{random.randint(10,99)}"
        })
        
    df_douyin = pd.DataFrame(douyin_records)
    
    # --- 生成视频号模拟数据 ---
    channels_records = []
    for _ in range(num_records):
        order_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        order_time = order_date.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59))
        
        status = random.choice(order_status)
        paid_time = (order_time + timedelta(minutes=random.randint(1, 60))).strftime('%Y-%m-%d %H:%M:%S') if status != '交易关闭' else '-'
        
        if status in ['已发货', '已完成']:
            ship_time = (order_time + timedelta(hours=random.randint(1, 48))).strftime('%Y-%m-%d %H:%M:%S')
        else:
            ship_time = '-'
            
        refund_choices, refund_weights = zip(*channels_refund_status)
        refund_stat = random.choices(refund_choices, weights=refund_weights)[0]
        
        # 视频号通常将商品名和编码放在一起
        target_item = random.choice(item_codes)
        
        channels_records.append({
            '订单号': f'WX{random.randint(1000000000, 9999999999)}',
            '订单下单时间': order_time.strftime('%Y-%m-%d %H:%M:%S'),
            '支付时间': paid_time,
            '订单发货时间': ship_time,
            '订单状态': status,
            '商品售后': refund_stat,
            '选购商品': f"2026春季新款连衣裙 TZX{target_item} 黑色 M",
            '商品编码(自定义)': f"WX-{target_item}"
        })
        
    df_channels = pd.DataFrame(channels_records)
    
    # 保存为 CSV
    os.makedirs('mock_data', exist_ok=True)
    
    # 抖音保存为 CSV (utf-8-sig)
    douyin_path = os.path.join('mock_data', 'mock_douyin_orders.csv')
    df_douyin.to_csv(douyin_path, index=False, encoding='utf-8-sig')
    print(f"✅ 成功生成抖音模拟数据: {douyin_path}")
    
    # 视频号保存为 Excel (模拟真实导出场景)
    channels_path = os.path.join('mock_data', 'mock_channels_orders.xlsx')
    df_channels.to_excel(channels_path, index=False)
    print(f"✅ 成功生成视频号模拟数据: {channels_path}")
    
    print("\n💡 你现在可以在网页端上传这两个文件进行测试了！")

if __name__ == '__main__':
    generate_mock_data()