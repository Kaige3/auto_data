# 平台列名映射配置
PLATFORM_CONFIG = {
    'douyin': {
        'name': '抖音小店',
        'required_columns': ['主订单编号', '订单提交时间'],
        'time_column': '订单提交时间',
        'rename_map': {}  # 抖音字段作为基准，无需重命名
    },
    'channels': {
        'name': '视频号',
        'required_columns': ['订单号', '订单下单时间'],
        'time_column': '订单下单时间',
        'rename_map': {
            '商品售后': '售后状态',
            '支付时间': '支付完成时间',
            '商品名称': '选购商品',
            '订单发货时间': '发货时间'
        }
    },
    'pinduoduo': {
        'name': '拼多多',
        'required_columns': ['订单号', '订单成交时间'],
        'time_column': '订单成交时间',
        'rename_map': {
            '支付时间': '支付完成时间',
            '商品': '选购商品',
            '商家编码-商品维度': '商家编码',
            # 拼多多的其他字段根据实际导出文件逐步添加映射
        }
    }
}