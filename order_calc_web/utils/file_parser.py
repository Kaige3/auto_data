import pandas as pd
from utils.config import PLATFORM_CONFIG

def get_file_headers(file):
    """只读取表头，不加载整个文件内容"""
    columns = []
    try:
        # 记录当前指针位置 (通常是0)
        file.seek(0)
        
        if file.filename.endswith('.csv'):
            try:
                # 尝试 utf-8-sig (nrows=0 只读取表头)
                df_head = pd.read_csv(file, nrows=0, encoding='utf-8-sig')
                columns = df_head.columns.tolist()
            except UnicodeDecodeError:
                file.seek(0)
                # 回退到 gb18030
                df_head = pd.read_csv(file, nrows=0, encoding='gb18030')
                columns = df_head.columns.tolist()
        elif file.filename.endswith(('.xls', '.xlsx')):
            # Excel 读取表头
            df_head = pd.read_excel(file, nrows=0)
            columns = df_head.columns.tolist()
            
    except Exception as e:
        print(f"Error reading headers: {e}")
        return None
    finally:
        # 无论成功失败，重置文件指针供后续读取
        file.seek(0)
    
    # 终极暴力清洗：去掉列名里所有的隐形制表符、换行符和首尾空格
    cleaned_columns = []
    for col in columns:
        c = str(col).replace('\n', '').replace('\r', '').replace('\t', '').strip().strip('"').strip("'")
        cleaned_columns.append(c)
        
    return cleaned_columns

def validate_platform_file(headers, platform):
    """
    基于平台配置，校验文件表头是否包含所需字段
    """
    config = PLATFORM_CONFIG.get(platform)
    if not config:
        return False, f"未知的平台类型: {platform}"
        
    required_cols = config.get('required_columns', [])
    missing_cols = [col for col in required_cols if col not in headers]
    
    if missing_cols:
        return False, f"上传的文件似乎不是【{config['name']}】的订单数据。缺少必须的列: {', '.join(missing_cols)}"
        
    return True, ""