import pandas as pd

def apply_convergence_logic(df, platform=None):
    """
    对 DataFrame 执行收敛逻辑
    规则：按货号分组，若该周期内总未完成数 < 总支付数的 1%，则将该货号所有日期数据聚合为 1 行。
    注意：传入的 df 必须是已经去重过（每个日期、货号只保留最新一条快照）的数据！
    """
    if df.empty:
        return []
        
    # 确保核心列为数值类型
    numeric_cols = ['paidOrders', 'shippedVolume', 'preShipRefund', 'postShipRefund', 'completed', 'uncompleted']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    converged_results = []
    grouped = df.groupby('itemCode')
    
    for item_code, group in grouped:
        group = group.sort_values(by='date')
        rows = group.to_dict('records')
        
        import datetime
        
        # 1. 先按自然周对所有行进行分组，不再按天剥离
        grouped_by_week = {}
        for r in rows:
            date_str = str(r.get('date', ''))
            try:
                dt = pd.to_datetime(date_str)
                year, week, _ = dt.isocalendar()
                week_key = f"{year}-W{week:02d}"
            except:
                week_key = "unknown_week"
            
            if week_key not in grouped_by_week:
                grouped_by_week[week_key] = []
            grouped_by_week[week_key].append(r)
            
        # 2. 以“周”为单位判断是否达到收敛标准
        for week_key, week_rows in grouped_by_week.items():
            if week_key == "unknown_week":
                for r in week_rows:
                    r['is_converged'] = False
                    converged_results.append(r)
                continue
                
            total_paid = sum(r.get('paidOrders', 0) for r in week_rows)
            total_uncompleted = sum(r.get('uncompleted', 0) for r in week_rows)
            
            # 判断这整个自然周的数据是否达到收敛标准（整体未完成率 < 1%）
            if total_paid > 0 and (total_uncompleted / total_paid) < 0.01:
                dates = [str(r['date']) for r in week_rows if r.get('date')]
                min_date, max_date = min(dates), max(dates)
                date_label = f"{min_date} 至 {max_date}" if min_date != max_date else min_date
                
                sum_shipped = sum(r.get('shippedVolume', 0) for r in week_rows)
                sum_pre = sum(r.get('preShipRefund', 0) for r in week_rows)
                sum_post = sum(r.get('postShipRefund', 0) for r in week_rows)
                sum_completed = sum(r.get('completed', 0) for r in week_rows)
                
                converged_row = {
                    'date': date_label,
                    'itemCode': item_code,
                    'paidOrders': total_paid,
                    'shippedVolume': sum_shipped,
                    'preShipRefund': sum_pre,
                    'postShipRefund': sum_post,
                    'completed': sum_completed,
                    'uncompleted': total_uncompleted,
                    'preRate': round((sum_pre / total_paid) * 100, 2) if total_paid else 0,
                    'postRate': round((sum_post / total_paid) * 100, 2) if total_paid else 0,
                    'totalRate': round(((sum_pre + sum_post) / total_paid) * 100, 2) if total_paid else 0,
                    'is_converged': True,
                    'update_time': week_rows[-1].get('update_time', '')
                }
                converged_results.append(converged_row)
            else:
                # 达不到收敛标准，这一周的每一天都原样保留（不合并）
                for r in week_rows:
                    r['is_converged'] = False
                    converged_results.append(r)
                
    # 按照货号升序，日期升序返回
    return sorted(converged_results, key=lambda x: (x['itemCode'], x['date']))