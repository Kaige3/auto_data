// 控制加载遮罩
export function toggleLoading(show) {
    document.getElementById('loading-overlay').style.display = show ? 'flex' : 'none';
}

export function renderYesterdayTable(data, selectedDate) {
    const container = document.getElementById('yesterdayTableContainer');
    if (!container) return;

    if (!data || data.length === 0) {
        container.innerHTML = `<div style="text-align:center; padding: 40px; color:var(--secondary);">暂无 ${selectedDate} 的数据</div>`;
        return;
    }

    // 按照平台分组，平台名相同时合并
    const grouped = {};
    data.forEach(item => {
        if (!grouped[item.platform_name]) {
            grouped[item.platform_name] = [];
        }
        grouped[item.platform_name].push(item);
    });

    // 提取日期的月/日用于标题
    let displayDate = selectedDate;
    if (selectedDate && selectedDate.includes('-')) {
        const parts = selectedDate.split('-');
        if (parts.length === 3) {
            displayDate = `${parseInt(parts[1])}/${parseInt(parts[2])}`;
        }
    }

    let html = `
        <table style="width: 100%; border-collapse: collapse; text-align: center; font-size: 13px; color: #000; background-color: #fff;">
            <tbody>
    `;

    for (const [platformName, items] of Object.entries(grouped)) {
        items.forEach((item, index) => {
            // 根据图片设计，每个条目都有自己的一个小表头（占据一整行）
            const headerTitle = `${platformName}（${item.shop_name}）-${item.itemCode}-${displayDate}发前退款率`;
            
            let abnormalOrdersHtml = '';
            let rowspan = 3;
            if (item.status === '不达标' && item.abnormalOrders && item.abnormalOrders.length > 0) {
                rowspan = 4;
                // 如果订单号很多，用一个可滚动的 div 包裹
                abnormalOrdersHtml = `
                    <tr>
                        <td colspan="7" style="border: 1px solid #000; padding: 6px; background-color: #fee2e2; text-align: left; font-size: 12px;">
                            <div style="font-weight: bold; color: #b91c1c; margin-bottom: 4px;">⚠️ 异常订单号列表 (${item.abnormalOrders.length}单):</div>
                            <div style="max-height: 80px; overflow-y: auto; color: #7f1d1d; word-break: break-all; padding: 4px; background: rgba(255,255,255,0.5); border-radius: 4px;">
                                ${item.abnormalOrders.join(', ')}
                            </div>
                        </td>
                    </tr>
                `;
            }

            html += `
                <!-- 顶部分隔线 (如果不是第一条) -->
                ${index === 0 ? '' : '<tr><td colspan="8" style="height: 10px; background-color: #fff; border: none;"></td></tr>'}
                
                <!-- 标题行 -->
                <tr>
                    <td rowspan="${rowspan}" style="width: 60px; background-color: #60a5fa; color: #000; font-weight: bold; border: 1px solid #000; vertical-align: middle;">
                        ${platformName}
                    </td>
                    <td colspan="7" style="background-color: #e2e8f0; font-weight: bold; font-size: 15px; padding: 8px; border: 1px solid #000;">
                        ${headerTitle}
                    </td>
                </tr>
                
                <!-- 字段行 -->
                <tr style="background-color: #cbd5e1; font-weight: bold;">
                    <td style="border: 1px solid #000; padding: 6px;">登记款号</td>
                    <td style="border: 1px solid #000; padding: 6px;">订单数</td>
                    <td style="border: 1px solid #000; padding: 6px;">待发数</td>
                    <td style="border: 1px solid #000; padding: 6px;">发货数</td>
                    <td style="border: 1px solid #000; padding: 6px;">发前退款</td>
                    <td style="border: 1px solid #000; padding: 6px;">发前退款率</td>
                    <td style="border: 1px solid #000; padding: 6px;">是否异常</td>
                </tr>
                
                <!-- 数据行 -->
                <tr>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #fef08a; font-weight: bold;">${item.itemCode}</td>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #fbcfe8;">${item.paidOrders}</td>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #dcfce7;">${item.pending}</td>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #fbcfe8;">${item.shippedVolume}</td>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #fbcfe8;">${item.preShipRefund}</td>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #dcfce7;">${item.preRate.toFixed(2)}%</td>
                    <td style="border: 1px solid #000; padding: 6px; background-color: #fef08a; font-weight: bold; color: ${item.status === '达标' ? '#000' : 'red'};">${item.status}</td>
                </tr>
                ${abnormalOrdersHtml}
            `;
        });
    }

    html += `
            </tbody>
        </table>
    `;

    container.innerHTML = html;
}

export function renderGlobalTable(globalData, snapshotsData) {
    const container = document.getElementById('globalTableContainer');
    if (!container) return;

    if (!globalData || globalData.length === 0) {
        container.innerHTML = '<div style="padding: 40px; text-align: center; color: var(--secondary);">暂无全局数据</div>';
        return;
    }

    // 将 snapshotsData 转换为便于查找的结构，按 平台-店铺-货号-日期 分组，并且按 update_time 倒序排列
    const snapshotMap = {};
    if (snapshotsData && snapshotsData.length > 0) {
        snapshotsData.forEach(row => {
            const p = row.platform_name || '未知平台';
            const s = row.shop_name || '未知店铺';
            const c = row.itemCode || '未知货号';
            const d = row.date || '未知日期';
            
            const key = `${p}_${s}_${c}_${d}`;
            if (!snapshotMap[key]) {
                snapshotMap[key] = [];
            }
            snapshotMap[key].push(row);
        });
    }

    // 1. 将扁平数据按 平台 -> 店铺 -> 货号 嵌套分组
    const grouped = {};
    globalData.forEach(row => {
        const p = row.platform_name || '未知平台';
        const s = row.shop_name || '未知店铺';
        const c = row.itemCode || '未知货号';
        if (!grouped[p]) grouped[p] = {};
        if (!grouped[p][s]) grouped[p][s] = {};
        if (!grouped[p][s][c]) grouped[p][s][c] = [];
        grouped[p][s][c].push(row);
    });

    let html = '';

    // 2. 遍历平台、店铺、货号
    for (const platform in grouped) {
        const shops = grouped[platform];
        
        for (const shop in shops) {
            const itemCodes = shops[shop];
            
            for (const itemCode in itemCodes) {
                const rows = itemCodes[itemCode];
                
                // 我们需要区分两种数据：
                // 1. 对于未收敛的具体日期（is_converged === false），我们需要从 snapshotMap 中提取该日期的所有历史快照进行展示。
                // 2. 对于已收敛的日期范围（is_converged === true），我们**不再展示**其具体的历史快照，而是**只展示一条**来自后端计算好的聚合汇总行（也就是 rows 中的那条记录）。
                
                const displayRows = [];
                
                rows.forEach(r => {
                    if (r.is_converged) {
                        // 收敛行，作为独立的一条展示，不带子快照
                        displayRows.push({
                            type: 'converged',
                            dateGroup: r.date, // e.g., "2026-03-01 至 2026-03-15"
                            snapshots: [r] // 只有它自己一条
                        });
                    } else {
                        // 未收敛的行，找到其在 snapshotMap 中的所有历史记录
                        const exactDate = r.date;
                        const key = `${platform}_${shop}_${itemCode}_${exactDate}`;
                        let snaps = [];
                        if (snapshotMap[key]) {
                            snaps = [...snapshotMap[key]];
                            // 升序排列，最旧的在上面，最新的在下面
                            snaps.sort((a, b) => (a.update_time || '').localeCompare(b.update_time || ''));
                        } else {
                            // 理论上不可能，防错
                            snaps = [r];
                        }
                        
                        displayRows.push({
                            type: 'unconverged',
                            dateGroup: exactDate,
                            snapshots: snaps
                        });
                    }
                });
                
                // 按日期升序排序
                displayRows.sort((a, b) => a.dateGroup.localeCompare(b.dateGroup));
                
                // 计算平台列的总 rowspan (包括数据行和空行)
                let totalRowsForPlatform = 0;
                displayRows.forEach(group => {
                    totalRowsForPlatform += group.snapshots.length + 1; // +1 是为了底部的空行
                });
                
                // 表头部分，完全按照图片字段
                html += `
                <div class="shop-excel-table" style="margin-bottom: 30px; box-shadow: 0 4px 6px rgba(0,0,0,0.3); overflow: hidden;">
                    <div style="overflow-x: auto;">
                    <table style="width: 100%; border-collapse: collapse; text-align: center; font-size: 12px; color: #000; background-color: #fff;">
                        <thead style="background-color: #e2e8f0; font-weight: bold;">
                            <tr>
                                <th colspan="13" style="background-color: #ffff00; color: #000; text-align: center; font-size: 14px; font-weight: bold; padding: 8px; border: 1px solid #000; border-bottom: none;">
                                    ${platform}（${shop}） -${itemCode}--
                                </th>
                            </tr>
                            <tr>
                                <th style="width: 30px; border: 1px solid #000; padding: 6px;"></th>
                                <th style="border: 1px solid #000; padding: 6px;">登记日期</th>
                                <th style="border: 1px solid #000; padding: 6px;">支付订单</th>
                                <th style="border: 1px solid #000; padding: 6px;">发前退款</th>
                                <th style="border: 1px solid #000; padding: 6px;">发货量</th>
                                <th style="border: 1px solid #000; padding: 6px;">已完成</th>
                                <th style="border: 1px solid #000; padding: 6px;">未完成</th>
                                <th style="border: 1px solid #000; padding: 6px;">发货退款</th>
                                <th style="border: 1px solid #000; padding: 6px;">退款率</th>
                                <th style="border: 1px solid #000; padding: 6px;">发前退货</th>
                                <th style="border: 1px solid #000; padding: 6px;">发货后退</th>
                                <th style="border: 1px solid #000; padding: 6px;">统计日期</th>
                                <th style="border: 1px solid #000; padding: 6px;">待发货</th>
                            </tr>
                        </thead>
                        <tbody>
                `;

                let isFirstRowInTable = true;
                
                // 颜色循环，用于区分不同的登记日期分组
                const groupColors = ['#fecdd3', '#a5f3fc', '#e2e8f0', '#fef08a'];

                displayRows.forEach((group, groupIdx) => {
                    const snapshots = group.snapshots;
                    const groupRowspan = snapshots.length;
                    
                    // 收敛行给一个固定的黄色背景
                    const groupBgColor = group.type === 'converged' ? '#fef08a' : groupColors[groupIdx % groupColors.length];
                    
                    // 格式化登记日期，例如 "3/11" 或 "2/11-2/23"
                    let displayDate = group.dateGroup;
                    if (displayDate.includes(' 至 ')) {
                        const parts = displayDate.split(' 至 ');
                        const d1 = new Date(parts[0]);
                        const d2 = new Date(parts[1]);
                        displayDate = `${d1.getMonth()+1}/${d1.getDate()}-${d2.getMonth()+1}/${d2.getDate()}`;
                    } else if (displayDate.includes('-')) {
                         const d1 = new Date(displayDate);
                         displayDate = `${d1.getMonth()+1}/${d1.getDate()}`;
                    }

                    snapshots.forEach((snap, snapIdx) => {
                        const snapRate = parseFloat(snap.totalRate || 0);
                        const snapPreRate = parseFloat(snap.preRate || 0);
                        const snapPostRate = parseFloat(snap.postRate || 0);
                        const snapPending = Math.max(0, snap.paidOrders - snap.shippedVolume - snap.preShipRefund);
                        
                        let platformCol = '';
                        if (isFirstRowInTable) {
                            platformCol = `<td rowspan="${totalRowsForPlatform}" style="background-color: #0284c7; width: 30px; border: 1px solid #000;"></td>`;
                            isFirstRowInTable = false;
                        }
                        
                        let dateCol = '';
                        if (snapIdx === 0) {
                            dateCol = `<td rowspan="${groupRowspan}" style="background-color: ${groupBgColor}; border: 1px solid #000; padding: 4px; color: #000; vertical-align: middle;">${displayDate}</td>`;
                        }

                        // 格式化统计日期
                        let statDateDisplay = '-';
                        if (snap.update_time) {
                            const dt = new Date(snap.update_time);
                            if (group.type === 'converged') {
                                // 收敛行，只显示最后一次统计的月日
                                statDateDisplay = `${dt.getMonth()+1}/${dt.getDate()}`;
                            } else {
                                // 未收敛行，如果有多条快照，显示具体时间以便区分
                                statDateDisplay = snapshots.length > 1 
                                    ? `${dt.getMonth()+1}/${dt.getDate()} ${dt.getHours()}:${String(dt.getMinutes()).padStart(2, '0')}`
                                    : `${dt.getMonth()+1}/${dt.getDate()}`;
                            }
                        }

                        // 根据规则动态判断退款率标红
                        let refundRateStyle = 'border: 1px solid #000; padding: 4px; background-color: #e0f2fe; color: #000;';
                        if (String(itemCode).includes('6050')) {
                            // JOJO 抖音小店 退货率 60% 标红
                            if (String(platform).includes('抖音小店') && String(shop).includes('JOJO')) {
                                if (snapRate > 60) {
                                    refundRateStyle = 'border: 1px solid #000; padding: 4px; background-color: #e0f2fe; color: red; font-weight: bold;';
                                }
                            } else {
                                // 剩下其他的平台店铺 6050 退货率 45% 标红
                                if (snapRate > 45) {
                                    refundRateStyle = 'border: 1px solid #000; padding: 4px; background-color: #e0f2fe; color: red; font-weight: bold;';
                                }
                            }
                        }

                        // 使用你提供的图片配色
                        html += `
                        <tr style="background-color: #fff;">
                            ${platformCol}
                            ${dateCol}
                            <td style="border: 1px solid #000; padding: 4px; background-color: #fbcfe8; color: #000;">${snap.paidOrders}</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #fbcfe8; color: #000;">${snap.preShipRefund}</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #fbcfe8; color: #000;">${snap.shippedVolume}</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #bbf7d0; color: #000;">${snap.completed}</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #bbf7d0; color: #000;">${snap.uncompleted}</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #bbf7d0; color: #000;">${snap.postShipRefund}</td>
                            <td style="${refundRateStyle}">${snapRate.toFixed(2)}%</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #e0f2fe; color: #000;">${snapPreRate.toFixed(2)}%</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #e0f2fe; color: #000;">${snapPostRate.toFixed(2)}%</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #a5f3fc; color: #000;">${statDateDisplay}</td>
                            <td style="border: 1px solid #000; padding: 4px; background-color: #fef08a; color: #000;">${snapPending}</td>
                        </tr>
                        `;
                    });
                    
                    // 为当前 dateGroup (登记日期分组) 底部添加一个空行，方便查看
                    html += `
                        <tr style="height: 20px; background-color: #fff;">
                            ${isFirstRowInTable ? '' : '' /* 平台列已经用 rowspan 跨过了，这里不需要加 */}
                            <td colspan="12" style="border: 1px solid #000; border-top: none; border-bottom: 2px solid #000;"></td>
                        </tr>
                    `;
                });

                html += `
                        </tbody>
                    </table>
                    </div>
                </div>
                `;
            }
        }
    }

    container.innerHTML = html;
}

export function renderCrawlerTable(data) {
    const container = document.getElementById('crawlerTableContainer');
    if (!container) return;

    if (!data || data.length === 0) {
        container.innerHTML = '<div style="padding: 40px; text-align: center; color: var(--secondary);">暂无采集到的素材数据</div>';
        return;
    }

    let html = `
        <table style="width: 100%; border-collapse: collapse; text-align: left; font-size: 13px; color: var(--text-main); table-layout: fixed;">
            <thead style="background-color: var(--bg-main); font-weight: bold; border-bottom: 1px solid var(--border);">
                <tr>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 50px; text-align: center;">
                        <div style="display: flex; align-items: center; justify-content: center; height: 100%; width: 100%; cursor: pointer;" onclick="document.getElementById('selectAllCrawler').click();">
                            <input type="checkbox" id="selectAllCrawler" style="cursor: pointer; width: 16px; height: 16px;" onclick="event.stopPropagation();">
                        </div>
                    </th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 60px;">ID</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 70px; white-space: nowrap;">状态</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 80px; white-space: nowrap;">来源</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 250px;">视频标题</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 200px;">话题/标签</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 80px;">播放量</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 150px;">互动数据 (赞/评/转/藏)</th>
                    <th style="padding: 12px; border-right: 1px solid var(--border); width: 140px;">发布时间</th>
                    <th style="padding: 12px; width: 80px;">链接</th>
                </tr>
            </thead>
            <tbody>
    `;

    data.forEach(item => {
        const topics = item.topics && item.topics.length ? item.topics.map(t => `<span style="display:inline-block; background:rgba(59,130,246,0.2); color:#3b82f6; padding:2px 6px; border-radius:4px; margin-right:4px; margin-bottom:4px; font-size:11px; white-space: nowrap;">#${t}</span>`).join('') : '';
        const bizTags = item.biz_tags && item.biz_tags.length ? item.biz_tags.map(t => `<span style="display:inline-block; background:rgba(16,185,129,0.2); color:#10b981; padding:2px 6px; border-radius:4px; margin-right:4px; margin-bottom:4px; font-size:11px; white-space: nowrap;">${t}</span>`).join('') : '';
        const tagsHtml = topics + bizTags;
        
        let interactData = '-';
        if (item.video_source === '主页视频') {
            interactData = `${item.digg_count || 0} / ${item.comment_count || 0} / ${item.share_count || 0} / ${item.collect_count || 0}`;
        }
        
        const shortUrl = item.share_url ? `<a href="${item.share_url}" target="_blank" style="color:#3b82f6; text-decoration:none; white-space: nowrap;">查看🔗</a>` : '-';
        const syncStatus = item.sync_feishu === 1 ? '<span style="color:#10b981; white-space: nowrap;">已同步</span>' : '<span style="color:#94a3b8; white-space: nowrap;">未同步</span>';

        html += `
            <tr style="border-bottom: 1px solid var(--border); transition: background-color 0.2s;" onmouseover="this.style.backgroundColor='var(--bg)'" onmouseout="this.style.backgroundColor='transparent'">
                <td style="padding: 0; border-right: 1px solid var(--border); text-align: center;">
                    <div style="display: flex; align-items: center; justify-content: center; width: 100%; height: 100%; min-height: 48px; cursor: pointer;" onclick="const cb = this.querySelector('input'); if(!cb.disabled) { cb.checked = !cb.checked; }">
                        <input type="checkbox" class="crawler-checkbox" value="${item.id}" ${item.sync_feishu === 1 ? 'disabled' : ''} style="cursor: pointer; width: 16px; height: 16px;" onclick="event.stopPropagation();">
                    </div>
                </td>
                <td style="padding: 12px; border-right: 1px solid var(--border); color: var(--secondary);">${item.id}</td>
                <td style="padding: 12px; border-right: 1px solid var(--border);">${syncStatus}</td>
                <td style="padding: 12px; border-right: 1px solid var(--border);">
                    <span style="display:inline-block; padding:2px 6px; border-radius:4px; font-size:11px; white-space: nowrap; background:${item.video_source === '主页视频' ? '#fef08a; color:#854d0e' : '#e0f2fe; color:#0369a1'};">${item.video_source || '-'}</span>
                </td>
                <td style="padding: 12px; border-right: 1px solid var(--border); overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${item.title}">${item.title || '-'}</td>
                <td style="padding: 12px; border-right: 1px solid var(--border);">${tagsHtml || '-'}</td>
                <td style="padding: 12px; border-right: 1px solid var(--border); font-weight: bold;">${(item.play_count || 0).toLocaleString()}</td>
                <td style="padding: 12px; border-right: 1px solid var(--border); color: var(--secondary); white-space: nowrap;">${interactData}</td>
                <td style="padding: 12px; border-right: 1px solid var(--border); color: var(--secondary); white-space: nowrap;">${item.publish_time || '-'}</td>
                <td style="padding: 12px;">${shortUrl}</td>
            </tr>
        `;
    });

    html += `
            </tbody>
        </table>
    `;

    container.innerHTML = html;
}
