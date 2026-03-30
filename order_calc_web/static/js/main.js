import { uploadFileData, fetchGlobalData, fetchYesterdayData, uploadQianchuanData, fetchQianchuanDiff, fetchQianchuanBatches, deleteQianchuanBatch } from './api.js';
import { toggleLoading, renderGlobalTable, renderYesterdayTable, renderCrawlerTable } from './ui.js';

// 暂存供导出使用的全局数据 
let currentGlobalData = [];

// --- 常量与 DOM 元素 ---
const SHOP_MAPPING = {
    douyin: ['唐造女装', 'JOJO', '蕉卜'],
    channels: ['佑砺','一亦','俊熙'],
    pinduoduo: ['午后写生旗舰店', '蕉卜旗舰店']
};

// 上传相关
const uploadBtn = document.getElementById('uploadBtn');
const fileInput = document.getElementById('fileInput');

// 昨日数据相关
const yesterdayDateInput = document.getElementById('yesterdayDateInput');
const exportYesterdayBtn = document.getElementById('exportYesterdayBtn');

function initShopSelection() {
    const platformInputs = document.querySelectorAll('input[name="platform"]');
    const shopContainer = document.getElementById('shop-select-container');
    const shopSelect = document.getElementById('shopSelect');

    platformInputs.forEach(input => {
        input.addEventListener('change', (e) => {
            const platform = e.target.value;
            const shops = SHOP_MAPPING[platform] || [];
            
            shopSelect.innerHTML = '<option value="">请选择店铺...</option>' + 
                                  shops.map(s => `<option value="${s}">${s}</option>`).join('');
            
            if (shops.length > 0) {
                shopContainer.style.display = 'block';
            } else {
                shopContainer.style.display = 'none';
            }
        });
    });
}

// --- 1. 事件监听注册 ---
function bindEvents() {
    initShopSelection();

    const navButtons = document.querySelectorAll('.nav-item');
    const modules = document.querySelectorAll('.biz-module');
    const dataSourceControlGroup = document.getElementById('dataSourceControlGroup');

    function activate(targetId) {
        modules.forEach(m => m.style.display = (m.id === targetId ? 'block' : 'none'));
        navButtons.forEach(btn => btn.classList.toggle('active', btn.getAttribute('data-target') === targetId));
        
        // 只有订单分析才需要显示数据源设置面板
        if (dataSourceControlGroup) {
            dataSourceControlGroup.style.display = (targetId === 'order-analysis') ? 'block' : 'none';
        }
    }
    activate('order-analysis');
    navButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.getAttribute('data-target');
            activate(targetId);
        });
    });

    // 默认加载全局数据视图
    toggleLoading(true);
    fetchGlobalData().then(res => {
        if (res.status === 'success') {
            if (res.data && res.data.length > 0) {
                currentGlobalData = res.data;
                renderGlobalTable(currentGlobalData, res.snapshots);
            }
        }
    }).catch(e => {
        console.error("加载全局数据失败", e);
    }).finally(() => {
        toggleLoading(false);
    });

    uploadBtn.addEventListener('click', handleUpload);

    // 绑定刚才写好的导出事件 
    const exportGlobalBtn = document.getElementById('exportGlobalBtn'); 
    if (exportGlobalBtn) { 
        exportGlobalBtn.addEventListener('click', () => { 
            exportToExcelWithStyles('globalTableContainer', '全盘汇总表', '电商全局数据汇总'); 
        }); 
    }

    // 绑定昨日数据的导出和日期变更事件
    if (yesterdayDateInput) {
        // 默认设置为昨天 (使用本地时间而不是 UTC 时间，防止时区导致日期差一天)
        const yesterday = new Date();
        yesterday.setDate(yesterday.getDate() - 1);
        const y = yesterday.getFullYear();
        const m = String(yesterday.getMonth() + 1).padStart(2, '0');
        const d = String(yesterday.getDate()).padStart(2, '0');
        yesterdayDateInput.value = `${y}-${m}-${d}`;

        yesterdayDateInput.addEventListener('change', async () => {
            toggleLoading(true);
            try {
                const res = await fetchYesterdayData(yesterdayDateInput.value);
                if (res.status === 'success') {
                    renderYesterdayTable(res.data, yesterdayDateInput.value);
                }
            } catch (e) {
                console.error("加载昨日数据失败", e);
            } finally {
                toggleLoading(false);
            }
        });
    }

    if (exportYesterdayBtn) {
        exportYesterdayBtn.addEventListener('click', () => {
            const dateStr = yesterdayDateInput ? yesterdayDateInput.value : '';
            exportToExcelWithStyles('yesterdayTableContainer', '昨日发前退款率', `昨日发前退款率_${dateStr}`);
        });
    }

    // --- 批量删除管理逻辑 ---
    const manageBatchesBtn = document.getElementById('manageBatchesBtn');
    const batchesModal = document.getElementById('batchesModal');
    const closeBatchesModal = document.getElementById('closeBatchesModal');
    const cancelBatchesBtn = document.getElementById('cancelBatchesBtn');
    const deleteSelectedBatchesBtn = document.getElementById('deleteSelectedBatchesBtn');
    const selectAllBatches = document.getElementById('selectAllBatches');
    const batchesTableBody = document.getElementById('batchesTableBody');

    if (manageBatchesBtn) {
        manageBatchesBtn.addEventListener('click', async () => {
            batchesModal.style.display = 'flex';
            await loadBatches();
        });
    }

    function closeModal() {
        batchesModal.style.display = 'none';
        selectAllBatches.checked = false;
    }

    if (closeBatchesModal) closeBatchesModal.addEventListener('click', closeModal);
    if (cancelBatchesBtn) cancelBatchesBtn.addEventListener('click', closeModal);

    if (selectAllBatches) {
        selectAllBatches.addEventListener('change', (e) => {
            const checkboxes = batchesTableBody.querySelectorAll('.batch-checkbox');
            checkboxes.forEach(cb => cb.checked = e.target.checked);
        });
    }

    if (deleteSelectedBatchesBtn) {
        deleteSelectedBatchesBtn.addEventListener('click', async () => {
            const checkboxes = batchesTableBody.querySelectorAll('.batch-checkbox:checked');
            const batchIds = Array.from(checkboxes).map(cb => cb.value);
            
            if (batchIds.length === 0) {
                alert('请先勾选要删除的批次！');
                return;
            }

            if (!confirm(`确定要彻底删除选中的 ${batchIds.length} 个批次吗？该操作不可恢复！`)) {
                return;
            }

            toggleLoading(true);
            try {
                const res = await fetch('/api/batches', {
                    method: 'DELETE',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ batch_ids: batchIds })
                }).then(r => r.json());

                if (res.status === 'success') {
                    alert(res.msg);
                    await loadBatches(); // 重新加载列表
                    await loadAllData(); // 重新刷新后台大盘数据
                } else {
                    alert('删除失败: ' + res.msg);
                }
            } catch (err) {
                console.error(err);
                alert('删除请求发生错误');
            } finally {
                toggleLoading(false);
            }
        });
    }

    // --- 千川投流差额 ---
    const uploadQianchuanBtn = document.getElementById('uploadQianchuanBtn');
    const qianchuanFileInput = document.getElementById('qianchuanFileInput');
    const searchQianchuanBtn = document.getElementById('searchQianchuanBtn');
    const qianchuanSearchInput = document.getElementById('qianchuanSearchInput');
    const qianchuanTableBody = document.getElementById('qianchuanTableBody');
    const qianchuanBatchSelect = document.getElementById('qianchuanBatchSelect');
    const qianchuanPrevBatchSelect = document.getElementById('qianchuanPrevBatchSelect');
    const deleteQianchuanBatchBtn = document.getElementById('deleteQianchuanBatchBtn');

    let currentQianchuanData = [];
    let currentSortColumn = 'current_total_cost'; // 默认按整体消耗排序
    let currentSortOrder = 'desc'; // 默认降序

    const renderQianchuanTable = (data, latestBatch, prevBatch) => {
        if (!data || data.length === 0) {
            qianchuanTableBody.innerHTML = '<tr><td colspan="8" style="text-align:center; padding: 20px; color: var(--secondary);">暂无数据</td></tr>';
            if (deleteQianchuanBatchBtn) deleteQianchuanBatchBtn.style.display = 'none';
            return;
        }

        if (qianchuanBatchSelect && latestBatch) {
            qianchuanBatchSelect.value = latestBatch;
            if (deleteQianchuanBatchBtn) deleteQianchuanBatchBtn.style.display = 'block';
        }
        
        if (qianchuanPrevBatchSelect) {
            qianchuanPrevBatchSelect.value = prevBatch || '';
        }

        // 排序逻辑
        data.sort((a, b) => {
            const valA = a[currentSortColumn] || (currentSortColumn === 'material_create_time' ? '' : 0);
            const valB = b[currentSortColumn] || (currentSortColumn === 'material_create_time' ? '' : 0);
            
            // 对时间字符串进行特殊排序处理
            if (currentSortColumn === 'material_create_time') {
                if (currentSortOrder === 'asc') {
                    return String(valA).localeCompare(String(valB));
                } else {
                    return String(valB).localeCompare(String(valA));
                }
            }
            
            if (currentSortOrder === 'asc') {
                return valA - valB;
            } else {
                return valB - valA;
            }
        });

        // 更新表头排序图标
        document.querySelectorAll('.sortable-col').forEach(th => {
            const iconSpan = th.querySelector('.sort-icon');
            if (th.getAttribute('data-sort') === currentSortColumn) {
                iconSpan.innerHTML = currentSortOrder === 'asc' ? '▲' : '▼';
                iconSpan.style.color = 'var(--text-main)';
            } else {
                iconSpan.innerHTML = '↕';
                iconSpan.style.color = 'var(--secondary)';
            }
        });

        let html = '';
        data.forEach(item => {
            const formatValue = (val) => val !== null && val !== undefined ? Number(val).toFixed(2) : '0.00';
            
            // 计算消耗类差异指标 (大于0红，小于0绿)
            const renderCostIndicator = (diff) => {
                if (diff > 0) return `<span style="color: #ef4444;">↑ ${formatValue(diff)}</span>`;
                if (diff < 0) return `<span style="color: #10b981;">↓ ${formatValue(Math.abs(diff))}</span>`;
                return '<span style="color: transparent;">-</span>'; // 占位保持高度一致
            };

            // 计算ROI类差异指标 (大于0绿，小于0红)
            const renderRoiIndicator = (diff) => {
                if (diff > 0) return `<span style="color: #10b981;">↑ ${formatValue(diff)}</span>`;
                if (diff < 0) return `<span style="color: #ef4444;">↓ ${formatValue(Math.abs(diff))}</span>`;
                return '<span style="color: transparent;">-</span>'; // 占位保持高度一致
            };

            html += `
                <tr style="border-bottom: 1px solid var(--border); transition: background-color 0.2s;">
                    <td style="padding: 12px 8px; word-break: break-all; color: var(--secondary); font-family: monospace;">${item.material_id}</td>
                    <td style="padding: 12px 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;" title="${item.material_name}">${item.material_name}</td>
                    <td style="padding: 12px 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; color: var(--secondary);" title="${item.tags}">${item.tags || '-'}</td>
                    <td style="padding: 12px 8px; font-size: 12px; color: var(--secondary);">${item.material_create_time || '-'}</td>
                    <td style="padding: 12px 8px;">
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <span>${formatValue(item.current_total_cost)}</span>
                            <span style="font-size: 12px;">${renderCostIndicator(item.diff_total_cost)}</span>
                        </div>
                    </td>
                    <td style="padding: 12px 8px;">
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <span>${formatValue(item.current_basic_cost)}</span>
                            <span style="font-size: 12px;">${renderCostIndicator(item.diff_basic_cost)}</span>
                        </div>
                    </td>
                    <td style="padding: 12px 8px;">
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <span>${formatValue(item.current_additional_cost)}</span>
                            <span style="font-size: 12px;">${renderCostIndicator(item.diff_additional_cost)}</span>
                        </div>
                    </td>
                    <td style="padding: 12px 8px;">
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <span style="font-weight: 500;">${formatValue(item.current_additional_roi)}</span>
                            <span style="font-size: 12px;">${renderRoiIndicator(item.diff_additional_roi)}</span>
                        </div>
                    </td>
                    <td style="padding: 12px 8px;">
                        <div style="display: flex; flex-direction: column; gap: 4px;">
                            <span style="font-weight: 500;">${formatValue(item.current_total_roi)}</span>
                            <span style="font-size: 12px;">${renderRoiIndicator(item.diff_total_roi)}</span>
                        </div>
                    </td>
                </tr>
            `;
        });
        qianchuanTableBody.innerHTML = html;
    };

    const loadQianchuanDiff = async () => {
        toggleLoading(true);
        try {
            const keyword = qianchuanSearchInput ? qianchuanSearchInput.value : '';
            const batchId = qianchuanBatchSelect ? qianchuanBatchSelect.value : '';
            const prevBatchId = qianchuanPrevBatchSelect ? qianchuanPrevBatchSelect.value : '';
            const res = await fetchQianchuanDiff(keyword, batchId, prevBatchId);
            if (res.status === 'success') {
                currentQianchuanData = res.data;
                renderQianchuanTable(currentQianchuanData, res.latest_batch, res.prev_batch);
            } else {
                alert('查询失败: ' + res.msg);
            }
        } catch (e) {
            console.error(e);
            alert('查询发生错误');
        } finally {
            toggleLoading(false);
        }
    };

    // 绑定表头排序点击事件
    document.querySelectorAll('.sortable-col').forEach(th => {
        th.addEventListener('click', () => {
            const sortKey = th.getAttribute('data-sort');
            if (currentSortColumn === sortKey) {
                // 如果已经是按该列排序，则切换顺序
                currentSortOrder = currentSortOrder === 'desc' ? 'asc' : 'desc';
            } else {
                // 如果是新列，默认降序
                currentSortColumn = sortKey;
                currentSortOrder = 'desc';
            }
            if (currentQianchuanData && currentQianchuanData.length > 0) {
                // 仅重新渲染表格（不用重新请求数据）
                const batchId = qianchuanBatchSelect ? qianchuanBatchSelect.value : '';
                const prevBatchId = qianchuanPrevBatchSelect ? qianchuanPrevBatchSelect.value : '';
                renderQianchuanTable(currentQianchuanData, batchId, prevBatchId);
            }
        });
    });

    const loadQianchuanBatches = async () => {
        if (!qianchuanBatchSelect || !qianchuanPrevBatchSelect) return;
        try {
            const res = await fetchQianchuanBatches();
            if (res.status === 'success' && res.data && res.data.length > 0) {
                let optionsHtml = '';
                
                res.data.forEach(batch => {
                    const tsMatch = batch.batch_id.match(/qc_(\d+)/);
                    let dateStr = '未知日期';
                    let displayTime = batch.batch_id;
                    if (tsMatch) {
                        const date = new Date(parseInt(tsMatch[1]) * 1000);
                        dateStr = `${date.getFullYear()}/${date.getMonth()+1}/${date.getDate()}`;
                        displayTime = date.toLocaleTimeString();
                    }
                    
                    // 构造展示名称: 原始文件名 + 上传时间
                    const fileName = batch.filename ? batch.filename.replace(/\.[^/.]+$/, "") : "未命名文件";
                    const displayName = `${fileName} (${dateStr} ${displayTime})`;
                    
                    optionsHtml += `<option value="${batch.batch_id}">${displayName}</option>`;
                });

                qianchuanBatchSelect.innerHTML = optionsHtml;
                qianchuanPrevBatchSelect.innerHTML = `<option value="">(无前置对比基准)</option>` + optionsHtml;
            } else {
                qianchuanBatchSelect.innerHTML = '<option value="">暂无数据</option>';
                qianchuanPrevBatchSelect.innerHTML = '<option value="">暂无数据</option>';
            }
            
            // 首次加载批次列表后，主动触发一次查询以初始化两边的数据和状态
            if (qianchuanBatchSelect.value) {
                loadQianchuanDiff();
            }
        } catch (e) {
            console.error("加载批次列表失败", e);
        }
    };

    // 绑定模块切换事件，切换到千川时加载批次
    const qianchuanNavBtn = document.querySelector('button[data-target="qianchuan-analysis"]');
    if (qianchuanNavBtn) {
        qianchuanNavBtn.addEventListener('click', () => {
            loadQianchuanBatches();
        });
    }

    if (qianchuanBatchSelect) {
        qianchuanBatchSelect.addEventListener('change', loadQianchuanDiff);
    }
    
    if (qianchuanPrevBatchSelect) {
        qianchuanPrevBatchSelect.addEventListener('change', loadQianchuanDiff);
    }

    if (searchQianchuanBtn) {
        searchQianchuanBtn.addEventListener('click', loadQianchuanDiff);
    }
    
    if (deleteQianchuanBatchBtn) {
        deleteQianchuanBatchBtn.addEventListener('click', async () => {
            const batchId = qianchuanBatchSelect ? qianchuanBatchSelect.value : '';
            if (!batchId) return;

            if (!confirm(`确定要彻底删除该批次数据吗？该操作不可恢复！`)) {
                return;
            }

            toggleLoading(true);
            try {
                const res = await deleteQianchuanBatch(batchId);
                if (res.status === 'success') {
                    alert('批次删除成功');
                    await loadQianchuanBatches(); // 重新加载批次下拉框
                    await loadQianchuanDiff(); // 刷新表格
                } else {
                    alert('删除失败: ' + res.msg);
                }
            } catch (err) {
                console.error(err);
                alert('删除发生错误');
            } finally {
                toggleLoading(false);
            }
        });
    }

    if (qianchuanSearchInput) {
        qianchuanSearchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                loadQianchuanDiff();
            }
        });
    }

    if (uploadQianchuanBtn) {
        uploadQianchuanBtn.addEventListener('click', async () => {
            if (!qianchuanFileInput.files || qianchuanFileInput.files.length === 0) {
                alert('请先选择文件！');
                return;
            }
            
            toggleLoading(true);
            try {
                const res = await uploadQianchuanData(qianchuanFileInput.files[0]);
                if (res.status === 'success') {
                    alert('千川数据上传成功');
                    qianchuanFileInput.value = ''; // 清空文件选择
                    await loadQianchuanBatches(); // 重新加载批次下拉框
                    if (qianchuanBatchSelect) {
                        qianchuanBatchSelect.value = res.batch_id; // 选中刚刚上传的新批次
                    }
                    await loadQianchuanDiff(); // 刷新表格
                } else {
                    alert('上传失败: ' + res.msg);
                }
            } catch (err) {
                console.error(err);
                alert('上传发生错误');
            } finally {
                toggleLoading(false);
            }
        });
    }

    // --- 素材采集中心 ---
    const refreshCrawlerBtn = document.getElementById('refreshCrawlerBtn');
    const syncFeishuBtn = document.getElementById('syncFeishuBtn');

    if (refreshCrawlerBtn) {
        refreshCrawlerBtn.addEventListener('click', loadCrawlerData);
    }
    
    async function loadCrawlerData() {
        const container = document.getElementById('crawlerTableContainer');
        if (!container) return;
        
        container.innerHTML = '<div style="padding: 40px; text-align: center; color: var(--secondary);">正在加载数据...</div>';
        try {
            const res = await fetch('/api/v1/video/list').then(r => r.json());
            if (res && res.code === 200) {
                renderCrawlerTable(res.data);
                bindCrawlerTableEvents();
            } else {
                container.innerHTML = `<div style="padding: 40px; text-align: center; color: red;">加载失败: ${res.message}</div>`;
            }
        } catch (e) {
            console.error(e);
            container.innerHTML = `<div style="padding: 40px; text-align: center; color: red;">网络或服务异常</div>`;
        }
    }

    function bindCrawlerTableEvents() {
        const selectAll = document.getElementById('selectAllCrawler');
        if (selectAll) {
            selectAll.addEventListener('change', (e) => {
                const checkboxes = document.querySelectorAll('.crawler-checkbox:not([disabled])');
                checkboxes.forEach(cb => cb.checked = e.target.checked);
            });
        }
    }

    if (syncFeishuBtn) {
        syncFeishuBtn.addEventListener('click', async () => {
            const checkboxes = document.querySelectorAll('.crawler-checkbox:checked');
            const ids = Array.from(checkboxes).map(cb => parseInt(cb.value));
            
            if (ids.length === 0) {
                alert('请先勾选需要同步的数据（已同步的数据不可重复勾选）');
                return;
            }

            if (!confirm(`确定要将这 ${ids.length} 条数据同步到飞书多维表格吗？`)) {
                return;
            }

            toggleLoading(true);
            try {
                const res = await fetch('/api/v1/video/sync_feishu', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ids: ids })
                }).then(r => r.json());

                if (res && res.code === 200) {
                    alert(`同步成功！成功: ${res.data.success_count} 条, 失败: ${res.data.fail_count} 条`);
                    loadCrawlerData(); // 刷新表格状态
                } else {
                    alert(`同步失败: ${res.message}`);
                }
            } catch (e) {
                console.error(e);
                alert('请求异常，请检查网络或后端日志');
            } finally {
                toggleLoading(false);
            }
        });
    }

    // 当切换到素材中心时，自动加载数据
    navButtons.forEach(btn => {
        btn.addEventListener('click', () => {
            const targetId = btn.getAttribute('data-target');
            if (targetId === 'media-crawler') {
                loadCrawlerData();
            }
        });
    });

    async function loadBatches() {
        batchesTableBody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding: 20px;">加载中...</td></tr>';
        try {
            const res = await fetch('/api/batches').then(r => r.json());
            if (res.status === 'success') {
                const data = res.data || [];
                if (data.length === 0) {
                    batchesTableBody.innerHTML = '<tr><td colspan="4" style="text-align:center; padding: 20px; color: var(--secondary);">暂无上传记录</td></tr>';
                    return;
                }
                
                batchesTableBody.innerHTML = data.map(b => `
                    <tr style="border-bottom: 1px solid var(--border);">
                        <td style="padding: 8px;"><input type="checkbox" class="batch-checkbox" value="${b.batch_id}"></td>
                        <td style="padding: 8px; color: var(--secondary); font-family: monospace;">${b.batch_id}</td>
                        <td style="padding: 8px;"><b>${b.shop_name}</b></td>
                        <td style="padding: 8px;">覆盖 ${b.date_count} 天数据</td>
                    </tr>
                `).join('');
                selectAllBatches.checked = false;
            } else {
                batchesTableBody.innerHTML = `<tr><td colspan="4" style="text-align:center; padding: 20px; color: red;">获取失败: ${res.msg}</td></tr>`;
            }
        } catch (err) {
            batchesTableBody.innerHTML = `<tr><td colspan="4" style="text-align:center; padding: 20px; color: red;">网络错误</td></tr>`;
        }
    }
}

async function loadAllData() {
    try {
        // 并行加载全局数据和昨日数据
        const dateStr = yesterdayDateInput ? yesterdayDateInput.value : '';
        const [globalRes, yesterdayRes] = await Promise.all([
            fetchGlobalData(),
            fetchYesterdayData(dateStr)
        ]);

        if (globalRes.status === 'success' && globalRes.data) {
            currentGlobalData = globalRes.data;
            renderGlobalTable(currentGlobalData, globalRes.snapshots);
        }

        if (yesterdayRes.status === 'success' && yesterdayRes.data) {
            renderYesterdayTable(yesterdayRes.data, dateStr);
        }
    } catch (e) {
        console.error("加载数据失败", e);
    }
}

// --- 2. 核心逻辑与 API 交互 ---
async function handleUpload() {
    const platformInput = document.querySelector('input[name="platform"]:checked');
    if (!platformInput) {
        alert('请选择所属平台');
        return;
    }

    const shopInput = document.getElementById('shopSelect');
    if (!shopInput.value) {
        alert('请选择所属店铺');
        return;
    }

    const file = fileInput.files[0];
    if (!file) {
        alert('请选择要上传的文件');
        return;
    }

    toggleLoading(true);
    try {
        const res = await uploadFileData(file, platformInput.value, shopInput.value);
        if (res.status === 'success') {
            // Re-fetch global data and show global view instead of single dashboard
            await loadAllData();
        } else {
            alert('解析失败: ' + res.msg);
        }
    } catch (e) {
        console.error(e);
        alert('上传发生错误');
    } finally {
        toggleLoading(false);
        fileInput.value = '';
    }
}

export function exportToExcelWithStyles(containerId = 'globalTableContainer', sheetName = '全盘汇总表', fileNamePrefix = '电商全局数据汇总') {
    const container = document.getElementById(containerId);
    if (!container || container.innerHTML.trim() === '') {
        return alert("没有可导出的数据！");
    }

    // Wrap the inner HTML in a proper HTML shell for Excel
    const template = `
        <html xmlns:o="urn:schemas-microsoft-com:office:office" 
              xmlns:x="urn:schemas-microsoft-com:office:excel" 
              xmlns="http://www.w3.org/TR/REC-html40">
        <head>
            <meta charset="UTF-8">
            <!--[if gte mso 9]>
            <xml>
                <x:ExcelWorkbook>
                    <x:ExcelWorksheets>
                        <x:ExcelWorksheet>
                            <x:Name>全盘汇总表</x:Name>
                            <x:WorksheetOptions>
                                <x:DisplayGridlines/>
                            </x:WorksheetOptions>
                        </x:ExcelWorksheet>
                    </x:ExcelWorksheets>
                </x:ExcelWorkbook>
            </xml>
            <![endif]-->
            <style>
                table { border-collapse: collapse; width: 100%; table-layout: fixed; }
                th, td { 
                    border: 1px solid #000; 
                    padding: 16px 24px !important; 
                    text-align: center; 
                    height: 40px;
                    white-space: nowrap;
                }
                /* Some basic resets to ensure Excel looks clean */
                .shop-excel-table { margin-bottom: 30px; }
            </style>
        </head>
        <body>
            ${container.innerHTML}
        </body>
        </html>
    `;

    // Create a Blob with the HTML content, prepending BOM for UTF-8 support in Excel
    const blob = new Blob(['\uFEFF' + template], { type: 'application/vnd.ms-excel;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    
    // Create a temporary link to trigger the download
    const link = document.createElement('a');
    link.href = url;
    link.download = `${fileNamePrefix}_${new Date().toISOString().slice(0, 10)}.xls`;
    
    // Append to body, click, and remove
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Clean up
    setTimeout(() => URL.revokeObjectURL(url), 100);
}

// --- 3. 初始化应用 ---
document.addEventListener('DOMContentLoaded', () => {
    bindEvents();
    // 默认加载全部数据（全局和昨日）
    toggleLoading(true);
    loadAllData().finally(() => {
        toggleLoading(false);
    });
});
