export async function uploadFileData(file, platform, shop) {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('platform', platform);
    if(shop) {
        formData.append('shop', shop);
    }

    const response = await fetch('/upload', { method: 'POST', body: formData });
    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }
    return await response.json();
}

export async function fetchGlobalData() {
    const response = await fetch(`/api/global_data?t=${Date.now()}`);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return await response.json();
}

export async function fetchYesterdayData(dateStr) {
    const params = new URLSearchParams({ t: Date.now() });
    if (dateStr) {
        params.append('date', dateStr);
    }
    const response = await fetch(`/api/yesterday_data?${params.toString()}`);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return await response.json();
}

export async function uploadQianchuanData(file) {
    const formData = new FormData();
    formData.append('file', file);
    const response = await fetch('/upload_qianchuan', { method: 'POST', body: formData });
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return await response.json();
}

export async function fetchQianchuanDiff(keyword = '', batchId = '', prevBatchId = '') {
    const params = new URLSearchParams({ t: Date.now() });
    if (keyword) {
        params.append('keyword', keyword);
    }
    if (batchId) {
        params.append('batch_id', batchId);
    }
    if (prevBatchId) {
        params.append('prev_batch_id', prevBatchId);
    }
    const response = await fetch(`/api/qianchuan_diff?${params.toString()}`);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return await response.json();
}

export async function fetchQianchuanBatches() {
    const response = await fetch(`/api/qianchuan_batches?t=${Date.now()}`);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return await response.json();
}

export async function deleteQianchuanBatch(batchId) {
    const response = await fetch(`/api/qianchuan_batches/${batchId}`, {
        method: 'DELETE'
    });
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    return await response.json();
}
