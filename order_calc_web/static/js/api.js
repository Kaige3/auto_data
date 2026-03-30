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
