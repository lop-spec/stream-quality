/* Pairing is per-tab memory only. No tokens in requests to GitHub, storage, query strings or logs. */
(() => {
  'use strict';
  const el = id => document.getElementById(id);
  let connection, timer, current, version = -1, selected = new Set();
  const fragment = new URLSearchParams(location.hash.slice(1));
  if (fragment.has('token')) { el('pair-token').value = fragment.get('token'); el('controller-url').value = fragment.get('controller') || location.origin; history.replaceState(null, '', location.pathname); }
  async function api(route, body) {
    if (!connection) throw Error('尚未配对');
    const response = await fetch(connection.url + route, { method: body ? 'POST' : 'GET', headers: { authorization: `Bearer ${connection.token}`, ...(body ? { 'content-type': 'application/json' } : {}) },
      body: body ? JSON.stringify(body) : undefined, credentials: 'omit', cache: 'no-store', redirect: 'error', signal: AbortSignal.timeout(10000) });
    const result = await response.json(); if (!response.ok) throw Error(result.error || `HTTP ${response.status}`); return result;
  }
  const status = message => { el('remote-status').textContent = message; };
  function budget() { el('remote-budget').textContent = `选中 ${selected.size}/${current?.catalog.length || 0} 节点 · 下载上限约 ${(selected.size * Number(el('remote-rounds').value) * 8.1).toFixed(1)} MiB；串行执行。达标流式并列，Mbps 不参与流式排名。`; }
  function render(data) {
    current = data; el('remote-panel').hidden = false;
    el('remote-run').disabled = data.state.running; el('remote-stop').disabled = !data.state.running;
    status(`${data.executionHost} 执行 · ${data.catalog.length} 个去重缓存节点 · ${data.state.running ? `${data.state.completed}/${data.state.total} · ${data.state.phase || '准备中'}` : data.state.cancelled ? '实际任务已取消' : data.state.error || '就绪'} · 测试源 ${data.endpoint}`);
    if (version !== data.version) {
      version = data.version;
      el('remote-nodes').replaceChildren(...data.catalog.map(node => {
        const card = document.createElement('div'); card.className = 'remote-node';
        const label = document.createElement('label'), check = document.createElement('input'), name = document.createElement('strong');
        check.type = 'checkbox'; check.checked = selected.has(node.key); check.onchange = () => { if (check.checked) selected.add(node.key); else selected.delete(node.key); budget(); };
        name.textContent = node.label; label.append(check, name);
        const affiliations = document.createElement('small'); affiliations.textContent = node.subscriptions.join(' / ');
        const metric = document.createElement('p'), r = data.history[node.key];
        metric.textContent = r?.ok ? `${r.stream.flowPass ? '流式达标' : '流式有波动'} · ${r.download.mbps.toFixed(1)} Mbps${r.download.shortSample ? '（短样本）' : ''} · ${r.verified ? '3/3 复测' : '单次初筛'} · ${r.location || ''}` : '尚无完整成功成绩';
        const attempt = document.createElement('small'); attempt.textContent = r?.lastAttempt?.status !== 'done' ? r?.lastAttempt?.error || '' : '';
        card.append(label, affiliations, metric, attempt); return card;
      }));
    }
    budget();
  }
  async function poll() {
    clearTimeout(timer);
    try { render(await api('/v1/status')); if (connection) timer = setTimeout(poll, 1200); }
    catch (error) { status(`执行层连接中断：${error.message}。不会静默改测手机网络；远端任务可能仍在执行。`); }
  }
  el('pair').onclick = async () => {
    try {
      if (document.getElementById('stop').disabled === false) throw Error('先停止当前浏览器测速');
      const url = new URL(el('controller-url').value.trim());
      connection = { url: StreamQuality.endpoint(url.href, ['127.0.0.1', 'localhost'].includes(url.hostname)), token: el('pair-token').value.trim() };
      if (!connection.token) throw Error('需要本次配对码');
      const data = await api('/v1/status'); selected = new Set(data.catalog.map(n => n.key)); version = -1; render(data);
      el('endpoint').value = data.endpoint;
      el('pair-token').value = ''; el('pair').disabled = true; el('disconnect').disabled = false; el('start').disabled = true; await poll();
    } catch (error) { connection = null; status(`连接失败：${error.message}。手机不能使用电脑的 127.0.0.1；请使用私人 HTTPS 地址。`); }
  };
  el('disconnect').onclick = async () => { try { await api('/v1/cancel', {}); clearTimeout(timer); connection = null; el('pair').disabled = false; el('disconnect').disabled = true; el('start').disabled = false; el('remote-panel').hidden = true; status('已送达取消请求并断开，配对码未保存。'); } catch (error) { status(`取消未送达：${error.message}；任务可能仍在运行，请在电脑停止。`); } };
  el('remote-run').onclick = async () => { try { await api('/v1/run', { keys: [...selected], rounds: Number(el('remote-rounds').value) }); await poll(); } catch (error) { status(`未启动：${error.message}`); } };
  el('remote-stop').onclick = async () => { try { await api('/v1/cancel', {}); await poll(); } catch (error) { status(`取消未送达：${error.message}`); } };
  el('select-all').onclick = () => { selected = new Set(current.catalog.map(n => n.key)); version = -1; render(current); };
  el('select-none').onclick = () => { selected.clear(); version = -1; render(current); };
  el('remote-rounds').onchange = budget;
})();
