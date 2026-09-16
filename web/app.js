/* Browser measures its own route only. No local-agent credentials or subscription storage. */
'use strict';
const $ = id => document.getElementById(id), SQ = globalThis.StreamQuality;
let controller, results = [];
if (['127.0.0.1', 'localhost'].includes(location.hostname)) $('endpoint').value = location.origin;
$('stop').onclick = () => controller?.abort();
$('start').onclick = async () => {
  if (controller) return;
  controller = new AbortController(); results = [];
  $('start').disabled = true; $('stop').disabled = false; $('export').disabled = true;
  $('endpoint').disabled = true; $('rounds').disabled = true; $('progress').hidden = false; $('results').hidden = true;
  const count = Number($('rounds').value), base = $('endpoint').value.trim();
  try {
    for (let i = 0; i < count; i++) {
      const result = await SQ.browserProbe(base, { signal: controller.signal,
        onProgress: phase => { $('status').textContent = `第 ${i + 1}/${count} 轮 · ${phase === 'stream' ? '完整 SSE 采样 20 秒（另需连接收尾）' : '连接测试源'}`; } });
      results.push(result); if (!SQ.isResult(result)) throw Error(result.error || '测量不完整');
    }
    const same = new Set(results.map(r => r.profileKey)).size === 1;
    if (!same) throw Error('边缘位置发生变化，保留单次数据但不合并排名');
    const median = fn => SQ.percentile(results.map(fn), .5), max = fn => Math.max(...results.map(fn));
    const pass = results.every(r => r.stream.flowPass), f = n => Number(n).toFixed(1);
    const cards = [['流式承载', pass ? '达标' : '有波动'], ['完整样本', `${results[0].stream.receivedSamples} 段/轮`],
      ['首段等待', `${f(median(r => r.stream.firstSampleMs))} ms`], ['额外停顿（最差）', `${f(max(r => r.stream.maxExtraGapMs))} ms`],
      ['延迟波动 P95−P5', `${f(median(r => r.stream.jitterMs))} ms`], ['完整成功', `${results.length}/${count} 次`]];
    $('cards').replaceChildren(...cards.map(([name, value]) => { const card = document.createElement('div'); card.className = 'card';
      const label = document.createElement('span'), metric = document.createElement('strong'); label.textContent = name; metric.textContent = value; card.append(label, metric); return card; }));
    $('details').textContent = `${results[0].endpoint} · ${results[0].location} · ${count === 3 ? '三轮完整复测' : '仅单次初筛'}。仅测 SSE 网络质量，不下载测速，不等于真实模型速度。`;
    $('results').hidden = false; $('status').textContent = 'SSE 测量完成；未调用模型、未测下载带宽。';
  } catch (error) { $('status').textContent = controller.signal.aborted ? '已停止，请求已取消；未完成轮次不产生合格成绩。' : `未完成：${error.message}。不据此认定节点不可用。`; }
  finally { controller = null; $('start').disabled = false; $('stop').disabled = true; $('endpoint').disabled = false; $('rounds').disabled = false; $('progress').hidden = true; $('export').disabled = !results.length; }
};
$('export').onclick = () => {
  const blob = new Blob([JSON.stringify({ execution: 'this browser', profile: SQ.PROFILE, results }, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob), link = document.createElement('a'); link.href = url; link.download = 'stream-quality-results.json'; link.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
};
