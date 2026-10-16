WATCHLIST_PANEL = r"""
<style>
  .wrap {grid-template-columns:minmax(0,1fr)}
  #contract-watch-panel {min-width:0}
  #contract-watch-panel table {min-width:860px}
  #contract-watch-panel label {display:flex;flex-direction:column;gap:6px;font-size:13px;color:var(--muted)}
  #contract-watch-panel input, #contract-watch-panel select {height:38px;border:1px solid var(--line);border-radius:8px;padding:0 10px;background:#fff;font-size:14px;max-width:100%}
  #contract-watch-panel input[type=number] {width:150px}
  #contract-watch-panel #cw-rules button {height:32px;padding:0 10px;font-size:12px;margin:2px}
  #contract-watch-panel #cw-results button {background:var(--brand-soft);color:var(--brand);border:1px solid var(--line);height:34px;font-size:13px}
  #contract-watch-panel [hidden] {display:none!important}
</style>
<section class="card" id="contract-watch-panel">
  <h2 style="margin:0 0 8px">合约持有观察 / 条件监控</h2>
  <p class="msg">可搜索所有在交易的 U 本位、币本位合约，无需现货配对。每 5 分钟检查；资金费为单期费率，涨跌幅为滚动 24h。加入观察代表手动标记持有。</p>
  <p class="msg" id="cw-status" role="status">正在读取监控状态…</p>
  <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:end">
    <label>合约市场<select id="cw-market"><option value="all">全部</option><option value="usdm">U 本位</option><option value="coinm">币本位</option></select></label>
    <label style="flex:1;min-width:220px">输入币种或合约<input id="cw-search" placeholder="例如 BTC、SOL、BTCUSD_PERP" autocomplete="off"></label>
    <button type="button" id="cw-refresh">刷新监控</button>
  </div>
  <p class="msg" id="cw-search-status" role="status"></p>
  <div id="cw-results" style="display:flex;gap:8px;flex-wrap:wrap;max-height:220px;overflow:auto"></div>
  <form id="cw-form" hidden style="margin-top:16px;padding:16px;border:1px solid var(--line);border-radius:12px">
    <h3 id="cw-selected" style="margin:0 0 12px"></h3>
    <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:end">
      <label>监控条件<select id="cw-condition">
        <option value="funding_cross_negative">资金费率正转负</option><option value="funding_below">资金费率低于 (%)</option><option value="funding_above">资金费率高于 (%)</option>
        <option value="change_above">24h 涨跌幅高于 (%)</option><option value="change_below">24h 涨跌幅低于 (%)</option>
        <option value="price_above">最新价高于</option><option value="price_below">最新价低于</option>
      </select></label>
      <label>阈值<input id="cw-threshold" type="number" step="any" value="0" required></label>
      <label>通知方式<select id="cw-channel"><option value="bark">Bark 推送</option><option value="web">仅页面提醒</option><option value="both">页面 + Bark</option></select></label>
      <label>提醒频率<select id="cw-frequency"><option value="daily">每天一次（北京时间）</option><option value="edge">每次重新触发</option><option value="repeat">持续满足时重复提醒</option></select></label>
      <label>重复间隔（分钟）<input id="cw-interval" type="number" min="5" max="10080" value="30" required></label>
      <label>连续满足次数<input id="cw-confirmations" type="number" min="1" max="12" value="1" required></label>
      <button type="submit" id="cw-save">保存监控</button><button type="button" id="cw-cancel">取消</button>
    </div>
    <p class="msg">例：资费为负 → 低于 0%；资费低于 -0.5% → 填 -0.5；涨幅超过 50% → 24h 高于 50%；跌幅超过 20% → 24h 低于 -20%。价格单位为合约报价币种。</p>
    <p class="msg">正转负先建立基线；数值阈值在首次检查已满足时即可提醒。“每次重新触发”须先恢复不满足再触发；连续确认按每 5 分钟采样计数。每天一次按每条规则计；同一合约可设置多条规则。</p>
  </form>
  <p class="msg" id="cw-message" role="status"></p>
  <div class="table-wrap" style="margin-top:12px"><table><thead><tr><th>合约</th><th>条件</th><th>通知 / 频率</th><th>最近检查</th><th>操作</th></tr></thead><tbody id="cw-rules"></tbody></table></div>
  <details style="margin-top:12px"><summary>Bark 通知设置</summary>
    <p class="msg" id="cw-bark-status"></p>
    <form id="cw-bark-form" style="display:flex;gap:10px;flex-wrap:wrap"><input id="cw-bark" type="password" autocomplete="new-password" placeholder="https://api.day.app/推送密钥" style="flex:1;min-width:260px" required><button type="submit">保存 Bark 地址</button></form>
    <p class="msg" id="cw-bark-message">地址保存在服务器，不会回显。留空不会修改原配置；保存不发送测试通知。</p>
  </details>
  <details style="margin-top:12px"><summary>最近提醒记录</summary><div id="cw-events"></div></details>
</section>
<script>
(() => {
  const el = (id) => document.getElementById('cw-' + id);
  const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  let rules = [], contracts = [], selected = null, editId = null, searchTimer, searchSeq = 0;
  const channelNames = {bark:'Bark', web:'页面', both:'页面 + Bark'};
  const frequencyName = (r) => r.frequency === 'daily' ? '每天一次' : r.frequency === 'edge' ? '每次重新触发' : `每 ${r.interval_minutes} 分钟`;
  const date = (v) => v ? new Date(v).toLocaleString('zh-CN', {timeZone:'Asia/Shanghai', hour12:false}) : '等待首次检查';
  async function api(url, body) {
    const response = await fetch(url, body ? {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)} : {});
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || (data.errors || []).join('；') || '请求失败');
    return data;
  }
  async function refresh() {
    try {
      const data = await api('/api/carry-watchlist');
      rules = data.rules || [];
      const stale = data.updated_at && Date.now() - Date.parse(data.updated_at) > 15*60*1000;
      el('status').textContent = `${rules.length} 条监控 · 最近检查 ${date(data.updated_at)}${data.bark_configured ? ' · Bark 已配置' : ' · Bark 未配置（可在下方设置）'}${stale ? ' · 检查已超过15分钟，请留意监控异常' : ''}${data.errors.length ? ' · ' + data.errors.join('；') : ''}`;
      el('bark-status').textContent = data.bark_configured ? 'Bark 已配置。' : 'Bark 未配置：选择 Bark 的规则暂时不能推送；可先选择页面提醒。';
      el('rules').innerHTML = rules.length ? rules.map(r => {
        const s = r.state || {}, unit = r.condition.startsWith('price_') ? '' : '%';
        const observed = s.value == null ? '—' : Number(s.value).toLocaleString('zh-CN', {maximumFractionDigits:6}) + unit;
        return `<tr><td><strong>${esc(r.symbol)}</strong><div class="sub">${r.market === 'usdm' ? 'U 本位' : '币本位'} · ${r.enabled ? '观察中' : '已暂停'}</div></td><td>${esc(r.label)}<div class="sub">连续 ${r.confirmations} 次确认</div></td><td>${esc(channelNames[r.channel])} · ${esc(frequencyName(r))}</td><td>${esc(observed)} · ${esc(s.error || (s.confirmed ? '条件已满足' : s.matched ? '等待连续确认' : '未触发'))}<div class="sub">${esc(date(s.checked_at))}</div>${s.last_notification?.error ? `<div class="sub">${esc(s.last_notification.error)}</div>` : ''}</td><td><button type="button" data-action="edit" data-id="${esc(r.id)}">编辑</button> <button type="button" data-action="toggle" data-id="${esc(r.id)}">${r.enabled ? '暂停' : '恢复'}</button> <button type="button" data-action="delete" data-id="${esc(r.id)}">删除</button></td></tr>`;
      }).join('') : '<tr><td colspan="5">还没有观察规则。输入合约名称，点击搜索结果添加。</td></tr>';
      el('events').innerHTML = data.events.length ? data.events.map(e => `<p>${esc(date(e.time))} · ${esc(e.body)} <span class="sub">${esc(e.channel === 'web' ? '页面已记录' : e.sent ? 'Bark 已发送' : e.error || '未发送')}</span></p>`).join('') : '<p class="msg">暂无提醒</p>';
      window.dispatchEvent(new CustomEvent('carry-watchlist-updated', {detail:data}));
    } catch (error) { el('status').textContent = '监控加载失败：' + error.message; }
  }
  async function search() {
    const seq = ++searchSeq;
    el('search-status').textContent = '正在查找合约…';
    try {
      const data = await api('/api/carry-contracts?q=' + encodeURIComponent(el('search').value.trim()) + '&market=' + el('market').value);
      if (seq !== searchSeq) return;
      contracts = data.rows;
      el('search-status').textContent = `匹配 ${data.total} 个合约${data.errors.length ? ' · ' + data.errors.join('；') : ''}`;
      el('results').innerHTML = contracts.map((c, i) => `<button type="button" data-contract="${i}">${esc(c.symbol)} · ${c.market === 'usdm' ? 'U 本位' : '币本位'}${c.perpetual ? '' : ' · 交割'}</button>`).join('') || '<span>没有匹配的合约</span>';
    } catch (error) { if (seq === searchSeq) { el('results').innerHTML = ''; el('search-status').textContent = error.message; } }
  }
  function syncFields() {
    el('threshold').disabled = el('condition').value === 'funding_cross_negative';
    el('interval').disabled = el('frequency').value !== 'repeat';
  }
  function openEditor(contract, rule = null) {
    selected = contract; editId = rule?.id || null;
    el('form').hidden = false;
    el('selected').textContent = `${rule ? '编辑' : '添加'} ${contract.symbol}（${contract.market === 'usdm' ? 'U 本位' : '币本位'}）`;
    for (const option of el('condition').options) option.disabled = !contract.perpetual && option.value.startsWith('funding_');
    el('condition').value = rule?.condition || (contract.perpetual ? 'funding_cross_negative' : 'change_above');
    el('threshold').value = rule?.threshold ?? (contract.perpetual ? 0 : 50);
    el('channel').value = rule?.channel || 'bark'; el('frequency').value = rule?.frequency || 'daily';
    el('interval').value = rule?.interval_minutes || 30; el('confirmations').value = rule?.confirmations || 1;
    syncFields(); el('form').scrollIntoView({behavior:'smooth', block:'nearest'});
  }
  el('search').addEventListener('input', () => { clearTimeout(searchTimer); ++searchSeq; searchTimer = setTimeout(search, 300); });
  el('market').addEventListener('change', search);
  el('results').addEventListener('click', e => { const b = e.target.closest('[data-contract]'); if (b) openEditor(contracts[Number(b.dataset.contract)]); });
  el('condition').addEventListener('change', syncFields); el('frequency').addEventListener('change', syncFields);
  el('cancel').addEventListener('click', () => { el('form').hidden = true; selected = null; editId = null; });
  el('form').addEventListener('submit', async e => {
    e.preventDefault(); if (!selected) return;
    el('save').disabled = true;
    try {
      await api('/api/carry-watchlist', {action:'save', rule:{id:editId, symbol:selected.symbol, market:selected.market, condition:el('condition').value, threshold:Number(el('threshold').value), channel:el('channel').value, frequency:el('frequency').value, interval_minutes:Number(el('interval').value), confirmations:Number(el('confirmations').value), enabled:editId ? rules.find(r => r.id === editId)?.enabled ?? true : true}});
      el('form').hidden = true; el('message').textContent = '监控已保存，下一轮检查将在约 5 分钟内执行。'; await refresh();
    } catch (error) { el('message').textContent = '保存失败：' + error.message; }
    finally { el('save').disabled = false; }
  });
  el('rules').addEventListener('click', async e => {
    const b = e.target.closest('[data-action]'); if (!b) return;
    const rule = rules.find(r => r.id === b.dataset.id); if (!rule) return;
    b.disabled = true;
    try {
      if (b.dataset.action === 'edit') {
        const data = await api('/api/carry-contracts?q=' + encodeURIComponent(rule.symbol) + '&market=' + rule.market);
        const contract = data.rows.find(c => c.symbol === rule.symbol);
        if (!contract) throw new Error('该合约已不在交易，可暂停或删除监控');
        openEditor(contract, rule);
      } else { await api('/api/carry-watchlist', {action:b.dataset.action, id:rule.id}); await refresh(); }
    } catch (error) { el('message').textContent = error.message; }
    finally { b.disabled = false; }
  });
  el('bark-form').addEventListener('submit', async e => {
    e.preventDefault(); const input = el('bark'), endpoint = input.value;
    try { await api('/api/carry-watchlist', {action:'configure_bark', endpoint}); input.value = ''; el('bark-message').textContent = 'Bark 地址已保存，下次监控自动生效。'; await refresh(); }
    catch (error) { el('bark-message').textContent = error.message; }
  });
  window.carryMonitor = {refresh, add: async symbol => {
    el('search').value = symbol;
    const data = await api('/api/carry-contracts?q=' + encodeURIComponent(symbol) + '&market=usdm');
    const contract = data.rows.find(c => c.symbol === symbol);
    if (!contract) throw new Error('找不到正在交易的合约');
    openEditor(contract);
  }};
  el('refresh').addEventListener('click', () => { refresh(); search(); });
  refresh(); search(); setInterval(refresh, 60000);
})();
</script>
"""
