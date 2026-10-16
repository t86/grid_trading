AIR_DROP_PAGE = r'''<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Alpha 空投监控</title><style>
:root{--bg:#f6f3ec;--line:#ded8cb;--ink:#252620;--muted:#716f65;--green:#087b74}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px system-ui,sans-serif}
main{max-width:1480px;margin:28px auto;padding:0 20px}section{background:white;border:1px solid var(--line);border-radius:20px;padding:24px;margin-bottom:18px}
h1{margin:0 0 12px;font-size:32px}p{line-height:1.7;color:var(--muted)}a{color:var(--green)}nav{display:flex;gap:18px;flex-wrap:wrap}
.status{display:flex;gap:12px;flex-wrap:wrap;margin:18px 0}.tag{padding:8px 12px;background:var(--bg);border-radius:10px}.bad{color:#b53627}
input,select{padding:10px;border:1px solid var(--line);border-radius:8px;font:inherit}input{width:260px;max-width:100%}.filters{display:flex;gap:12px;flex-wrap:wrap}
.table{overflow:auto;margin-top:18px}table{border-collapse:collapse;width:100%;min-width:980px}th,td{padding:13px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}th{background:var(--bg);white-space:nowrap}td:first-child{font-weight:700}small{display:block;color:var(--muted);font-weight:400;margin-top:6px}details{max-width:550px}summary{cursor:pointer;color:var(--green)}.text{white-space:pre-wrap;line-height:1.65}
</style></head><body><main><section><h1>Alpha 空投监控</h1>
<p>独立于 X 的官方消息监控。Telegram 公开频道每轮完成后约 30 秒再次检查，官网公告约每 5 分钟补充；网络、防护和发布渠道会影响延迟。所有时间均为北京时间。</p>
<nav><a href="/portal">统一入口</a><a href="/basis">套利与持有观察</a><a href="/market-data">资金费率</a><a href="https://t.me/binance_wallet_announcements" target="_blank" rel="noopener">钱包公告频道</a><a href="https://www.binance.com/en/square/profile/BinanceWallet" target="_blank" rel="noopener">广场官方账号</a></nav>
<div id="status" class="status">正在加载…</div><p>通知包括新预告、领取条件变化和开始时间提醒；历史消息首次导入不会补发过时通知。开始时间已到 ≠ 奖池仍可领取，资格和剩余奖励请到币安 App → Alpha Events 核验。</p>
</section><section><div class="filters"><input id="search" placeholder="搜索币种 / 公告正文" aria-label="搜索空投"><select id="sort" aria-label="排序"><option value="latest">最新公告优先</option><option value="start">开始时间升序</option><option value="points">积分门槛升序</option></select></div>
<p id="summary"></p><div class="table"><table><thead><tr><th>币种 / 阶段</th><th>领取开始</th><th>积分门槛 / 消耗</th><th>数量 / 降分规则</th><th>官方来源 / 时间</th><th>通知与原文</th></tr></thead><tbody id="rows"></tbody></table></div></section></main>
<script>
let data={events:[]};
const $=id=>document.getElementById(id);
const esc=v=>String(v??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const time=v=>v?new Date(v).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'待公布';
const labels={preview:'预告',details:'详细条件',live:'公告称已上线'};
function link(url){try{const u=new URL(url);return u.protocol==='https:'&&['t.me','www.binance.com','x.com'].includes(u.hostname)?esc(u.href):'#';}catch{return '#';}}
function render(){
 let rows=(data.events||[]).filter(e=>(String(e.symbol||'')+' '+e.text).toLowerCase().includes($('search').value.toLowerCase()));
 if($('sort').value==='start')rows.sort((a,b)=>(Date.parse(a.scheduled_at)||Infinity)-(Date.parse(b.scheduled_at)||Infinity));
 if($('sort').value==='points')rows.sort((a,b)=>(a.points_threshold??Infinity)-(b.points_threshold??Infinity));
 $('summary').textContent=`共 ${rows.length} 个空投事件 · 更新时间 ${time(data.checked_at)} · ${data.stale?'监控数据过期或尚未运行':'监控进程近期有检查记录'}`;
 $('rows').innerHTML=rows.length?rows.map(e=>{
  const n=(e.notices||[]).at(-1)||{}, source=(e.sources||[]).map(s=>`<a href="${link(s.url)}" target="_blank" rel="noopener">${esc(s.source)}</a><small>发布 ${esc(time(s.published_at))}<br>首次发现 ${esc(time(s.first_seen_at))}</small>`).join('<br>');
  const notice=n.bark_sent_at?`Bark 已发送 ${time(n.bark_sent_at)}`:n.suppressed?'历史 / 过期，未通知':n.bark_error||'等待通知或 Bark 尚未配置';
  const drop=e.threshold_drop?e.threshold_drop.split('/'):[];
  return `<tr><td>${esc(e.symbol||'待公布')}<small>${esc(labels[e.stage]||e.stage)}</small></td><td>${esc(time(e.scheduled_at))}<small>${e.scheduled_at&&Date.parse(e.scheduled_at)<=Date.now()?'开始时间已过，状态需 App 确认':''}</small></td><td>${esc(e.points_threshold??'待公布')} / ${esc(e.points_cost??'待公布')}</td><td>${esc(e.quantity||'待公布')}<small>${drop.length?`未领完时每 ${esc(drop[1])} 分钟降 ${esc(drop[0])} 分`:''}</small></td><td>${source}</td><td>${esc(notice)}<details><summary>查看公告</summary><p class="text">${esc(e.text)}</p></details></td></tr>`;
 }).join(''):'<tr><td colspan="6">尚无命中的空投公告。请查看上方数据源状态；来源故障不会被显示成“没有空投”。</td></tr>';
}
async function refresh(){try{const r=await fetch('/api/alpha-airdrops',{cache:'no-store'});if(!r.ok)throw Error(`HTTP ${r.status}`);data=await r.json();
 const sources=Object.entries(data.sources||{}).map(([k,s])=>`<span class="tag ${s.ok?'':'bad'}">${esc(k)} ${s.ok?'正常':'采集失败'} · ${esc(time(s.checked_at))}${s.error?`<small>${esc(s.error)}</small>`:''}${s.retry_at?`<small>退避至 ${esc(time(s.retry_at))}</small>`:''}</span>`).join('');
 $('status').innerHTML=`<span class="tag">Bark ${data.bark_configured?'已配置':'未配置'}</span><span class="tag">Telegram：公开页面，无需登录</span>${sources||'<span class="tag bad">监控尚未产生数据</span>'}`;render();
 }catch(e){$('status').textContent=`加载失败：${e.message}；保留上次数据。`;}}
$('search').oninput=render;$('sort').onchange=render;refresh();setInterval(refresh,15000);
</script></body></html>'''
