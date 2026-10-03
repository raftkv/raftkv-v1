const fs = require('fs');
const path = require('path');
const f = path.join('./dashboard.html');
let html = fs.readFileSync(f, 'utf8');

// ============ 1. CSS: insert before </style> ============
const storeCSS = `
/* === Store Module === */
.store-kpi-grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(90px,1fr)); gap:4px; margin-bottom:8px; }
.store-kpi-card { background:linear-gradient(135deg,rgba(0,6,16,.7),rgba(0,2,6,.7)); border:1px solid rgba(255,215,64,.06); border-radius:4px; padding:6px 8px; text-align:center; position:relative; overflow:hidden; transition:border-color .3s,box-shadow .3s; }
.store-kpi-card:hover { border-color:rgba(255,215,64,.35); box-shadow:0 0 20px rgba(255,215,64,.15); }
.store-kpi-card .sk-val { font-family:'Orbitron',monospace; font-size:1.3rem; font-weight:900; line-height:1.1; }
.store-kpi-card .sk-lbl { font-size:.42rem; color:var(--dm); font-weight:600; letter-spacing:.4px; margin-top:1px; }
.store-list { list-style:none; flex:1; overflow-y:auto; }
.store-list::-webkit-scrollbar { width:2px; }
.store-list::-webkit-scrollbar-thumb { background:rgba(255,215,64,.06); border-radius:2px; }
.store-row { display:flex; align-items:center; gap:6px; padding:5px 8px; border-bottom:1px solid rgba(255,255,255,.02); font-size:.56rem; transition:background .2s; cursor:pointer; }
.store-row:hover { background:rgba(255,215,64,.03); }
.store-status-dot { width:7px; height:7px; border-radius:50%; flex-shrink:0; }
.store-status-dot.normal { background:var(--gr); box-shadow:0 0 6px var(--gr); }
.store-status-dot.warning { background:var(--am); box-shadow:0 0 6px var(--am); }
.store-status-dot.offline { background:var(--dr); box-shadow:0 0 6px var(--dr); animation:blink 1.2s infinite; }
.store-status-dot.recovered { background:var(--cy); box-shadow:0 0 6px var(--cy); }
.store-name { flex:1; color:var(--tx); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.store-metric { font-family:'Orbitron',monospace; font-size:.52rem; color:var(--gd); min-width:55px; text-align:right; }
.store-link-badge { font-size:.40rem; padding:1px 5px; border-radius:8px; font-weight:600; }
.store-link-badge.online { background:rgba(0,230,118,.08); color:var(--gr); border:1px solid rgba(0,230,118,.2); }
.store-link-badge.offline { background:rgba(255,45,85,.1); color:var(--dr); border:1px solid rgba(255,45,85,.25); animation:blink 1.2s infinite; }
.store-link-badge.recovered { background:rgba(0,229,255,.08); color:var(--cy); border:1px solid rgba(0,229,255,.2); }
.store-alert-item { display:flex; align-items:flex-start; gap:6px; padding:4px 6px; border-bottom:1px solid rgba(255,255,255,.02); font-size:.54rem; }
.store-alert-item:hover { background:rgba(255,215,64,.03); }
.alert-level { padding:1px 5px; border-radius:3px; font-size:.40rem; font-weight:700; flex-shrink:0; }
.alert-level.critical { background:rgba(255,45,85,.1); color:var(--dr); border:1px solid rgba(255,45,85,.2); }
.alert-level.warn { background:rgba(255,171,64,.08); color:var(--am); border:1px solid rgba(255,171,64,.15); }
.alert-level.info { background:rgba(0,229,255,.08); color:var(--cy); border:1px solid rgba(0,229,255,.15); }
.alert-time { font-family:'JetBrains Mono',monospace; font-size:.46rem; color:var(--dm); min-width:50px; flex-shrink:0; }
.alert-msg { flex:1; color:var(--tx); line-height:1.3; }
`;

if (!html.includes('store-kpi-grid')) {
    html = html.replace('</style>', storeCSS + '</style>');
    console.log('1. Store CSS inserted');
} else {
    console.log('1. Store CSS already exists');
}

// ============ 2. HTML: insert store panels before </div><!-- /main --> ============
const storeHTML = `
    <!-- ============ PANEL 7 — 右-中：连锁门店总览 ============ -->
    <section class="panel panel-r p5" style="grid-column:3;grid-row:1;">
      <div class="p-hd">
        <span class="icon" style="background:linear-gradient(135deg,var(--gd),var(--am));box-shadow:0 0 10px var(--gd);">&#x2302;</span>
        连锁门店总览
        <span class="badge" style="color:var(--gd);border-color:rgba(255,215,64,.25);background:rgba(255,215,64,.05);">STORES</span>
      </div>
      <div class="p-bd" style="display:flex;flex-direction:column">
        <div class="store-kpi-grid">
          <div class="store-kpi-card"><div class="sk-val val-gd" id="skTurnover">12.8w</div><div class="sk-lbl">区域今日营业额</div></div>
          <div class="store-kpi-card"><div class="sk-val val-cy" id="skTraffic">1,247</div><div class="sk-lbl">区域今日客流</div></div>
          <div class="store-kpi-card"><div class="sk-val val-gd" id="skAvgOrder">73.2</div><div class="sk-lbl">客单价(元)</div></div>
          <div class="store-kpi-card"><div class="sk-val val-pr" id="skConversion">41%</div><div class="sk-lbl">进店转化率</div></div>
          <div class="store-kpi-card"><div class="sk-val val-dr" id="skOffline">18</div><div class="sk-lbl">断网累计(分钟)</div></div>
          <div class="store-kpi-card"><div class="sk-val val-am" id="skDispute">3</div><div class="sk-lbl">未结争议单</div></div>
          <div class="store-kpi-card"><div class="sk-val val-am" id="skInvDiff">1.8%</div><div class="sk-lbl">盘点差异率</div></div>
        </div>
        <div class="divider"></div>
        <ul class="store-list" id="storeList"></ul>
      </div>
    </section>

    <!-- ============ PANEL 8 — 右-下：门店告警 ============ -->
    <section class="panel panel-r p6" style="grid-column:3;grid-row:2;">
      <div class="p-hd">
        <span class="icon" style="background:linear-gradient(135deg,var(--dr),var(--am));box-shadow:0 0 10px var(--dr);">&#x26A0;</span>
        门店告警与存证
        <span class="badge" style="color:var(--dr);border-color:rgba(255,45,85,.25);background:rgba(255,45,85,.05);">ALERTS</span>
      </div>
      <div class="p-bd" style="display:flex;flex-direction:column">
        <ul class="store-list" id="storeAlertList" style="flex:1;overflow-y:auto;margin-bottom:6px"></ul>
        <div class="divider"></div>
        <div class="stat-row">
          <span class="s-lbl">SM3存证链</span>
          <span class="s-val pr" id="saChainDepth">15,592</span>
          <span class="s-lbl">签名算法</span>
          <span class="s-val gd">SM2</span>
        </div>
        <div class="stat-row">
          <span class="s-lbl">断网门店</span>
          <span class="s-val err" id="saOfflineCount">1</span>
          <span class="s-lbl">RPO</span>
          <span class="s-val gr">0 (零丢失)</span>
        </div>
      </div>
    </section>
`;

// Replace existing p5 and p6 panels with store panels
if (!html.includes('skTurnover')) {
    // Find and replace the existing right-column panels (p5 and p6)
    const p5Start = html.indexOf('<!-- ============ PANEL 5');
    const p6End = html.indexOf('</section>', html.indexOf('<!-- ============ PANEL 6')) + '</section>'.length;
    if (p5Start !== -1 && p6End !== -1) {
        html = html.slice(0, p5Start) + storeHTML + html.slice(p6End);
        console.log('2. Store HTML panels inserted (replaced p5+p6)');
    } else {
        console.log('2. Could not find p5/p6 markers, inserting before /main');
        html = html.replace('</div><!-- /main -->', storeHTML + '</div><!-- /main -->');
    }
} else {
    console.log('2. Store HTML already exists');
}

// ============ 3. JS: insert store module data + rendering ============
const storeJS = `
// =====================================================
// Store Module — 连锁门店赋能模块
// =====================================================
var StoreModule = (function() {
  var mockStores = [
    {store_id:'ST-310104-0001',name:'南京东路旗舰店',status:'normal',turnover_today:18620.50,traffic_today:245,link_status:'online',open_alerts:0},
    {store_id:'ST-310104-0002',name:'淮海中路店',status:'normal',turnover_today:12480.00,traffic_today:178,link_status:'online',open_alerts:0},
    {store_id:'ST-310104-0003',name:'徐家汇店',status:'warning',turnover_today:8640.50,traffic_today:132,link_status:'online',open_alerts:2},
    {store_id:'ST-310104-0004',name:'浦东陆家嘴店',status:'normal',turnover_today:15320.80,traffic_today:201,link_status:'online',open_alerts:0},
    {store_id:'ST-310104-0005',name:'静安寺店',status:'offline',turnover_today:3240.20,traffic_today:48,link_status:'offline',open_alerts:1},
    {store_id:'ST-310104-0006',name:'五角场店',status:'normal',turnover_today:9870.30,traffic_today:156,link_status:'online',open_alerts:0},
    {store_id:'ST-310104-0007',name:'中山公园店',status:'recovered',turnover_today:7650.00,traffic_today:112,link_status:'recovered',open_alerts:1},
    {store_id:'ST-310104-0008',name:'虹桥店',status:'normal',turnover_today:11200.60,traffic_today:167,link_status:'online',open_alerts:0}
  ];

  var mockAlerts = [
    {alert_id:'AL-0005-001',store_id:'ST-310104-0005',level:'critical',type:'offline',msg:'门店本地网络中断，已切换边缘节点独立运行',raised_ts:'2026-07-07T09:48:11+08:00'},
    {alert_id:'AL-0003-001',store_id:'ST-310104-0003',level:'warn',type:'inventory_diff',msg:'盘点差异率2.3%超出阈值，SM3存证已固化',raised_ts:'2026-07-07T10:15:30+08:00'},
    {alert_id:'AL-0003-002',store_id:'ST-310104-0003',level:'warn',type:'sign_fail',msg:'班次交接签名校验异常，已触发二次确认',raised_ts:'2026-07-07T11:02:44+08:00'},
    {alert_id:'AL-0007-001',store_id:'ST-310104-0007',level:'info',type:'offline',msg:'网络已恢复，积压数据正在批量补传(backlog)',raised_ts:'2026-07-07T10:30:00+08:00'}
  ];

  var mockKPI = {
    total_turnover_today: 128022.90,
    total_traffic: 1247,
    avg_order_value: 73.23,
    conversion_rate: 0.41,
    offline_minutes_today: 18,
    dispute_tickets_open: 3,
    inventory_diff_rate: 0.018
  };

  function renderStores() {
    var list = document.getElementById('storeList');
    if (!list) return;
    var h = '';
    for (var i = 0; i < mockStores.length; i++) {
      var s = mockStores[i];
      h += '<li class="store-row" title="' + s.store_id + ' | ' + s.name + ' | ' + s.status + '">' +
           '<span class="store-status-dot ' + s.status + '"></span>' +
           '<span class="store-name">' + s.name + '</span>' +
           '<span class="store-metric">' + (s.turnover_today/10000).toFixed(1) + 'w</span>' +
           '<span class="store-link-badge ' + s.link_status + '">' + s.link_status + '</span>' +
           '</li>';
    }
    list.innerHTML = h;
  }

  function renderAlerts() {
    var list = document.getElementById('storeAlertList');
    if (!list) return;
    var h = '';
    for (var i = 0; i < mockAlerts.length; i++) {
      var a = mockAlerts[i];
      var ts = a.raised_ts ? a.raised_ts.substring(11,16) : '--:--';
      h += '<li class="store-alert-item">' +
           '<span class="alert-level ' + a.level + '">' + a.level + '</span>' +
           '<span class="alert-time">' + ts + '</span>' +
           '<span class="alert-msg">' + a.msg + '</span>' +
           '</li>';
    }
    list.innerHTML = h;
  }

  function updateKPI() {
    var k = mockKPI;
    k.total_turnover_today += (Math.random()-0.4)*2000;
    k.total_traffic += Math.floor((Math.random()-0.4)*20);
    k.avg_order_value = k.total_turnover_today / Math.max(1,k.total_traffic);
    k.conversion_rate = Math.max(0.2,Math.min(0.6,k.conversion_rate+(Math.random()-0.5)*0.02));
    k.offline_minutes_today += Math.random() > 0.9 ? 1 : 0;
    k.inventory_diff_rate = Math.max(0.005,Math.min(0.04,k.inventory_diff_rate+(Math.random()-0.5)*0.002));

    var setT = function(id,v){var e=document.getElementById(id);if(e)e.textContent=v;};
    setT('skTurnover',(k.total_turnover_today/10000).toFixed(1)+'w');
    setT('skTraffic',k.total_traffic.toLocaleString());
    setT('skAvgOrder',k.avg_order_value.toFixed(1));
    setT('skConversion',(k.conversion_rate*100).toFixed(0)+'%');
    setT('skOffline',k.offline_minutes_today);
    setT('skDispute',k.dispute_tickets_open);
    setT('skInvDiff',(k.inventory_diff_rate*100).toFixed(1)+'%');

    var offlineCount = mockStores.filter(function(s){return s.link_status==='offline';}).length;
    setT('saOfflineCount',offlineCount);
    setT('saChainDepth',(15592+Math.floor(Math.random()*5)).toLocaleString());
  }

  function tick() { updateKPI(); renderStores(); renderAlerts(); }

  return { tick: tick, init: tick };
})();

if (typeof StoreModule !== 'undefined') { StoreModule.init(); setInterval(function(){ StoreModule.tick(); }, 2000); }
`;

if (!html.includes('StoreModule')) {
    // Insert before the closing </script> tag
    var scriptCloseIdx = html.lastIndexOf('</script>');
    if (scriptCloseIdx !== -1) {
        html = html.slice(0, scriptCloseIdx) + storeJS + '\n' + html.slice(scriptCloseIdx);
        console.log('3. Store JS module inserted');
    }
} else {
    console.log('3. Store JS already exists');
}

fs.writeFileSync(f, html, 'utf8');
console.log('\nAll store module changes applied!');