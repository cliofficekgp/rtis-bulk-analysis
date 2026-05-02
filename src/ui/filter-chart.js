import { calcBrakingDist } from '../utils/helpers.js';

let _fc = null;  // { allPts, sntMs, row, resultIdx }
let _brakingDistResult = null; // { distM, violIdx, stopIdx, stoppedAtZero }
let _trackLayer = null, _startMk = null, _endMk = null;

let _leafletMap = null;
let _speedChartInstance = null;
let _getSpeedLimit = () => 63;
let _analysisResults = [];
let _perSecondData = [];

export function setupFilterChartContext(mapInstance, chartInstance, getSpeedLimitFn, results, perSecData) {
    _leafletMap = mapInstance;
    _speedChartInstance = chartInstance;
    if (getSpeedLimitFn) _getSpeedLimit = getSpeedLimitFn;
    if (results) _analysisResults = results;
    if (perSecData) _perSecondData = perSecData;
}

export function getSpeedChartInstance() {
    return _speedChartInstance;
}

export function showViolPanel(row, speedAtSignal, speedLimit, matchQuality, diffSecStr, distToFsdM, nearestFsdSig, distToRtisM) {
    var p = document.getElementById('violPanel');
    var isV = (row.resultClass === 'violation' || row.resultClass === 'violation-multi');
    var isAmbigOverspeed = (row.resultClass === 'ambiguous' && speedAtSignal > speedLimit);
    var showAsViol = isV || isAmbigOverspeed;
    p.className = showAsViol ? '' : 'complied-panel';
    document.getElementById('vpTitle').innerHTML =
        (showAsViol ? '🚨 ' : '✅ ') + (showAsViol ? 'VIOLATION' : row.result) +
        ' <span class="vp-drag-hint">✥ drag</span>';
    var sc  = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
    var mLine = matchQuality==='exact'
        ? '<span style="color:#16a34a;font-size:0.72rem">✓ Exact match (Δ'+diffSecStr+')</span>'
        : matchQuality==='closest'
        ? '<span style="color:#b45309;font-size:0.72rem">⚠ Closest (Δ'+diffSecStr+')</span>'
        : '<span style="color:#9ca3af;font-size:0.72rem">RTIS ping — no per-sec</span>';
    var fLine = distToFsdM!=null ? '<br>📍 FSD: <b>'+distToFsdM.toFixed(0)+' m</b>'+(nearestFsdSig?' ('+nearestFsdSig.dir+' S'+(nearestFsdSig.sigNo||'?')+')':'') : '';
    var rLine = (distToRtisM!=null && distToRtisM>1) ? '<br>🚂 RTIS H: <b>'+distToRtisM.toFixed(0)+' m</b>' : '';
    
    var trendHtml = '';
    if (row.perSecMatch && row.perSecMatch.point) {
        var _ridx = _analysisResults.indexOf(row);
        var _pts = _ridx >= 0 && _perSecondData[_ridx] ? _perSecondData[_ridx] : null;
        if (_pts && _pts.length) {
            var _sntT = row.perSecMatch.point.time.getTime();
            var _mi = 0, _md = Infinity;
            _pts.forEach(function(p,i){ var d=Math.abs(p.time.getTime()-_sntT); if(d<_md){_md=d;_mi=i;} });
            var _trend = [], _limit = _getSpeedLimit()||63;
            for (var _t = _mi-5; _t <= _mi+5; _t++) {
                if (_t < 0 || _t >= _pts.length) { _trend.push('<span style="color:#d1d5db">—</span>'); continue; }
                var _sp = _pts[_t].speed;
                var _col = isNaN(_sp) ? '#9ca3af' : (_sp > _limit ? '#dc2626' : '#16a34a');
                var _bold = _t === _mi ? 'font-weight:900;text-decoration:underline;font-size:1rem;' : '';
                _trend.push('<span style="color:'+_col+';'+_bold+'">'+(_sp||'?')+'</span>');
            }
            trendHtml = '<br><span style="font-size:0.72rem;color:#6b7280;">±5s trend:</span><br>' +
                '<span style="font-size:0.8rem;letter-spacing:0.5px;">' + _trend.join(' → ') + '</span>' +
                '<span style="font-size:0.7rem;color:#9ca3af;"> km/h</span>';
        }
    }
    document.getElementById('vpBody').innerHTML =
        'Train: <b>'+row.trainNo+'</b><br>' +
        'Signal: <b>'+row.sigNo+'</b> @ '+row.signalTime+'<br>' +
        'Speed @ SNT: <b style="color:'+sc+'">'+speedAtSignal+' km/h</b><br>' +
        'RTIS Speed: '+row.speed+' km/h<br>' +
        mLine + fLine + rLine + trendHtml;
    p.style.display = 'block';
    p.style.right = '14px'; p.style.top = '60px'; p.style.left = 'auto';
}

export function msToTimeStr(ms){ return new Date(ms).toTimeString().slice(0,8); }
export function timeToMs(tStr, refDate){
    var p=tStr.split(':').map(Number), d=new Date(refDate);
    d.setHours(p[0]||0, p[1]||0, p[2]||0, 0); return d.getTime();
}

export function buildStnOptions(sel, pts, nearMs) {
    var seen=[], opts=[];
    pts.forEach(function(p){
        if(p.stn && p.stn!=='—' && seen.indexOf(p.stn)<0){ seen.push(p.stn); opts.push({s:p.stn,ms:p.time.getTime()}); }
    });
    sel.innerHTML='';
    opts.forEach(function(o){
        var el=document.createElement('option');
        el.value=o.ms; el.textContent=o.s;
        if(nearMs!=null && Math.abs(o.ms-nearMs)<60000) el.selected=true;
        sel.appendChild(el);
    });
}

export function initFilterUI(pts, sntMs, resultIdx, row) {
    _fc = { allPts:pts, sntMs:sntMs, resultIdx:resultIdx, row:row };
    var f=pts[0].time.getTime(), t=pts[pts.length-1].time.getTime();
    var ms=document.getElementById('mapFilterStrip');
    ms.style.display='flex';
    document.getElementById('mfFromTime').value=msToTimeStr(f);
    document.getElementById('mfToTime').value=msToTimeStr(t);
    buildStnOptions(document.getElementById('mfFromStn'),pts,f);
    buildStnOptions(document.getElementById('mfToStn'),pts,t);
    document.getElementById('mfPointCount').textContent='';
    var gs=document.getElementById('gfStrip');
    gs.style.display='flex';
    document.getElementById('gfFromTime').value=msToTimeStr(f);
    document.getElementById('gfToTime').value=msToTimeStr(t);
    buildStnOptions(document.getElementById('gfFromStn'),pts,f);
    buildStnOptions(document.getElementById('gfToStn'),pts,t);
    document.getElementById('gfPointCount').textContent='';
}

export function getFiltered(pref) {
    if(!_fc) return [];
    var fMs=timeToMs(document.getElementById(pref+'FromTime').value, _fc.allPts[0].time);
    var tMs=timeToMs(document.getElementById(pref+'ToTime').value,   _fc.allPts[0].time);
    return _fc.allPts.filter(function(p){ var m=p.time.getTime(); return m>=fMs&&m<=tMs; });
}

export function mfSyncTimeToStn(side){
    if(!_fc)return;
    var k=side==='from'?'From':'To';
    var tEl=document.getElementById('mf'+k+'Time'), sEl=document.getElementById('mf'+k+'Stn');
    var ms=timeToMs(tEl.value, _fc.allPts[0].time);
    var bi=-1,bd=Infinity;
    for(var i=0;i<sEl.options.length;i++){var d=Math.abs(parseInt(sEl.options[i].value)-ms);if(d<bd){bd=d;bi=i;}}
    if(bi>=0) sEl.selectedIndex=bi;
}

export function mfSyncStnToTime(side){
    if(!_fc)return;
    var k=side==='from'?'From':'To';
    var sEl=document.getElementById('mf'+k+'Stn'), tEl=document.getElementById('mf'+k+'Time');
    if(sEl.value) tEl.value=msToTimeStr(parseInt(sEl.value));
}

export function mfApply(){
    if(!_fc||!_leafletMap)return;
    var pts=getFiltered('mf');
    document.getElementById('mfPointCount').textContent=pts.length+' pts';
    rebuildMapPath(pts);
}

export function mfReset(){
    if(!_fc)return;
    var pts=_fc.allPts;
    document.getElementById('mfFromTime').value=msToTimeStr(pts[0].time.getTime());
    document.getElementById('mfToTime').value=msToTimeStr(pts[pts.length-1].time.getTime());
    buildStnOptions(document.getElementById('mfFromStn'),pts,pts[0].time.getTime());
    buildStnOptions(document.getElementById('mfToStn'),pts,pts[pts.length-1].time.getTime());
    mfApply();
}

export function gfSyncTimeToStn(side){
    if(!_fc)return;
    var k=side==='from'?'From':'To';
    var tEl=document.getElementById('gf'+k+'Time'), sEl=document.getElementById('gf'+k+'Stn');
    var ms=timeToMs(tEl.value, _fc.allPts[0].time);
    var bi=-1,bd=Infinity;
    for(var i=0;i<sEl.options.length;i++){var d=Math.abs(parseInt(sEl.options[i].value)-ms);if(d<bd){bd=d;bi=i;}}
    if(bi>=0) sEl.selectedIndex=bi;
}

export function gfSyncStnToTime(side){
    if(!_fc)return;
    var k=side==='from'?'From':'To';
    var sEl=document.getElementById('gf'+k+'Stn'), tEl=document.getElementById('gf'+k+'Time');
    if(sEl.value) tEl.value=msToTimeStr(parseInt(sEl.value));
}

export function gfApply(){
    if(!_fc)return;
    var pts=getFiltered('gf');
    document.getElementById('gfPointCount').textContent=pts.length+' pts';
    drawChart(pts, _fc.sntMs, _fc.row);
}

export function gfReset(){
    if(!_fc)return;
    var pts=_fc.allPts;
    document.getElementById('gfFromTime').value=msToTimeStr(pts[0].time.getTime());
    document.getElementById('gfToTime').value=msToTimeStr(pts[pts.length-1].time.getTime());
    buildStnOptions(document.getElementById('gfFromStn'),pts,pts[0].time.getTime());
    buildStnOptions(document.getElementById('gfToStn'),pts,pts[pts.length-1].time.getTime());
    gfApply();
}

export function rebuildMapPath(pts) {
    if(!_leafletMap||!pts||pts.length<2)return;
    if(_trackLayer){_leafletMap.removeLayer(_trackLayer);_trackLayer=null;}
    if(_startMk)   {_leafletMap.removeLayer(_startMk);_startMk=null;}
    if(_endMk)     {_leafletMap.removeLayer(_endMk);_endMk=null;}
    var ll=pts.filter(function(p){return p.lat&&p.lon;}).map(function(p){return[p.lat,p.lon];});
    if(ll.length<2)return;
    _trackLayer=L.polyline(ll,{color:'#3b82f6',weight:3,opacity:0.7,dashArray:'7,5'}).addTo(_leafletMap);
    var mkS=L.divIcon({html:'<div style="width:11px;height:11px;border-radius:50%;background:#10b981;border:2px solid #065f46;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>',iconSize:[11,11],iconAnchor:[5,5]});
    var mkE=L.divIcon({html:'<div style="width:11px;height:11px;border-radius:50%;background:#f97316;border:2px solid #9a3412;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>',iconSize:[11,11],iconAnchor:[5,5]});
    _startMk=L.marker(ll[0],{icon:mkS,zIndexOffset:200}).bindTooltip('▶ Start').addTo(_leafletMap);
    _endMk  =L.marker(ll[ll.length-1],{icon:mkE,zIndexOffset:200}).bindTooltip('■ End').addTo(_leafletMap);
    _leafletMap.fitBounds(_trackLayer.getBounds(),{padding:[40,40]});
}

export function drawChart(pts, sntMs, row) {
    var speedLimit = _getSpeedLimit();
    var labels=[], speeds=[], violIdx=-1, bd=Infinity;
    pts.forEach(function(p,i){
        labels.push(p.time.toLocaleTimeString());
        speeds.push(!isNaN(p.speed)?p.speed:null);
        var d=Math.abs(p.time.getTime()-sntMs);
        if(d<bd){bd=d;violIdx=i;}
    });

    _brakingDistResult = calcBrakingDist(pts, sntMs, violIdx);

    var pc=speeds.map(function(s,i){return i===violIdx?'#ef4444':(s!==null&&s>speedLimit?'#f97316':'#3b82f6');});
    var pr=speeds.map(function(s,i){return i===violIdx?8:3;});
    var ll=speeds.map(function(){return speedLimit;});
    var ctx=document.getElementById('speedChart').getContext('2d');
    if(_speedChartInstance){_speedChartInstance.destroy();_speedChartInstance=null;}
    _speedChartInstance=new window.Chart(ctx,{
        type:'line',
        data:{labels:labels,datasets:[
            {label:'Speed (km/h)',data:speeds,borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,0.08)',
             borderWidth:2,pointBackgroundColor:pc,pointRadius:pr,pointHoverRadius:7,tension:0.3,fill:true,spanGaps:true},
            {label:'Speed Limit ('+speedLimit+' km/h)',data:ll,borderColor:'#ef4444',
             borderWidth:2,borderDash:[8,4],pointRadius:0,fill:false}
        ]},
        options:{responsive:true,maintainAspectRatio:false,animation:{duration:300},
            plugins:{
                legend:{position:'top',labels:{font:{size:11}}},
                tooltip:{callbacks:{afterLabel:function(c){return c.dataIndex===violIdx?'⚠ Signal: '+row.sigNo+' @ '+row.signalTime:'';}}}
            },
            scales:{
                x:{ticks:{maxTicksLimit:12,font:{size:10}},grid:{color:'#f1f5f9'}},
                y:{title:{display:true,text:'Speed (km/h)',font:{size:11}},min:0,
                   suggestedMax:Math.max(speedLimit+20,(Math.max.apply(null,speeds.filter(Boolean))||0)+10),
                   grid:{color:'#f1f5f9'}}
            }
        },
        plugins:[
            {id:'vLine',afterDraw:function(chart){
                if(violIdx<0||violIdx>=chart.data.labels.length)return;
                var meta=chart.getDatasetMeta(0);
                if(!meta.data[violIdx])return;
                var x=meta.data[violIdx].x,c=chart.ctx;
                c.save();c.beginPath();c.moveTo(x,chart.chartArea.top);c.lineTo(x,chart.chartArea.bottom);
                c.strokeStyle='rgba(239,68,68,0.7)';c.lineWidth=2;c.setLineDash([5,3]);c.stroke();
                c.fillStyle='#ef4444';c.font='bold 10px sans-serif';c.fillText('🚨 '+row.sigNo,x+4,chart.chartArea.top+14);
                c.restore();
            }},
            {id:'brakingDist',afterDraw:function(chart){
                if(!_brakingDistResult)return;
                var vi=_brakingDistResult.violIdx, si=_brakingDistResult.stopIdx;
                var meta=chart.getDatasetMeta(0);
                if(!meta.data[vi]||!meta.data[si])return;
                var x1=meta.data[vi].x, x2=meta.data[si].x;
                if(x2<x1+2)x2=x1+2;
                var top=chart.chartArea.top, bot=chart.chartArea.bottom;
                var c=chart.ctx;
                c.save();
                c.fillStyle='rgba(251,146,60,0.13)';
                c.fillRect(x1,top,x2-x1,bot-top);
                c.strokeStyle='rgba(234,88,12,0.65)';c.lineWidth=1.5;c.setLineDash([4,3]);
                c.beginPath();c.moveTo(x2,top);c.lineTo(x2,bot);c.stroke();
                c.setLineDash([]);
                var icon=_brakingDistResult.stoppedAtZero?'🛑 Stop':'→ End';
                var label=icon+': '+_brakingDistResult.distM+' m';
                c.font='bold 11px Arial';
                var tw=c.measureText(label).width+16;
                var midX=(x1+x2)/2, lx=Math.max(x1+2,midX-tw/2);
                if(lx+tw>chart.chartArea.right-4)lx=chart.chartArea.right-tw-6;
                var ly=top+56;
                c.fillStyle='rgba(255,255,255,0.96)';
                c.strokeStyle='rgba(234,88,12,0.8)';c.lineWidth=1.5;
                c.beginPath();
                c.moveTo(lx+5,ly-14);c.lineTo(lx+tw-5,ly-14);
                c.arcTo(lx+tw,ly-14,lx+tw,ly-9,5);c.lineTo(lx+tw,ly+4);
                c.arcTo(lx+tw,ly+9,lx+tw-5,ly+9,5);c.lineTo(lx+5,ly+9);
                c.arcTo(lx,ly+9,lx,ly+4,5);c.lineTo(lx,ly-9);
                c.arcTo(lx,ly-14,lx+5,ly-14,5);c.closePath();
                c.fill();c.stroke();
                c.fillStyle='#c2410c';c.textAlign='left';
                c.fillText(label,lx+8,ly+2);
                c.restore();
            }}
        ]
    });
    var brakingHtml = '';
    if (_brakingDistResult) {
        var _bdIcon  = _brakingDistResult.stoppedAtZero ? '🛑' : '→';
        var _bdLabel = _brakingDistResult.stoppedAtZero ? 'Dist to stop' : 'Dist to window end';
        brakingHtml = '<span style="background:#fff7ed;border:1px solid #fed7aa;border-radius:5px;padding:2px 9px;color:#c2410c;font-weight:700;font-size:0.78rem;">' +
            _bdIcon + ' ' + _bdLabel + ': <b>' + _brakingDistResult.distM + ' m</b></span>';
    }
    document.getElementById('graphInfo').innerHTML =
        '<span>🚂 Train: <b>'+row.trainNo+'</b></span>' +
        '<span>📡 Signal: <b>'+row.sigNo+'</b> @ '+row.signalTime+'</span>' +
        '<span>⚡ RTIS Speed: <b style="color:'+(row.speed>speedLimit?'#dc2626':'#16a34a')+'">'+row.speed+' km/h</b></span>' +
        (row.speedPerSec!=null?'<span>🎯 Per-sec @ SNT: <b style="color:'+(row.speedPerSec>speedLimit?'#dc2626':'#16a34a')+'">'+row.speedPerSec+' km/h</b></span>':'') +
        '<span>🏁 Limit: <b>'+speedLimit+' km/h</b></span>' +
        '<span>📊 Points shown: <b>'+pts.length+'</b></span>' +
        '<span>🔴 Red dot = violation moment</span>' +
        brakingHtml;
}
