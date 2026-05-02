import { getCol, parseSmartDate, isValidGPS, getDistance, cleanStationName, calcBrakingDist } from '../utils/helpers.js';
import { matchPerSecToSNT } from '../core/gps-handler.js';

export function reportProgress(label, pct) {
    document.getElementById('reportProgressLabel').textContent = label;
    document.getElementById('reportProgressBar').style.width = pct + '%';
    document.getElementById('reportProgressPct').textContent = pct + '%';
}

// ══════════════════════════════════════════════════════════════
// MAP CAPTURE — Esri REST Export API method
// ══════════════════════════════════════════════════════════════
export function captureMapForRow(row, resultIdx, ctx) {
    return new Promise(function(resolve) {
        var speedLimit   = ctx.getSpeedLimit();
        var hasPersec    = ctx.perSecondData[resultIdx] && ctx.perSecondData[resultIdx].length > 0;
        var cleanStation = row.fsdStation || cleanStationName(row.station.split(' → ')[0]);

        // ── Resolve violation coordinates ─────────────────────────────
        var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
            var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
            var d = new Date(base);
            var tp = (row.signalTime || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
            if (tp) d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0);
            return d;
        })();
        var matchResult = row.perSecMatch || (hasPersec ? matchPerSecToSNT(ctx.perSecondData[resultIdx], sntDateObj) : null);

        var violLat, violLon, speedAtSignal;
        var sntMatchedIdx = 0;
        if (hasPersec && matchResult && matchResult.point && isValidGPS(matchResult.point.lat, matchResult.point.lon)) {
            violLat       = matchResult.point.lat;
            violLon       = matchResult.point.lon;
            speedAtSignal = !isNaN(matchResult.point.speed) ? matchResult.point.speed : row.speed;
            sntMatchedIdx = matchResult.idx || 0;
        } else {
            violLat       = row.violationLat;
            violLon       = row.violationLon;
            speedAtSignal = (row.speedPerSec != null) ? row.speedPerSec : row.speed;
        }
        if (!isValidGPS(violLat, violLon)) {
            resolve(mapFallbackCanvas(row, null, null, speedAtSignal, speedLimit)); return;
        }

        // ── Collect FSD signals — live data or row cache (session restore) ──
        var fsdSignals = [];
        if (ctx.dataFSD.length > 0) {
            ctx.dataFSD.forEach(function(r) {
                var stn = cleanStationName(getCol(r, ['Station','STATION','STN_CODE','Station Name']));
                if (stn !== cleanStation) return;
                var lat = parseFloat(getCol(r, ['Latitude','Lattitude','LAT','Lat','GPS_LAT']));
                var lon = parseFloat(getCol(r, ['Longitude','LON','Lon','GPS_LON']));
                var dir = String(getCol(r, ['DIRN','Direction','DIR']) || '').toUpperCase().trim();
                var sigNo = getCol(r, ['SIGNUMBER','Signal','SIGNAL','Signal No','SIG_ID']);
                if (!isValidGPS(lat, lon)) return;
                fsdSignals.push({ lat:lat, lon:lon, dir:dir, sigNo:sigNo });
            });
        }
        if (fsdSignals.length === 0 && row.cachedFsdSignals && row.cachedFsdSignals.length) {
            fsdSignals = row.cachedFsdSignals.filter(function(s){ return isValidGPS(s.lat, s.lon); });
        }

        // ── Build per-sec track window (same ±40 as buildMap) ─────────
        var trackPts = [], localViolIdx = -1;
        if (hasPersec && ctx.perSecondData[resultIdx]) {
            var allPts = ctx.perSecondData[resultIdx];
            if (!matchResult || matchResult.idx === undefined) {
                var bd = Infinity;
                allPts.forEach(function(p,i){ var d=Math.abs(p.time-sntDateObj); if(d<bd){bd=d;sntMatchedIdx=i;} });
            }
            var s2 = Math.max(0, sntMatchedIdx - 80), e2 = Math.min(allPts.length-1, sntMatchedIdx+80);
            trackPts    = allPts.slice(s2, e2+1);
            localViolIdx = sntMatchedIdx - s2;
        }

        // ── Image canvas size ──────────────────────────────────────────
        var IW = 860, IH = 420;

        function _mercX(lon) { return lon * 20037508.342789244 / 180; }
        function _mercY(lat) {
            var r = lat * Math.PI / 180;
            return Math.log(Math.tan(Math.PI / 4 + r / 2)) * 20037508.342789244 / Math.PI;
        }

        var HALF_Y = 200;
        var HALF_X = HALF_Y * (IW / IH);

        var cMx = _mercX(violLon);
        var cMy = _mercY(violLat);
        var mxMin = cMx - HALF_X, mxMax = cMx + HALF_X;
        var myMin = cMy - HALF_Y, myMax = cMy + HALF_Y;

        function latLonToPixel(lat, lon) {
            var mx = _mercX(lon), my = _mercY(lat);
            var x = Math.round((mx - mxMin) / (mxMax - mxMin) * IW);
            var y = Math.round((myMax - my) / (myMax - myMin) * IH);
            return { x: x, y: y };
        }

        var bbox = mxMin + ',' + myMin + ',' + mxMax + ',' + myMax;
        var esriUrl = 'https://server.arcgisonline.com/arcgis/rest/services/World_Imagery/MapServer/export' +
            '?bbox=' + encodeURIComponent(bbox) +
            '&bboxSR=3857' +
            '&size=' + IW + '%2C' + IH +
            '&imageSR=3857' +
            '&format=png32' +
            '&transparent=false' +
            '&f=image';

        var satImg = new Image();
        satImg.crossOrigin = 'anonymous';

        satImg.onload = function() {
            var canvas = document.createElement('canvas');
            canvas.width = IW; canvas.height = IH;
            var ctx2d = canvas.getContext('2d');

            ctx2d.drawImage(satImg, 0, 0, IW, IH);

            if (trackPts.length > 1) {
                ctx2d.beginPath();
                ctx2d.strokeStyle = 'rgba(59,130,246,0.85)';
                ctx2d.lineWidth = 3;
                ctx2d.setLineDash([8, 5]);
                var first = true;
                trackPts.forEach(function(p) {
                    if (!isValidGPS(p.lat, p.lon)) return;
                    var px = latLonToPixel(p.lat, p.lon);
                    if (first) { ctx2d.moveTo(px.x, px.y); first = false; }
                    else ctx2d.lineTo(px.x, px.y);
                });
                ctx2d.stroke();
                ctx2d.setLineDash([]);
            }

            trackPts.forEach(function(p, i) {
                if (i === localViolIdx || !isValidGPS(p.lat, p.lon)) return;
                var px = latLonToPixel(p.lat, p.lon);
                if (px.x < -10 || px.x > IW+10 || px.y < -10 || px.y > IH+10) return;
                var proximity = 1 - Math.min(1, Math.abs(i - localViolIdx) / 82);
                ctx2d.beginPath();
                ctx2d.arc(px.x, px.y, 4.5, 0, Math.PI*2);
                ctx2d.fillStyle = 'rgba(59,130,246,' + (0.45 + 0.5*proximity) + ')';
                ctx2d.fill();
                ctx2d.strokeStyle = 'rgba(30,64,175,0.9)'; ctx2d.lineWidth = 1.2;
                ctx2d.stroke();
            });

            fsdSignals.forEach(function(sig) {
                var px = latLonToPixel(sig.lat, sig.lon);
                var col = sig.dir === 'UP' ? '#8b5cf6' : (sig.dir === 'DN' ? '#f59e0b' : '#6b7280');
                ctx2d.fillStyle = col;
                ctx2d.strokeStyle = 'white'; ctx2d.lineWidth = 1.5;
                ctx2d.beginPath();
                var s = 9;
                roundRect(ctx2d, px.x-s, px.y-s, s*2, s*2, 3);
                ctx2d.fill(); ctx2d.stroke();
                ctx2d.fillStyle = 'white'; ctx2d.font = 'bold 10px Arial'; ctx2d.textAlign = 'center';
                ctx2d.fillText(sig.dir === 'UP' ? '▲' : (sig.dir === 'DN' ? '▼' : '●'), px.x, px.y+4);
                ctx2d.fillStyle = col; ctx2d.font = 'bold 9px Arial'; ctx2d.textAlign = 'left';
                ctx2d.fillText('S' + (sig.sigNo||'?'), px.x+s+3, px.y+4);
            });

            var vPx = latLonToPixel(violLat, violLon);
            ctx2d.beginPath();
            ctx2d.arc(vPx.x, vPx.y, 22, 0, Math.PI*2);
            ctx2d.fillStyle = 'rgba(239,68,68,0.25)'; ctx2d.fill();
            ctx2d.beginPath();
            ctx2d.arc(vPx.x, vPx.y, 16, 0, Math.PI*2);
            ctx2d.fillStyle = '#ef4444'; ctx2d.fill();
            ctx2d.strokeStyle = '#7f1d1d'; ctx2d.lineWidth = 3; ctx2d.stroke();
            ctx2d.beginPath();
            ctx2d.arc(vPx.x, vPx.y, 5, 0, Math.PI*2);
            ctx2d.fillStyle = 'white'; ctx2d.fill();

            var lx = vPx.x + 22, ly = vPx.y - 22;
            if (lx + 130 > IW) lx = vPx.x - 155;
            if (ly < 30) ly = vPx.y + 30;
            var sc = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
            var labelText = row.sigNo + ' — ' + speedAtSignal + ' km/h';
            ctx2d.font = 'bold 12px Arial';
            var tw = ctx2d.measureText(labelText).width + 16;
            ctx2d.fillStyle = 'rgba(255,255,255,0.96)';
            ctx2d.strokeStyle = sc; ctx2d.lineWidth = 2;
            roundRect(ctx2d, lx, ly-16, tw, 22, 5);
            ctx2d.fill(); ctx2d.stroke();
            ctx2d.fillStyle = sc; ctx2d.textAlign = 'left';
            ctx2d.fillText(labelText, lx+8, ly+1);

            ctx2d.beginPath();
            ctx2d.moveTo(lx > vPx.x ? lx : lx+tw, ly+3);
            ctx2d.lineTo(vPx.x, vPx.y-16);
            ctx2d.strokeStyle = sc; ctx2d.lineWidth = 1.5; ctx2d.setLineDash([4,3]);
            ctx2d.stroke(); ctx2d.setLineDash([]);

            ctx2d.fillStyle = 'rgba(0,0,0,0.55)';
            ctx2d.fillRect(0, IH-18, IW, 18);
            ctx2d.fillStyle = 'white'; ctx2d.font = '9px Arial'; ctx2d.textAlign = 'center';
            ctx2d.fillText('Esri World Imagery · © Esri, DigitalGlobe, GeoEye, Earthstar Geographics', IW/2, IH-5);

            var isViol = (row.resultClass === 'violation' || row.resultClass === 'violation-multi');
            var panelLines = [];
            panelLines.push({ text: (isViol ? '🚨 VIOLATION' : '✅ ' + row.result), bold: true, color: isViol ? '#dc2626' : '#16a34a', size: 11 });
            panelLines.push({ text: 'Train: ' + row.trainNo, bold: false, color: '#111', size: 10.5 });
            panelLines.push({ text: 'Signal: ' + row.sigNo + ' @ ' + row.signalTime, bold: false, color: '#111', size: 10.5 });
            var spdColor = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
            panelLines.push({ text: 'Speed @ SNT: ' + speedAtSignal + ' km/h', bold: true, color: spdColor, size: 10.5 });
            panelLines.push({ text: 'RTIS Speed: ' + row.speed + ' km/h', bold: false, color: '#374151', size: 10 });

            var mqText = matchResult
                ? (matchResult.quality === 'exact'
                    ? '✓ Exact match (Δ0.0s)'
                    : '⚠ Closest (Δ' + (matchResult.diffMs/1000).toFixed(1) + 's)')
                : 'RTIS ping — no per-sec';
            panelLines.push({ text: mqText, bold: false, color: matchResult && matchResult.quality === 'exact' ? '#16a34a' : '#b45309', size: 9.5 });

            if (row.cachedFsdSignals && row.cachedFsdSignals.length) {
                var trainDir2 = row.direction || row.dirTrain || '';
                var dirSigs2 = row.cachedFsdSignals.filter(function(s){ return s.dir === trainDir2; });
                if (!dirSigs2.length) dirSigs2 = row.cachedFsdSignals;
                var dFsd = null, nFsd = null;
                dirSigs2.forEach(function(s) {
                    if (!isValidGPS(s.lat, s.lon)) return;
                    var d = getDistance(violLat, violLon, s.lat, s.lon) * 1000;
                    if (dFsd === null || d < dFsd) { dFsd = d; nFsd = s; }
                });
                if (dFsd !== null) {
                    panelLines.push({ text: '📍 FSD: ' + dFsd.toFixed(0) + ' m' + (nFsd ? ' (' + nFsd.dir + ' S' + (nFsd.sigNo||'?') + ')' : ''), bold: false, color: '#6b7280', size: 10 });
                }
            }

            if (matchResult && matchResult.point && ctx.perSecondData[resultIdx]) {
                var _pts2 = ctx.perSecondData[resultIdx];
                var _sntT2 = matchResult.point.time.getTime();
                var _mi2 = 0, _md2 = Infinity;
                _pts2.forEach(function(p,i){ var d=Math.abs(p.time.getTime()-_sntT2); if(d<_md2){_md2=d;_mi2=i;} });
                var tPartsA = [], tPartsB = [];
                for (var _t = _mi2-5; _t <= _mi2+5; _t++) {
                    var _val = (_t < 0 || _t >= _pts2.length) ? '—' : String(_pts2[_t].speed != null ? _pts2[_t].speed : '?');
                    if (_t <= _mi2) tPartsA.push(_val); else tPartsB.push(_val);
                }
                panelLines.push({ text: '±5s: ' + tPartsA.join('→'), bold: false, color: '#374151', size: 9 });
                if (tPartsB.length) panelLines.push({ text: '     ' + tPartsB.join('→') + ' km/h', bold: false, color: '#374151', size: 9 });
            }

            var PX = 10, PY = 10;
            var PAD2 = 9, LINE_H = 15;
            var panelW = 210, panelH = PAD2 + panelLines.length * LINE_H + PAD2;

            if (PX + panelW > IW - 10) PX = IW - panelW - 10;
            if (PY + panelH > IH - 25) PY = IH - panelH - 25;

            ctx2d.shadowColor = 'rgba(0,0,0,0.35)';
            ctx2d.shadowBlur = 8; ctx2d.shadowOffsetX = 2; ctx2d.shadowOffsetY = 2;
            ctx2d.fillStyle = 'rgba(255,255,255,0.97)';
            roundRect(ctx2d, PX, PY, panelW, panelH, 8);
            ctx2d.fill();
            ctx2d.shadowColor = 'transparent'; ctx2d.shadowBlur = 0; ctx2d.shadowOffsetX = 0; ctx2d.shadowOffsetY = 0;

            ctx2d.fillStyle = isViol ? '#ef4444' : '#22c55e';
            roundRect(ctx2d, PX, PY, 4, panelH, 4);
            ctx2d.fill();

            ctx2d.strokeStyle = isViol ? 'rgba(239,68,68,0.35)' : 'rgba(34,197,94,0.35)';
            ctx2d.lineWidth = 1;
            roundRect(ctx2d, PX, PY, panelW, panelH, 8);
            ctx2d.stroke();

            var ly2 = PY + PAD2 + 11;
            ctx2d.textAlign = 'left';
            panelLines.forEach(function(line) {
                ctx2d.font = (line.bold ? 'bold ' : '') + line.size + 'px Arial';
                ctx2d.fillStyle = line.color;
                ctx2d.fillText(line.text, PX + 10, ly2);
                ly2 += LINE_H;
            });

            resolve(canvas.toDataURL('image/png'));
        };

        satImg.onerror = function() {
            fetchOsmFallback(row, violLat, violLon, speedAtSignal, speedLimit, trackPts, localViolIdx, fsdSignals, latLonToPixel, IW, IH, resolve);
        };

        satImg.src = esriUrl;
    });
}

function roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x+r, y);
    ctx.lineTo(x+w-r, y); ctx.arcTo(x+w, y, x+w, y+r, r);
    ctx.lineTo(x+w, y+h-r); ctx.arcTo(x+w, y+h, x+w-r, y+h, r);
    ctx.lineTo(x+r, y+h); ctx.arcTo(x, y+h, x, y+h-r, r);
    ctx.lineTo(x, y+r); ctx.arcTo(x, y, x+r, y, r);
    ctx.closePath();
}

function fetchOsmFallback(row, violLat, violLon, speedAtSignal, speedLimit, trackPts, localViolIdx, fsdSignals, latLonToPixel, IW, IH, resolve) {
    var canvas = document.createElement('canvas');
    canvas.width = IW; canvas.height = IH;
    var ctx = canvas.getContext('2d');
    ctx.fillStyle = '#1e293b';
    ctx.fillRect(0, 0, IW, IH);
    ctx.strokeStyle = '#334155'; ctx.lineWidth = 0.8;
    for (var gx = 0; gx < IW; gx += 80) { ctx.beginPath(); ctx.moveTo(gx,0); ctx.lineTo(gx,IH); ctx.stroke(); }
    for (var gy = 0; gy < IH; gy += 80) { ctx.beginPath(); ctx.moveTo(0,gy); ctx.lineTo(IW,gy); ctx.stroke(); }
    if (trackPts.length > 1) {
        ctx.beginPath(); ctx.strokeStyle = 'rgba(96,165,250,0.8)'; ctx.lineWidth = 2.5; ctx.setLineDash([7,4]);
        var first = true;
        trackPts.forEach(function(p) {
            if (!isValidGPS(p.lat, p.lon)) return;
            var px = latLonToPixel(p.lat, p.lon);
            first ? (ctx.moveTo(px.x,px.y), first=false) : ctx.lineTo(px.x,px.y);
        });
        ctx.stroke(); ctx.setLineDash([]);
    }
    var vPx = latLonToPixel(violLat, violLon);
    ctx.beginPath(); ctx.arc(vPx.x, vPx.y, 18, 0, Math.PI*2);
    ctx.fillStyle = '#ef4444'; ctx.fill();
    ctx.strokeStyle = '#fca5a5'; ctx.lineWidth = 3; ctx.stroke();
    ctx.beginPath(); ctx.arc(vPx.x, vPx.y, 6, 0, Math.PI*2);
    ctx.fillStyle = 'white'; ctx.fill();
    ctx.fillStyle = 'white'; ctx.font = 'bold 14px Arial'; ctx.textAlign = 'center';
    ctx.fillText('Train ' + row.trainNo + '  |  ' + row.station + '  |  ' + row.sigNo, IW/2, 30);
    ctx.font = '11px Arial'; ctx.fillStyle = speedAtSignal > speedLimit ? '#fca5a5' : '#86efac';
    ctx.fillText(speedAtSignal + ' km/h  (Limit ' + speedLimit + ' km/h)  @ ' + row.signalTime, IW/2, 50);
    ctx.fillStyle = '#94a3b8'; ctx.font = '10px Arial';
    ctx.fillText('GPS: ' + violLat.toFixed(5) + ', ' + violLon.toFixed(5), IW/2, IH - 22);
    ctx.fillStyle = '#475569';
    ctx.fillText('(Satellite imagery unavailable)', IW/2, IH - 8);
    resolve(canvas.toDataURL('image/png'));
}

function mapFallbackCanvas(row, lat, lon, speed, speedLimit) {
    var c = document.createElement('canvas');
    c.width = 860; c.height = 420;
    var ctx = c.getContext('2d');
    ctx.fillStyle = '#1e293b';
    ctx.fillRect(0, 0, c.width, c.height);
    ctx.strokeStyle = '#334155'; ctx.lineWidth = 1;
    for (var i = 0; i < c.width; i += 60) { ctx.beginPath(); ctx.moveTo(i,0); ctx.lineTo(i,c.height); ctx.stroke(); }
    for (var j = 0; j < c.height; j += 60) { ctx.beginPath(); ctx.moveTo(0,j); ctx.lineTo(c.width,j); ctx.stroke(); }
    ctx.fillStyle = '#ef4444';
    ctx.beginPath(); ctx.arc(c.width/2, c.height/2, 18, 0, Math.PI*2); ctx.fill();
    ctx.fillStyle = '#fef2f2'; ctx.beginPath(); ctx.arc(c.width/2, c.height/2, 6, 0, Math.PI*2); ctx.fill();
    ctx.fillStyle = 'white'; ctx.font = 'bold 16px Arial'; ctx.textAlign = 'center';
    ctx.fillText('Train ' + row.trainNo + '  |  ' + row.station + '  |  Signal ' + row.sigNo, c.width/2, c.height/2 - 35);
    ctx.font = '13px Arial';
    ctx.fillStyle = speed > speedLimit ? '#fca5a5' : '#86efac';
    ctx.fillText('Speed: ' + speed + ' km/h  (Limit: ' + speedLimit + ' km/h)  @ ' + row.signalTime, c.width/2, c.height/2 + 35);
    ctx.fillStyle = '#94a3b8'; ctx.font = '11px Arial';
    ctx.fillText('GPS: ' + (lat||'?') + ', ' + (lon||'?'), c.width/2, c.height/2 + 55);
    ctx.fillStyle = '#475569'; ctx.font = '10px Arial';
    ctx.fillText('(Satellite imagery unavailable — map tiles could not be loaded)', c.width/2, c.height - 15);
    return c.toDataURL('image/png');
}

export function captureChartForRow(row, resultIdx, ctxParams) {
    var _pts2 = ctxParams.perSecondData[resultIdx] || [];
    if (_pts2.length && row.sntTimeISO) {
        var _sntMs2 = new Date(row.sntTimeISO).getTime();
        var _vi2 = 0, _bd2 = Infinity;
        _pts2.forEach(function(p, i) { var d = Math.abs(p.time.getTime() - _sntMs2); if (d < _bd2) { _bd2 = d; _vi2 = i; } });
        row._pdfBrakingDist = calcBrakingDist(_pts2, _sntMs2, _vi2);
    } else {
        row._pdfBrakingDist = null;
    }
    return new Promise(function(resolve) {
        var canvas = document.getElementById('reportChartCanvas');
        canvas.width = 900; canvas.height = 400;
        var ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, 900, 400);

        var speedLimit = ctxParams.getSpeedLimit();
        var hasPersec  = ctxParams.perSecondData[resultIdx] && ctxParams.perSecondData[resultIdx].length > 0;
        var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
            var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
            var d = new Date(base); var tp = (row.signalTime || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
            if (tp) d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0); return d;
        })();

        var displayPts = [], violIdx = -1;
        if (hasPersec) {
            var allPts = ctxParams.perSecondData[resultIdx];
            var grMatch = row.perSecMatch || matchPerSecToSNT(allPts, sntDateObj);
            var matchIdx = grMatch ? (grMatch.idx || 0) : 0;
            var violMs = sntDateObj.getTime(), w3 = 3 * 60 * 1000;
            var timePts = allPts.filter(function(p) { return Math.abs(p.time.getTime() - violMs) <= w3; });
            if (timePts.length < 5) {
                displayPts = allPts.slice(Math.max(0, matchIdx - 120), Math.min(allPts.length, matchIdx + 121));
            } else {
                displayPts = timePts;
            }
            if (displayPts.length < 3) displayPts = allPts;
            violIdx = grMatch ? displayPts.findIndex(function(p) { return p === grMatch.point; }) : -1;
            if (violIdx < 0 && grMatch && grMatch.point) {
                var bestD = Infinity;
                displayPts.forEach(function(p, i) { var d = Math.abs(p.time - grMatch.point.time); if (d < bestD) { bestD = d; violIdx = i; } });
            }
        } else {
            var trainKey = String(row.trainNo), stnKey = row.stationKey || cleanStationName(row.station.split('→')[0].trim());
            ctxParams.dataRTIS.forEach(function(r) {
                var t = getCol(r, ['Train Number','Train No.','TRAIN','Train']), stn = cleanStationName(getCol(r, ['Station','STATION','STN_CODE']));
                if (String(t) !== trainKey || stn !== stnKey) return;
                var tm = parseSmartDate(getCol(r, ['Event Time','TIME','EventTime','Date']));
                var spd = parseFloat(getCol(r, ['Speed','SPEED']));
                if (tm && !isNaN(spd)) displayPts.push({ time: tm, speed: spd });
            });
            displayPts.sort(function(a, b) { return a.time - b.time; });
            if (!displayPts.length) { var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr); displayPts.push({ time: base, speed: parseFloat(row.speed) }); }
            var violMs2 = sntDateObj.getTime();
            var bestD2 = Infinity;
            displayPts.forEach(function(p, i) { var d = Math.abs(p.time.getTime() - violMs2); if (d < bestD2) { bestD2 = d; violIdx = i; } });
        }

        var labels = displayPts.map(function(p) { return p.time.toTimeString().slice(0, 8); });
        var speeds = displayPts.map(function(p) { return (p.speed != null && !isNaN(p.speed)) ? p.speed : null; });
        var ll = speeds.map(function() { return speedLimit; });
        var ptColors = speeds.map(function(s, i) { return i === violIdx ? '#ef4444' : (s != null && s > speedLimit ? '#f97316' : '#3b82f6'); });
        var ptRadii = speeds.map(function(s, i) { return i === violIdx ? 8 : 3; });

        var tempChart = new window.Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    { label: 'Speed (km/h)', data: speeds, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.08)',
                      borderWidth: 2, pointBackgroundColor: ptColors, pointRadius: ptRadii, tension: 0.3, fill: true, spanGaps: true },
                    { label: 'Limit (' + speedLimit + ' km/h)', data: ll, borderColor: '#ef4444',
                      borderWidth: 2, borderDash: [8, 4], pointRadius: 0, fill: false }
                ]
            },
            options: {
                responsive: false, animation: { duration: 0 },
                plugins: {
                    legend: { position: 'top', labels: { font: { size: 12 } } },
                    title: {
                        display: true,
                        text: 'Train ' + row.trainNo + '  ·  ' + row.station + '  ·  Signal ' + row.sigNo + '  ·  ' + row.signalTime,
                        font: { size: 13, weight: 'bold' }, color: '#1e293b', padding: { bottom: 8 }
                    }
                },
                scales: {
                    x: { ticks: { maxTicksLimit: 14, font: { size: 10 } }, grid: { color: '#f1f5f9' } },
                    y: { title: { display: true, text: 'Speed (km/h)', font: { size: 11 } }, min: 0,
                         suggestedMax: Math.max(speedLimit + 20, (Math.max.apply(null, speeds.filter(Boolean)) || 0) + 10),
                         grid: { color: '#f1f5f9' } }
                }
            },
            plugins: [{
                id: 'vLine',
                afterDraw: function(chart) {
                    if (violIdx < 0 || violIdx >= chart.data.labels.length) return;
                    var meta = chart.getDatasetMeta(0);
                    if (!meta.data[violIdx]) return;
                    var x = meta.data[violIdx].x, c = chart.ctx;
                    c.save(); c.beginPath(); c.moveTo(x, chart.chartArea.top); c.lineTo(x, chart.chartArea.bottom);
                    c.strokeStyle = 'rgba(239,68,68,0.7)'; c.lineWidth = 2; c.setLineDash([5, 3]); c.stroke();
                    var spd = speeds[violIdx];
                    c.fillStyle = '#dc2626'; c.font = 'bold 11px Arial'; c.setLineDash([]);
                    c.fillText('🚨 ' + row.sigNo + '  ' + (spd != null ? spd + ' km/h' : ''), x + 5, chart.chartArea.top + 16);
                    c.restore();
                }
            }, {
                id: 'pdfBrakingDist',
                afterDraw: function(chart) {
                    var pdfBd = (function() {
                        if (!hasPersec || !displayPts.length || violIdx < 0) return null;
                        var sntMs2 = sntDateObj.getTime();
                        var winS = displayPts[0].time.getTime();
                        var winE = displayPts[displayPts.length-1].time.getTime();
                        if (sntMs2 < winS || sntMs2 > winE) return null;
                        var tot = 0, stopI = displayPts.length - 1, stopped = false;
                        for (var bi = violIdx; bi < displayPts.length; bi++) {
                            var bp = displayPts[bi];
                            if (bp.distFromSpeed != null && !isNaN(bp.distFromSpeed)) tot += bp.distFromSpeed;
                            if (!isNaN(bp.speed) && bp.speed < 1) { stopI = bi; stopped = true; break; }
                        }
                        return { distM: Math.round(tot), vi: violIdx, si: stopI, stopped: stopped };
                    })();
                    if (!pdfBd) return;
                    var meta2 = chart.getDatasetMeta(0);
                    if (!meta2.data[pdfBd.vi] || !meta2.data[pdfBd.si]) return;
                    var x1 = meta2.data[pdfBd.vi].x, x2 = meta2.data[pdfBd.si].x;
                    if (x2 < x1 + 2) x2 = x1 + 2;
                    var top2 = chart.chartArea.top, bot2 = chart.chartArea.bottom;
                    var c2 = chart.ctx;
                    c2.save();
                    c2.fillStyle = 'rgba(251,146,60,0.13)';
                    c2.fillRect(x1, top2, x2 - x1, bot2 - top2);
                    c2.strokeStyle = 'rgba(234,88,12,0.65)'; c2.lineWidth = 1.5; c2.setLineDash([4,3]);
                    c2.beginPath(); c2.moveTo(x2, top2); c2.lineTo(x2, bot2); c2.stroke();
                    c2.setLineDash([]);
                    var bdLabel = (pdfBd.stopped ? '🛑 Stop' : '→ End') + ': ' + pdfBd.distM + ' m';
                    c2.font = 'bold 11px Arial';
                    var tw2 = c2.measureText(bdLabel).width + 16;
                    var midX = (x1 + x2) / 2, lx2 = Math.max(x1 + 2, midX - tw2/2);
                    if (lx2 + tw2 > chart.chartArea.right - 4) lx2 = chart.chartArea.right - tw2 - 6;
                    var ly3 = top2 + 80;
                    c2.fillStyle = 'rgba(255,255,255,0.96)';
                    c2.strokeStyle = 'rgba(234,88,12,0.8)'; c2.lineWidth = 1.5;
                    c2.beginPath();
                    c2.moveTo(lx2+5,ly3-14); c2.lineTo(lx2+tw2-5,ly3-14);
                    c2.arcTo(lx2+tw2,ly3-14,lx2+tw2,ly3-9,5); c2.lineTo(lx2+tw2,ly3+4);
                    c2.arcTo(lx2+tw2,ly3+9,lx2+tw2-5,ly3+9,5); c2.lineTo(lx2+5,ly3+9);
                    c2.arcTo(lx2,ly3+9,lx2,ly3+4,5); c2.lineTo(lx2,ly3-9);
                    c2.arcTo(lx2,ly3-14,lx2+5,ly3-14,5); c2.closePath();
                    c2.fill(); c2.stroke();
                    c2.fillStyle = '#c2410c'; c2.textAlign = 'left';
                    c2.fillText(bdLabel, lx2 + 8, ly3 + 2);
                    c2.restore();
                }
            }]
        });

        setTimeout(function() {
            var dataUrl = canvas.toDataURL('image/png');
            tempChart.destroy();
            resolve(dataUrl);
        }, 200);
    });
}
