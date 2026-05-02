import * as L from 'leaflet';
import { isValidGPS, getDistance, cleanStationName, getCol } from '../utils/helpers.js';

// ══════════════════════════════════════════════════════════════
// MAP VIEWER  —  extracted from main.js
// All logic is identical to the original; no core changes.
// ══════════════════════════════════════════════════════════════

var leafletMap = null;

// Module-level toast state (mirrors _toastTimer etc. in main.js)
var _toastTimer = null, _toastInterval = null, _toastPaused = false, _toastElapsed = 0;

// Track layer references (needed by closeMap)
var _trackLayer = null, _startMk = null, _endMk = null;

// ── Public: open map modal ────────────────────────────────────
export function openMap(resultIdx, analysisResults, perSecondData) {
    var row = analysisResults[resultIdx];
    if (!row) return;

    var dirLabel = row.direction || row.dirTrain || row.dirSig || '';
    var dirBadge = dirLabel
        ? ' <span style="background:' + (dirLabel==='UP'?'#6366f1':'#f59e0b') +
          ';color:white;border-radius:4px;padding:1px 7px;font-size:0.72rem;font-weight:700;margin-left:4px;">' +
          dirLabel + '</span>'
        : '';

    document.getElementById('mapModal').classList.add('open');
    document.getElementById('mapTitle').innerHTML =
        'Train ' + row.trainNo + ' &mdash; ' + row.station + ' &mdash; ' + row.sigNo + dirBadge;
    var _mapResultLabel = (row.resultClass === 'ambiguous' && ((row.speedPerSec !== null && row.speedPerSec !== undefined ? row.speedPerSec : row.speed) > (parseFloat(document.getElementById('inputSpeedLimit').value) || 63))) ? 'VIOLATION' : row.result;
    document.getElementById('mapSubtitle').innerText =
        _mapResultLabel + ' | Speed: ' + (row.speedPerSec !== null && row.speedPerSec !== undefined ? row.speedPerSec : row.speed) + ' km/h | SNT: ' + row.signalTime;

    setTimeout(function() { buildMap(row, resultIdx, analysisResults, perSecondData); }, 80);
}

export function closeMap() {
    document.getElementById('mapModal').classList.remove('open');
    if (leafletMap) { leafletMap.remove(); leafletMap = null; }
    document.getElementById('violPanel').style.display = 'none';
    document.getElementById('mapFilterStrip').style.display = 'none';
    _trackLayer = null; _startMk = null; _endMk = null;
}

// ── Public: demotion toast ────────────────────────────────────
export function showDemotionToast(trainNo, distM, direction, reason) {
    clearTimeout(_toastTimer);
    clearInterval(_toastInterval);
    _toastPaused = false;
    _toastElapsed = 0;

    var body = document.getElementById('demotionToastBody');
    body.innerHTML =
        'Train <b>' + trainNo + '</b> moved from <b style="color:#dc2626">VIOLATION</b> → <b style="color:#15803d">Complied</b>.<br>' +
        'Per-sec GPS at SNT time is <b>' + Math.round(distM) + ' m</b> from the ' +
        direction + ' FSD signal (threshold: 200 m).<br>' +
        '<span style="color:#6b7280;font-size:0.8rem;">' + reason + '</span>';

    var toast = document.getElementById('demotionToast');
    toast.classList.add('toast-visible');
    toast.style.cursor = '';
    toast.style.borderLeftColor = '';
    document.getElementById('demotionToastBar').style.background = '';
    document.getElementById('demotionToastLabel').innerHTML =
        'Auto-closing in <span id="demotionToastSecs">8</span>s';

    var bar  = document.getElementById('demotionToastBar');
    var total = 8000, step = 100;
    bar.style.transition = 'none';
    bar.style.width = '100%';

    _toastInterval = setInterval(function() {
        if (_toastPaused) return;
        _toastElapsed += step;
        var pct = Math.max(0, 100 - (_toastElapsed / total * 100));
        bar.style.width = pct + '%';
        var secsEl = document.getElementById('demotionToastSecs');
        if (secsEl) secsEl.textContent = Math.ceil((total - _toastElapsed) / 1000);
        if (_toastElapsed >= total) { clearInterval(_toastInterval); closeDemotionToast(); }
    }, step);
}

export function closeDemotionToast() {
    clearTimeout(_toastTimer);
    clearInterval(_toastInterval);
    _toastPaused = false;
    document.getElementById('demotionToast').classList.remove('toast-visible');
}

// ── Init toast pause-on-hold behaviour (call once on page load) ──
export function initToastListeners() {
    var toast = document.getElementById('demotionToast');
    toast.addEventListener('mousedown', function(e) {
        if (e.target.classList.contains('toast-close')) return;
        _toastPaused = true;
        toast.style.cursor = 'grab';
        toast.style.borderLeftColor = '#f59e0b';
        document.getElementById('demotionToastBar').style.background = '#f59e0b';
        document.getElementById('demotionToastLabel').textContent = '⏸ Held — release to resume';
    });
    document.addEventListener('mouseup', function() {
        if (!_toastPaused) return;
        _toastPaused = false;
        toast.style.cursor = '';
        toast.style.borderLeftColor = '';
        document.getElementById('demotionToastBar').style.background = '';
        var remaining = Math.ceil((8000 - _toastElapsed) / 1000);
        document.getElementById('demotionToastLabel').innerHTML =
            'Auto-closing in <span id="demotionToastSecs">' + remaining + '</span>s';
    });
}

// ── Private: build Leaflet map ────────────────────────────────
function buildMap(row, resultIdx, analysisResults, perSecondData) {
    if (leafletMap) { leafletMap.remove(); leafletMap = null; }
    document.getElementById('mapDiv').innerHTML = '';

    var speedLimit   = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
    var hasPersec    = perSecondData[resultIdx] && perSecondData[resultIdx].length > 0;
    var cleanStation = row.fsdStation || cleanStationName(row.station.split(' → ')[0]);

    // Resolve SNT datetime (ground-truth anchor)
    var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
        var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
        var d = new Date(base);
        var tp = (row.signalTime || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
        if (tp) d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0);
        return d;
    })();

    // Run or use cached SNT-time match
    var matchResult = row.perSecMatch || (hasPersec ? matchPerSecToSNT(perSecondData[resultIdx], sntDateObj) : null);

    // Violation point coords — SNT-matched if available
    var violLat, violLon, speedAtSignal, violTimeStr, matchQuality = 'none', diffSecStr = '—';
    var sntMatchedIdx = 0;
    if (hasPersec && matchResult && matchResult.point) {
        violLat       = matchResult.point.lat;
        violLon       = matchResult.point.lon;
        speedAtSignal = !isNaN(matchResult.point.speed) ? matchResult.point.speed : row.speed;
        violTimeStr   = matchResult.point.time.toLocaleTimeString();
        matchQuality  = matchResult.quality;
        diffSecStr    = (matchResult.diffMs / 1000).toFixed(1) + 's';
        sntMatchedIdx = matchResult.idx || 0;
    } else {
        violLat       = row.violationLat || 22.5;
        violLon       = row.violationLon || 88.0;
        speedAtSignal = (row.speedPerSec !== null && row.speedPerSec !== undefined) ? row.speedPerSec : row.speed;
        violTimeStr   = row.rtisTime;
    }

    // Collect FSD signals
    var fsdSignals = [];
    if (window._dataFSD && window._dataFSD.length > 0) {
        window._dataFSD.forEach(function(r) {
            var stn = cleanStationName(getCol(r, ['Station','STATION','STN_CODE','Station Name']));
            if (stn !== cleanStation) return;
            var lat = parseFloat(getCol(r, ['Latitude','Lattitude','LAT','Lat','GPS_LAT']));
            var lon = parseFloat(getCol(r, ['Longitude','LON','Lon','GPS_LON']));
            var dir = String(getCol(r, ['DIRN','Direction','DIR']) || '').toUpperCase().trim();
            var sigNo = getCol(r, ['SIGNUMBER','Signal','SIGNAL','Signal No','SIG_ID']);
            if (!isValidGPS(lat, lon)) return;
            fsdSignals.push({ lat:lat, lon:lon, dir:dir, sigNo:sigNo, stn:stn });
        });
    }
    if (fsdSignals.length === 0 && row.cachedFsdSignals && row.cachedFsdSignals.length) {
        fsdSignals = row.cachedFsdSignals.filter(function(s){ return isValidGPS(s.lat, s.lon); })
            .map(function(s){ return Object.assign({ stn: cleanStation }, s); });
        if (window._log) window._log('ℹ️  Using cached FSD signals for ' + cleanStation + ' (' + fsdSignals.length + ')');
    }

    // Distance: violation point → nearest FSD signal matching train direction
    var trainDir = row.direction || row.dirTrain || row.dirSig || '';
    var matchedDirSignals = fsdSignals.filter(function(s){ return s.dir === trainDir; });
    if (matchedDirSignals.length === 0) matchedDirSignals = fsdSignals;

    var distToFsdM = null, nearestFsdSig = null;
    matchedDirSignals.forEach(function(sig) {
        var d = getDistance(violLat, violLon, sig.lat, sig.lon) * 1000;
        if (distToFsdM === null || d < distToFsdM) { distToFsdM = d; nearestFsdSig = sig; }
    });

    // Distance: SNT-matched point → RTIS ping GPS
    var rtisLat = row.violationLat, rtisLon = row.violationLon;
    var distToRtisM = (isValidGPS(rtisLat, rtisLon) && isValidGPS(violLat, violLon))
        ? getDistance(violLat, violLon, rtisLat, rtisLon) * 1000
        : null;

    leafletMap = L.map('mapDiv').setView([violLat, violLon], 15);
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
        attribution: 'Tiles © Esri — Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP',
        maxZoom: 19
    }).addTo(leafletMap);
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}', {
        attribution: '', maxZoom: 19, opacity: 0.55
    }).addTo(leafletMap);

    // Violation popup builder
    function violPopupHTML() {
        var sc = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
        var isAmbigOverspeed = (row.resultClass === 'ambiguous' && speedAtSignal > speedLimit);
        var displayLabel = (isAmbigOverspeed) ? 'VIOLATION' : row.result;
        var matchLine = '';
        if      (matchQuality === 'exact')   matchLine = '<br><span style="color:#16a34a;font-size:0.72rem">✓ Exact SNT match (Δ' + diffSecStr + ')</span>';
        else if (matchQuality === 'closest') matchLine = '<br><span style="color:#b45309;font-size:0.72rem">⚠ Closest point (Δ' + diffSecStr + ' — outside ±5s)</span>';
        else                                 matchLine = '<br><span style="color:#9ca3af;font-size:0.72rem">RTIS ping (no per-sec data)</span>';

        var fsdDistLine  = distToFsdM  !== null
            ? '<br>📍 Dist to FSD Signal: <b>' + distToFsdM.toFixed(0)  + ' m</b>' +
              (nearestFsdSig ? ' (' + nearestFsdSig.dir + ' S' + (nearestFsdSig.sigNo||'?') + ')' : '')
            : '';
        var rtisDistLine = distToRtisM !== null && distToRtisM > 1
            ? '<br>🚂 Dist to RTIS H event: <b>' + distToRtisM.toFixed(0) + ' m</b>'
            : (distToRtisM !== null ? '<br>🚂 Dist to RTIS H event: <b>&lt;1 m</b> (same point)' : '');

        return "<div class='popup-violation'>" +
            "<strong>🚨 " + displayLabel + "</strong><hr style='margin:4px 0'>" +
            "Train: <b>" + row.trainNo + "</b><br>" +
            "SNT Signal: <b>" + row.sigNo + "</b> @ " + row.signalTime + "<br>" +
            "Speed @ SNT: <b style='color:" + sc + "'>" + speedAtSignal + " km/h</b><br>" +
            "RTIS Speed: " + row.speed + " km/h<br>" +
            "Matched Time: <b>" + violTimeStr + "</b>" +
            matchLine +
            "<hr style='margin:4px 0;border-color:#e5e7eb'>" +
            fsdDistLine + rtisDistLine +
            "</div>";
    }

    if (!hasPersec) {
        // CASE A: No per-second — static RTIS pin
        L.circleMarker([violLat, violLon], {
            radius:14, fillColor: row.resultClass==='complied'?'#22c55e':'#ef4444',
            color:'#1e293b', weight:3, fillOpacity:0.9
        }).addTo(leafletMap)
        .bindTooltip((function(){ var _l = (row.resultClass === 'ambiguous' && row.speed > speedLimit) ? 'VIOLATION' : row.result; return _l + ' · ' + row.speed + ' km/h'; })(), {permanent:false});
        if (window._showViolPanel) window._showViolPanel(row, speedAtSignal, speedLimit, matchQuality, diffSecStr, distToFsdM, nearestFsdSig, distToRtisM);

    } else {
        // CASE B: Per-second track — SNT-anchored ±40 window
        var allTrainPoints = perSecondData[resultIdx];

        if (!matchResult || matchResult.idx === undefined) {
            var bestD2 = Infinity;
            allTrainPoints.forEach(function(p,i) {
                var d = Math.abs(p.time - sntDateObj);
                if (d < bestD2) { bestD2 = d; sntMatchedIdx = i; }
            });
        }

        var start       = Math.max(0, sntMatchedIdx - 40);
        var end         = Math.min(allTrainPoints.length - 1, sntMatchedIdx + 40);
        var trackPoints = allTrainPoints.slice(start, end + 1);
        var localViolIdx= sntMatchedIdx - start;

        if (trackPoints.length > 1) {
            L.polyline(trackPoints.map(function(p){ return [p.lat, p.lon]; }),
                { color:'#3b82f6', weight:3, opacity:0.7, dashArray:'7,5' }
            ).addTo(leafletMap);
        }

        if (trackPoints.length > 0) {
            L.circleMarker([trackPoints[0].lat, trackPoints[0].lon],
                { radius:7, fillColor:'#10b981', color:'#065f46', weight:2, fillOpacity:1 })
                .bindTooltip('▶ Path Start').addTo(leafletMap);
            var last = trackPoints[trackPoints.length-1];
            L.circleMarker([last.lat, last.lon],
                { radius:7, fillColor:'#f97316', color:'#9a3412', weight:2, fillOpacity:1 })
                .bindTooltip('■ Path End').addTo(leafletMap);
        }

        var trainNo = row.trainNo;
        trackPoints.forEach(function(p, i) {
            var isViol = (i === localViolIdx);
            var opts;
            if (isViol) {
                var fillCol = (matchQuality === 'closest') ? '#f59e0b' : '#ef4444';
                var ringCol = (matchQuality === 'closest') ? '#92400e' : '#7f1d1d';
                opts = { radius:14, fillColor:fillCol, color:ringCol, weight:3, fillOpacity:0.95, opacity:1 };
            } else {
                var proximity = 1 - (Math.abs(i - localViolIdx) / 41);
                opts = { radius:4, fillColor:'#3b82f6', color:'#1e40af', weight:1.5,
                         fillOpacity:0.3 + (0.55*proximity), opacity:1 };
            }
            var pSpeed = !isNaN(p.speed) ? p.speed : null;
            var spCol  = (pSpeed !== null && pSpeed > speedLimit) ? '#dc2626' : '#16a34a';
            var m = L.circleMarker([p.lat, p.lon], opts).addTo(leafletMap);
            if (isViol) {
                m.bindTooltip((function(){ var _l = (row.resultClass === 'ambiguous' && speedAtSignal > speedLimit) ? 'VIOLATION' : row.result; return _l + ' · ' + speedAtSignal + ' km/h'; })(), {permanent:false});
                if (window._showViolPanel) window._showViolPanel(row, speedAtSignal, speedLimit, matchQuality, diffSecStr, distToFsdM, nearestFsdSig, distToRtisM);
            } else {
                m.bindPopup(
                    "<div class='popup-rtis'><strong>🚂 Pt " + (i+1) + "/" + trackPoints.length + "</strong>" +
                    "<hr style='margin:4px 0;border-color:#e5e7eb'>" +
                    "Train: <b>" + trainNo + "</b><br>" +
                    "Speed: <b style='color:" + spCol + "'>" + (pSpeed!==null ? pSpeed+' km/h':'—') + "</b><br>" +
                    "Time: <b>" + p.time.toLocaleTimeString() + "</b></div>", { maxWidth:200 }
                );
            }
        });

        // Warning label if outside ±5s
        if (matchQuality === 'closest' && trackPoints[localViolIdx]) {
            var warnIcon = L.divIcon({
                html: "<div style='background:#fef3c7;border:1.5px solid #f59e0b;border-radius:6px;" +
                      "padding:2px 6px;font-size:0.68rem;color:#92400e;white-space:nowrap;" +
                      "box-shadow:0 1px 4px rgba(0,0,0,0.2);font-weight:600;'>⚠ Outside ±5s (Δ" + diffSecStr + ")</div>",
                className:'', iconSize:[150,24], iconAnchor:[-6,12]
            });
            L.marker([trackPoints[localViolIdx].lat, trackPoints[localViolIdx].lon],
                     { icon:warnIcon, interactive:false }).addTo(leafletMap);
        }

        // Draw RTIS ping marker if meaningfully different from SNT-matched point
        if (isValidGPS(rtisLat, rtisLon) && distToRtisM !== null && distToRtisM > 5) {
            var rtisIcon = L.divIcon({
                html: "<div style='background:#3b82f6;border:2px solid #1e40af;width:14px;height:14px;" +
                      "border-radius:3px;box-shadow:0 1px 5px rgba(0,0,0,0.4);'></div>",
                className:'', iconSize:[14,14], iconAnchor:[7,7]
            });
            L.marker([rtisLat, rtisLon], { icon:rtisIcon }).addTo(leafletMap)
             .bindPopup(
                "<div class='popup-rtis'><strong>🔵 RTIS H Event Ping</strong>" +
                "<hr style='margin:4px 0;border-color:#e5e7eb'>" +
                "Train: <b>" + row.trainNo + "</b><br>" +
                "RTIS Speed: <b>" + row.speed + " km/h</b><br>" +
                "RTIS Time: <b>" + row.rtisTime + "</b><br>" +
                "Distance from SNT point: <b>" + distToRtisM.toFixed(0) + " m</b>" +
                "</div>", { maxWidth:220 }
             );
            L.polyline([[rtisLat, rtisLon],[violLat, violLon]],
                { color:'#60a5fa', weight:2, dashArray:'5,4', opacity:0.85 }
            ).addTo(leafletMap)
             .bindTooltip('RTIS↔SNT: ' + distToRtisM.toFixed(0) + 'm', { sticky:true });
        }
    }

    // Plot FSD signals
    fsdSignals.forEach(function(sig) {
        var isUP   = (sig.dir === 'UP');
        var fill   = isUP ? '#8b5cf6' : '#f59e0b';
        var border = isUP ? '#5b21b6' : '#b45309';
        var arrow  = isUP ? '▲' : '▼';
        var icon   = L.divIcon({
            html: "<div style='background:" + fill + ";border:2.5px solid " + border + ";" +
                  "width:22px;height:22px;border-radius:4px;display:flex;align-items:center;" +
                  "justify-content:center;font-size:11px;color:#fff;font-weight:bold;" +
                  "box-shadow:0 2px 6px rgba(0,0,0,0.45);'>" + arrow + "</div>",
            className:'', iconSize:[22,22], iconAnchor:[11,11]
        });
        L.marker([sig.lat, sig.lon], { icon:icon }).addTo(leafletMap)
         .bindPopup(
            "<div class='popup-signal'><strong>📡 FSD Signal</strong>" +
            "<hr style='margin:4px 0;border-color:#e5e7eb'>" +
            "Station: <b>" + sig.stn + "</b><br>Dir: <b style='color:" + fill + "'>" + sig.dir + " " + arrow + "</b><br>" +
            "Sig No: <b>" + (sig.sigNo!=null ? sig.sigNo : '—') + "</b></div>", { maxWidth:210 }
         );
    });

    // Fit bounds
    var bounds = [];
    if (hasPersec && perSecondData[resultIdx]) {
        var s2 = Math.max(0, sntMatchedIdx - 40);
        var e2 = Math.min(perSecondData[resultIdx].length-1, sntMatchedIdx+40);
        bounds = perSecondData[resultIdx].slice(s2,e2+1).map(function(p){ return [p.lat,p.lon]; });
    }
    if (isValidGPS(violLat, violLon)) bounds.push([violLat, violLon]);
    bounds = bounds.concat(fsdSignals.map(function(s){ return [s.lat, s.lon]; }));
    if (bounds.length > 1) leafletMap.fitBounds(bounds, { padding:[50,50] });

    // Init map filter strip when per-sec data loaded
    if (hasPersec && perSecondData[resultIdx] && perSecondData[resultIdx].length > 0) {
        if (window._initFilterUI) window._initFilterUI(perSecondData[resultIdx], sntDateObj.getTime(), resultIdx, row);
    } else {
        document.getElementById('mapFilterStrip').style.display = 'none';
    }
}

// ── matchPerSecToSNT — used by buildMap internally ────────────
function matchPerSecToSNT(perSecArr, sntTimeObj) {
    if (!perSecArr || perSecArr.length === 0) return { point: null, diffMs: null, quality: 'none' };
    var sntMs   = sntTimeObj instanceof Date ? sntTimeObj.getTime() : new Date(sntTimeObj).getTime();
    var EXACT_MS = 5000;
    var bestIdx  = 0, bestDiff = Infinity;
    perSecArr.forEach(function(p, i) {
        if (!p.time) return;
        var d = Math.abs(p.time.getTime() - sntMs);
        if (d < bestDiff) { bestDiff = d; bestIdx = i; }
    });
    var quality = bestDiff <= EXACT_MS ? 'exact' : 'closest';
    return { point: perSecArr[bestIdx], idx: bestIdx, diffMs: bestDiff, quality: quality };
}

// ── Expose leafletMap reference for filter strip redraws ──────
export function getLeafletMap() { return leafletMap; }
