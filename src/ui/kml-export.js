import { isValidGPS, getDistance } from '../utils/helpers.js';

// ══════════════════════════════════════════════════════════════
// KML EXPORT SYSTEM  —  extracted from main.js
// All logic is identical to the original; no core changes.
// ══════════════════════════════════════════════════════════════

// ── Public: open row-selection dialog ────────────────────────
export function openKmlDialog(analysisResults, perSecondData) {
    var tbody = document.getElementById('kmlDialogTbody');
    tbody.innerHTML = '';
    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
    var rowsFound  = 0;

    analysisResults.forEach(function(row, idx) {
        if (!perSecondData[idx] || !perSecondData[idx].length) return;
        rowsFound++;
        var violSpeed = (row.speedPerSec != null) ? row.speedPerSec : row.speed;
        var overLimit = violSpeed > speedLimit;
        var isViol    = (row.resultClass === 'violation' || row.resultClass === 'violation-multi' || row.perSecPromoted);

        var tr = document.createElement('tr');
        tr.style.cssText = 'cursor:pointer;transition:background 0.12s;';
        tr.onmouseover = function(){ this.style.background = '#f0f9ff'; };
        tr.onmouseout  = function(){ this.style.background = this._sel ? '#e0f2fe' : ''; };

        var resultBadge = isViol
            ? '<span style="background:#fee2e2;color:#991b1b;padding:1px 7px;border-radius:9999px;font-size:0.7rem;font-weight:700;">🚨 VIOLATION</span>'
            : '<span style="background:#dcfce7;color:#166534;padding:1px 7px;border-radius:9999px;font-size:0.7rem;font-weight:700;">✅ ' + row.result + '</span>';

        var speedHtml = '<span style="color:' + (overLimit ? '#dc2626' : '#16a34a') + ';font-weight:700;">' + violSpeed + ' km/h</span>';

        tr.innerHTML =
            '<td style="padding:8px 10px;"><input type="checkbox" class="kml-row-chk" data-idx="' + idx + '" style="width:16px;height:16px;cursor:pointer;accent-color:#0891b2;"></td>' +
            '<td style="padding:8px 10px;font-weight:700;color:#1e293b;">'  + (row.trainNo  || '—') + '</td>' +
            '<td style="padding:8px 10px;color:#475569;">'                  + (row.loco     || '—') + '</td>' +
            '<td style="padding:8px 10px;color:#475569;max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + (row.station || '—') + '</td>' +
            '<td style="padding:8px 10px;color:#475569;">'                  + (row.rtisTime || '—') + '</td>' +
            '<td style="padding:8px 10px;color:#475569;">'                  + (row.signalTime || '—') + '</td>' +
            '<td style="padding:8px 10px;font-weight:600;">'                + (row.sigNo    || '—') + '</td>' +
            '<td style="padding:8px 10px;color:#475569;">'                  + (row.travelDir || row.direction || '—') + '</td>' +
            '<td style="padding:8px 10px;">'                                + speedHtml + '</td>' +
            '<td style="padding:8px 10px;">'                                + resultBadge + '</td>' +
            '<td style="padding:8px 6px;color:#64748b;font-size:0.72rem;">' + (perSecondData[idx] ? perSecondData[idx].length + ' pts' : '—') + '</td>';

        tr.addEventListener('click', function(e) {
            if (e.target.tagName === 'INPUT') return;
            var chk = this.querySelector('.kml-row-chk');
            chk.checked = !chk.checked;
            this._sel = chk.checked;
            this.style.background = chk.checked ? '#e0f2fe' : '';
            _kmlUpdateCount();
        });
        tr.querySelector('input').addEventListener('change', function() {
            tr._sel = this.checked;
            tr.style.background = this.checked ? '#e0f2fe' : '';
            _kmlUpdateCount();
        });

        tbody.appendChild(tr);
    });

    if (!rowsFound) {
        tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;padding:24px;color:#94a3b8;">No rows with per-second data. Upload per-second CSV files first.</td></tr>';
    }

    _kmlUpdateCount();
    document.getElementById('kmlDialogModal').style.display = 'flex';
}

export function kmlSelectAll(val) {
    document.querySelectorAll('.kml-row-chk').forEach(function(chk) {
        chk.checked = val;
        var tr = chk.closest('tr');
        if (tr) { tr._sel = val; tr.style.background = val ? '#e0f2fe' : ''; }
    });
    _kmlUpdateCount();
}

export function closeKmlDialog() {
    document.getElementById('kmlDialogModal').style.display = 'none';
}

export async function generateSelectedKML(analysisResults, perSecondData, log) {
    var selected = [];
    document.querySelectorAll('.kml-row-chk:checked').forEach(function(chk) {
        var idx = parseInt(chk.getAttribute('data-idx'));
        if (!isNaN(idx) && analysisResults[idx] && perSecondData[idx] && perSecondData[idx].length) {
            selected.push({ row: analysisResults[idx], ri: idx });
        }
    });
    if (!selected.length) return;

    closeKmlDialog();

    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
    log('🌍 Building KML for ' + selected.length + ' selected row(s)…');

    var kml = [];
    kml.push('<?xml version="1.0" encoding="UTF-8"?>');
    kml.push('<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2">');
    kml.push('<Document>');
    kml.push('<n>' + _kmlEsc('Signal Violations — ' + new Date().toLocaleDateString()) + '</n>');
    kml.push('<description>Generated by Railway Signal Violation Dashboard</description>');
    kml.push(_kmlDocStyles());

    // Process ONE row at a time, yielding between each to keep browser alive
    for (var i = 0; i < selected.length; i++) {
        await new Promise(function(r){ setTimeout(r, 0); });
        kml.push(_kmlViolationFolder(selected[i].row, selected[i].ri, perSecondData[selected[i].ri], speedLimit));
        log('   ✓ Row ' + (i+1) + '/' + selected.length + ': Train ' + selected[i].row.trainNo + ' · ' + selected[i].row.sigNo);
    }

    await new Promise(function(r){ setTimeout(r, 0); });
    kml.push('</Document>');
    kml.push('</kml>');

    var blob = new Blob([kml.join('\n')], { type: 'application/vnd.google-earth.kml+xml' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'signal_violations_' + new Date().toISOString().slice(0, 10) + '.kml';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    log('✅ KML exported — open in Google Earth Pro or import to Google My Maps.');
}

// ── Private helpers ───────────────────────────────────────────

function _kmlUpdateCount() {
    var checked = document.querySelectorAll('.kml-row-chk:checked').length;
    var total   = document.querySelectorAll('.kml-row-chk').length;
    var label   = document.getElementById('kmlSelCount');
    if (label) label.textContent = checked + ' / ' + total + ' selected';
    var genBtn  = document.getElementById('kmlGenerateBtn');
    if (genBtn) { genBtn.disabled = checked === 0; genBtn.style.opacity = checked === 0 ? '0.45' : '1'; }
}

function _kmlEsc(str) {
    return String(str || '')
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&apos;');
}

function _kmlDocStyles() {
    return [
        '<Style id="trackGreen"><LineStyle><color>ff5ec522</color><width>3</width></LineStyle></Style>',
        '<Style id="trackAmber"><LineStyle><color>ff0b9ef5</color><width>3</width></LineStyle></Style>',
        '<Style id="trackRed"  ><LineStyle><color>ff4444ef</color><width>4</width></LineStyle></Style>',
        '<Style id="trackAnim"><LineStyle><color>600000ff</color><width>2</width></LineStyle>' +
          '<IconStyle><scale>0.5</scale><Icon><href>http://maps.google.com/mapfiles/kml/shapes/track.png</href></Icon></IconStyle></Style>',
        '<Style id="pinViolation"><IconStyle><color>ff1414ef</color><scale>1.6</scale>' +
          '<Icon><href>http://maps.google.com/mapfiles/kml/paddle/red-circle.png</href></Icon>' +
          '</IconStyle><LabelStyle><scale>1.0</scale><color>ff1414ef</color></LabelStyle></Style>',
        '<Style id="pinFsdUp"><IconStyle><color>fff65c8b</color><scale>1.2</scale>' +
          '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/triangle.png</href></Icon>' +
          '</IconStyle><LabelStyle><scale>0.8</scale></LabelStyle></Style>',
        '<Style id="pinFsdDn"><IconStyle><color>ff0b9ef5</color><scale>1.2</scale>' +
          '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/triangle.png</href></Icon>' +
          '</IconStyle><LabelStyle><scale>0.8</scale></LabelStyle></Style>',
        '<Style id="pinRtis"><IconStyle><color>fff6823b</color><scale>1.0</scale>' +
          '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/square.png</href></Icon>' +
          '</IconStyle><LabelStyle><scale>0.8</scale></LabelStyle></Style>'
    ].join('\n');
}

function _buildSpeedSegments(pts, speedLimit) {
    var segments = [], curStyle = null, curPts = [];
    for (var i = 0; i < pts.length; i++) {
        var p = pts[i];
        if (!isValidGPS(p.lat, p.lon)) continue;
        var spd   = (!isNaN(p.speed) && p.speed !== null) ? p.speed : 0;
        var style = spd > speedLimit * 1.05 ? 'trackRed'
                  : spd > speedLimit         ? 'trackAmber'
                  :                            'trackGreen';
        if (style !== curStyle) {
            if (curPts.length > 1) segments.push({ style: curStyle, pts: curPts.slice() });
            curStyle = style;
            curPts   = curPts.length ? [curPts[curPts.length - 1], p] : [p];
        } else {
            curPts.push(p);
        }
    }
    if (curPts.length > 1) segments.push({ style: curStyle, pts: curPts });
    return segments;
}

function _kmlViolationFolder(row, ri, pts, speedLimit) {
    var s = [];
    var isViol    = (row.resultClass === 'violation' || row.resultClass === 'violation-multi' || row.perSecPromoted);
    var violSpeed = (row.speedPerSec != null) ? row.speedPerSec : row.speed;
    var excess    = Math.max(0, violSpeed - speedLimit).toFixed(1);

    var violLat, violLon, violTimeISO;
    if (row.perSecMatch && row.perSecMatch.point && isValidGPS(row.perSecMatch.point.lat, row.perSecMatch.point.lon)) {
        violLat     = row.perSecMatch.point.lat;
        violLon     = row.perSecMatch.point.lon;
        violTimeISO = row.perSecMatch.point.time.toISOString();
    } else {
        violLat     = row.violationLat;
        violLon     = row.violationLon;
        violTimeISO = row.violationTimeStr || '';
    }

    s.push('<Folder>');
    s.push('<n>' + _kmlEsc('Train ' + row.trainNo + ' — ' + row.station + ' — ' + row.sigNo +
           ' [' + (isViol ? 'VIOLATION' : row.result.toUpperCase()) + ']') + '</n>');
    s.push('<open>1</open>');
    s.push('<description><![CDATA[' +
        '<b>' + (isViol ? '🚨 VIOLATION' : '✅ ' + row.result) + '</b><br/>' +
        'Train: <b>' + row.trainNo + '</b> &nbsp; Loco: ' + (row.loco || '—') + '<br/>' +
        'Station: ' + row.station + ' &nbsp; Signal: <b>' + row.sigNo + '</b><br/>' +
        'Direction: ' + (row.direction || '—') + '<br/>' +
        'SNT Time: <b>' + row.signalTime + '</b><br/>' +
        'Speed @ Signal: <b>' + violSpeed + ' km/h</b> (Limit: ' + speedLimit + ' km/h)<br/>' +
        'Excess: <b>+' + excess + ' km/h</b>' +
        ']]></description>');

    // ── 1. Animated gx:Track ──────────────────────────────────
    var validPts = [];
    for (var vi = 0; vi < pts.length; vi++) {
        if (isValidGPS(pts[vi].lat, pts[vi].lon)) validPts.push(pts[vi]);
    }
    if (validPts.length > 1) {
        var whenArr  = new Array(validPts.length);
        var coordArr = new Array(validPts.length);
        for (var wi = 0; wi < validPts.length; wi++) {
            whenArr[wi]  = '<when>'     + validPts[wi].time.toISOString() + '</when>';
            coordArr[wi] = '<gx:coord>' + validPts[wi].lon + ' ' + validPts[wi].lat + ' 0</gx:coord>';
        }
        s.push('<Folder><n>⏱ Animated Track (time slider)</n><visibility>1</visibility>');
        s.push('<Placemark><n>GPS Track — ' + _kmlEsc(String(row.trainNo)) + '</n><styleUrl>#trackAnim</styleUrl>');
        s.push('<gx:Track><altitudeMode>clampToGround</altitudeMode>');
        s.push(whenArr.join('\n'));
        s.push(coordArr.join('\n'));
        s.push('</gx:Track></Placemark></Folder>');
    }

    // ── 2. Speed-coded track ──────────────────────────────────
    s.push('<Folder><n>🎨 Speed-Coded Track</n><visibility>1</visibility>');
    var segs = _buildSpeedSegments(pts, speedLimit);
    for (var si = 0; si < segs.length; si++) {
        var seg = segs[si];
        if (seg.pts.length < 2) continue;
        var coordParts = new Array(seg.pts.length);
        for (var ci = 0; ci < seg.pts.length; ci++) {
            coordParts[ci] = seg.pts[ci].lon + ',' + seg.pts[ci].lat + ',0';
        }
        s.push('<Placemark><styleUrl>#' + seg.style + '</styleUrl>');
        s.push('<LineString><altitudeMode>clampToGround</altitudeMode><tessellate>1</tessellate>');
        s.push('<coordinates>' + coordParts.join(' ') + '</coordinates>');
        s.push('</LineString></Placemark>');
    }
    s.push('</Folder>');

    // ── 3. Violation pin ──────────────────────────────────────
    if (isValidGPS(violLat, violLon)) {
        var mqLine = row.speedMatchQ === 'exact'   ? '✓ Exact per-sec match'
                   : row.speedMatchQ === 'closest' ? '⚠ Closest match (Δ' + row.speedDiffSec + 's)'
                   :                                 'RTIS-resolution speed';
        s.push('<Folder><n>' + (isViol ? '🚨' : '✅') + ' Violation Pin</n><visibility>1</visibility>');
        s.push('<Placemark>');
        s.push('<n>' + _kmlEsc((isViol ? '🚨 VIOLATION' : '✅ ' + row.result) +
               ' · T' + row.trainNo + ' · ' + row.sigNo + ' · ' + violSpeed + ' km/h') + '</n>');
        s.push('<description><![CDATA[' +
            '<b>' + (isViol ? '🚨 VIOLATION' : '✅ ' + row.result) + '</b><hr/>' +
            'Train: <b>' + row.trainNo + '</b><br/>Loco: ' + (row.loco || '—') + '<br/>' +
            'Station: ' + row.station + '<br/>Signal: <b>' + row.sigNo + '</b> (' + (row.direction || '—') + ')<br/>' +
            'SNT Time: <b>' + row.signalTime + '</b><br/>' +
            'Speed @ Signal: <b style="color:red">' + violSpeed + ' km/h</b><br/>' +
            'Speed Limit: ' + speedLimit + ' km/h<br/>' +
            'Excess: <b>+' + excess + ' km/h</b><br/>' +
            mqLine +
            (row.perSecDemoted  ? '<br/><i>📋 Demoted: ' + _kmlEsc(row.perSecDemotedReason || '') + '</i>' : '') +
            (row.perSecPromoted ? '<br/><i>🚀 Promoted: ' + _kmlEsc(row.perSecPromotedReason || '') + '</i>' : '') +
            (row.dirMismatch    ? '<br/><i>↔ Dir mismatch: train=' + (row.direction||'?') + ' sig=' + _kmlEsc(row.dirSig||'?') + '</i>' : '') +
            ']]></description>');
        s.push('<styleUrl>#pinViolation</styleUrl>');
        if (violTimeISO) s.push('<TimeStamp><when>' + violTimeISO + '</when></TimeStamp>');
        s.push('<Point><coordinates>' + violLon + ',' + violLat + ',0</coordinates></Point>');
        s.push('</Placemark></Folder>');
    }

    // ── 4. FSD signals ────────────────────────────────────────
    var fsdSigs = (row.cachedFsdSignals || []).filter(function(sg) { return isValidGPS(sg.lat, sg.lon); });
    if (fsdSigs.length > 0) {
        s.push('<Folder><n>📡 FSD Signals</n><visibility>1</visibility>');
        for (var fi = 0; fi < fsdSigs.length; fi++) {
            var sig  = fsdSigs[fi];
            var isUp = sig.dir === 'UP';
            var dSig = isValidGPS(violLat, violLon)
                ? (getDistance(violLat, violLon, sig.lat, sig.lon) * 1000).toFixed(0) + ' m from violation'
                : 'N/A';
            s.push('<Placemark>');
            s.push('<n>' + (isUp ? '▲' : '▼') + ' ' + sig.dir + ' S' + (sig.sigNo || '?') + '</n>');
            s.push('<description><![CDATA[<b>FSD Signal</b><br/>' +
                'Station: ' + _kmlEsc(row.stationKey || row.station) + '<br/>' +
                'Direction: <b>' + sig.dir + '</b><br/>' +
                'Signal No: ' + (sig.sigNo || '?') + '<br/>' +
                'Distance: ' + dSig + ']]></description>');
            s.push('<styleUrl>#pinFsd' + (isUp ? 'Up' : 'Dn') + '</styleUrl>');
            s.push('<Point><coordinates>' + sig.lon + ',' + sig.lat + ',0</coordinates></Point>');
            s.push('</Placemark>');
        }
        s.push('</Folder>');
    }

    // ── 5. RTIS H-event ping ──────────────────────────────────
    if (isValidGPS(row.violationLat, row.violationLon)) {
        var _dToViol = isValidGPS(violLat, violLon)
            ? getDistance(violLat, violLon, row.violationLat, row.violationLon) * 1000 : 0;
        if (_dToViol > 5) {
            s.push('<Folder><n>🔵 RTIS H-Event Ping</n><visibility>1</visibility>');
            s.push('<Placemark>');
            s.push('<n>🔵 RTIS Ping — ' + _kmlEsc(String(row.trainNo)) + '</n>');
            s.push('<description><![CDATA[<b>RTIS H Event</b><br/>' +
                'Train: ' + row.trainNo + '<br/>' +
                'RTIS Speed: ' + row.speed + ' km/h<br/>' +
                'RTIS Time: ' + row.rtisTime + '<br/>' +
                'Distance from SNT-matched point: ' + _dToViol.toFixed(0) + ' m]]></description>');
            s.push('<styleUrl>#pinRtis</styleUrl>');
            s.push('<Point><coordinates>' + row.violationLon + ',' + row.violationLat + ',0</coordinates></Point>');
            s.push('</Placemark></Folder>');
        }
    }

    s.push('</Folder>');
    return s.join('\n');
}
