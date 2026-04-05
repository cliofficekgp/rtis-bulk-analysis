import { jsPDF } from 'jspdf';

// ══════════════════════════════════════════════════════════════
// PDF REPORT SYSTEM  —  extracted from main.js
// All logic is identical to the original; no core changes.
// ══════════════════════════════════════════════════════════════

// Module-level state (mirrors _pdfViolations in main.js)
var _pdfViolations = [];

// ── Public entry: open dialog ─────────────────────────────────
export function openPdfDialog(analysisResults) {
    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;

    _pdfViolations = analysisResults.filter(function(r) {
        var isViol    = r.resultClass === 'violation' || r.resultClass === 'violation-multi';
        var hasPerSec = r.speedPerSec !== null && r.speedPerSec !== undefined;
        var isRedSpeed = hasPerSec && r.speedPerSec > speedLimit;
        return isViol && isRedSpeed;
    });

    if (!_pdfViolations.length) {
        alert('No violations with per-second GPS data showing over-speed found.\n\nUpload per-second CSV files for violation rows first, then try again.');
        return;
    }

    var container = document.getElementById('pdfViolationCards');
    container.innerHTML = '';

    _pdfViolations.forEach(function(row, vi) {
        var violSpeed = (row.speedPerSec != null) ? row.speedPerSec : row.speed;
        var overSpeed = Math.max(0, violSpeed - speedLimit).toFixed(1);
        var dirLabel  = row.direction || row.dirTrain || row.dirSig || '—';

        // Outer card wrapper
        var card = document.createElement('div');
        card.style.cssText = 'border:1.5px solid #fca5a5;border-radius:10px;background:white;box-shadow:0 2px 8px rgba(153,27,27,0.08);overflow:visible;';
        card.setAttribute('data-vi', vi);

        // ── Accordion header (clickable) ──
        var hdr = document.createElement('div');
        hdr.style.cssText = 'background:linear-gradient(90deg,#1e3a5f 0%,#1e40af 100%);padding:11px 16px;display:flex;justify-content:space-between;align-items:center;cursor:pointer;border-radius:8px;transition:border-radius 0.2s;user-select:none;';
        hdr.innerHTML =
            '<div style="display:flex;align-items:center;gap:10px;">' +
                '<span style="background:#dc2626;color:white;font-size:0.7rem;font-weight:700;padding:2px 7px;border-radius:4px;">#' + (vi+1) + '</span>' +
                '<span style="color:white;font-weight:700;font-size:0.9rem;">Train ' + row.trainNo + '</span>' +
                '<span style="color:#93c5fd;font-size:0.78rem;">Loco ' + (row.loco||'—') + '</span>' +
            '</div>' +
            '<div style="display:flex;align-items:center;gap:12px;">' +
                '<span style="color:#fca5a5;font-size:0.75rem;">' + row.station + ' &nbsp;·&nbsp; ' + row.sigNo + ' &nbsp;·&nbsp; ' + row.signalTime + '</span>' +
                '<span style="background:#fee2e2;color:#dc2626;font-size:0.72rem;font-weight:700;padding:2px 8px;border-radius:4px;">' + violSpeed + ' km/h</span>' +
                '<span id="pdfArrow_'+vi+'" style="color:#93c5fd;font-size:1rem;transition:transform 0.25s;">▾</span>' +
            '</div>';

        // ── Accordion body ──
        var body = document.createElement('div');
        body.id = 'pdfBody_' + vi;
        body.style.cssText = 'padding:16px;display:block;';

        // Info chips row
        var chips = document.createElement('div');
        chips.style.cssText = 'display:flex;flex-wrap:wrap;gap:8px;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid #f1f5f9;';
        var chipData = [
            ['🚂 Train', row.trainNo, false],
            ['🔧 Loco', row.loco||'—', false],
            ['📍 Station', row.station, false],
            ['🚦 Signal', row.sigNo, false],
            ['🧭 Direction', dirLabel, false],
            ['⏱ SNT Time', row.signalTime, false],
            ['⚡ Speed', violSpeed + ' km/h', true],
            ['🏁 Limit', speedLimit + ' km/h', false],
            ['📈 Excess', '+' + overSpeed + ' km/h', true],
        ];
        chipData.forEach(function(c) {
            var chip = document.createElement('div');
            chip.style.cssText = 'background:' + (c[2]?'#fee2e2':'#f1f5f9') + ';border:1px solid ' + (c[2]?'#fca5a5':'#e2e8f0') + ';border-radius:6px;padding:5px 10px;font-size:0.78rem;';
            chip.innerHTML = '<span style="color:#64748b;font-weight:500;">' + c[0] + ': </span><span style="color:' + (c[2]?'#dc2626':'#1e293b') + ';font-weight:700;">' + c[1] + '</span>';
            chips.appendChild(chip);
        });
        body.appendChild(chips);

        // LP Details label
        var lpLabel = document.createElement('div');
        lpLabel.style.cssText = 'font-size:0.72rem;font-weight:700;color:#1e3a5f;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:10px;display:flex;align-items:center;gap:6px;';
        lpLabel.innerHTML = '<span style="display:inline-block;width:3px;height:13px;background:#1d4ed8;border-radius:2px;"></span> Loco Pilot Details';
        body.appendChild(lpLabel);

        // LP input grid
        var grid = document.createElement('div');
        grid.style.cssText = 'display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px;';

        var _sntDate = (function(){
            try {
                var d = row.sntTimeISO ? new Date(row.sntTimeISO)
                      : (row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr));
                if (isNaN(d)) return '';
                return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
            } catch(e){ return ''; }
        })();

        var _fields = [
            ['Train No.',      'trainNo_'+vi,     String(row.trainNo), 'text',   true ],
            ['LP Name',        'lpName_'+vi,      'e.g. Ramesh Kumar', 'text',   false],
            ['LP HQ',          'lpHQ_'+vi,        'e.g. Gaya',         'text',   false],
            ['LP CLI Name.',   'lpCLI_'+vi,       'e.g. Ramesh Kumar', 'text',   false],
            ['Train Date',     'trainDate_'+vi,   _sntDate,            'date',   true ],
            ['Journey Date',   'journeyDate_'+vi, _sntDate,            'date',   true ],
        ];
        _fields.forEach(function(f) {
            var wrap = document.createElement('div');
            var lbl = document.createElement('label');
            lbl.setAttribute('for', f[1]);
            lbl.style.cssText = 'display:block;font-size:0.72rem;font-weight:600;color:#475569;margin-bottom:4px;';
            lbl.textContent = f[0];
            var inp = document.createElement('input');
            inp.type = f[3];
            inp.id = f[1];
            var isAuto = f[4];
            if (isAuto) { inp.value = f[2]; }
            else        { inp.value = ''; inp.placeholder = f[2]; }
            inp.style.cssText = 'width:100%;border:1.5px solid ' + (isAuto?'#93c5fd':'#cbd5e1') + ';border-radius:6px;padding:7px 10px;font-size:0.83rem;color:#1e293b;box-sizing:border-box;outline:none;transition:border-color 0.15s;background:' + (isAuto?'#eff6ff':'white') + ';font-weight:' + (isAuto?'700':'400') + ';';
            inp.addEventListener('focus', function(){ this.style.borderColor='#1d4ed8'; this.style.boxShadow='0 0 0 3px rgba(29,78,216,0.12)'; });
            inp.addEventListener('blur',  function(){
                this.style.borderColor = isAuto ? '#93c5fd' : '#cbd5e1';
                this.style.boxShadow='none';
            });
            wrap.appendChild(lbl);
            wrap.appendChild(inp);
            grid.appendChild(wrap);
        });
        body.appendChild(grid);

        // Remarks
        var remWrap = document.createElement('div');
        var remLbl = document.createElement('label');
        remLbl.setAttribute('for', 'remarks_'+vi);
        remLbl.style.cssText = 'display:block;font-size:0.72rem;font-weight:600;color:#475569;margin-bottom:4px;';
        remLbl.textContent = 'Remarks / Additional Notes (optional)';
        var remTa = document.createElement('textarea');
        remTa.id = 'remarks_' + vi;
        remTa.rows = 2;
        remTa.placeholder = 'Any additional information about this violation…';
        remTa.style.cssText = 'width:100%;border:1.5px solid #cbd5e1;border-radius:6px;padding:7px 10px;font-size:0.83rem;color:#1e293b;resize:vertical;box-sizing:border-box;outline:none;transition:border-color 0.15s;background:white;font-family:inherit;';
        remTa.addEventListener('focus', function(){ this.style.borderColor='#1d4ed8'; this.style.boxShadow='0 0 0 3px rgba(29,78,216,0.12)'; });
        remTa.addEventListener('blur',  function(){ this.style.borderColor='#cbd5e1'; this.style.boxShadow='none'; });
        remWrap.appendChild(remLbl);
        remWrap.appendChild(remTa);
        body.appendChild(remWrap);

        // ── Delete button ──
        var delBtn = document.createElement('button');
        delBtn.title = 'Remove this violation from PDF batch';
        delBtn.style.cssText = 'background:rgba(220,38,38,0.18);border:1.5px solid rgba(252,165,165,0.5);color:#fca5a5;border-radius:5px;width:24px;height:24px;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:0.85rem;line-height:1;flex-shrink:0;padding:0;';
        delBtn.innerHTML = '&#10005;';
        delBtn.addEventListener('click', function(e) {
            e.stopPropagation();
            e.preventDefault();
            card.style.transition = 'opacity 0.18s';
            card.style.opacity = '0';
            setTimeout(function() {
                if (card.parentNode) card.parentNode.removeChild(card);
                var remaining = document.getElementById('pdfViolationCards').children.length;
                var countEl = document.getElementById('pdfDialogViolCount');
                if (countEl) countEl.textContent = remaining + ' violation' + (remaining !== 1 ? 's' : '') + ' — fill LP details below';
            }, 180);
        });
        hdr.lastElementChild.appendChild(delBtn);

        // Toggle accordion on header click
        hdr.addEventListener('click', function() {
            var isOpen = body.style.display !== 'none';
            body.style.display = isOpen ? 'none' : 'block';
            document.getElementById('pdfArrow_'+vi).style.transform = isOpen ? 'rotate(-90deg)' : 'rotate(0deg)';
            if (isOpen) {
                hdr.style.borderRadius = '8px';
            } else {
                hdr.style.borderRadius = '8px 8px 0 0';
            }
        });
        // Start open for first card, closed for rest
        if (vi > 0) {
            body.style.display = 'none';
            hdr.style.borderRadius = '8px';
            setTimeout(function(idx){ return function(){ var a=document.getElementById('pdfArrow_'+idx); if(a) a.style.transform='rotate(-90deg)'; }; }(vi), 0);
        }

        card.appendChild(hdr);
        card.appendChild(body);
        container.appendChild(card);
    });

    document.getElementById('pdfDialogViolCount').textContent = _pdfViolations.length + ' violation' + (_pdfViolations.length!==1?'s':'') + ' — fill LP details below';
    var modal = document.getElementById('pdfDialogModal');
    modal.style.display = 'flex';
    container.scrollTop = 0;
}

export function closePdfDialog() {
    document.getElementById('pdfDialogModal').style.display = 'none';
}

export function pdfExpandAll(open) {
    _pdfViolations.forEach(function(_, vi) {
        var body  = document.getElementById('pdfBody_' + vi);
        var arrow = document.getElementById('pdfArrow_' + vi);
        if (!body || !arrow) return;
        body.style.display  = open ? 'block' : 'none';
        arrow.style.transform = open ? 'rotate(0deg)' : 'rotate(-90deg)';
    });
}

export async function generateAllPdfs(analysisResults, _captureMapForRow, _captureChartForRow, _reportProgress) {
    if (!_pdfViolations.length) return;

    var container = document.getElementById('pdfViolationCards');
    var activeVis = Array.from(container.querySelectorAll('[data-vi]'))
                         .map(function(el){ return parseInt(el.getAttribute('data-vi')); });

    if (!activeVis.length) { alert('No violations remaining — all were deleted.'); return; }

    // Capture LP field values NOW (before dialog closes)
    var lpData = {};
    activeVis.forEach(function(vi) {
        lpData[vi] = {
            trainNo:     (document.getElementById('trainNo_'     + vi) || {value:''}).value || String(_pdfViolations[vi].trainNo),
            lpName:      (document.getElementById('lpName_'      + vi) || {value:''}).value || '',
            lpHQ:        (document.getElementById('lpHQ_'        + vi) || {value:''}).value || '',
            lpCLI:       (document.getElementById('lpCLI_'       + vi) || {value:''}).value || '',
            trainDate:   (document.getElementById('trainDate_'   + vi) || {value:''}).value || '',
            journeyDate: (document.getElementById('journeyDate_' + vi) || {value:''}).value || '',
            remarks:     (document.getElementById('remarks_'     + vi) || {value:''}).value || ''
        };
    });

    closePdfDialog();

    var modal = document.getElementById('reportProgressModal');
    modal.style.display = 'flex';
    _reportProgress('Starting PDF generation…', 0);

    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
    var total = activeVis.length;

    for (var i = 0; i < total; i++) {
        var vi  = activeVis[i];
        var row = _pdfViolations[vi];
        var resultIdx = analysisResults.indexOf(row);

        var trainNoEdited = lpData[vi].trainNo;
        var lpName = lpData[vi].lpName;
        var lpHQ   = lpData[vi].lpHQ;
        var lpCLI  = lpData[vi].lpCLI;
        var remarks= lpData[vi].remarks;

        var pct = Math.round((i / total) * 85);
        _reportProgress('(' + (i+1) + '/' + total + ') Train ' + row.trainNo + ' — opening map, zooming ×3, screenshotting…', pct);
        await new Promise(function(r){ setTimeout(r, 80); });

        var mapDataUrl   = await _captureMapForRow(row, resultIdx);
        _reportProgress('(' + (i+1) + '/' + total + ') Train ' + row.trainNo + ' — rendering speed graph…', pct + 5);
        var chartDataUrl = await _captureChartForRow(row, resultIdx);

        _reportProgress('(' + (i+1) + '/' + total + ') Train ' + row.trainNo + ' — building PDF…', pct + 8);
        await new Promise(function(r){ setTimeout(r, 20); });

        var trainDate   = lpData[vi].trainDate;
        var journeyDate = lpData[vi].journeyDate;
        await _buildSinglePdf(row, speedLimit, trainNoEdited, lpName, lpHQ, lpCLI, trainDate, journeyDate, remarks, mapDataUrl, chartDataUrl);
    }

    _reportProgress('Done! ' + total + ' PDF(s) generated.', 100);
    await new Promise(function(r){ setTimeout(r, 1800); });
    modal.style.display = 'none';
}

// ── Private: build and save one PDF ──────────────────────────
async function _buildSinglePdf(row, speedLimit, trainNoEdited, lpName, lpHQ, lpCLI, trainDate, journeyDate, remarks, mapDataUrl, chartDataUrl) {
    var doc = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
    var W = 210, H = 297;
    var mg = 10;
    var cW = W - mg*2;
    var y = 0;

    var violSpeed = (row.speedPerSec != null) ? row.speedPerSec : row.speed;
    var overSpeed = Math.max(0, violSpeed - speedLimit).toFixed(1);
    var dirLabel  = row.direction || row.dirTrain || row.dirSig || '—';
    var sigLabel  = (dirLabel === 'UP' ? 'UP' : dirLabel === 'DN' ? 'DN' : dirLabel) + ' Home Signal';
    var today     = new Date().toLocaleDateString('en-IN', { day:'2-digit', month:'short', year:'numeric' });
    function _fmtDate(iso) {
        if (!iso) return '—';
        try { var d = new Date(iso); return d.toLocaleDateString('en-IN', {day:'2-digit',month:'short',year:'numeric'}); }
        catch(e){ return iso; }
    }

    // ── Header Banner (18mm) ──
    doc.setFillColor(120, 20, 20);
    doc.rect(0, 0, W, 18, 'F');
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(14);
    doc.setTextColor(255, 255, 255);
    doc.text('SIGNAL VIOLATION REPORT', W/2, 8.5, { align: 'center' });
    doc.setFontSize(7.5);
    doc.setFont('helvetica', 'normal');
    doc.text('Generated: ' + today + '   |   Train: ' + trainNoEdited + '   |   Station: ' + row.station + '   |   SNT Time: ' + row.signalTime, W/2, 14.5, { align: 'center' });
    y = 21;

    // ── Train & Signal Info ──
    y = _pdfSH(doc, 'TRAIN & SIGNAL INFORMATION', y, mg, W);
    y = _pdfTbl(doc, [
        ['Train No.',    String(trainNoEdited),     'Loco No.',     String(row.loco || '—')],
        ['Station',      String(row.station),      'Signal No.',   String(row.sigNo)],
        ['Signal Type',  sigLabel,                 'Direction',    dirLabel],
        ['SNT Time',     String(row.signalTime),   'RTIS Speed',   String(row.speed) + ' km/h'],
    ], y, mg, W, false, 5);
    y += 2;

    // ── Violation Summary ──
    y = _pdfSH(doc, 'VIOLATION SUMMARY', y, mg, W);
    y = _pdfTbl(doc, [
        ['Speed at Signal', violSpeed + ' km/h',  'Speed Limit',  speedLimit + ' km/h'],
        ['Excess Speed',   '+' + overSpeed + ' km/h', 'Data Source', 'Per-Sec GPS (verified)'],
    ], y, mg, W, true, 5);
    y += 2;

    // ── LP Details ──
    y = _pdfSH(doc, 'LOCO PILOT DETAILS', y, mg, W);
    y = _pdfTbl(doc, [
        ['LP Name',      lpName  || '—',            'LP HQ',        lpHQ    || '—'],
        ['LP CLI Name.', lpCLI   || '—',            'Remarks',      remarks || '—'],
        ['Train Date',   _fmtDate(trainDate),       'Journey Date', _fmtDate(journeyDate)],
    ], y, mg, W, false, 5);
    y += 2;

    // ── Map ──
    var remaining = H - y - 14;
    var mapH  = Math.min(Math.round(remaining * 0.52), 78);
    var chartH = remaining - mapH - 16;

    y = _pdfSH(doc, 'LOCATION MAP', y, mg, W);
    if (mapDataUrl) {
        try { doc.addImage(mapDataUrl, 'PNG', mg, y, cW, mapH); }
        catch(e) { _pdfNoImage(doc, 'Map unavailable', row.violationLat, row.violationLon, mg, y, cW, mapH); }
    } else {
        _pdfNoImage(doc, 'Map unavailable', row.violationLat, row.violationLon, mg, y, cW, mapH);
    }
    y += mapH + 2;

    // ── Speed Graph ──
    y = _pdfSH(doc, 'SPEED PROFILE GRAPH', y, mg, W);
    if (chartDataUrl && chartH > 15) {
        try { doc.addImage(chartDataUrl, 'PNG', mg, y, cW, chartH); }
        catch(e) { doc.setFontSize(8); doc.setTextColor(150,150,150); doc.text('[Speed graph unavailable]', mg, y + 6); }
    }

    // ── Footer ──
    doc.setDrawColor(180, 30, 30);
    doc.setLineWidth(0.35);
    doc.line(mg, H - 10, W - mg, H - 10);
    doc.setFontSize(7); doc.setTextColor(140,140,140); doc.setFont('helvetica','normal');
    doc.text('Railway Signal Violation Dashboard  |  Confidential  |  ' + today, mg, H - 5.5);
    doc.text('Page 1 of 1', W - mg, H - 5.5, { align: 'right' });

    // ── Save ──
    function _safe(val, fb) {
        return String(val||fb||'NA').trim().replace(/[^a-zA-Z0-9]/g,'_').replace(/_+/g,'_').replace(/^_|_$/g,'')||'NA';
    }
    var _violDate = (function(){
        try {
            var d = row.sntTimeISO ? new Date(row.sntTimeISO)
                  : (row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr));
            if (isNaN(d)) throw 0;
            return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
        } catch(e){ return new Date().toISOString().slice(0,10); }
    })();
    doc.save(
        'Violation_' + _safe(trainNoEdited) +
        '_' + _safe(row.loco) +
        '_' + _safe(lpName,'NA') +
        '_' + _safe(lpHQ,'NA') +
        '_' + _safe(String(row.station).split(/[\s\u2192\->]+/)[0]) +
        '_' + _safe(row.sigNo) +
        '_' + _violDate + '.pdf'
    );
}

// ── Private: PDF helpers ──────────────────────────────────────
function _pdfNoImage(doc, label, lat, lon, x, y, w, h) {
    doc.setFillColor(248, 250, 252);
    doc.rect(x, y, w, h, 'F');
    doc.setDrawColor(203,213,225); doc.setLineWidth(0.3);
    doc.rect(x, y, w, h);
    doc.setFontSize(9); doc.setTextColor(148,163,184); doc.setFont('helvetica','italic');
    doc.text(label + (lat ? '  (GPS: ' + (lat||'') + ', ' + (lon||'') + ')' : ''), x + w/2, y + h/2, { align:'center' });
}

function _pdfSH(doc, text, y, mg, W) {
    doc.setFillColor(30, 58, 95);
    doc.rect(mg, y, W - mg*2, 6, 'F');
    doc.setFont('helvetica', 'bold');
    doc.setFontSize(7.5);
    doc.setTextColor(255, 255, 255);
    doc.text(text, mg + 2.5, y + 4.2);
    return y + 7;
}

function _pdfTbl(doc, rows, y, mg, W, highlight, rowH) {
    rowH = rowH || 6;
    var colW = (W - mg*2) / 4;
    rows.forEach(function(r) {
        doc.setFillColor(248, 250, 252);
        doc.rect(mg,           y, colW, rowH, 'F');
        doc.rect(mg + colW*2,  y, colW, rowH, 'F');
        if (highlight) { doc.setFillColor(254, 226, 226); } else { doc.setFillColor(255,255,255); }
        doc.rect(mg + colW,    y, colW, rowH, 'F');
        doc.rect(mg + colW*3,  y, colW, rowH, 'F');
        doc.setDrawColor(209, 213, 219); doc.setLineWidth(0.18);
        doc.rect(mg, y, W - mg*2, rowH);
        doc.line(mg + colW,   y, mg + colW,   y + rowH);
        doc.line(mg + colW*2, y, mg + colW*2, y + rowH);
        doc.line(mg + colW*3, y, mg + colW*3, y + rowH);
        doc.setFont('helvetica', 'normal'); doc.setFontSize(6.8); doc.setTextColor(100,116,139);
        doc.text(String(r[0]), mg + 1.5,           y + rowH - 1.8);
        doc.text(String(r[2]), mg + colW*2 + 1.5,  y + rowH - 1.8);
        var vc = highlight ? [180,20,20] : [15,23,42];
        doc.setFont('helvetica', 'bold'); doc.setFontSize(8); doc.setTextColor(vc[0],vc[1],vc[2]);
        var v1 = doc.splitTextToSize(String(r[1]||'—'), colW - 3);
        var v2 = doc.splitTextToSize(String(r[3]||'—'), colW - 3);
        doc.text(v1[0]||'', mg + colW + 1.5,   y + rowH - 1.5);
        doc.text(v2[0]||'', mg + colW*3 + 1.5, y + rowH - 1.5);
        y += rowH;
    });
    return y;
}
