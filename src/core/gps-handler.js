import Papa from 'papaparse';
import { getCol, parseSmartDate, isValidGPS, getDistance } from '../utils/helpers.js';

// ══════════════════════════════════════════════════════════
// EXACT SNT-TIME MATCHING  (±5 s tolerance)
// Returns { point, idx, diffMs, quality }
//   quality: 'exact'   → diff ≤ 5 s
//            'closest' → diff > 5 s  (fallback, show warning)
//            'none'    → no per-sec data at all
// ══════════════════════════════════════════════════════════
export function matchPerSecToSNT(perSecArr, sntTimeObj) {
    if (!perSecArr || perSecArr.length === 0) return { point: null, diffMs: null, quality: 'none' };

    var sntMs   = sntTimeObj instanceof Date ? sntTimeObj.getTime() : new Date(sntTimeObj).getTime();
    var EXACT_MS = 5000;   // ±5 seconds

    var bestIdx  = 0;
    var bestDiff = Infinity;

    perSecArr.forEach(function(p, i) {
        if (!p.time) return;
        var d = Math.abs(p.time.getTime() - sntMs);
        if (d < bestDiff) { bestDiff = d; bestIdx = i; }
    });

    var quality = bestDiff <= EXACT_MS ? 'exact' : 'closest';
    return {
        point  : perSecArr[bestIdx],
        idx    : bestIdx,
        diffMs : bestDiff,
        quality: quality
    };
}

// ==========================================
// PER-SECOND FILE UPLOAD HANDLER
// ==========================================
// ctx: {
//   analysisResults, perSecondData, fsdMap, _batchRunning,
//   log, renderTable, saveSession, showDemotionToast,
//   _updateKmlButton, _initFilterUI, _fc,
//   getSpeedLimit(), getFsdThreshold()
// }
export function handlePerSecFile(event, rowIdx, _onDone, ctx) {
    var file = event.target.files[0];
    if (!file) { if (_onDone) _onDone(); return; }

    ctx.log("📁 Loading per-second data for row " + rowIdx + ": " + file.name);

    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        dynamicTyping: true,
        complete: function(results) {
            var data = results.data;

            // Parse per-second rows
            var parsedData = [];
            data.forEach(function(row) {
                var timeRaw = getCol(row, ['Logging Time','TIME','Time','EventTime']);
                var lat     = parseFloat(getCol(row, ['Latitude','Lattitude','LAT','Lat']));
                var lon     = parseFloat(getCol(row, ['Longitude','LON','Lon','LONGITUDE']));
                var speed   = parseFloat(getCol(row, ['Speed','SPEED']));
                var time    = parseSmartDate(timeRaw);
                if (time && isValidGPS(lat, lon)) {
                    parsedData.push({
                        time : time,
                        lat  : lat,
                        lon  : lon,
                        speed: speed,
                        stn  : getCol(row, ['last/cur stationCode','Station','STATION','STN_CODE']) || '—',
                        distFromSpeed: (function(){ var d=parseFloat(getCol(row,['distFromSpeed','DistFromSpeed','DISTFROMSPEED','dist_from_speed'])); return isNaN(d)?null:d; })()
                    });
                }
            });

            // Sort by time ascending
            parsedData.sort(function(a, b) { return a.time - b.time; });
            ctx.perSecondData[rowIdx] = parsedData;

            // Enable KML export button now that per-sec data exists
            ctx._updateKmlButton();

            // ── EXACT SNT-TIME MATCH ──────────────────────────────
            var row = ctx.analysisResults[rowIdx];
            if (row) {
                // Build SNT Date from stored signal time string + violation date
                var sntBase = row.violationTime instanceof Date
                                ? row.violationTime
                                : new Date(row.violationTimeStr);

                // Try to reconstruct SNT datetime: use sntTimeISO if stored, else parse signalTime
                var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
                    var d    = new Date(sntBase);
                    var tStr = row.signalTime || '';
                    var tp   = tStr.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
                    if (tp) {
                        d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0);
                    }
                    return d;
                })();

                var match = matchPerSecToSNT(parsedData, sntDateObj);

                // Store match result on the row object
                row.perSecMatch = match;

                var speedLimit = ctx.getSpeedLimit();
                if (match.quality !== 'none' && match.point) {
                    var diffSec = (match.diffMs / 1000).toFixed(1);
                    var speedPs = !isNaN(match.point.speed) ? match.point.speed : null;

                    ctx.log('🎯 SNT-time match [' + match.quality + ']: diff=' + diffSec + 's' +
                        (speedPs !== null ? ', speed=' + speedPs + ' km/h' : '') +
                        ' | resultClass=' + row.resultClass +
                        ' dirMismatch=' + row.dirMismatch);

                    // ── STEP 1: Store per-sec speed on row FIRST ──────────────────────────────
                    if (speedPs !== null) {
                        row.speedPerSec  = speedPs;
                        row.speedMatchQ  = match.quality;
                        row.speedDiffSec = diffSec;
                    }

                    // ── STEP 2: Shared FSD distance calculation ──────────────────────────────
                    var pLat = match.point.lat;
                    var pLon = match.point.lon;
                    var trainDir = row.direction || row.dirTrain || '';
                    var stnKey   = row.fsdStation;
                    var psDistToFsd = null;
                    var fsdThreshold = ctx.getFsdThreshold();

                    if (speedPs !== null && speedPs > speedLimit && isValidGPS(pLat, pLon)) {
                        var _sigList = [];
                        if (stnKey && ctx.fsdMap[stnKey]) {
                            var _ds = (ctx.fsdMap[stnKey][trainDir] || []);
                            if (!_ds.length) _ds = (ctx.fsdMap[stnKey].UP || []).concat(ctx.fsdMap[stnKey].DN || []);
                            _sigList = _ds;
                        } else if (row.cachedFsdSignals && row.cachedFsdSignals.length) {
                            _sigList = row.cachedFsdSignals.filter(function(s){ return s.dir === trainDir; });
                            if (!_sigList.length) _sigList = row.cachedFsdSignals;
                            ctx.log('ℹ️  Using cachedFsdSignals for FSD dist (fsdMap not in memory)');
                        }
                        if (_sigList.length) {
                            var minD = Infinity;
                            _sigList.forEach(function(sig) {
                                if (!isValidGPS(sig.lat, sig.lon)) return;
                                var d = getDistance(pLat, pLon, sig.lat, sig.lon) * 1000;
                                if (d < minD) minD = d;
                            });
                            if (isFinite(minD)) psDistToFsd = minD;
                            ctx.log('📍 per-sec→FSD dist = ' + psDistToFsd.toFixed(0) + 'm (threshold=' + fsdThreshold + 'm, dir=' + trainDir + ')');
                        }
                    }

                    // ── STEP 3: DEMOTION — violation row that is far from FSD signal ────────
                    var classChanged = false;
                    if (speedPs !== null && speedPs > speedLimit &&
                        (row.resultClass === 'violation' || row.resultClass === 'violation-multi') &&
                        psDistToFsd !== null && psDistToFsd > fsdThreshold) {

                        row.resultClass   = 'complied';
                        row.result        = 'Complied';
                        row.perSecDemoted = true;
                        row.perSecDemotedReason =
                            'Per-sec GPS at SNT time is ' + psDistToFsd.toFixed(0) +
                            ' m from FSD signal (' + trainDir + ') — exceeds threshold ' +
                            fsdThreshold + ' m. Train was not at the signal when SNT fired.';
                        ctx.log('✅ DEMOTED to Complied: dist ' + psDistToFsd.toFixed(0) + 'm > ' + fsdThreshold + 'm, row ' + rowIdx);
                        ctx.showDemotionToast(row.trainNo, psDistToFsd, trainDir, row.perSecDemotedReason);
                        classChanged = true;
                    }

                    // ── STEP 4: PROMOTION — complied row with per-sec overspeed ──
                    if (speedPs !== null && speedPs > speedLimit &&
                        row.resultClass === 'complied' &&
                        !row.dirMismatch &&
                        !row.perSecDemoted) {

                        var tooFarFromSignal = (psDistToFsd !== null && psDistToFsd > fsdThreshold);
                        if (!tooFarFromSignal) {
                            row.resultClass    = 'violation';
                            row.result         = 'VIOLATION';
                            row.perSecPromoted = true;
                            row.perSecPromotedReason =
                                'Per-sec GPS confirms ' + speedPs + ' km/h at SNT time — ' +
                                'exceeds ' + speedLimit + ' km/h limit.' +
                                (psDistToFsd !== null
                                    ? ' GPS distance to FSD signal: ' + psDistToFsd.toFixed(0) + 'm (within ' + fsdThreshold + 'm).'
                                    : ' (No FSD distance check — FSD data not available.)') +
                                ' RTIS speed was ' + row.speed + ' km/h (under limit at minute precision).';
                            ctx.log('🚨 PROMOTED to VIOLATION: row=' + rowIdx +
                                ' train=' + row.trainNo +
                                ' per-sec=' + speedPs + ' km/h > ' + speedLimit + ' km/h' +
                                (psDistToFsd !== null ? ' dist=' + psDistToFsd.toFixed(0) + 'm' : ' no-fsd-dist'));
                            classChanged = true;
                        } else {
                            ctx.log('ℹ️ Complied row NOT promoted: overspeed but GPS ' +
                                psDistToFsd.toFixed(0) + 'm > threshold ' + fsdThreshold + 'm — not at signal point.');
                        }
                    }

                    // ── STEP 5: Re-render + save if classification changed ────────────────
                    if (classChanged) {
                        if (!ctx.isBatchRunning()) { ctx.renderTable(); ctx.saveSession(); }
                    } else if (speedPs !== null) {
                        // No class change — just patch the speed cell in place (faster than re-render)
                        ['violation','complied','ambiguous'].forEach(function(tabId) {
                            var tbody = document.getElementById('tbody-' + tabId);
                            if (!tbody) return;
                            var gBtn = tbody.querySelector('#graphBtn' + rowIdx);
                            if (!gBtn) return;
                            var tr = gBtn.closest('tr');
                            if (!tr) return;
                            var tds = tr.querySelectorAll('td');
                            if (tds.length > 7) {
                                var colorCls = speedPs > speedLimit ? 'speed-warn' : 'speed-exact';
                                var badgeCls = match.quality === 'exact' ? 'match-badge-exact' : 'match-badge-tol';
                                var badgeTxt = match.quality === 'exact' ? '✓ exact' : '⚠ +' + diffSec + 's';
                                tds[7].innerHTML =
                                    '<span class="' + colorCls + '">' + speedPs + '</span>' +
                                    '<span class="' + badgeCls + '" title="Per-second speed at SNT time (diff=' + diffSec + 's)">' + badgeTxt + '</span>';
                            }
                            if (tds.length > 11) {
                                tds[11].innerHTML = '<span class="precision-flag-sec" title="Per-second file loaded">✓ sec</span>';
                            }
                        });
                    }

                    // Update Map + Graph button styles
                    ctx._updateKmlButton();
                    var mapBtnDom = document.getElementById('mapBtn' + rowIdx);
                    if (mapBtnDom) {
                        mapBtnDom.style.background = 'linear-gradient(135deg,#059669 0%,#047857 100%)';
                        mapBtnDom.title = 'View detailed track (' + parsedData.length + ' pts)';
                    }
                    var graphBtnDom = document.getElementById('graphBtn' + rowIdx);
                    if (graphBtnDom) {
                        graphBtnDom.classList.add('has-persec');
                        graphBtnDom.title = 'Speed-Time Graph (±3 min per-second · exact SNT match)';
                    }

                    // ── Warn if outside ±5 s ─────────────────────────────
                    if (match.quality === 'closest') {
                        ctx.log('⚠️  Closest point is ' + diffSec + 's away from SNT time — outside ±5s tolerance. Marker shown with warning.');
                    }

                } else {
                    ctx.log('⚠️  No per-second points to match for row ' + rowIdx);
                }
            }

            // MOD4: init filter context (for the currently open graph if same row)
            if (ctx._fc && ctx._fc.resultIdx === rowIdx) {
                ctx._initFilterUI(parsedData, ctx._fc.sntMs, rowIdx, ctx._fc.row);
            }

            ctx.log("✅ Loaded " + parsedData.length + " per-second points for row " + rowIdx);
            if (_onDone) _onDone();   // signal batch queue: this file is done
        },
        error: function(err) {
            ctx.log("❌ Error parsing per-second file: " + err.message);
            if (!ctx.isBatchRunning()) alert("Error parsing CSV file. Check format.");
            if (_onDone) _onDone();   // still resolve so queue continues
        }
    });
}

// ══════════════════════════════════════════════════════════
// BATCH PER-SEC UPLOAD
// Matches files to violation rows by loco number in filename
// ══════════════════════════════════════════════════════════
// ctx: same as handlePerSecFile ctx, plus:
//   setBatchState(running, total, done)
export async function handleBatchPerSec(event, ctx) {
    var files = Array.from(event.target.files);
    if (!files.length) return;
    event.target.value = '';   // reset immediately so same files can be re-selected

    ctx.log('📂 Batch upload: ' + files.length + ' file(s) selected — building queue...');

    // ── Build assignment list (file → [rowIdx, ...]) synchronously ────────
    var assignments = [];   // [{ file, rowIdx, label }]
    var skipped = 0;

    files.forEach(function(file) {
        var fname = file.name;
        var nums  = fname.match(/\d{4,}/g) || [];
        var rowsForFile = [];

        // Priority 1: Device ID (first number in filename)
        if (nums.length) {
            var firstNum = nums[0];
            ctx.analysisResults.forEach(function(r, ri) {
                if (String(r.deviceId || '').trim() === firstNum && r.speed > 50) rowsForFile.push(ri);
            });
            if (rowsForFile.length)
                ctx.log('📡 Device ID match: ' + fname + ' → Device ' + firstNum + ' (' + rowsForFile.length + ' row(s) > 50 km/h)');
        }

        // Priority 2: Loco number fallback
        if (rowsForFile.length === 0 && nums.length) {
            var matchedLocos = new Set();
            nums.forEach(function(n) {
                var loco = parseInt(n);
                ctx.analysisResults.forEach(function(r) { 
                    if (parseInt(r.loco) === loco && r.speed > 50) matchedLocos.add(loco); 
                });
            });
            for (var ri2 = 0; ri2 < ctx.analysisResults.length; ri2++) {
                var r = ctx.analysisResults[ri2];
                if (matchedLocos.has(parseInt(r.loco)) && r.speed > 50) rowsForFile.push(ri2);
            }
            if (rowsForFile.length)
                ctx.log('🔢 Loco fallback: ' + fname + ' → loco(s) ' + Array.from(matchedLocos).join(',') + ' (' + rowsForFile.length + ' row(s) > 50 km/h)');
        }

        if (rowsForFile.length === 0) {
            ctx.log('⚠️  Skipped: ' + fname + ' — No matched rows with speed > 50 km/h (nums: ' + nums.join(',') + ')');
            skipped++;
            return;
        }

        rowsForFile.forEach(function(ri) {
            var r = ctx.analysisResults[ri];
            assignments.push({
                file: file,
                rowIdx: ri,
                label: fname + ' → Dev ' + (r.deviceId || '?') + ' / Loco ' + r.loco +
                       ' [' + r.station + ' · ' + r.sigNo + ']'
            });
        });
    });

    if (assignments.length === 0) {
        ctx.log('⚠️  No rows matched — nothing to process.');
        return;
    }

    // ── Show progress bar ──────────────────────────────────────────────────
    var prog = document.getElementById('batchProgressBar');
    var progWrap = document.getElementById('batchProgressWrap');
    var progLabel = document.getElementById('batchProgressLabel');
    if (progWrap) {
        progWrap.style.display = 'block';
        prog.style.width = '0%';
        progLabel.textContent = '0 / ' + assignments.length;
    }

    // ── Serial queue: one file at a time ──────────────────────────────────
    ctx.setBatchState(true, assignments.length, 0);

    ctx.log('🔄 Processing ' + assignments.length + ' assignment(s) serially (1 at a time)...');

    for (var ai = 0; ai < assignments.length; ai++) {
        var asgn = assignments[ai];
        ctx.log('  [' + (ai + 1) + '/' + assignments.length + '] ' + asgn.label);

        await new Promise(function(resolve) {
            handlePerSecFile({ target: { files: [asgn.file] } }, asgn.rowIdx, resolve, ctx);
        });

        ctx.setBatchState(true, assignments.length, ai + 1);
        if (progWrap) {
            var pct = Math.round(100 * (ai + 1) / assignments.length);
            prog.style.width = pct + '%';
            progLabel.textContent = (ai + 1) + ' / ' + assignments.length;
        }

        // Yield to browser every 5 files to keep UI responsive
        if ((ai + 1) % 5 === 0) {
            await new Promise(function(r){ setTimeout(r, 30); });
        }
    }

    // ── All done: single renderTable + saveSession ────────────────────────
    ctx.setBatchState(false, 0, 0);
    ctx.renderTable();
    ctx.saveSession();

    if (progWrap) {
        prog.style.width = '100%';
        progLabel.textContent = '✓ Done';
        setTimeout(function(){ progWrap.style.display = 'none'; }, 2500);
    }

    ctx.log('✅ Batch complete: ' + assignments.length + ' processed, ' + skipped + ' skipped.');
}
