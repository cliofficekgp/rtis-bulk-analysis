import { getCol, parseSmartDate, cleanStationName, isValidGPS, getDistance,
         extractSignalNumber, extractDirectionFromMessage, getDirectionFromSignal,
         getDistanceLabel } from '../utils/helpers.js';

// ══════════════════════════════════════════════════════════════
// CORE ANALYSIS ENGINE — extracted from main.js
// All logic is identical to the original; no core changes.
// ══════════════════════════════════════════════════════════════

/**
 * executeAnalysis — pure computation, no DOM side effects.
 * Reads config values from the DOM (inputs), but writes nothing to DOM.
 *
 * Returns: { analysisResults, noTrainResults, fsdMap, allFsdStationNames, counts }
 */
export function executeAnalysis(dataRTIS, dataSNT, dataFSD, stationMappingCache, manualOverrides, log) {

    var timeWindow = parseInt(document.getElementById('inputTimeWindow').value);
    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value);
    var maxDist    = parseFloat(document.getElementById('inputMaxDist').value);

    log('🚀 === ANALYSIS STARTED ===');
    log('⚙️  Configuration: TimeWindow=' + timeWindow + 's, SpeedLimit=' + speedLimit + 'km/h, MaxDist=' + maxDist + 'km');

    // ── Reset ────────────────────────────────────────────────────
    var cntTotal = 0, cntNoTrain = 0, cntFuzzyMatches = 0;
    var analysisResults = [];
    var noTrainResults  = [];

    // ── Step 1: Map FSD Data ────────────────────────────────────
    var fsdMap = {};
    dataFSD.forEach(function(row) {
        var station = getCol(row, ['Station','STATION','STN_CODE','Station Name']);
        var sigNo   = getCol(row, ['SIGNUMBER','Signal','SIGNAL','Signal No','SIG_ID']);
        var lat     = getCol(row, ['Latitude','Lattitude','LAT','Lat','GPS_LAT']);
        var lon     = getCol(row, ['Longitude','LON','Lon','GPS_LON']);
        var dir     = getCol(row, ['DIRN','Direction','DIR']);

        if (station) {
            station = cleanStationName(station);
            if (!station) return;
            if (!fsdMap[station]) fsdMap[station] = { UP:[], DN:[] };
            var direction = String(dir || '').toUpperCase().trim();
            if (direction === 'UP' || direction === 'DN') {
                fsdMap[station][direction].push({
                    lat: parseFloat(lat),
                    lon: parseFloat(lon),
                    id: sigNo
                });
            }
        }
    });

    log('✅ Step 1: Mapped ' + Object.keys(fsdMap).length + ' stations from FSD master data');
    var allFsdStationNames = Object.keys(fsdMap).sort();

    var totalUpSignals = 0, totalDnSignals = 0;
    Object.keys(fsdMap).forEach(function(stn) {
        totalUpSignals += fsdMap[stn].UP.length;
        totalDnSignals += fsdMap[stn].DN.length;
    });
    log('   📍 Total UP signals: ' + totalUpSignals + ', DN signals: ' + totalDnSignals);

    // ── Step 2: Pre-process RTIS ────────────────────────────────
    var rtisByStation = {}, rtisStationGPS = {}, allRtisByLoco = {};

    dataRTIS.forEach(function(row) {
        var stn     = getCol(row, ['Station','STATION','STN_CODE']);
        var timeRaw = getCol(row, ['Event Time','TIME','EventTime','Date']);
        var lat     = parseFloat(getCol(row, ['Latitude','Lattitude','LAT','Lat']));
        var lon     = parseFloat(getCol(row, ['Longitude','LON','Lon']));
        if (!stn) return;
        var cleanedStn = cleanStationName(stn);
        if (!cleanedStn) return;

        if (!rtisStationGPS[cleanedStn] && isValidGPS(lat, lon))
            rtisStationGPS[cleanedStn] = { lat:lat, lon:lon };

        if (!rtisByStation[cleanedStn]) rtisByStation[cleanedStn] = [];

        var locoKey = String(getCol(row, ['Loco No.','LOCO','Loco','LN']) || '').trim();
        var evt = {
            train    : getCol(row, ['Train Number','Train No.','TRAIN','Train']),
            loco     : locoKey,
            deviceId : String(getCol(row, ['Device Id','Device ID','DeviceId','DEVICE_ID','Device_Id']) || '').trim(),
            station  : cleanedStn,
            lat: lat, lon: lon,
            speed    : parseFloat(getCol(row, ['Speed','SPEED'])),
            time     : parseSmartDate(timeRaw),
            timeRaw  : String(timeRaw || ''),
            hasSeconds: /\d{1,2}:\d{2}:\d{2}/.test(String(timeRaw || ''))
        };

        rtisByStation[cleanedStn].push(evt);
        if (locoKey) {
            if (!allRtisByLoco[locoKey]) allRtisByLoco[locoKey] = [];
            allRtisByLoco[locoKey].push(evt);
        }
    });

    // Sort each loco's events by time ascending
    Object.keys(allRtisByLoco).forEach(function(loco) {
        allRtisByLoco[loco].sort(function(a, b) {
            return (a.time ? a.time.getTime() : 0) - (b.time ? b.time.getTime() : 0);
        });
    });

    var totalRTISEvents = 0;
    Object.keys(rtisByStation).forEach(function(s) { totalRTISEvents += rtisByStation[s].length; });
    log('✅ Step 2: RTIS preprocessed — ' + Object.keys(rtisByStation).length + ' stations, ' + totalRTISEvents + ' events');

    // ── Helper: 5-point FSD direction vote ──────────────────────
    function getFsdDirectionVote(anchor, allRtisByLoco, fsdMap) {
        var locoKey    = String(anchor.loco || '').trim();
        var locoEvents = allRtisByLoco[locoKey] || [];
        var anchorMs   = anchor.time ? anchor.time.getTime() : 0;

        var anchorIdx = -1, bestDiff = Infinity;
        locoEvents.forEach(function(e, i) {
            var d = Math.abs((e.time ? e.time.getTime() : 0) - anchorMs);
            if (d < bestDiff && e.station === anchor.station) { bestDiff = d; anchorIdx = i; }
        });
        if (anchorIdx < 0) {
            bestDiff = Infinity;
            locoEvents.forEach(function(e, i) {
                var d = Math.abs((e.time ? e.time.getTime() : 0) - anchorMs);
                if (d < bestDiff) { bestDiff = d; anchorIdx = i; }
            });
        }
        if (anchorIdx < 0) anchorIdx = 0;

        var wantBefore = 2, wantAfter = 2;
        var avBefore   = anchorIdx;
        var avAfter    = locoEvents.length - 1 - anchorIdx;
        if (avBefore < wantBefore) wantAfter  = Math.min(avAfter,  wantAfter  + (wantBefore - avBefore));
        if (avAfter  < wantAfter)  wantBefore = Math.min(avBefore, wantBefore + (wantAfter  - avAfter));

        var window5 = [];
        for (var b = wantBefore; b >= 1; b--)
            if (anchorIdx - b >= 0) window5.push(locoEvents[anchorIdx - b]);
        window5.push(anchor);
        for (var a = 1; a <= wantAfter; a++)
            if (anchorIdx + a < locoEvents.length) window5.push(locoEvents[anchorIdx + a]);

        var upVotes = 0, dnVotes = 0;
        window5.forEach(function(evt) {
            var stn = evt.station;
            if (!stn || !fsdMap[stn]) return;
            var bestDist = Infinity, bestDir = null;
            ['UP','DN'].forEach(function(dir) {
                (fsdMap[stn][dir] || []).forEach(function(sig) {
                    var d = getDistance(evt.lat, evt.lon, sig.lat, sig.lon);
                    if (d < bestDist) { bestDist = d; bestDir = dir; }
                });
            });
            if      (bestDir === 'UP') upVotes++;
            else if (bestDir === 'DN') dnVotes++;
        });

        if (upVotes > dnVotes) return 'UP';
        if (dnVotes > upVotes) return 'DN';

        var upD = Infinity, dnD = Infinity;
        if (fsdMap[anchor.station]) {
            (fsdMap[anchor.station].UP || []).forEach(function(s) {
                var d = getDistance(anchor.lat, anchor.lon, s.lat, s.lon);
                if (d < upD) upD = d;
            });
            (fsdMap[anchor.station].DN || []).forEach(function(s) {
                var d = getDistance(anchor.lat, anchor.lon, s.lat, s.lon);
                if (d < dnD) dnD = d;
            });
        }
        return (upD <= dnD) ? 'UP' : 'DN';
    }

    // ── Step 3: Match SNT → RTIS ────────────────────────────────
    var debugLimit   = 15;
    var TIME_WINDOW_MS = timeWindow * 1000;

    dataSNT.forEach(function(sntRow) {
        var stationRaw = getCol(sntRow, ['STATION','Station','STN_CODE']);
        var message    = getCol(sntRow, ['FAULT MESSAGE','Message','MESSAGE','Log']);
        var timeRaw    = getCol(sntRow, ['OCCURED TIME','OCCURED_TIME','TIME','Date','Time','SHOWN TIME']);

        if (!stationRaw || !timeRaw) return;
        cntTotal++;

        var station = cleanStationName(stationRaw);
        if (!station) return;

        var sntTime = parseSmartDate(timeRaw);
        if (!sntTime) return;

        var sigNum = extractSignalNumber(message);
        if (!sigNum) {
            if (debugLimit > 0) {
                log('⚠️ No signal number in: \'' + String(message||'').substring(0,60) + '\'');
                debugLimit--;
            }
            cntNoTrain++;
            noTrainResults.push({ station:station, rtisTime:'—', signalTime:sntTime.toLocaleTimeString(),
                sigNo:'?', dirSig:'?', reason:'Signal number not found in SNT message' });
            return;
        }

        var sigID       = 'S' + sigNum;
        var msgDir      = extractDirectionFromMessage(message);
        var computedDir = msgDir || getDirectionFromSignal(sigNum);
        if (msgDir) log('🧭 Dir from message: ' + msgDir + ' for ' + sigID + ' @ ' + station);

        var sntSpeed = (function() {
            var v = getCol(sntRow, ['TRAIN SPEED(KMPH)','TRAIN SPEED (KMPH)','TRAINSPEED','SPEED(KMPH)']);
            var f = parseFloat(v);
            return isNaN(f) ? null : f;
        })();

        // Map SNT station to FSD station
        var originalStation = station;
        var usedFuzzyMatch  = false;
        var rtisGps         = rtisStationGPS[station] || { lat:null, lon:null };

        if (!fsdMap[station]) {
            var mapping = resolveStation(station, rtisGps.lat, rtisGps.lon, fsdMap, stationMappingCache, manualOverrides);
            if (mapping) {
                station        = mapping.fsdStation;
                usedFuzzyMatch = (mapping.method !== 'exact');
                if (usedFuzzyMatch) cntFuzzyMatches++;
            }
        }

        if (!fsdMap[station]) {
            cntNoTrain++;
            noTrainResults.push({ station:originalStation, rtisTime:'—',
                signalTime:sntTime.toLocaleTimeString(), sigNo:sigID, dirSig:computedDir,
                reason:'Station not in FSD' });
            return;
        }

        var stationRTIS = rtisByStation[originalStation] || rtisByStation[station] || [];
        var sntMs          = sntTime.getTime();
        var sntHasSeconds  = /\d{1,2}:\d{2}:\d{2}/.test(String(getCol(sntRow, ['OCCURED TIME','OCCURED_TIME','TIME','Date','Time','SHOWN TIME']) || ''));
        var sntUpperBuf    = sntHasSeconds ? 30000 : 59000;

        var candidates = stationRTIS.filter(function(evt) {
            if (!evt.time) return false;
            var t   = evt.time.getTime();
            var buf = evt.hasSeconds ? 0 : 59000;
            return t >= (sntMs - TIME_WINDOW_MS - buf) && t <= (sntMs + sntUpperBuf);
        });

        if (candidates.length === 0) {
            cntNoTrain++;
            noTrainResults.push({ station:originalStation, rtisTime:'—',
                signalTime:sntTime.toLocaleTimeString(), sigNo:sigID, dirSig:computedDir,
                reason:'No RTIS event in ' + timeWindow + 's window' });
            return;
        }

        var maxDistKm = parseFloat(document.getElementById('inputMaxDist').value) || 5.0;
        candidates = candidates.filter(function(evt) {
            if (!isValidGPS(evt.lat, evt.lon)) return false;
            var minD = Infinity;
            ['UP','DN'].forEach(function(dir) {
                (fsdMap[station][dir] || []).forEach(function(sig) {
                    var d = getDistance(evt.lat, evt.lon, sig.lat, sig.lon);
                    if (d < minD) minD = d;
                });
            });
            return isFinite(minD) && minD <= maxDistKm;
        });

        if (candidates.length === 0) {
            cntNoTrain++;
            noTrainResults.push({ station:originalStation, rtisTime:'—',
                signalTime:sntTime.toLocaleTimeString(), sigNo:sigID, dirSig:computedDir,
                reason:'All RTIS events beyond Max Dist (' + maxDistKm + ' km)' });
            return;
        }

        candidates.sort(function(a, b) {
            return Math.abs(sntMs - a.time.getTime()) - Math.abs(sntMs - b.time.getTime());
        });

        // Deduplicate: same loco within 5 seconds = same train event
        var seen = {}, dedupedCandidates = [];
        candidates.forEach(function(c) {
            var tBucket = Math.floor(c.time.getTime() / 5000) * 5000;
            var key = String(c.loco) + '_' + tBucket;
            if (!seen[key]) { seen[key] = true; dedupedCandidates.push(c); }
        });
        candidates = dedupedCandidates;

        var anyOverSpeed = candidates.some(function(c) { return c.speed > speedLimit; });

        candidates.forEach(function(best) {
            var diffSeconds = (sntMs - best.time.getTime()) / 1000;

            var voteDir  = getFsdDirectionVote(best, allRtisByLoco, fsdMap);
            var minDist  = Infinity;
            ['UP','DN'].forEach(function(dir) {
                (fsdMap[station][dir] || []).forEach(function(sig) {
                    var d = getDistance(best.lat, best.lon, sig.lat, sig.lon);
                    if (d < minDist) minDist = d;
                });
            });
            var distMeters = isFinite(minDist) ? (minDist * 1000) : null;
            var distLabel  = distMeters !== null ? getDistanceLabel(distMeters) : '—';

            var cachedFsdSignals = (function() {
                if (!fsdMap[station]) return [];
                return (fsdMap[station].UP || []).map(function(s){ return Object.assign({dir:'UP'}, s); })
                    .concat((fsdMap[station].DN || []).map(function(s){ return Object.assign({dir:'DN'}, s); }));
            })();

            var baseRow = {
                trainNo: best.train, loco: best.loco, deviceId: best.deviceId || '',
                station: originalStation + (usedFuzzyMatch ? ' → ' + station : ''),
                stationKey: station,
                rtisTime: best.time.toLocaleTimeString(),
                signalTime: sntTime.toLocaleTimeString(),
                sntTimeISO: sntTime.toISOString(),
                sigNo: sigID, dirTrain: voteDir, dirSig: computedDir,
                direction: voteDir, travelDir: voteDir,
                speed: best.speed,
                speedPerSec: null, speedMatchQ: null, speedDiffSec: null, perSecMatch: null,
                sntSpeed: sntSpeed,
                diff: diffSeconds.toFixed(1),
                distM: distMeters !== null ? distMeters.toFixed(0) : '—',
                distLabel: distLabel,
                precisionFlag: best.hasSeconds ? 'sec' : 'min',
                violationLat: best.lat, violationLon: best.lon,
                violationTime: best.time, violationTimeStr: best.time.toISOString(),
                fsdStation: station, trainLoco: best.loco,
                cachedFsdSignals: cachedFsdSignals
            };

            // Direction mismatch → Complied
            if (voteDir !== computedDir) {
                analysisResults.push(Object.assign({}, baseRow, {
                    result: 'Complied', resultClass: 'complied', dirMismatch: true
                }));
                return;
            }

            // Direction matches — determine classification
            var isOverSpeed = best.speed > speedLimit;
            var finalResult, finalClass;
            if (candidates.length > 1 && anyOverSpeed && diffSeconds >= 0) {
                finalResult = 'Ambiguous'; finalClass = 'ambiguous';
            } else if (isOverSpeed && diffSeconds >= 0) {
                finalResult = 'VIOLATION'; finalClass = 'violation';
            } else {
                finalResult = 'Complied';  finalClass = 'complied';
            }

            var ambigMeta = {};
            if (finalClass === 'ambiguous') {
                var overSpeeds = candidates.filter(function(c){ return c.speed > speedLimit; });
                ambigMeta = {
                    ambigCount: candidates.length,
                    ambigLocos:  candidates.map(function(c){ return c.loco||'?'; }).join(', '),
                    ambigSpeeds: overSpeeds.map(function(c){ return c.speed+' km/h (loco '+c.loco+')'; }).join(', '),
                    ambigSpeedLimit: speedLimit
                };
            }

            analysisResults.push(Object.assign({}, baseRow, {
                result: finalResult, resultClass: finalClass, dirMismatch: false
            }, ambigMeta));
        });
    });

    // ── Deduplicate results ──────────────────────────────────────
    var seenResults = {}, dedupedResults = [];
    analysisResults.forEach(function(r) {
        var key = String(r.trainNo) + '_' + String(r.loco) + '_' +
                  String(r.rtisTime) + '_' + String(r.sntTimeISO) + '_' + String(r.sigNo);
        if (!seenResults[key]) { seenResults[key] = true; dedupedResults.push(r); }
    });
    analysisResults = dedupedResults;

    // ── Sort: Violations first, then Ambiguous, then Complied ───
    analysisResults.sort(function(a, b) {
        var rank = function(r) { return r === 'VIOLATION' ? 0 : r === 'Ambiguous' ? 1 : 2; };
        var ra = rank(a.result), rb = rank(b.result);
        if (ra !== rb) return ra - rb;
        return b.speed - a.speed;
    });

    var cntViolation = analysisResults.filter(function(r){ return r.resultClass === 'violation'; }).length;
    var cntAmbiguous = analysisResults.filter(function(r){ return r.resultClass === 'ambiguous'; }).length;
    var cntComplied  = analysisResults.filter(function(r){ return r.resultClass === 'complied'; }).length;

    log('✅ Step 3: Matching complete');
    log('   🚨 Violations: ' + cntViolation + ' | ⚠️ Ambiguous: ' + cntAmbiguous + ' | ✅ Complied: ' + cntComplied);
    log('   ℹ️  No Train Found: ' + cntNoTrain + ' (out of ' + cntTotal + ' SNT events)');
    log('   🔍 Fuzzy Matches Used: ' + cntFuzzyMatches);

    return {
        analysisResults,
        noTrainResults,
        fsdMap,
        allFsdStationNames,
        counts: { total: cntTotal, noTrain: cntNoTrain, violation: cntViolation,
                  ambiguous: cntAmbiguous, complied: cntComplied, fuzzy: cntFuzzyMatches }
    };
}

// ── Station resolver (fuzzy mapping) — inlined here to avoid circular import ──
// mirrors resolveStation in main.js exactly
function resolveStation(station, rtisLat, rtisLon, fsdMap, stationMappingCache, manualOverrides) {
    // Manual override wins
    var manual = manualOverrides && manualOverrides[station];
    if (manual && fsdMap[manual]) {
        return { fsdStation: manual, method: 'manual', distKm: null };
    }
    // Cached result
    var cached = stationMappingCache && stationMappingCache[station];
    if (cached && fsdMap[cached.fsdStation]) return cached;

    var fsdKeys = Object.keys(fsdMap);

    // Exact name match after normalisation
    for (var i = 0; i < fsdKeys.length; i++) {
        if (fsdKeys[i] === station) {
            var result = { fsdStation: fsdKeys[i], method: 'exact', distKm: 0 };
            if (stationMappingCache) stationMappingCache[station] = result;
            return result;
        }
    }

    // Near-match by name similarity (starts-with or contains)
    for (var j = 0; j < fsdKeys.length; j++) {
        var fk = fsdKeys[j];
        if (fk.startsWith(station) || station.startsWith(fk)) {
            var res2 = { fsdStation: fk, method: 'name-sim', distKm: null };
            if (stationMappingCache) stationMappingCache[station] = res2;
            return res2;
        }
    }

    // GPS proximity fallback
    if (isValidGPS(rtisLat, rtisLon)) {
        var bestDist = Infinity, bestKey = null;
        fsdKeys.forEach(function(fk) {
            var signals = (fsdMap[fk].UP || []).concat(fsdMap[fk].DN || []);
            signals.forEach(function(sig) {
                if (!isValidGPS(sig.lat, sig.lon)) return;
                var d = getDistance(rtisLat, rtisLon, sig.lat, sig.lon);
                if (d < bestDist) { bestDist = d; bestKey = fk; }
            });
        });
        if (bestKey && bestDist < 2) {  // within 2 km
            var res3 = { fsdStation: bestKey, method: 'proximity', distKm: bestDist };
            if (stationMappingCache) stationMappingCache[station] = res3;
            return res3;
        }
    }

    return null;
}
