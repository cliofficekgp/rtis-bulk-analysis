import { getCol, cleanStationName, extractSignalNumber, isValidGPS, parseSmartDate } from '../utils/helpers.js';

// --- RTIS Validation ---
export function validateRTISData(dataRTIS) {
    var report = {
        totalRows: dataRTIS.length,
        invalidGPS: 0,
        invalidDates: 0,
        missingSpeed: 0,
        missingStation: 0,
        uniqueStations: new Set(),
        dateRange: { min: null, max: null }
    };

    dataRTIS.forEach(function(row) {
        var lat = parseFloat(getCol(row, ['Latitude', 'Lattitude', 'LAT', 'Lat']));
        var lon = parseFloat(getCol(row, ['Longitude', 'LON', 'Lon']));
        var speed = getCol(row, ['Speed', 'SPEED']);
        var station = getCol(row, ['Station', 'STATION', 'STN_CODE']);
        var timeRaw = getCol(row, ['Event Time', 'TIME', 'EventTime', 'Date']);

        if (!isValidGPS(lat, lon)) report.invalidGPS++;
        if (!speed && speed !== 0) report.missingSpeed++;
        if (!station) report.missingStation++;
        else {
            station = cleanStationName(station);
            if (station) report.uniqueStations.add(station);
        }

        var date = parseSmartDate(timeRaw);
        if (!date) {
            report.invalidDates++;
        } else {
            if (!report.dateRange.min || date < report.dateRange.min) report.dateRange.min = date;
            if (!report.dateRange.max || date > report.dateRange.max) report.dateRange.max = date;
        }
    });

    return report;
}

// --- SNT Validation ---
export function validateSNTData(dataSNT) {
    var report = {
        totalRows: dataSNT.length,
        invalidDates: 0,
        missingStation: 0,
        missingMessage: 0,
        noSignalID: 0,
        uniqueStations: new Set(),
        uniqueSignals: new Set(),
        dateRange: { min: null, max: null }
    };

    dataSNT.forEach(function(row) {
        var station = getCol(row, ['STATION', 'Station', 'STN_CODE']);
        var message = getCol(row, ['FAULT MESSAGE', 'Message', 'MESSAGE', 'Log']);
        var timeRaw = getCol(row, ['OCCURED TIME', 'TIME', 'Date', 'Time', 'SHOWN TIME']);
        var sntSpd  = getCol(row, ['TRAIN SPEED(KMPH)', 'TRAIN SPEED (KMPH)', 'TRAINSPEED', 'SPEED(KMPH)']);
        if (sntSpd !== null && sntSpd !== undefined && !isNaN(parseFloat(sntSpd))) report.hasSntSpeed = (report.hasSntSpeed || 0) + 1;

        if (!station) report.missingStation++;
        else {
            station = cleanStationName(station);
            if (station) report.uniqueStations.add(station);
        }

        if (!message) report.missingMessage++;
        else {
            var sigNum = extractSignalNumber(message);
            if (!sigNum) report.noSignalID++;
            else report.uniqueSignals.add('S' + sigNum);
        }

        var date = parseSmartDate(timeRaw);
        if (!date) {
            report.invalidDates++;
        } else {
            if (!report.dateRange.min || date < report.dateRange.min) report.dateRange.min = date;
            if (!report.dateRange.max || date > report.dateRange.max) report.dateRange.max = date;
        }
    });

    return report;
}

// --- FSD Validation ---
export function validateFSDData(dataFSD) {
    var report = {
        totalRows: dataFSD.length,
        invalidGPS: 0,
        missingStation: 0,
        missingSignal: 0,
        missingDirection: 0,
        uniqueStations: new Set(),
        uniqueSignals: new Set()
    };

    dataFSD.forEach(function(row) {
        var station = getCol(row, ['Station', 'STATION', 'STN_CODE', 'Station Name']);
        var sigNo = getCol(row, ['SIGNUMBER', 'Signal', 'SIGNAL', 'Signal No', 'SIG_ID']);
        var lat = getCol(row, ['Latitude', 'Lattitude', 'LAT', 'Lat', 'GPS_LAT']);
        var lon = getCol(row, ['Longitude', 'LON', 'Lon', 'GPS_LON']);
        var dir = getCol(row, ['DIRN', 'Direction', 'DIR']);

        if (!station) report.missingStation++;
        else {
            station = cleanStationName(station);
            if (station) report.uniqueStations.add(station);
        }

        if (!sigNo) report.missingSignal++;
        else report.uniqueSignals.add(String(sigNo).trim());

        if (!isValidGPS(lat, lon)) report.invalidGPS++;
        if (!dir) report.missingDirection++;
    });

    return report;
}

// --- Cross-Validation between RTIS, SNT, FSD ---
// findFuzzyFn: function(rtisStation, lat, lon, fsdStationMap) - station fuzzy matcher
export function performCrossValidation(dataRTIS, dataFSD, validationReport, findFuzzyFn, log) {
    var report = {
        stationsInRTISNotInFSD: [],
        stationsInSNTNotInFSD: [],
        signalsInSNTNotInFSD: [],
        rtisFuzzyMatches: []
    };

    // Build FSD station map for fuzzy matching
    var tempFSDMap = {};
    dataFSD.forEach(function(row) {
        var station = getCol(row, ['Station', 'STATION', 'STN_CODE', 'Station Name']);
        var lat = getCol(row, ['Latitude', 'Lattitude', 'LAT', 'Lat', 'GPS_LAT']);
        var lon = getCol(row, ['Longitude', 'LON', 'Lon', 'GPS_LON']);
        var dir = getCol(row, ['DIRN', 'Direction', 'DIR']);
        
        if (station) {
            station = cleanStationName(station);
            if (!station) return;
            
            if (!tempFSDMap[station]) tempFSDMap[station] = {UP: [], DN: []};
            
            var direction = String(dir || '').toUpperCase().trim();
            if (direction === 'UP' || direction === 'DN') {
                tempFSDMap[station][direction].push({
                    lat: parseFloat(lat),
                    lon: parseFloat(lon)
                });
            }
        }
    });

    // Check RTIS stations against FSD with fuzzy matching
    validationReport.rtis.uniqueStations.forEach(function(stn) {
        if (!validationReport.fsd.uniqueStations.has(stn)) {
            // Try fuzzy match
            var rtisGPS = null;
            for (var i = 0; i < dataRTIS.length; i++) {
                var rowStn = getCol(dataRTIS[i], ['Station', 'STATION', 'STN_CODE']);
                if (cleanStationName(rowStn) === stn) {
                    var lat = parseFloat(getCol(dataRTIS[i], ['Latitude', 'Lattitude', 'LAT', 'Lat']));
                    var lon = parseFloat(getCol(dataRTIS[i], ['Longitude', 'LON', 'Lon']));
                    if (isValidGPS(lat, lon)) {
                        rtisGPS = {lat: lat, lon: lon};
                        break;
                    }
                }
            }
            
            if (rtisGPS) {
                var fuzzyMatch = findFuzzyFn(stn, rtisGPS.lat, rtisGPS.lon, tempFSDMap);
                if (fuzzyMatch) {
                    report.rtisFuzzyMatches.push({
                        rtis: stn,
                        fsd: fuzzyMatch.fsdStation,
                        distance: fuzzyMatch.distance
                    });
                } else {
                    report.stationsInRTISNotInFSD.push(stn);
                }
            } else {
                report.stationsInRTISNotInFSD.push(stn);
            }
        }
    });

    // Check SNT stations against FSD
    validationReport.snt.uniqueStations.forEach(function(stn) {
        if (!validationReport.fsd.uniqueStations.has(stn)) {
            report.stationsInSNTNotInFSD.push(stn);
        }
    });

    // --- Date Range Overlap Check ---
    var rtisMin = validationReport.rtis.dateRange && validationReport.rtis.dateRange.min;
    var rtisMax = validationReport.rtis.dateRange && validationReport.rtis.dateRange.max;
    var sntMin  = validationReport.snt.dateRange  && validationReport.snt.dateRange.min;
    var sntMax  = validationReport.snt.dateRange  && validationReport.snt.dateRange.max;

    if (rtisMin && rtisMax && sntMin && sntMax) {
        var overlapOk = rtisMin <= sntMax && sntMin <= rtisMax;
        var warn = document.getElementById('dateOverlapWarning');
        var detail = document.getElementById('dateOverlapDetail');
        if (!overlapOk) {
            warn.classList.remove('hidden');
            detail.innerHTML =
                'RTIS range: <b>' + rtisMin.toLocaleDateString() + ' – ' + rtisMax.toLocaleDateString() + '</b> &nbsp;|&nbsp; ' +
                'SNT range: <b>' + sntMin.toLocaleDateString() + ' – ' + sntMax.toLocaleDateString() + '</b><br>' +
                'These date ranges do not overlap. Analysis will likely return zero matches. Please verify your files cover the same date period.';
            log("🔴 WARNING: RTIS and SNT date ranges do not overlap! Matches will be empty.");
        } else {
            warn.classList.add('hidden');
            log("✅ Date Range Check: RTIS and SNT dates overlap correctly.");
        }
    }

    return report;
}

// --- Display Validation Report in DOM ---
export function displayValidationReport(validationReport, log) {
    var panel = document.getElementById('validationPanel');
    panel.classList.remove('hidden');

    // RTIS Validation
    var rtisDiv = document.getElementById('rtisValidation');
    var rtis = validationReport.rtis;
    rtisDiv.innerHTML = `
        <div>Total Rows: <span class="font-semibold">${rtis.totalRows}</span></div>
        <div>Unique Stations: <span class="font-semibold">${rtis.uniqueStations.size}</span></div>
        ${rtis.invalidGPS > 0 ? '<div class="error-badge">⚠️ Invalid GPS: ' + rtis.invalidGPS + '</div>' : '<div class="success-badge">✓ All GPS Valid</div>'}
        ${rtis.invalidDates > 0 ? '<div class="error-badge">⚠️ Invalid Dates: ' + rtis.invalidDates + '</div>' : '<div class="success-badge">✓ All Dates Valid</div>'}
        ${rtis.missingSpeed > 0 ? '<div class="warning-badge">Missing Speed: ' + rtis.missingSpeed + '</div>' : ''}
        ${rtis.dateRange.min ? '<div class="text-gray-600 mt-1">Date Range: ' + rtis.dateRange.min.toLocaleDateString() + ' to ' + rtis.dateRange.max.toLocaleDateString() + '</div>' : ''}
    `;

    // SNT Validation
    var sntDiv = document.getElementById('sntValidation');
    var snt = validationReport.snt;
    sntDiv.innerHTML = `
        <div>Total Rows: <span class="font-semibold">${snt.totalRows}</span></div>
        <div>Unique Stations: <span class="font-semibold">${snt.uniqueStations.size}</span></div>
        <div>Unique Signals: <span class="font-semibold">${snt.uniqueSignals.size}</span></div>
        ${snt.noSignalID > 0 ? '<div class="warning-badge">No Signal ID: ' + snt.noSignalID + '</div>' : '<div class="success-badge">✓ All Have Signal ID</div>'}
        ${snt.invalidDates > 0 ? '<div class="error-badge">⚠️ Invalid Dates: ' + snt.invalidDates + '</div>' : '<div class="success-badge">✓ All Dates Valid</div>'}
        ${snt.dateRange.min ? '<div class="text-gray-600 mt-1">Date Range: ' + snt.dateRange.min.toLocaleDateString() + ' to ' + snt.dateRange.max.toLocaleDateString() + '</div>' : ''}
    `;

    // FSD Validation
    var fsdDiv = document.getElementById('fsdValidation');
    var fsd = validationReport.fsd;
    fsdDiv.innerHTML = `
        <div>Total Rows: <span class="font-semibold">${fsd.totalRows}</span></div>
        <div>Unique Stations: <span class="font-semibold">${fsd.uniqueStations.size}</span></div>
        <div>Unique Signals: <span class="font-semibold">${fsd.uniqueSignals.size}</span></div>
        ${fsd.invalidGPS > 0 ? '<div class="error-badge">⚠️ Invalid GPS: ' + fsd.invalidGPS + '</div>' : '<div class="success-badge">✓ All GPS Valid</div>'}
        ${fsd.missingDirection > 0 ? '<div class="warning-badge">Missing Direction: ' + fsd.missingDirection + '</div>' : '<div class="success-badge">✓ All Have Direction</div>'}
    `;

    // Cross Validation
    var crossDiv = document.getElementById('crossValidation');
    var cross = validationReport.cross;
    var warnings = [];
    
    if (cross.rtisFuzzyMatches && cross.rtisFuzzyMatches.length > 0) {
        var fuzzyList = cross.rtisFuzzyMatches.map(function(m) {
            return m.rtis + '→' + m.fsd + ' (' + (m.distance * 1000).toFixed(0) + 'm)';
        }).join(', ');
        warnings.push('<div class="mb-2"><span class="warning-badge">🔍</span> <strong>' + cross.rtisFuzzyMatches.length + ' RTIS stations matched by proximity:</strong> ' + fuzzyList + '</div>');
    }
    
    if (cross.stationsInRTISNotInFSD.length > 0) {
        warnings.push('<div class="mb-2"><span class="error-badge">⚠️</span> <strong>' + cross.stationsInRTISNotInFSD.length + ' RTIS stations not in FSD:</strong> ' + cross.stationsInRTISNotInFSD.join(', ') + '</div>');
    }
    
    if (cross.stationsInSNTNotInFSD.length > 0) {
        warnings.push('<div class="mb-2"><span class="error-badge">⚠️</span> <strong>' + cross.stationsInSNTNotInFSD.length + ' SNT stations not in FSD:</strong> ' + cross.stationsInSNTNotInFSD.join(', ') + '</div>');
    }

    if (warnings.length === 0) {
        crossDiv.innerHTML = '<div class="success-badge">✓ All stations cross-referenced successfully</div>';
    } else {
        crossDiv.innerHTML = '<div class="font-semibold mb-2">Cross-Reference Issues:</div>' + warnings.join('');
    }

    log("✅ Data Validation Complete");
}
