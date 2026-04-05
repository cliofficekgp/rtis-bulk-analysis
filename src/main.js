import Papa from 'papaparse';
import * as L from 'leaflet';
import Chart from 'chart.js/auto';
import html2canvas from 'html2canvas';
import * as XLSX from 'xlsx';
import leafletImage from 'leaflet-image';
import { jsPDF } from 'jspdf';

window.Papa = Papa;
window.L = L;
window.Chart = Chart;
window.html2canvas = html2canvas;
window.XLSX = XLSX;
window.leafletImage = leafletImage;
window.jsPDF = jsPDF;

// --- Global State ---
    var dataRTIS = [];
    var dataSNT = [];
    var sntFileCount = 0;
    var sntFileNames = [];
    var dataFSD = [];
    var analysisResults = [];
    var fsdMap = {};                // promoted to global so handlePerSecFile can access it
    var _batchRunning = false;      // true while serial batch queue is active (blocks renderTable mid-batch)
    var _batchTotal   = 0;          // total assignments in current batch
    var _batchDone    = 0;          // completed so far
    var noTrainResults = [];
    var perSecondData = {};
    var validationReport = { rtis:{}, snt:{}, fsd:{}, cross:{} };
    var filesLoaded = { rtis:false, snt:false, fsd:false };
    var currentTab = 'violation';

    // Station mapping: RTIS code → { fsdStation, method, distKm }
    // Persists for entire session, survives re-runs
    var stationMappingCache = {};   // auto + fuzzy results
    var manualOverrides    = {};    // user edits from mapping editor

    // Speed chart instance (destroy before re-creating)
    var speedChartInstance = null;
    var allFsdStationNames = [];    // populated after FSD loads

    // --- Tab Switching ---
    function switchTab(tab) {
        currentTab = tab;
        document.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.remove('active'); });
        document.querySelectorAll('.tab-content').forEach(function(c) { c.classList.remove('active'); });
        document.querySelector('.tab-btn.tab-' + tab).classList.add('active');
        document.getElementById('tab-' + tab).classList.add('active');
    }

    // --- Helper: Logger ---
    function log(msg) {
        var div = document.getElementById('debugLog');
        if (!div) return;
        var time = new Date().toLocaleTimeString();
        div.innerHTML += "<div><span class='text-gray-400'>[" + time + "]</span> " + msg + "</div>";
        div.scrollTop = div.scrollHeight;
    }

    // --- Helper: Detect File Type from CSV Headers ---
    function detectFileType(data) {
        if (!data || data.length === 0) return null;
        
        // Check first 10 rows to find actual headers (in case of metadata at top)
        for (var rowIdx = 0; rowIdx < Math.min(10, data.length); rowIdx++) {
            var row = data[rowIdx];
            if (!row) continue;
            
            var headers = Object.keys(row).map(function(h) { return h.toUpperCase(); });
            var headerStr = headers.join(' ');
            var rowValsStr = Object.values(row).map(String).join(' ').toUpperCase();
            var combinedStr = headerStr + ' ' + rowValsStr;
            
            // RTIS Detection: Contains "LOCO" and "LATTITUDE" or "LATITUDE"
            if ((combinedStr.includes('LOCO') || combinedStr.includes('LOCOMOTIVE')) && 
                (combinedStr.includes('LATTITUDE') || combinedStr.includes('LATITUDE') || combinedStr.includes('LAT'))) {
                return 'rtis';
            }
            
            // SNT Detection: Contains "STATION" and ("FAULT" or "MESSAGE") and ("OCCURRED" or "TIME")
            // Also matches new format which has TRAIN SPEED(KMPH) column
            var hasSntStation = combinedStr.includes('STATION');
            var hasSntMessage = combinedStr.includes('FAULT') || combinedStr.includes('MESSAGE');
            var hasSntTime    = combinedStr.includes('OCCUR') || combinedStr.includes('TIME') || combinedStr.includes('SHOWN');
            var hasSntSpeed   = combinedStr.includes('TRAIN SPEED') || combinedStr.includes('KMPH');
            if (hasSntStation && (hasSntMessage || hasSntSpeed) && hasSntTime) {
                return 'snt';
            }
            
            // FSD Detection: Contains "STATION" and "DIRN" or "DIRECTION"
            if (combinedStr.includes('STATION') && 
                (combinedStr.includes('DIRN') || combinedStr.includes('DIRECTION'))) {
                return 'fsd';
            }
        }
        
        return null;
    }

    // --- Helper: Update Status UI ---
    function updateFileStatus(type, loaded, filename, rowCount) {
        var statusDiv = document.getElementById('status-' + type);
        var cardDiv = document.getElementById('status-' + type + '-card');
        var nameDiv = document.getElementById('file-' + type + '-name');

        if (loaded) {
            if (type === 'snt') {
                statusDiv.innerHTML = '✅ ' + dataSNT.length + ' rows (' + sntFileCount + ' file' + (sntFileCount !== 1 ? 's' : '') + ')';
                if (nameDiv) nameDiv.innerHTML = sntFileNames.map(function(n){ return '<span style="display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + n + '</span>'; }).join('');
            } else {
                statusDiv.innerHTML = '✅ Loaded (' + rowCount + ' rows)';
                if (nameDiv) nameDiv.innerHTML = filename;
            }
            statusDiv.className = 'text-sm font-bold text-green-600';
            cardDiv.className = 'border rounded p-3 text-center bg-green-50 border-green-300';
        } else {
            statusDiv.innerHTML = '❌ Not Loaded';
            statusDiv.className = 'text-sm font-bold text-gray-400';
            cardDiv.className = 'border rounded p-3 text-center';
            if (nameDiv) nameDiv.innerHTML = '';
        }

        checkAllFilesLoaded();
    }

    // --- Helper: Check if All Files Loaded ---
    function checkAllFilesLoaded() {
        var allLoaded = filesLoaded.rtis && filesLoaded.snt && filesLoaded.fsd;
        var btnRun = document.getElementById('btnRun');
        
        if (allLoaded) {
            btnRun.disabled = false;
            btnRun.classList.remove('bg-gray-400');
            btnRun.classList.add('bg-blue-600', 'hover:bg-blue-700');
            log("✅ All files loaded. Ready to run analysis!");
        } else {
            btnRun.disabled = true;
            btnRun.classList.remove('bg-blue-600', 'hover:bg-blue-700');
            btnRun.classList.add('bg-gray-400');
        }
    }

    // --- XLSX File Handler (SheetJS) — SNT only, always merges ---
    function handleXlsxFile(file) {
        log("📊 Reading XLSX: " + file.name);
        var reader = new FileReader();
        reader.onload = function(e) {
            try {
                var data = new Uint8Array(e.target.result);
                var workbook = XLSX.read(data, { type: 'array', cellDates: false, raw: false });

                // Process every sheet in the workbook (some exports split sections per sheet)
                var allRows = [];
                workbook.SheetNames.forEach(function(sheetName) {
                    var sheet = workbook.Sheets[sheetName];
                    // Get raw array-of-arrays so we can apply our own header-repair logic
                    var aoaRaw = XLSX.utils.sheet_to_json(sheet, { header: 1, defval: '' });

                    // ── Find real header row (same logic as SNT CSV repair) ──
                    var headerRowIdx = -1;
                    for (var i = 0; i < Math.min(30, aoaRaw.length); i++) {
                        var rowStr = aoaRaw[i].map(String).join(' ').toUpperCase();
                        if (rowStr.includes('STATION') &&
                            (rowStr.includes('FAULT') || rowStr.includes('MESSAGE') || rowStr.includes('TRAIN SPEED') || rowStr.includes('KMPH')) &&
                            (rowStr.includes('OCCUR') || rowStr.includes('TIME') || rowStr.includes('SHOWN'))) {
                            headerRowIdx = i;
                            break;
                        }
                    }

                    if (headerRowIdx === -1) {
                        log("Sheet [" + sheetName + "] - no SNT header row found, skipping.");
                        return;
                    }

                    var headers = aoaRaw[headerRowIdx].map(function(h){ return String(h).trim(); });
                    log("Sheet [" + sheetName + "] - header at row " + (headerRowIdx+1) + ", cols: " + headers.filter(Boolean).join(', '));

                    for (var r = headerRowIdx + 1; r < aoaRaw.length; r++) {
                        var rowArr = aoaRaw[r];
                        // Skip fully empty rows
                        if (!rowArr.some(function(v){ return String(v).trim() !== ''; })) continue;
                        var obj = {};
                        headers.forEach(function(h, idx) {
                            if (h) obj[h] = rowArr[idx] !== undefined ? rowArr[idx] : '';
                        });
                        allRows.push(obj);
                    }
                });

                if (allRows.length === 0) {
                    log("❌ " + file.name + " — no SNT data rows found in any sheet.");
                    alert("⚠️ No SNT data found in " + file.name + ". Please check the file format.");
                    return;
                }

                // Confirm it looks like SNT data
                var fileType = detectFileType(allRows);
                if (fileType !== 'snt') {
                    log("File " + file.name + " - detected as [" + (fileType||'unknown') + "], expected SNT.");
                    alert("⚠️ " + file.name + " does not appear to be an SNT file. Detected: " + (fileType || 'unknown'));
                    return;
                }

                // Merge into dataSNT
                dataSNT = dataSNT.concat(allRows);
                sntFileCount++;
                sntFileNames.push(file.name);
                filesLoaded.snt = true;
                log("✅ XLSX SNT merged: " + allRows.length + " rows from " + file.name + " (total: " + dataSNT.length + " rows, " + sntFileCount + " files)");

                validateSNTData();
                updateFileStatus('snt', true, file.name, dataSNT.length);

                if (filesLoaded.rtis && filesLoaded.snt && filesLoaded.fsd) {
                    performCrossValidation();
                    displayValidationReport();
                }
            } catch(err) {
                log("❌ XLSX parse error for " + file.name + ": " + err.message);
                alert("⚠️ Failed to read " + file.name + ": " + err.message);
            }
        };
        reader.readAsArrayBuffer(file);
    }

    // --- File Loading Logic with Auto-Detection ---
    function handleFile(file) {
        log("📁 Reading file: " + file.name);

        Papa.parse(file, {
            header: true,
            skipEmptyLines: true,
            dynamicTyping: true,
            complete: function(results) {
                var rawData = results.data;
                
                // Detect file type
                var fileType = detectFileType(rawData);
                
                if (!fileType) {
                    log("❌ Could not detect file type for: " + file.name);
                    alert("⚠️ Invalid CSV format. Please upload valid RTIS, SNT, or FSD file.");
                    return;
                }
                
                // For SNT: always merge (no confirm needed). For RTIS/FSD: confirm overwrite.
                if (filesLoaded[fileType] && fileType !== 'snt') {
                    var overwrite = confirm(fileType.toUpperCase() + " file already loaded. Do you want to replace it?");
                    if (!overwrite) return;
                }
                
                // --- SPECIAL FIX FOR SNT FILES WITH GARBAGE HEADERS ---
                if (fileType === 'snt') {
                    log("⚠️ Checking for metadata in SNT file...");
                    
                    var headerRowIndex = -1;
                    var foundHeaders = false;
                    
                    // Find the row that contains actual column headers (STATION, FAULT MESSAGE/TIME, OCCURRED TIME, etc.)
                    for (var i = 0; i < Math.min(30, rawData.length); i++) {
                        var row = rawData[i];
                        if (!row) continue;
                        
                        var headers = Object.keys(row).map(function(h) { return h.toUpperCase(); });
                        var headerStr = headers.join(' ');
                        var rowValsStr = Object.values(row).map(String).join(' ').toUpperCase();
                        var combinedStr = headerStr + ' ' + rowValsStr;
                        
                        // Recognise both old format (FAULT MESSAGE + OCCURED TIME)
                        // and new format (TRAIN SPEED(KMPH) + SHOWN TIME)
                        var _hasStn  = combinedStr.includes('STATION');
                        var _hasMsg  = combinedStr.includes('FAULT') || combinedStr.includes('MESSAGE');
                        var _hasTime = combinedStr.includes('OCCUR') || combinedStr.includes('TIME') || combinedStr.includes('SHOWN');
                        var _hasSpd  = combinedStr.includes('TRAIN SPEED') || combinedStr.includes('KMPH');
                        if (_hasStn && (_hasMsg || _hasSpd) && _hasTime) {
                            headerRowIndex = i;
                            foundHeaders = true;
                            log("   -> Found SNT header row at position " + (i + 1));
                            break;
                        }
                    }
                    
                    // If metadata found (header row is not at position 0), rebuild the data
                    if (foundHeaders && headerRowIndex > 0) {
                        log("   -> Removing " + headerRowIndex + " metadata row(s)...");
                        
                        var headerRow = rawData[headerRowIndex];
                        var newHeaders = Object.values(headerRow).map(function(v) { 
                            return String(v || '').trim(); 
                        });
                        
                        var cleanData = [];
                        for (var j = headerRowIndex + 1; j < rawData.length; j++) {
                            var oldRow = rawData[j];
                            if (!oldRow) continue;
                            
                            var oldVals = Object.values(oldRow).map(function(v) { return v; });
                            var newRow = {};
                            
                            for (var k = 0; k < newHeaders.length; k++) {
                                if (newHeaders[k]) {
                                    newRow[newHeaders[k]] = oldVals[k];
                                }
                            }
                            
                            // Only include rows that have actual data (not empty)
                            if (Object.values(newRow).some(function(v) { return v; })) {
                                cleanData.push(newRow);
                            }
                        }
                        
                        rawData = cleanData;
                        log("   -> SNT Repaired: " + rawData.length + " clean data rows ready");
                    }
                }
                
                // Store data
                if (fileType === 'rtis') {
                    dataRTIS = rawData;
                    filesLoaded.rtis = true;
                    validateRTISData();
                } else if (fileType === 'snt') {
                    // SNT always merges — accumulate from all files
                    dataSNT = dataSNT.concat(rawData);
                    sntFileCount++;
                    sntFileNames.push(file.name);
                    filesLoaded.snt = true;
                    validateSNTData();
                } else if (fileType === 'fsd') {
                    dataFSD = rawData;
                    filesLoaded.fsd = true;
                    validateFSDData();
                }
                
                log("✅ Loaded " + fileType.toUpperCase() + ": " + rawData.length + " rows");
                updateFileStatus(fileType, true, file.name, rawData.length);

                // Perform cross-validation if all files loaded
                if (filesLoaded.rtis && filesLoaded.snt && filesLoaded.fsd) {
                    performCrossValidation();
                    displayValidationReport();
                }
            }
        });
    }

    // --- Setup Drag & Drop and File Input ---
    document.addEventListener('DOMContentLoaded', function() {
        // Auto-restore session from localStorage if available
        setTimeout(function() {
            if (typeof loadSession === 'function') {
                var restored = loadSession();
                if (restored) log('✅ Session auto-restored. Re-upload per-sec files to restore speed data.');
            }
        }, 400);

        var dropZone = document.getElementById('dropZone');
        var fileInput = document.getElementById('fileInput');
        var btnRun = document.getElementById('btnRun');
        
        if (!dropZone || !fileInput) {
            console.error('Drop zone or file input element not found');
            return;
        }
        
        // Ensure Run button is disabled on load
        if (btnRun) {
            btnRun.disabled = true;
        }
        
        // Click to browse
        dropZone.addEventListener('click', function() {
            fileInput.click();
        });
        
        // Prevent default drag behaviors
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(function(eventName) {
            dropZone.addEventListener(eventName, function(e) {
                e.preventDefault();
                e.stopPropagation();
            }, false);
        });
        
        // Highlight drop zone when dragging over it
        ['dragenter', 'dragover'].forEach(function(eventName) {
            dropZone.addEventListener(eventName, function() {
                dropZone.classList.add('border-blue-500', 'bg-blue-50');
            }, false);
        });
        
        ['dragleave', 'drop'].forEach(function(eventName) {
            dropZone.addEventListener(eventName, function() {
                dropZone.classList.remove('border-blue-500', 'bg-blue-50');
            }, false);
        });
        
        // Handle dropped files
        dropZone.addEventListener('drop', function(e) {
            var files = e.dataTransfer.files;
            for (var i = 0; i < files.length; i++) {
                var fname = files[i].name.toLowerCase();
                if (fname.endsWith('.csv')) {
                    handleFile(files[i]);
                } else if (fname.endsWith('.xlsx') || fname.endsWith('.xls')) {
                    handleXlsxFile(files[i]);
                }
            }
        });

        // Handle selected files
        fileInput.addEventListener('change', function(e) {
            var files = e.target.files;
            for (var i = 0; i < files.length; i++) {
                var fname = files[i].name.toLowerCase();
                if (fname.endsWith('.csv')) {
                    handleFile(files[i]);
                } else if (fname.endsWith('.xlsx') || fname.endsWith('.xls')) {
                    handleXlsxFile(files[i]);
                }
            }
            fileInput.value = ''; // Reset input to allow re-uploading same file
        });
    });

    // --- Helper: Robust Date Parser ---
    function parseSmartDate(dateStr) {
        if (!dateStr) return null;
        dateStr = String(dateStr).trim();

        // Strip trailing milliseconds written with colon separator: "HH:MM:SS:mmm" → "HH:MM:SS"
        // e.g. "03/24/2026 00:14:55:984" → "03/24/2026 00:14:55"
        dateStr = dateStr.replace(/(\d{2}:\d{2}:\d{2}):\d{1,3}$/, '$1');

        // 1. Try Standard JS Date (handles ISO, RFC, most US formats)
        var d = new Date(dateStr);
        if (!isNaN(d.getTime()) && d.getFullYear() > 2000) return d;

        // 2. Detect MM/DD/YYYY vs DD/MM/YYYY and parse manually.
        //    Pattern: (A)/(B)/(YYYY) [HH:MM[:SS]]
        var parts = dateStr.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
        if (parts) {
            var year = parseInt(parts[3]);
            if (year < 100) year += 2000;
            var a = parseInt(parts[1]);   // first numeric group
            var b = parseInt(parts[2]);   // second numeric group
            var hh = parseInt(parts[4] || 0);
            var mm = parseInt(parts[5] || 0);
            var ss = parseInt(parts[6] || 0);

            // Determine month/day order:
            // If a > 12 it must be the day (DD/MM/YYYY).
            // If b > 12 it must be the day, so a is month (MM/DD/YYYY).
            // If both ≤ 12 we cannot tell from value alone — use the format
            //   that produces a valid date closest to today as a tiebreak,
            //   but for this railway data MM/DD/YYYY (US export) is the
            //   dominant format, so default to that when ambiguous.
            var month, day;
            if (a > 12) {
                // a is definitely day
                day   = a;
                month = b;
            } else if (b > 12) {
                // b is definitely day
                day   = b;
                month = a;
            } else {
                // Ambiguous: treat as MM/DD/YYYY (matches the SNT export format)
                month = a;
                day   = b;
            }

            var result = new Date(year, month - 1, day, hh, mm, ss);
            if (!isNaN(result.getTime())) return result;
        }
        return null;
    }

    // --- Helper: Column Getter (Case Insensitive) ---
    function getCol(row, candidates) {
        if (!row) return null;
        for (var i = 0; i < candidates.length; i++) {
            var key = candidates[i];
            if (row.hasOwnProperty(key)) return row[key];
            var rowKeys = Object.keys(row);
            for (var k = 0; k < rowKeys.length; k++) {
                if (rowKeys[k].trim().toUpperCase() === key.toUpperCase()) {
                    return row[rowKeys[k]];
                }
            }
        }
        return null;
    }

    // --- Helper: GPS Validation ---
    function isValidGPS(lat, lon) {
        if (!lat || !lon) return false;
        lat = parseFloat(lat);
        lon = parseFloat(lon);
        return !isNaN(lat) && !isNaN(lon) && 
               Math.abs(lat) <= 90 && 
               Math.abs(lon) <= 180 &&
               !(lat === 0 && lon === 0); // Filter null island
    }

    // --- Helper: Haversine Distance ---
    function getDistance(lat1, lon1, lat2, lon2) {
        if(!lat1 || !lon1 || !lat2 || !lon2) return 9999;
        var R = 6371; 
        var dLat = deg2rad(lat2 - lat1);
        var dLon = deg2rad(lon2 - lon1);
        var a = 
            Math.sin(dLat/2) * Math.sin(dLat/2) +
            Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * 
            Math.sin(dLon/2) * Math.sin(dLon/2); 
        var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a)); 
        return R * c;
    }

    function deg2rad(deg) { return deg * (Math.PI/180); }

    // --- Helper: Direction from Signal Number ---
    function getDirectionFromSignal(sigNum) {
        return (sigNum % 2 !== 0) ? 'UP' : 'DN';
    }

    // --- Helper: Clean Station Name ---
    // Rule: take only the leading alphanumeric characters.
    // Stop and discard everything from the first space, dash or underscore onwards.
    // Examples: HDS_DIG->HDS, JJKR-DIG->JJKR, KGP E-CABIN->KGP, MPD-SSI->MPD,
    //           AMTA-SSI K->AMTA, NMP_HUMP->NMP, TMZ JN_SSI M->TMZ, KGPW->KGPW
    function cleanStationName(station) {
        if (!station) return null;
        station = String(station).trim().toUpperCase();
        var match = station.match(/^([A-Z0-9]+)/);
        if (!match || !match[1]) return null;
        return match[1];
    }

    // ─────────────────────────────────────────────────────────────
    // SMART STATION MAPPING  (3-tier cascade, session-cached)
    // Tier 1: Exact match
    // Tier 2: Substring — RTIS contains FSD code OR FSD contains RTIS code
    //         e.g.  KGPE → KGP,  TMGP → KGP
    // ─────────────────────────────────────────────────────────────
    // Station resolver — 3 tiers, ALL subject to hard 5 km GPS limit
    // Tier 1: Exact name match (no distance check needed)
    // Tier 2: Substring match (RTIS contains FSD or vice-versa) AND GPS ≤ 5 km
    // Tier 3: Pure GPS proximity ≤ 5 km (any FSD station, no prefix filter)
    // Manual overrides always win.
    // ─────────────────────────────────────────────────────────────
    var PROXIMITY_LIMIT_KM = 5.0;

    function resolveStation(rtisStation, rtisLat, rtisLon, fsdMap) {
        if (!rtisStation) return null;

        // Manual override always wins
        if (manualOverrides[rtisStation]) {
            var mo = manualOverrides[rtisStation];
            return { fsdStation: mo, method: 'manual', distKm: null };
        }

        // Session cache (auto)
        if (stationMappingCache[rtisStation]) return stationMappingCache[rtisStation];

        var fsdKeys = Object.keys(fsdMap);
        var hasGps  = isValidGPS(rtisLat, rtisLon);

        // Helper: representative GPS distance from RTIS point to FSD station
        function fsdDist(fk) {
            if (!hasGps) return 9999;
            var sigs = (fsdMap[fk].UP || []).concat(fsdMap[fk].DN || []);
            if (sigs.length === 0) return 9999;
            return getDistance(rtisLat, rtisLon, sigs[0].lat, sigs[0].lon);
        }

        // --- Tier 1: Exact ---
        if (fsdMap[rtisStation]) {
            var r1 = { fsdStation: rtisStation, method: 'exact', distKm: 0 };
            stationMappingCache[rtisStation] = r1;
            return r1;
        }

        // --- Tier 2: Name-similarity AND GPS ≤ 5 km ---
        // Accept if: shorter code is a prefix of the longer code
        //         OR both codes share a common prefix of ≥ 2 characters
        // (e.g. KGPW→KGP: KGP is prefix of KGPW ✓, MRFO→MRGM: shared prefix MR ✓)
        // BALT→BLTR and similar edge cases: use manual override in mapping editor
        var nameMatches = [];
        fsdKeys.forEach(function(fk) {
            var a = rtisStation.toUpperCase();
            var b = fk.toUpperCase();
            var shorter = a.length <= b.length ? a : b;
            var longer  = a.length <= b.length ? b : a;
            // Rule A: shorter is a prefix of longer
            var ruleA = longer.startsWith(shorter);
            // Rule B: shared prefix >= 2
            var pfx = 0;
            for (var ci = 0; ci < Math.min(a.length, b.length); ci++) {
                if (a[ci] === b[ci]) pfx++; else break;
            }
            var ruleB = pfx >= 2;
            if (ruleA || ruleB) {
                var d = fsdDist(fk);
                if (d <= PROXIMITY_LIMIT_KM) nameMatches.push({ fsdStation: fk, distKm: d, rule: ruleA ? 'prefix-of' : 'shared-pfx' });
            }
        });
        if (nameMatches.length > 0) {
            nameMatches.sort(function(a,b){ return a.distKm - b.distKm; });
            var r2 = { fsdStation: nameMatches[0].fsdStation, method: 'name-sim', distKm: nameMatches[0].distKm };
            stationMappingCache[rtisStation] = r2;
            log('🔗 Name-sim match: ' + rtisStation + ' → ' + r2.fsdStation + ' (' + (r2.distKm*1000).toFixed(0) + 'm, ' + nameMatches[0].rule + ')');
            return r2;
        }

        // --- Tier 3: Pure GPS proximity ≤ 5 km (no prefix filter) ---
        if (!hasGps) { stationMappingCache[rtisStation] = null; return null; }
        var proxMatches = [];
        fsdKeys.forEach(function(fk) {
            var d = fsdDist(fk);
            if (d <= PROXIMITY_LIMIT_KM) proxMatches.push({ fsdStation: fk, distKm: d });
        });
        if (proxMatches.length > 0) {
            proxMatches.sort(function(a,b){ return a.distKm - b.distKm; });
            var r3 = { fsdStation: proxMatches[0].fsdStation, method: 'proximity', distKm: proxMatches[0].distKm };
            stationMappingCache[rtisStation] = r3;
            log('📍 Proximity match: ' + rtisStation + ' → ' + r3.fsdStation + ' (' + (r3.distKm*1000).toFixed(0) + 'm)');
            return r3;
        }

        stationMappingCache[rtisStation] = null;
        return null;
    }

    // --- Helper: Fuzzy Station Matching (kept for backward compat in cross-validation) ---
    function findFuzzyStationMatch(rtisStation, rtisLat, rtisLon, fsdStationMap) {
        var res = resolveStation(rtisStation, rtisLat, rtisLon, fsdStationMap);
        if (res && res.method !== 'exact') return { fsdStation: res.fsdStation, distance: res.distKm || 0 };
        return null;
    }

    // --- Helper: Extract Signal Number ---
    function extractSignalNumber(message) {
        if (!message) return null;
        message = String(message).trim();
        
        // Pattern to match: S1, S3, SA3, AS3, SA5, SA-5, S-5, etc.
        // Capture the number part only
        var patterns = [
            /S-?(\d+)/i,      // S1, S-5
            /SA-?(\d+)/i,     // SA3, SA-5
            /AS-?(\d+)/i,     // AS3, AS-5
            /(\d+)S/i         // 3S (sometimes seen)
        ];
        
        for (var i = 0; i < patterns.length; i++) {
            var match = message.match(patterns[i]);
            if (match && match[1]) {
                return parseInt(match[1]);
            }
        }
        
        return null;
    }

    // --- Helper: Extract direction (UP/DN) from SNT fault message ---
    // New-format messages embed direction: "Train passed on UP HOME S1 HECR at speed"
    // Old-format messages: "S1 Train passed on Yellow aspect" — no direction keyword
    // Returns 'UP', 'DN', or null (fall back to odd/even rule)
    function extractDirectionFromMessage(message) {
        if (!message) return null;
        var msg = String(message).toUpperCase();
        // Look for explicit UP/DN HOME or UP/DN direction keywords
        // Patterns: "ON UP HOME", "ON DN HOME", "UP HOME", "DN HOME",
        //           "PASSED UP", "PASSED DN", " UP ", " DN "
        if (/ON\s+UP\s+HOME/.test(msg))   return 'UP';
        if (/ON\s+DN\s+HOME/.test(msg))   return 'DN';
        if (/ON\s+DOWN\s+HOME/.test(msg)) return 'DN';
        if (/UP\s+HOME/.test(msg))        return 'UP';
        if (/DN\s+HOME/.test(msg))        return 'DN';
        if (/DOWN\s+HOME/.test(msg))      return 'DN';
        if (/PASSED\s+ON\s+UP/.test(msg)) return 'UP';
        if (/PASSED\s+ON\s+DN/.test(msg)) return 'DN';
        // Standalone UP/DN as a word — only match if not part of another word
        var upMatch = msg.match(/(UP|DN|DOWN)/);
        if (upMatch) return upMatch[1] === 'DOWN' ? 'DN' : upMatch[1];
        return null;
    }

    // --- Helper: Distance Label ---
    function getDistanceLabel(distMeters) {
        if (distMeters < 50) return "Very Close";
        if (distMeters < 150) return "Close";
        return "Far";
    }

    // --- Data Validation Functions ---
    function validateRTISData() {
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

        validationReport.rtis = report;
        return report;
    }

    function validateSNTData() {
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

        validationReport.snt = report;
        return report;
    }

    function validateFSDData() {
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

        validationReport.fsd = report;
        return report;
    }

    function performCrossValidation() {
        var report = {
            stationsInRTISNotInFSD: [],
            stationsInSNTNotInFSD: [],
            signalsInSNTNotInFSD: [],
            rtisFuzzyMatches: []
        };

        // Build FSD station map for fuzzy matching
        var fsdStationList = Array.from(validationReport.fsd.uniqueStations);
        
        // Create a temporary FSD map with GPS for fuzzy matching
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
                    var fuzzyMatch = findFuzzyStationMatch(stn, rtisGPS.lat, rtisGPS.lon, tempFSDMap);
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

        validationReport.cross = report;

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

    function displayValidationReport() {
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

    // --- Helper: Compute direction of travel from sequential GPS pings ---
    // Returns 'UP', 'DN', or null based on the bearing between two consecutive RTIS pings
    // UP = generally moving in the positive station-code direction (northward or as per railway convention)
    // We approximate by checking if the train's previous GPS point is in the opposite direction
    function getTravelDirection(allPingsForTrain, currentLat, currentLon, currentTime) {
        if (!allPingsForTrain || allPingsForTrain.length < 2) return null;
        // Sort by time
        var sorted = allPingsForTrain.slice().sort(function(a, b) { return a.time - b.time; });
        // Find the current ping index
        var idx = -1;
        for (var i = 0; i < sorted.length; i++) {
            if (sorted[i].time && Math.abs(sorted[i].time - currentTime) < 500) { idx = i; break; }
        }
        if (idx < 0) {
            // fallback: find closest time
            var minDiff = Infinity;
            for (var j = 0; j < sorted.length; j++) {
                var d = Math.abs(sorted[j].time - currentTime);
                if (d < minDiff) { minDiff = d; idx = j; }
            }
        }
        // Get a reference point: prefer the ping just before current, else the one after
        var refPing = null;
        if (idx > 0 && sorted[idx-1].time) refPing = sorted[idx-1];
        else if (idx < sorted.length-1 && sorted[idx+1].time) refPing = sorted[idx+1];
        if (!refPing || !isValidGPS(refPing.lat, refPing.lon)) return null;

        // Compute bearing delta: if train moved north relative to reference → UP, else DN
        // Simple proxy: compare latitude. Higher latitude ~ UP direction in Indian railways (generally)
        var latDiff = currentLat - refPing.lat;
        var lonDiff = currentLon - refPing.lon;
        // Use whichever axis has more movement
        if (Math.abs(latDiff) >= Math.abs(lonDiff)) {
            return latDiff >= 0 ? 'UP' : 'DN';
        } else {
            return lonDiff >= 0 ? 'UP' : 'DN';
        }
    }

    // --- Core Analysis ---
    function runAnalysis() {
        if (!dataRTIS.length || !dataSNT.length || !dataFSD.length) {
            alert("Please load all 3 files.");
            return;
        }

        var btn = document.getElementById('btnRun');
        btn.innerHTML = 'Processing...';
        btn.disabled = true;

        setTimeout(function() {
            executeLogic();
            btn.innerHTML = 'Run Analysis';
            btn.disabled = false;
        }, 100);
    }

    function executeLogic() {
        log("🚀 === ANALYSIS STARTED ===");
        log("⚙️  Configuration: TimeWindow=" + timeWindow + "s (RTIS before SNT), SpeedLimit=" + speedLimit + "km/h, MaxDist=" + maxDist + "km");
        
        var timeWindow = parseInt(document.getElementById('inputTimeWindow').value);
        var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value);
        var maxDist = parseFloat(document.getElementById('inputMaxDist').value);

        // --- Reset Metrics Counters ---
        var cntTotal = 0;
        var cntNoTrain = 0;
        var cntFuzzyMatches = 0;
        analysisResults = [];
        noTrainResults = [];

        // 1. Map FSD Data - Build lookup by Station and Direction
        fsdMap = {};
        
        dataFSD.forEach(function(row) {
            var station = getCol(row, ['Station', 'STATION', 'STN_CODE', 'Station Name']);
            var sigNo = getCol(row, ['SIGNUMBER', 'Signal', 'SIGNAL', 'Signal No', 'SIG_ID']);
            var lat = getCol(row, ['Latitude', 'Lattitude', 'LAT', 'Lat', 'GPS_LAT']);
            var lon = getCol(row, ['Longitude', 'LON', 'Lon', 'GPS_LON']);
            var dir = getCol(row, ['DIRN', 'Direction', 'DIR']);

            if (station) {
                station = cleanStationName(station);
                if (!station) return;
                
                if (!fsdMap[station]) fsdMap[station] = {UP: [], DN: []};
                
                // Store signal by its direction from DIRN column
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
        
        log("✅ Step 1: Mapped " + Object.keys(fsdMap).length + " stations from FSD master data");
        allFsdStationNames = Object.keys(fsdMap).sort();
        
        var totalUpSignals = 0;
        var totalDnSignals = 0;
        Object.keys(fsdMap).forEach(function(stn) {
            totalUpSignals += fsdMap[stn].UP.length;
            totalDnSignals += fsdMap[stn].DN.length;
        });
        log("   📍 Total UP signals: " + totalUpSignals + ", DN signals: " + totalDnSignals);

        // 2. Pre-process RTIS
        //    Build: rtisByStation (for 60s window lookup)
        //           allRtisByLoco  (for ±2 neighbour lookup, sorted by time)
        //           rtisStationGPS (for fuzzy station mapping)
        var rtisByStation = {};
        var rtisStationGPS = {};
        var allRtisByLoco  = {};

        dataRTIS.forEach(function(row) {
            var stn     = getCol(row, ['Station', 'STATION', 'STN_CODE']);
            var timeRaw = getCol(row, ['Event Time', 'TIME', 'EventTime', 'Date']);
            var lat     = parseFloat(getCol(row, ['Latitude', 'Lattitude', 'LAT', 'Lat']));
            var lon     = parseFloat(getCol(row, ['Longitude', 'LON', 'Lon']));
            if (!stn) return;
            var cleanedStn = cleanStationName(stn);
            if (!cleanedStn) return;

            if (!rtisStationGPS[cleanedStn] && isValidGPS(lat, lon))
                rtisStationGPS[cleanedStn] = { lat: lat, lon: lon };

            if (!rtisByStation[cleanedStn]) rtisByStation[cleanedStn] = [];

            var locoKey = String(getCol(row, ['Loco No.', 'LOCO', 'Loco', 'LN']) || '').trim();

            var evt = {
                train      : getCol(row, ['Train Number', 'Train No.', 'TRAIN', 'Train']),
                loco       : locoKey,
                deviceId   : String(getCol(row, ['Device Id', 'Device ID', 'DeviceId', 'DEVICE_ID', 'Device_Id']) || '').trim(),
                station    : cleanedStn,
                lat        : lat,
                lon        : lon,
                speed      : parseFloat(getCol(row, ['Speed', 'SPEED'])),
                time       : parseSmartDate(timeRaw),
                timeRaw    : String(timeRaw || ''),
                hasSeconds : /\d{1,2}:\d{2}:\d{2}/.test(String(timeRaw || ''))
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
        log("✅ Step 2: RTIS preprocessed — " + Object.keys(rtisByStation).length + " stations, " + totalRTISEvents + " events");

        // ── Helper: 5-point FSD direction vote ──────────────────────────────
        // anchor: the matched RTIS event
        // Returns 'UP' or 'DN'
        function getFsdDirectionVote(anchor, allRtisByLoco, fsdMap) {
            var locoKey    = String(anchor.loco || '').trim();
            var locoEvents = allRtisByLoco[locoKey] || [];
            var anchorMs   = anchor.time ? anchor.time.getTime() : 0;

            // Find anchor index (match by station + closest time)
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

            // Collect window: 2 before + anchor + 2 after (compensate short sides)
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

            // For each point: find closest FSD signal at that station → read UP/DN label
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

            // Majority wins; tie → anchor-only proximity fallback
            if (upVotes > dnVotes) return 'UP';
            if (dnVotes > upVotes) return 'DN';

            // Exact tie fallback: anchor station proximity
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

        // 3. Simplified Matching — no deduplication
        //    One RTIS event may match multiple SNT events (and vice versa).
        //    For each SNT event: find ALL RTIS events at same station in [SNT-60s, SNT].
        //    Max Dist: skip RTIS events where closest FSD signal > maxDist km.
        //    Direction mismatch → Complied.
        //    Multiple candidates with any speed > limit → Ambiguous.
        analysisResults = [];
        var debugLimit   = 15;
        var TIME_WINDOW_MS = timeWindow * 1000;

        dataSNT.forEach(function(sntRow) {
            var stationRaw = getCol(sntRow, ['STATION', 'Station', 'STN_CODE']);
            var message    = getCol(sntRow, ['FAULT MESSAGE', 'Message', 'MESSAGE', 'Log']);
            var timeRaw    = getCol(sntRow, ['OCCURED TIME', 'OCCURED_TIME', 'TIME', 'Date', 'Time', 'SHOWN TIME']);

            if (!stationRaw || !timeRaw) return;
            cntTotal++;

            var station = cleanStationName(stationRaw);
            if (!station) return;

            var sntTime = parseSmartDate(timeRaw);
            if (!sntTime) return;

            // Extract signal number
            var sigNum = extractSignalNumber(message);
            if (!sigNum) {
                if (debugLimit > 0) {
                    log("⚠️ No signal number in: '" + String(message||'').substring(0,60) + "'");
                    debugLimit--;
                }
                cntNoTrain++;
                noTrainResults.push({ station: station, rtisTime:'—', signalTime: sntTime.toLocaleTimeString(),
                    sigNo:'?', dirSig:'?', reason:'Signal number not found in SNT message' });
                return;
            }

            var sigID = 'S' + sigNum;
            // Direction: prefer explicit direction in message (new SNT format),
            // fall back to odd/even signal number rule (old format + safety net)
            var msgDir      = extractDirectionFromMessage(message);
            var computedDir = msgDir || getDirectionFromSignal(sigNum);
            if (msgDir) {
                log('🧭 Dir from message: ' + msgDir + ' for ' + sigID + ' @ ' + station);
            }
            // Extract SNT-provided speed (new format only, display/info only)
            var sntSpeed = (function() {
                var v = getCol(sntRow, ['TRAIN SPEED(KMPH)', 'TRAIN SPEED (KMPH)', 'TRAINSPEED', 'SPEED(KMPH)']);
                var f = parseFloat(v);
                return isNaN(f) ? null : f;
            })();

            // Map SNT station to FSD station
            var originalStation = station;
            var usedFuzzyMatch  = false;
            var rtisGps         = rtisStationGPS[station] || { lat: null, lon: null };

            if (!fsdMap[station]) {
                var mapping = resolveStation(station, rtisGps.lat, rtisGps.lon, fsdMap);
                if (mapping) {
                    station        = mapping.fsdStation;
                    usedFuzzyMatch = (mapping.method !== 'exact');
                    if (usedFuzzyMatch) cntFuzzyMatches++;
                }
            }

            if (!fsdMap[station]) {
                cntNoTrain++;
                noTrainResults.push({ station: originalStation, rtisTime:'—',
                    signalTime: sntTime.toLocaleTimeString(), sigNo: sigID, dirSig: computedDir,
                    reason: 'Station not in FSD' });
                return;
            }

            // Find ALL RTIS events at this station in [SNT-60s, SNT]
            var stationRTIS = rtisByStation[originalStation] || rtisByStation[station] || [];
            var sntMs          = sntTime.getTime();
            // Detect whether the SNT timestamp has second-level precision
            // (e.g. "00:14:55:984" has seconds; "00:14" is minute-only)
            var sntHasSeconds  = /\d{1,2}:\d{2}:\d{2}/.test(String(getCol(sntRow, ['OCCURED TIME','OCCURED_TIME','TIME','Date','Time','SHOWN TIME']) || ''));
            // Upper buffer: RTIS pings slightly AFTER SNT time are still valid
            //   • Second-precision SNT: allow 30s after (Excel strips seconds → RTIS lands at :00, SNT at :03)
            //   • Minute-only SNT:      allow 59s after (sntMs = HH:MM:00, real event could be :59 of that minute)
            var sntUpperBuf    = sntHasSeconds ? 30000 : 59000;

            var candidates = stationRTIS.filter(function(evt) {
                if (!evt.time) return false;
                var t   = evt.time.getTime();
                var buf = evt.hasSeconds ? 0 : 59000;  // RTIS lower buffer (minute-precision RTIS)
                return t >= (sntMs - TIME_WINDOW_MS - buf) && t <= (sntMs + sntUpperBuf);
            });

            if (candidates.length === 0) {
                cntNoTrain++;
                noTrainResults.push({ station: originalStation, rtisTime:'—',
                    signalTime: sntTime.toLocaleTimeString(), sigNo: sigID, dirSig: computedDir,
                    reason: 'No RTIS event in ' + timeWindow + 's window' });
                return;
            }

            // Apply Max Dist filter: skip RTIS events too far from any FSD signal
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
                noTrainResults.push({ station: originalStation, rtisTime:'—',
                    signalTime: sntTime.toLocaleTimeString(), sigNo: sigID, dirSig: computedDir,
                    reason: 'All RTIS events beyond Max Dist (' + maxDistKm + ' km)' });
                return;
            }

            // Sort by closest time to SNT
            candidates.sort(function(a, b) {
                return Math.abs(sntMs - a.time.getTime()) - Math.abs(sntMs - b.time.getTime());
            });

            // Deduplicate: same loco within 5 seconds = same train event (handles consecutive RTIS rows)
            var seen = {}, dedupedCandidates = [];
            candidates.forEach(function(c) {
                // Round time to nearest 5-second bucket for dedup key
                var tBucket = Math.floor(c.time.getTime() / 5000) * 5000;
                var key = String(c.loco) + '_' + tBucket;
                if (!seen[key]) { seen[key] = true; dedupedCandidates.push(c); }
            });
            candidates = dedupedCandidates;

            // Check if any candidate is over speed limit
            var anyOverSpeed = candidates.some(function(c) { return c.speed > speedLimit; });

            // Build result rows for ALL candidates
            candidates.forEach(function(best) {
                var diffSeconds = (sntMs - best.time.getTime()) / 1000;

                // 5-point FSD direction vote
                var voteDir = getFsdDirectionVote(best, allRtisByLoco, fsdMap);

                // Distance to closest FSD signal
                var minDist = Infinity;
                ['UP','DN'].forEach(function(dir) {
                    (fsdMap[station][dir] || []).forEach(function(sig) {
                        var d = getDistance(best.lat, best.lon, sig.lat, sig.lon);
                        if (d < minDist) minDist = d;
                    });
                });
                var distMeters = isFinite(minDist) ? (minDist * 1000) : null;
                var distLabel  = distMeters !== null ? getDistanceLabel(distMeters) : '—';

                // Direction mismatch → Complied
                if (voteDir !== computedDir) {
                    analysisResults.push({
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
                        result: 'Complied', resultClass: 'complied',
                        diff: diffSeconds.toFixed(1),
                        distM: distMeters !== null ? distMeters.toFixed(0) : '—',
                        distLabel: distLabel,
                        precisionFlag: best.hasSeconds ? 'sec' : 'min',
                        dirMismatch: true,
                        violationLat: best.lat, violationLon: best.lon,
                        violationTime: best.time, violationTimeStr: best.time.toISOString(),
                        fsdStation: station, trainLoco: best.loco,
                        cachedFsdSignals: (function() {
                            if (!fsdMap[station]) return [];
                            return (fsdMap[station].UP || []).map(function(s){ return Object.assign({dir:'UP'}, s); })
                                .concat((fsdMap[station].DN || []).map(function(s){ return Object.assign({dir:'DN'}, s); }));
                        })()
                    });
                    return;
                }

                // Direction matches — determine result
                var isOverSpeed = best.speed > speedLimit;
                var finalResult, finalClass;
                if (candidates.length > 1 && anyOverSpeed && diffSeconds >= 0) {
                    // Multiple candidates and at least one speeding → Ambiguous for all
                    finalResult = 'Ambiguous';
                    finalClass  = 'ambiguous';
                } else if (isOverSpeed && diffSeconds >= 0) {
                    finalResult = 'VIOLATION';
                    finalClass  = 'violation';
                } else {
                    finalResult = 'Complied';
                    finalClass  = 'complied';
                }

                // Build ambig metadata for tooltip
                var ambigMeta = {};
                if (finalClass === 'ambiguous') {
                    var overSpeeds = candidates.filter(function(c){ return c.speed > speedLimit; });
                    ambigMeta = {
                        ambigCount: candidates.length,
                        ambigLocos: candidates.map(function(c){ return c.loco||'?'; }).join(', '),
                        ambigSpeeds: overSpeeds.map(function(c){ return c.speed+' km/h (loco '+c.loco+')'; }).join(', '),
                        ambigSpeedLimit: speedLimit
                    };
                }
                analysisResults.push(Object.assign({
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
                    result: finalResult, resultClass: finalClass,
                    diff: diffSeconds.toFixed(1),
                    distM: distMeters !== null ? distMeters.toFixed(0) : '—',
                    distLabel: distLabel,
                    precisionFlag: best.hasSeconds ? 'sec' : 'min',
                    dirMismatch: false,
                    violationLat: best.lat, violationLon: best.lon,
                    violationTime: best.time, violationTimeStr: best.time.toISOString(),
                    fsdStation: station, trainLoco: best.loco,
                    // Cache FSD signals so map works after session restore (when dataFSD is empty)
                    cachedFsdSignals: (function() {
                        if (!fsdMap[station]) return [];
                        return (fsdMap[station].UP || []).map(function(s){ return Object.assign({dir:'UP'}, s); })
                            .concat((fsdMap[station].DN || []).map(function(s){ return Object.assign({dir:'DN'}, s); }));
                    })()
                }, ambigMeta));
            });
        });

        // Deduplicate analysisResults: only drop rows that are truly identical
        // (same train + loco + RTIS time + SNT time + signal)
        // Do NOT dedup on sntTimeISO alone — same train can legitimately appear at
        // the same signal multiple times with different RTIS events.
        var seenResults = {}, dedupedResults = [];
        analysisResults.forEach(function(r) {
            var key = String(r.trainNo) + '_' + String(r.loco) + '_' +
                      String(r.rtisTime) + '_' + String(r.sntTimeISO) + '_' + String(r.sigNo);
            if (!seenResults[key]) {
                seenResults[key] = true;
                dedupedResults.push(r);
            }
        });
        analysisResults = dedupedResults;

        // Sort: VIOLATION first, Ambiguous second, Complied last, then speed desc
        analysisResults.sort(function(a, b) {
            var rank = function(r) { return r === 'VIOLATION' ? 0 : r === 'Ambiguous' ? 1 : 2; };
            var ra = rank(a.result), rb = rank(b.result);
            if (ra !== rb) return ra - rb;
            return b.speed - a.speed;
        });

        var cntViolation = analysisResults.filter(function(r) { return r.resultClass === 'violation'; }).length;
        var cntAmbiguous = analysisResults.filter(function(r) { return r.resultClass === 'ambiguous'; }).length;
        var cntComplied  = analysisResults.filter(function(r) { return r.resultClass === 'complied'; }).length;

        document.getElementById('metricTotal').innerText     = cntTotal;
        document.getElementById('metricNoTrain').innerText   = cntNoTrain;
        document.getElementById('metricComplied').innerText  = cntComplied;
        document.getElementById('metricAmbiguous').innerText = cntAmbiguous;
        document.getElementById('metricViolation').innerText = cntViolation;

        log("✅ Step 3: Matching complete");
        log("   🚨 Violations: " + cntViolation + " | ⚠️ Ambiguous: " + cntAmbiguous + " | ✅ Complied: " + cntComplied);
        log("   ℹ️  No Train Found: " + cntNoTrain + " (out of " + cntTotal + " SNT events)");
        log("   🔍 Fuzzy Matches Used: " + cntFuzzyMatches);

        renderTable();
    }
 

    function buildTableHeader(tabId) {
        var cols = (tabId === 'notrain')
            ? ['Signal Time','Station','Sig No','Dir (Sig)','Reason']
            : tabId === 'demoted'
            ? ['Train No','Loco','Station','RTIS Time','Signal Time','Sig No','Dir','Speed @ SNT','Result','Demotion Reason','Map','Graph']
            : ['Train No','Loco','Station','RTIS Time','Signal Time','Sig No','Dir(GPS)','Speed','Result','Diff(s)','Dist','Source','Per-Sec','Map','Graph'];
        var thead = document.getElementById('thead-' + tabId);
        thead.innerHTML = '<tr>' + cols.map(function(c) {
            return '<th class="px-2 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">' + c + '</th>';
        }).join('') + '</tr>';
    }

    function renderTable() {
        ['violation','complied','ambiguous','notrain'].forEach(buildTableHeader);

        var exportBtn = document.getElementById('btnExport');
        exportBtn.disabled = analysisResults.length === 0;
        var wordBtn = document.getElementById('btnWordReport');
        if (wordBtn) wordBtn.disabled = analysisResults.length === 0;
        var batchBtn = document.getElementById('btnBatchUpload');
        if (batchBtn) batchBtn.style.display = analysisResults.length > 0 ? 'inline-block' : 'none';
        _updateKmlButton();

        var groups = { violation: [], complied: [], ambiguous: [] };
        analysisResults.forEach(function(row, idx) {
            if (row.resultClass === 'violation' || row.resultClass === 'violation-multi') groups.violation.push({row:row, idx:idx});
            else if (row.resultClass === 'ambiguous') groups.ambiguous.push({row:row, idx:idx});
            else groups.complied.push({row:row, idx:idx});
        });

        // Update badges
        var demotedRows = analysisResults.filter(function(r){ return r.perSecDemoted; });
        document.getElementById('badge-violation').innerText = groups.violation.length;
        document.getElementById('badge-complied').innerText = groups.complied.length;
        document.getElementById('badge-ambiguous').innerText = groups.ambiguous.length;
        document.getElementById('badge-notrain').innerText = noTrainResults.length;
        document.getElementById('badge-demoted').innerText = demotedRows.length;
        document.getElementById('metricNoTrain').innerText = noTrainResults.length;
        // Also update metric cards (these may change after per-sec demotion)
        document.getElementById('metricViolation').innerText = groups.violation.length;
        document.getElementById('metricComplied').innerText  = groups.complied.length;
        document.getElementById('metricAmbiguous').innerText = groups.ambiguous.length;

        // Render matched tabs
        ['violation','complied','ambiguous'].forEach(function(tabId) {
            var tbody = document.getElementById('tbody-' + tabId);
            tbody.innerHTML = '';
            var items = groups[tabId];
            var nomsg = document.getElementById('nomsg-' + tabId);

            if (items.length === 0) {
                nomsg.classList.remove('hidden');
                return;
            }
            nomsg.classList.add('hidden');

            items.forEach(function(item) {
                var row = item.row;
                var idx = item.idx;
                var hasPsec = perSecondData[idx] && perSecondData[idx].length > 0;
                var precHtml = hasPsec
                    ? '<span class="precision-flag-sec" title="Per-second file loaded — exact SNT-matched speed">✓ sec</span>'
                    : (row.precisionFlag === 'min'
                        ? '<span class="precision-flag" title="RTIS event — minute precision (59s buffer applied)">📍 Event ⏱</span>'
                        : '<span style="display:inline-block;background:#f1f5f9;color:#475569;border:1px solid #e2e8f0;border-radius:4px;font-size:0.7rem;padding:1px 5px;" title="RTIS event — second precision">📍 Event</span>');

                var speedLimit2 = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
                var speedCellHtml;
                if (row.speedPerSec !== null && row.speedPerSec !== undefined) {
                    var colorCls = row.speedPerSec > speedLimit2 ? 'speed-warn' : 'speed-exact';
                    var badgeCls = row.speedMatchQ === 'exact' ? 'match-badge-exact' : 'match-badge-tol';
                    var badgeTxt = row.speedMatchQ === 'exact' ? '✓ exact' : '⚠ Δ' + row.speedDiffSec + 's';
                    speedCellHtml = "<td class='px-2 py-2 font-bold'>" +
                        "<span class='" + colorCls + "'>" + row.speedPerSec + "</span>" +
                        "<span class='" + badgeCls + "' title='Per-second speed at SNT time'>" + badgeTxt + "</span>" +
                        "</td>";
                } else {
                    speedCellHtml = "<td class='px-2 py-2 font-bold " +
                        (parseFloat(row.speed) > speedLimit2 ? 'text-red-600' : 'text-green-700') +
                        "'>" + row.speed + "</td>";
                }

                var tr = document.createElement('tr');
                tr.innerHTML =
                    "<td class='px-2 py-2 text-gray-900' style='word-wrap:break-word;white-space:normal;'>" + row.trainNo + "</td>" +
                    "<td class='px-2 py-2 text-gray-500' title='" + (row.deviceId ? 'Device ID: ' + row.deviceId : 'No Device ID') + "'>" + row.loco + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.station + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.rtisTime + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.signalTime + "</td>" +
                    "<td class='px-2 py-2 font-medium'>" + row.sigNo + "</td>" +
                    "<td class='px-2 py-2 text-gray-500' title='GPS-movement direction'>" + row.travelDir + "</td>" +
                    speedCellHtml +
                    (function(){
                    if (row.resultClass === 'ambiguous') {
                        var sameLocos = row.ambigLocos || '';
                        var tip = '<b>Why Ambiguous?</b><br>' +
                            (row.ambigCount ? row.ambigCount + ' RTIS events matched this signal window.' : 'Multiple trains at station in 60s window.') +
                            (sameLocos ? '<br>Locos: ' + sameLocos : '') +
                            '<br>At least one is over speed limit (' + (row.ambigSpeedLimit||63) + ' km/h).<br>Cannot confirm which triggered the SNT event.';
                        return "<td class='px-2 py-2'><span class='ambig-cell'>" +
                               "<span class='px-2 text-xs font-semibold rounded-full ambiguous ambig-badge'>⚠ Ambiguous</span>" +
                               "<span class='ambig-tooltip'>" + tip + "</span>" +
                               "</span></td>";
                    }
                    if (row.perSecDemoted) {
                        return "<td class='px-2 py-2'><span class='ambig-cell'>" +
                               "<span class='px-2 text-xs font-semibold rounded-full complied ambig-badge' style='text-decoration:underline dotted #16a34a;cursor:help;'>✅ Complied*</span>" +
                               "<span class='ambig-tooltip' style='border-left-color:#22c55e;'><b>📍 Demoted from VIOLATION</b><br>" + (row.perSecDemotedReason||'') + "</span>" +
                               "</span></td>";
                    }
                    if (row.perSecPromoted) {
                        return "<td class='px-2 py-2'><span class='ambig-cell'>" +
                               "<span class='px-2 text-xs font-semibold rounded-full violation ambig-badge' style='text-decoration:underline dotted #dc2626;cursor:help;'>🚨 VIOLATION†</span>" +
                               "<span class='ambig-tooltip' style='border-left-color:#ef4444;'><b>🚀 Promoted from Complied</b><br>" + (row.perSecPromotedReason||'') + "</span>" +
                               "</span></td>";
                    }
                    if (row.dirMismatch) {
                        return "<td class='px-2 py-2'><span class='ambig-cell'>" +
                               "<span class='px-2 text-xs font-semibold rounded-full complied ambig-badge' style='text-decoration:underline dotted #16a34a;cursor:help;'>✅ Complied ↔</span>" +
                               "<span class='ambig-tooltip' style='border-left-color:#6366f1;'><b>↔ Direction Mismatch</b><br>Train GPS direction: <b>" + (row.dirTrain||'?') + "</b><br>Signal computed direction: <b>" + (row.dirSig||'?') + "</b><br>Train was not heading toward this signal — complied by direction.</span>" +
                               "</span></td>";
                    }
                    return "<td class='px-2 py-2'><span class='px-2 text-xs font-semibold rounded-full " + row.resultClass + "'>" + row.result + "</span></td>";
                })() +
                    "<td class='px-2 py-2 text-gray-500'>" + row.diff + "</td>" +
                    "<td class='px-2 py-2 text-gray-700' title='" + row.distM + "m'>" + row.distLabel + "</td>" +
                    "<td class='px-2 py-2'>" + precHtml + "</td>" +
                    (function(){
                    var devId  = row.deviceId || '';
                    var hint   = devId ? devId + '_PrimaryGPSData…' : 'Loco ' + row.loco;
                    var title  = devId
                        ? 'Expected file: ' + devId + '_PrimaryGPSData_YYYY-MM-DD.csv  |  Device ID: ' + devId + '  |  Loco: ' + row.loco
                        : 'Upload per-second GPS file for Loco ' + row.loco;
                    var loaded = hasPsec;
                    return "<td class='px-2 py-2' style='min-width:145px;'>" +
                        "<label style='display:block;cursor:pointer;'>" +
                        "<div style='font-size:0.65rem;color:" + (loaded ? '#059669' : '#0891b2') + ";font-weight:600;margin-bottom:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:140px;' title='" + title.replace(/'/g,"\\'") + "'>" +
                        (loaded ? '✓ ' : '📂 ') + hint +
                        "</div>" +
                        "<input type='file' accept='.csv' style='display:none;' onchange='handlePerSecFile(event," + idx + ")' />" +
                        "<div style='font-size:0.65rem;padding:2px 6px;border-radius:4px;border:1px dashed " + (loaded ? '#059669' : '#0891b2') + ";color:" + (loaded ? '#059669' : '#0891b2') + ";text-align:center;white-space:nowrap;'>" +
                        (loaded ? '✓ Loaded — re-upload' : 'Upload GPS file') +
                        "</div></label></td>";
                })() +
                    "<td class='px-2 py-2'><button class='btn-map' id='mapBtn" + idx + "' onclick='openMap(" + idx + ")'" +
                    " style='" + (hasPsec ? "background:linear-gradient(135deg,#059669 0%,#047857 100%);" : "") + "'" +
                    " title='" + (hasPsec ? "View detailed track (" + (perSecondData[idx]||[]).length + " pts)" : "View on Map") + "'>" +
                    "<svg xmlns='http://www.w3.org/2000/svg' width='13' height='13' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'><path d='M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z'/><circle cx='12' cy='10' r='3'/></svg> Map</button></td>" +
                    "<td class='px-2 py-2'><button class='btn-graph" + (hasPsec ? " has-persec" : "") + "' id='graphBtn" + idx + "' onclick='openGraph(" + idx + ")'" +
                    " title='" + (hasPsec ? "Speed-Time Graph (±3 min per-second · exact SNT match)" : "Speed-Time Graph") + "'>" +
                    "<svg xmlns='http://www.w3.org/2000/svg' width='13' height='13' fill='none' stroke='currentColor' stroke-width='2.5' viewBox='0 0 24 24'><polyline points='22 12 18 12 15 21 9 3 6 12 2 12'/></svg> Graph</button></td>";
                tbody.appendChild(tr);
            });
        });

        // Render Demoted tab
        buildTableHeader('demoted');
        var tbodyDemoted = document.getElementById('tbody-demoted');
        tbodyDemoted.innerHTML = '';
        var nomsgDemoted = document.getElementById('nomsg-demoted');
        if (demotedRows.length === 0) {
            nomsgDemoted.classList.remove('hidden');
        } else {
            nomsgDemoted.classList.add('hidden');
            demotedRows.forEach(function(row) {
                var idx = analysisResults.indexOf(row);
                var tr = document.createElement('tr');
                tr.style.background = '#f0fdf4';
                tr.innerHTML =
                    "<td class='px-2 py-2 font-bold text-gray-900'>" + row.trainNo + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.loco + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.station + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.rtisTime + "</td>" +
                    "<td class='px-2 py-2 text-gray-500'>" + row.signalTime + "</td>" +
                    "<td class='px-2 py-2 font-medium'>" + row.sigNo + "</td>" +
                    "<td class='px-2 py-2'>" + row.travelDir + "</td>" +
                    "<td class='px-2 py-2 font-bold'>" +
                        (row.speedPerSec != null
                            ? "<span class='speed-warn'>" + row.speedPerSec + "</span> <span class='match-badge-exact'>✓ exact</span>"
                            : "<span class='text-red-600'>" + row.speed + "</span>") +
                    "</td>" +
                    "<td class='px-2 py-2'><span class='complied' style='padding:2px 8px;font-size:0.75rem;font-weight:700;border-radius:9999px;'>✅ Complied*</span></td>" +
                    "<td class='px-2 py-2 text-gray-600' style='max-width:320px;font-size:0.78rem;'>" + (row.perSecDemotedReason || '—') + "</td>" +
                    "<td class='px-2 py-2'><button class='btn-map' style='background:linear-gradient(135deg,#059669 0%,#047857 100%);' onclick='openMap(" + idx + ")'>🗺 Map</button></td>" +
                    "<td class='px-2 py-2'><button class='btn-graph has-persec' onclick='openGraph(" + idx + ")'>📈 Graph</button></td>";
                tbodyDemoted.appendChild(tr);
            });
        }

        // Render No Match tab
        var tbodyNT = document.getElementById('tbody-notrain');
        tbodyNT.innerHTML = '';
        var nomsgNT = document.getElementById('nomsg-notrain');
        if (noTrainResults.length === 0) {
            nomsgNT.classList.remove('hidden');
        } else {
            nomsgNT.classList.add('hidden');
            noTrainResults.forEach(function(r) {
                var tr = document.createElement('tr');
                tr.innerHTML =
                    "<td class='px-2 py-2 text-gray-500'>" + r.signalTime + "</td>" +
                    "<td class='px-2 py-2 text-gray-700'>" + r.station + "</td>" +
                    "<td class='px-2 py-2 font-medium'>" + r.sigNo + "</td>" +
                    "<td class='px-2 py-2'>" + r.dirSig + "</td>" +
                    "<td class='px-2 py-2 text-orange-700 text-xs'>" + (r.reason || '—') + "</td>";
                tbodyNT.appendChild(tr);
            });
        }
        // Auto-save session after every render
        saveSession();
    }

    // ══════════════════════════════════════════════════════════════
    // FEATURE 2: SESSION PERSISTENCE via localStorage
    // Saves analysis results + per-sec metadata (NOT raw points — too large)
    // ══════════════════════════════════════════════════════════════
    var SESSION_KEY = 'syViolationSession_v19';

    function saveSession() {
        try {
            // Build lightweight per-sec metadata (speeds + match info only, not raw 86k points)
            var psMetadata = {};
            Object.keys(perSecondData).forEach(function(idx) {
                var pts = perSecondData[idx];
                if (pts && pts.length) {
                    psMetadata[idx] = { pointCount: pts.length, loaded: true };
                }
            });
            var session = {
                savedAt: new Date().toISOString(),
                analysisResults: analysisResults,
                noTrainResults: noTrainResults,
                psMetadata: psMetadata,
                manualOverrides: manualOverrides,
                config: {
                    timeWindow: document.getElementById('inputTimeWindow').value,
                    speedLimit: document.getElementById('inputSpeedLimit').value,
                    maxDist: document.getElementById('inputMaxDist').value,
                    fsdThreshold: document.getElementById('inputFsdThreshold').value
                }
            };
            var json = JSON.stringify(session);
            if (json.length > 4 * 1024 * 1024) {
                log('⚠️ Session too large for localStorage — skipping auto-save');
                return;
            }
            localStorage.setItem(SESSION_KEY, json);
        } catch(e) {
            log('⚠️ Session save failed: ' + e.message);
        }
    }

    function loadSession() {
        try {
            var json = localStorage.getItem(SESSION_KEY);
            if (!json) return false;
            var session = JSON.parse(json);
            if (!session.analysisResults || !session.analysisResults.length) return false;

            analysisResults = session.analysisResults;
            // Restore Date objects (JSON stringifies them)
            analysisResults.forEach(function(r) {
                if (r.violationTime && typeof r.violationTime === 'string') r.violationTime = new Date(r.violationTime);
            });
            noTrainResults = session.noTrainResults || [];
            manualOverrides = session.manualOverrides || {};

            // Restore config inputs
            if (session.config) {
                if (session.config.timeWindow) document.getElementById('inputTimeWindow').value = session.config.timeWindow;
                if (session.config.speedLimit) document.getElementById('inputSpeedLimit').value = session.config.speedLimit;
                if (session.config.maxDist) document.getElementById('inputMaxDist').value = session.config.maxDist;
                if (session.config.fsdThreshold) document.getElementById('inputFsdThreshold').value = session.config.fsdThreshold;
            }

            var savedAt = session.savedAt ? new Date(session.savedAt).toLocaleString() : '?';
            log('🔄 Restored session from ' + savedAt + ' — ' + analysisResults.length + ' results');
            if (session.psMetadata) {
                var psCount = Object.keys(session.psMetadata).length;
                if (psCount) log('ℹ️  ' + psCount + ' per-second file(s) were loaded in previous session — please re-upload to restore per-sec speeds');
            }
            renderTable();
            return true;
        } catch(e) {
            log('⚠️ Session restore failed: ' + e.message);
            return false;
        }
    }

    function clearSession() {
        localStorage.removeItem(SESSION_KEY);
        log('🗑️ Session cleared');
    }

    // ==========================================
    // PER-SECOND FILE UPLOAD HANDLER
    // ==========================================
    function handlePerSecFile(event, rowIdx, _onDone) {
        var file = event.target.files[0];
        if (!file) { if (_onDone) _onDone(); return; }

        log("📁 Loading per-second data for row " + rowIdx + ": " + file.name);

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
                perSecondData[rowIdx] = parsedData;

                // Enable KML export button now that per-sec data exists
                _updateKmlButton();

                // ── EXACT SNT-TIME MATCH ──────────────────────────────
                var row = analysisResults[rowIdx];
                if (row) {
                    // Build SNT Date from stored signal time string + violation date
                    // row.violationTime holds the RTIS event Date; use its date part + SNT time string
                    var sntBase = row.violationTime instanceof Date
                                    ? row.violationTime
                                    : new Date(row.violationTimeStr);

                    // Try to reconstruct SNT datetime: use sntTimeISO if stored, else parse signalTime
                    var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
                        // Combine date from sntBase + time from row.signalTime (HH:MM:SS)
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

                    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
                    if (match.quality !== 'none' && match.point) {
                        var diffSec = (match.diffMs / 1000).toFixed(1);
                        var speedPs = !isNaN(match.point.speed) ? match.point.speed : null;

                        log('🎯 SNT-time match [' + match.quality + ']: diff=' + diffSec + 's' +
                            (speedPs !== null ? ', speed=' + speedPs + ' km/h' : '') +
                            ' | resultClass=' + row.resultClass +
                            ' dirMismatch=' + row.dirMismatch);

                        // ── STEP 1: Store per-sec speed on row FIRST ──────────────────────────────
                        // Must happen before renderTable() so it renders the correct speed cell.
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
                        var fsdThreshold = parseFloat(document.getElementById('inputFsdThreshold').value) || 200;

                        if (speedPs !== null && speedPs > speedLimit && isValidGPS(pLat, pLon)) {
                            // Build signal list: prefer live fsdMap (analysis in memory),
                            // fall back to cachedFsdSignals on the row (survives session restore)
                            var _sigList = [];
                            if (stnKey && fsdMap[stnKey]) {
                                var _ds = (fsdMap[stnKey][trainDir] || []);
                                if (!_ds.length) _ds = (fsdMap[stnKey].UP || []).concat(fsdMap[stnKey].DN || []);
                                _sigList = _ds;
                            } else if (row.cachedFsdSignals && row.cachedFsdSignals.length) {
                                // Session restore path: use signals cached at analysis time
                                _sigList = row.cachedFsdSignals.filter(function(s){ return s.dir === trainDir; });
                                if (!_sigList.length) _sigList = row.cachedFsdSignals;
                                log('ℹ️  Using cachedFsdSignals for FSD dist (fsdMap not in memory)');
                            }
                            if (_sigList.length) {
                                var minD = Infinity;
                                _sigList.forEach(function(sig) {
                                    if (!isValidGPS(sig.lat, sig.lon)) return;
                                    var d = getDistance(pLat, pLon, sig.lat, sig.lon) * 1000;
                                    if (d < minD) minD = d;
                                });
                                if (isFinite(minD)) psDistToFsd = minD;
                                log('📍 per-sec→FSD dist = ' + psDistToFsd.toFixed(0) + 'm (threshold=' + fsdThreshold + 'm, dir=' + trainDir + ')');
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
                            log('✅ DEMOTED to Complied: dist ' + psDistToFsd.toFixed(0) + 'm > ' + fsdThreshold + 'm, row ' + rowIdx);
                            showDemotionToast(row.trainNo, psDistToFsd, trainDir, row.perSecDemotedReason);
                            classChanged = true;
                        }

                        // ── STEP 4: PROMOTION — complied row (correct direction) with per-sec overspeed ──
                        // Conditions:
                        //   1. row.resultClass === 'complied'  (not violation/ambiguous)
                        //   2. row.dirMismatch is false/undefined  (direction was correct at analysis time)
                        //   3. row.perSecDemoted is falsy  (not demoted in this same run)
                        //   4. speedPs > speedLimit
                        //   5. Either no FSD distance available, OR GPS is within threshold of signal
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
                                log('🚨 PROMOTED to VIOLATION: row=' + rowIdx +
                                    ' train=' + row.trainNo +
                                    ' per-sec=' + speedPs + ' km/h > ' + speedLimit + ' km/h' +
                                    (psDistToFsd !== null ? ' dist=' + psDistToFsd.toFixed(0) + 'm' : ' no-fsd-dist'));
                                classChanged = true;
                            } else {
                                log('ℹ️ Complied row NOT promoted: overspeed but GPS ' +
                                    psDistToFsd.toFixed(0) + 'm > threshold ' + fsdThreshold + 'm — not at signal point.');
                            }
                        }

                        // ── STEP 5: Re-render + save if classification changed ────────────────
                        // During batch upload (_batchRunning) we suppress individual renderTable
                        // calls — the batch dispatcher does one final renderTable at the end.
                        if (classChanged) {
                            if (!_batchRunning) { renderTable(); saveSession(); }
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
                        _updateKmlButton();
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
                            log('⚠️  Closest point is ' + diffSec + 's away from SNT time — outside ±5s tolerance. Marker shown with warning.');
                        }

                    } else {
                        log('⚠️  No per-second points to match for row ' + rowIdx);
                    }
                }

                // MOD4: init filter context (for the currently open graph if same row)
                if (_fc && _fc.resultIdx === rowIdx) {
                    _initFilterUI(parsedData, _fc.sntMs, rowIdx, _fc.row);
                }

                log("✅ Loaded " + parsedData.length + " per-second points for row " + rowIdx);
                if (_onDone) _onDone();   // signal batch queue: this file is done
            },
            error: function(err) {
                log("❌ Error parsing per-second file: " + err.message);
                if (!_batchRunning) alert("Error parsing CSV file. Check format.");
                if (_onDone) _onDone();   // still resolve so queue continues
            }
        });
    }

    // ==========================================
    // MAP FUNCTIONALITY
    // ==========================================
    var leafletMap = null;

    function openMap(resultIdx) {
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
        document.getElementById('mapSubtitle').innerText =
            row.result + ' | Speed: ' + (row.speedPerSec !== null && row.speedPerSec !== undefined ? row.speedPerSec : row.speed) + ' km/h | SNT: ' + row.signalTime;

        setTimeout(function() { buildMap(row, resultIdx); }, 80);
    }

    // ── Demotion toast ──────────────────────────────────────────────
    var _toastTimer = null, _toastInterval = null, _toastPaused = false, _toastElapsed = 0;
    function showDemotionToast(trainNo, distM, direction, reason) {
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
        var secs = document.getElementById('demotionToastSecs');
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
    function closeDemotionToast() {
        clearTimeout(_toastTimer);
        clearInterval(_toastInterval);
        _toastPaused = false;
        document.getElementById('demotionToast').classList.remove('toast-visible');
    }

    // Pause countdown while mouse button held on toast; resume on release anywhere
    (function() {
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
    })();


    function closeMap() {
        document.getElementById('mapModal').classList.remove('open');
        if (leafletMap) { leafletMap.remove(); leafletMap = null; }
        document.getElementById('violPanel').style.display = 'none';
        document.getElementById('mapFilterStrip').style.display = 'none';
        _trackLayer=null; _startMk=null; _endMk=null;
    }

    function buildMap(row, resultIdx) {
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

        // Collect FSD signals — use live dataFSD if loaded, fall back to row cache (session restore)
        var fsdSignals = [];
        if (dataFSD.length > 0) {
            dataFSD.forEach(function(r) {
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
        // Fallback: use signals cached in the result row (available even after session restore)
        if (fsdSignals.length === 0 && row.cachedFsdSignals && row.cachedFsdSignals.length) {
            fsdSignals = row.cachedFsdSignals.filter(function(s){ return isValidGPS(s.lat, s.lon); })
                .map(function(s){ return Object.assign({ stn: cleanStation }, s); });
            log('ℹ️  Using cached FSD signals for ' + cleanStation + ' (' + fsdSignals.length + ')');
        }

        // ── Distance: violation point → nearest FSD signal matching train direction ──
        var trainDir = row.direction || row.dirTrain || row.dirSig || '';
        var matchedDirSignals = fsdSignals.filter(function(s){ return s.dir === trainDir; });
        if (matchedDirSignals.length === 0) matchedDirSignals = fsdSignals; // fallback any

        var distToFsdM = null, nearestFsdSig = null;
        matchedDirSignals.forEach(function(sig) {
            var d = getDistance(violLat, violLon, sig.lat, sig.lon) * 1000; // → metres
            if (distToFsdM === null || d < distToFsdM) { distToFsdM = d; nearestFsdSig = sig; }
        });

        // ── Distance: SNT-matched point → RTIS ping GPS (RTIS H event) ──────────
        var rtisLat = row.violationLat, rtisLon = row.violationLon;
        var distToRtisM = (isValidGPS(rtisLat, rtisLon) && isValidGPS(violLat, violLon))
            ? getDistance(violLat, violLon, rtisLat, rtisLon) * 1000
            : null;

        leafletMap = L.map('mapDiv').setView([violLat, violLon], 15);
        // Esri World Imagery (satellite) base layer
        L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
            attribution: 'Tiles © Esri — Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP',
            maxZoom: 19
        }).addTo(leafletMap);
        // Road/label overlay so station names remain visible on satellite
        L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Transportation/MapServer/tile/{z}/{y}/{x}', {
            attribution: '', maxZoom: 19, opacity: 0.55
        }).addTo(leafletMap);

        // Violation popup builder
        function violPopupHTML() {
            var sc = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
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
                "<strong>🚨 " + row.result + "</strong><hr style='margin:4px 0'>" +
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
            .bindTooltip(row.result + ' · ' + row.speed + ' km/h', {permanent:false});
            showViolPanel(row, speedAtSignal, speedLimit, matchQuality, diffSecStr, distToFsdM, nearestFsdSig, distToRtisM);

        } else {
            // CASE B: Per-second track — SNT-anchored ±40 window
            var allTrainPoints = perSecondData[resultIdx];

            // If matchResult doesn't have idx, find it now
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
                    m.bindTooltip(row.result + ' · ' + speedAtSignal + ' km/h', {permanent:false});
                    showViolPanel(row, speedAtSignal, speedLimit, matchQuality, diffSecStr, distToFsdM, nearestFsdSig, distToRtisM);
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

            // Draw RTIS ping marker (blue square) if meaningfully different from SNT-matched point
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
                // Draw dashed line between RTIS ping and SNT-matched violation point
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

        // MOD4: init map filter strip when per-sec data loaded
        if (hasPersec && perSecondData[resultIdx] && perSecondData[resultIdx].length > 0) {
            _initFilterUI(perSecondData[resultIdx], sntDateObj.getTime(), resultIdx, row);
        } else {
            document.getElementById('mapFilterStrip').style.display = 'none';
        }
    }

    // ==========================================
    // CSV DOWNLOAD
    // ==========================================
    // ══════════════════════════════════════════════════════════
    // FEATURE 1: BATCH PER-SEC UPLOAD
    // Matches files to violation rows by loco number in filename
    // ══════════════════════════════════════════════════════════
    async function handleBatchPerSec(event) {
        var files = Array.from(event.target.files);
        if (!files.length) return;
        event.target.value = '';   // reset immediately so same files can be re-selected

        log('📂 Batch upload: ' + files.length + ' file(s) selected — building queue...');

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
                analysisResults.forEach(function(r, ri) {
                    if (String(r.deviceId || '').trim() === firstNum && r.speed > 50) rowsForFile.push(ri);
                });
                if (rowsForFile.length)
                    log('📡 Device ID match: ' + fname + ' → Device ' + firstNum + ' (' + rowsForFile.length + ' row(s) > 50 km/h)');
            }

            // Priority 2: Loco number fallback
            if (rowsForFile.length === 0 && nums.length) {
                var matchedLocos = new Set();
                nums.forEach(function(n) {
                    var loco = parseInt(n);
                    analysisResults.forEach(function(r) { 
                        if (parseInt(r.loco) === loco && r.speed > 50) matchedLocos.add(loco); 
                    });
                });
                for (var ri2 = 0; ri2 < analysisResults.length; ri2++) {
                    var r = analysisResults[ri2];
                    if (matchedLocos.has(parseInt(r.loco)) && r.speed > 50) rowsForFile.push(ri2);
                }
                if (rowsForFile.length)
                    log('🔢 Loco fallback: ' + fname + ' → loco(s) ' + Array.from(matchedLocos).join(',') + ' (' + rowsForFile.length + ' row(s) > 50 km/h)');
            }

            if (rowsForFile.length === 0) {
                log('⚠️  Skipped: ' + fname + ' — No matched rows with speed > 50 km/h (nums: ' + nums.join(',') + ')');
                skipped++;
                return;
            }

            rowsForFile.forEach(function(ri) {
                var r = analysisResults[ri];
                assignments.push({
                    file: file,
                    rowIdx: ri,
                    label: fname + ' → Dev ' + (r.deviceId || '?') + ' / Loco ' + r.loco +
                           ' [' + r.station + ' · ' + r.sigNo + ']'
                });
            });
        });

        if (assignments.length === 0) {
            log('⚠️  No rows matched — nothing to process.');
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
        // Processing 20 files in parallel kills the browser — each PapaParse
        // callback fires simultaneously, triggers concurrent renderTable() calls,
        // races on shared analysisResults state, and OOMs the JS heap.
        _batchRunning = true;
        _batchTotal   = assignments.length;
        _batchDone    = 0;

        log('🔄 Processing ' + assignments.length + ' assignment(s) serially (1 at a time)...');

        for (var ai = 0; ai < assignments.length; ai++) {
            var asgn = assignments[ai];
            log('  [' + (ai + 1) + '/' + assignments.length + '] ' + asgn.label);

            await new Promise(function(resolve) {
                handlePerSecFile({ target: { files: [asgn.file] } }, asgn.rowIdx, resolve);
            });

            _batchDone = ai + 1;
            if (progWrap) {
                var pct = Math.round(100 * _batchDone / assignments.length);
                prog.style.width = pct + '%';
                progLabel.textContent = _batchDone + ' / ' + assignments.length;
            }

            // Yield to browser every 5 files to keep UI responsive
            if ((ai + 1) % 5 === 0) {
                await new Promise(function(r){ setTimeout(r, 30); });
            }
        }

        // ── All done: single renderTable + saveSession ────────────────────────
        _batchRunning = false;
        renderTable();
        saveSession();

        if (progWrap) {
            prog.style.width = '100%';
            progLabel.textContent = '✓ Done';
            setTimeout(function(){ progWrap.style.display = 'none'; }, 2500);
        }

        log('✅ Batch complete: ' + assignments.length + ' processed, ' + skipped + ' skipped.');
    }

    function downloadCSV() {
        if (!analysisResults.length && !noTrainResults.length) return;

        // Sheet 1: Matched results
        var headers = ["Train No","Loco","Station","RTIS Time","Signal Time","Sig No","Dir (GPS Travel)","RTIS Speed","Per-Sec Speed @ SNT","Match Quality","Match Diff(s)","Result","Demoted","Demotion Reason","Diff(s)","Dist(m)","RTIS Precision"];
        var csvRows = [headers.join(',')];
        analysisResults.forEach(function(row) {
            csvRows.push([
                row.trainNo, row.loco, row.station,
                row.rtisTime, row.signalTime, row.sigNo,
                row.travelDir, row.speed,
                row.speedPerSec !== null && row.speedPerSec !== undefined ? row.speedPerSec : '',
                row.speedMatchQ || '', row.speedDiffSec || '',
                row.result,
                row.perSecDemoted ? 'YES' : '',
                row.perSecDemotedReason || '',
                row.diff, row.distM, row.precisionFlag
            ].map(function(v){ return '"' + String(v||'').replace(/"/g,'""') + '"'; }).join(','));
        });

        // Separator + Sheet 2: No Match records
        if (noTrainResults.length > 0) {
            csvRows.push('');
            csvRows.push('"=== NO MATCH (SNT events with no RTIS train found) ==="');
            csvRows.push(['"Signal Time"','"Station"','"Sig No"','"Dir (Sig)"','"Reason"'].join(','));
            noTrainResults.forEach(function(r) {
                csvRows.push([r.signalTime, r.station, r.sigNo, r.dirSig, r.reason]
                    .map(function(v){ return '"' + String(v||'').replace(/"/g,'""') + '"'; }).join(','));
            });
        }

        var blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
        var link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = "Signal_Violations_v10.csv";
        link.click();
    }

    // ══════════════════════════════════════════════════════════════
    // EXACT SNT-TIME MATCHING  (±5 s tolerance)
    // Returns { point, diffMs, quality }
    //   quality: 'exact'   → diff ≤ 5 s
    //            'closest' → diff > 5 s  (fallback, show warning)
    //            'none'    → no per-sec data at all
    // SNT time is the ground-truth anchor for the violation moment.
    // ══════════════════════════════════════════════════════════════
    function matchPerSecToSNT(perSecArr, sntTimeObj) {
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

    // ══════════════════════════════════════════════════════════════
    // WORD VIOLATION REPORT GENERATOR
    // One page per violation: info table + map screenshot + speed graph
    // ══════════════════════════════════════════════════════════════

    function _reportProgress(label, pct) {
        document.getElementById('reportProgressLabel').textContent = label;
        document.getElementById('reportProgressBar').style.width = pct + '%';
        document.getElementById('reportProgressPct').textContent = pct + '%';
    }

    // ══════════════════════════════════════════════════════════════
    // MAP CAPTURE — Esri REST Export API method
    //
    // Steps:
    //  1. Calculate a tight bounding box around the violation point
    //  2. Fetch satellite PNG directly from Esri's MapServer/export
    //     endpoint — no Leaflet, no html2canvas, no tile timing
    //  3. Draw the base image onto a canvas
    //  4. Project lat/lon → pixel using the bounding box
    //  5. Draw per-sec track polyline + FSD signal markers + violation pin
    //  6. Return dataURL — always clean, always aligned
    // ══════════════════════════════════════════════════════════════
    function _captureMapForRow(row, resultIdx) {
        return new Promise(function(resolve) {
            var speedLimit   = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
            var hasPersec    = perSecondData[resultIdx] && perSecondData[resultIdx].length > 0;
            var cleanStation = row.fsdStation || cleanStationName(row.station.split(' → ')[0]);

            // ── Resolve violation coordinates ─────────────────────────────
            var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
                var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
                var d = new Date(base);
                var tp = (row.signalTime || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
                if (tp) d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0);
                return d;
            })();
            var matchResult = row.perSecMatch || (hasPersec ? matchPerSecToSNT(perSecondData[resultIdx], sntDateObj) : null);

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
                resolve(_mapFallbackCanvas(row, null, null, speedAtSignal, speedLimit)); return;
            }

            // ── Collect FSD signals — live data or row cache (session restore) ──
            var fsdSignals = [];
            if (dataFSD.length > 0) {
                dataFSD.forEach(function(r) {
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
            if (hasPersec && perSecondData[resultIdx]) {
                var allPts = perSecondData[resultIdx];
                if (!matchResult || matchResult.idx === undefined) {
                    var bd = Infinity;
                    allPts.forEach(function(p,i){ var d=Math.abs(p.time-sntDateObj); if(d<bd){bd=d;sntMatchedIdx=i;} });
                }
                // Use ±80 points so context is available even at tight zoom
                var s2 = Math.max(0, sntMatchedIdx - 80), e2 = Math.min(allPts.length-1, sntMatchedIdx+80);
                trackPts    = allPts.slice(s2, e2+1);
                localViolIdx = sntMatchedIdx - s2;
            }

            // ── Image canvas size ──────────────────────────────────────────
            var IW = 860, IH = 420;

            // ── Web Mercator helpers (EPSG:3857) ───────────────────────────
            // CRITICAL: bbox AND pixel projection must use the same CRS.
            // Esri tiles are EPSG:3857. Using lat/lon linear projection causes
            // track-drift because Mercator Y is non-linear with latitude.
            function _mercX(lon) { return lon * 20037508.342789244 / 180; }
            function _mercY(lat) {
                var r = lat * Math.PI / 180;
                return Math.log(Math.tan(Math.PI / 4 + r / 2)) * 20037508.342789244 / Math.PI;
            }

            // ── Bbox centred exactly on violation point in Mercator space ──
            // halfMercY is the vertical half-extent in Mercator metres.
            // "Zoom ×3" from an auto-fit of ±40 per-sec points (~1500m span)
            // means half-extent ÷ 2³ ≈ 190m. Use 200m for a small margin.
            // halfMercX scaled to canvas aspect ratio so the image fills
            // exactly without geographic distortion.
            var HALF_Y = 200;                         // metres in Mercator
            var HALF_X = HALF_Y * (IW / IH);          // ~410m — matches 860:420

            var cMx = _mercX(violLon);                // Mercator centre X
            var cMy = _mercY(violLat);                // Mercator centre Y

            var mxMin = cMx - HALF_X, mxMax = cMx + HALF_X;
            var myMin = cMy - HALF_Y, myMax = cMy + HALF_Y;

            // ── lat/lon → pixel (Mercator, exact match to Esri render) ─────
            function latLonToPixel(lat, lon) {
                var mx = _mercX(lon), my = _mercY(lat);
                var x = Math.round((mx - mxMin) / (mxMax - mxMin) * IW);
                var y = Math.round((myMax - my) / (myMax - myMin) * IH);
                return { x: x, y: y };
            }

            // ── Fetch Esri World Imagery via REST Export (EPSG:3857 bbox) ──
            // Request both bboxSR and imageSR as 3857 — Esri renders native
            // Mercator tiles → no internal reprojection → no resampling artefacts.
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
                var ctx = canvas.getContext('2d');

                // ── Draw satellite base ───────────────────────────────────
                ctx.drawImage(satImg, 0, 0, IW, IH);

                // ── Draw per-sec track polyline ───────────────────────────
                if (trackPts.length > 1) {
                    ctx.beginPath();
                    ctx.strokeStyle = 'rgba(59,130,246,0.85)';
                    ctx.lineWidth = 3;
                    ctx.setLineDash([8, 5]);
                    var first = true;
                    trackPts.forEach(function(p) {
                        if (!isValidGPS(p.lat, p.lon)) return;
                        var px = latLonToPixel(p.lat, p.lon);
                        if (first) { ctx.moveTo(px.x, px.y); first = false; }
                        else ctx.lineTo(px.x, px.y);
                    });
                    ctx.stroke();
                    ctx.setLineDash([]);
                }

                // ── Draw non-violation track dots (every point) ────────
                trackPts.forEach(function(p, i) {
                    if (i === localViolIdx || !isValidGPS(p.lat, p.lon)) return;
                    var px = latLonToPixel(p.lat, p.lon);
                    // Skip points outside visible canvas (clipping)
                    if (px.x < -10 || px.x > IW+10 || px.y < -10 || px.y > IH+10) return;
                    var proximity = 1 - Math.min(1, Math.abs(i - localViolIdx) / 82);
                    ctx.beginPath();
                    ctx.arc(px.x, px.y, 4.5, 0, Math.PI*2);
                    ctx.fillStyle = 'rgba(59,130,246,' + (0.45 + 0.5*proximity) + ')';
                    ctx.fill();
                    ctx.strokeStyle = 'rgba(30,64,175,0.9)'; ctx.lineWidth = 1.2;
                    ctx.stroke();
                });

                // ── Draw FSD signal markers ───────────────────────────────
                fsdSignals.forEach(function(sig) {
                    var px = latLonToPixel(sig.lat, sig.lon);
                    var col = sig.dir === 'UP' ? '#8b5cf6' : (sig.dir === 'DN' ? '#f59e0b' : '#6b7280');
                    // Square marker
                    ctx.fillStyle = col;
                    ctx.strokeStyle = 'white'; ctx.lineWidth = 1.5;
                    ctx.beginPath();
                    var s = 9;
                    roundRect(ctx, px.x-s, px.y-s, s*2, s*2, 3);
                    ctx.fill(); ctx.stroke();
                    // Direction arrow
                    ctx.fillStyle = 'white'; ctx.font = 'bold 10px Arial'; ctx.textAlign = 'center';
                    ctx.fillText(sig.dir === 'UP' ? '▲' : (sig.dir === 'DN' ? '▼' : '●'), px.x, px.y+4);
                    // Label
                    ctx.fillStyle = col; ctx.font = 'bold 9px Arial'; ctx.textAlign = 'left';
                    ctx.fillText('S' + (sig.sigNo||'?'), px.x+s+3, px.y+4);
                });

                // ── Draw violation pin (large red circle) ─────────────────
                var vPx = latLonToPixel(violLat, violLon);
                // Outer glow
                ctx.beginPath();
                ctx.arc(vPx.x, vPx.y, 22, 0, Math.PI*2);
                ctx.fillStyle = 'rgba(239,68,68,0.25)'; ctx.fill();
                // Main circle
                ctx.beginPath();
                ctx.arc(vPx.x, vPx.y, 16, 0, Math.PI*2);
                ctx.fillStyle = '#ef4444'; ctx.fill();
                ctx.strokeStyle = '#7f1d1d'; ctx.lineWidth = 3; ctx.stroke();
                // Inner white dot
                ctx.beginPath();
                ctx.arc(vPx.x, vPx.y, 5, 0, Math.PI*2);
                ctx.fillStyle = 'white'; ctx.fill();

                // ── Speed label callout near violation pin ────────────────
                var lx = vPx.x + 22, ly = vPx.y - 22;
                // Keep label inside canvas
                if (lx + 130 > IW) lx = vPx.x - 155;
                if (ly < 30) ly = vPx.y + 30;
                var sc = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
                var labelText = row.sigNo + ' — ' + speedAtSignal + ' km/h';
                ctx.font = 'bold 12px Arial';
                var tw = ctx.measureText(labelText).width + 16;
                // Background pill
                ctx.fillStyle = 'rgba(255,255,255,0.96)';
                ctx.strokeStyle = sc; ctx.lineWidth = 2;
                roundRect(ctx, lx, ly-16, tw, 22, 5);
                ctx.fill(); ctx.stroke();
                // Text
                ctx.fillStyle = sc; ctx.textAlign = 'left';
                ctx.fillText(labelText, lx+8, ly+1);

                // Connector line from label to pin
                ctx.beginPath();
                ctx.moveTo(lx > vPx.x ? lx : lx+tw, ly+3);
                ctx.lineTo(vPx.x, vPx.y-16);
                ctx.strokeStyle = sc; ctx.lineWidth = 1.5; ctx.setLineDash([4,3]);
                ctx.stroke(); ctx.setLineDash([]);

                // ── Attribution strip ─────────────────────────────────────
                ctx.fillStyle = 'rgba(0,0,0,0.55)';
                ctx.fillRect(0, IH-18, IW, 18);
                ctx.fillStyle = 'white'; ctx.font = '9px Arial'; ctx.textAlign = 'center';
                ctx.fillText('Esri World Imagery · © Esri, DigitalGlobe, GeoEye, Earthstar Geographics', IW/2, IH-5);

                // ── Violation info panel (mirrors showViolPanel) ───────────
                // Build info lines
                var isViol = (row.resultClass === 'violation' || row.resultClass === 'violation-multi');
                var panelLines = [];
                panelLines.push({ text: (isViol ? '🚨 VIOLATION' : '✅ ' + row.result), bold: true, color: isViol ? '#dc2626' : '#16a34a', size: 11 });
                panelLines.push({ text: 'Train: ' + row.trainNo, bold: false, color: '#111', size: 10.5 });
                panelLines.push({ text: 'Signal: ' + row.sigNo + ' @ ' + row.signalTime, bold: false, color: '#111', size: 10.5 });
                var spdColor = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
                panelLines.push({ text: 'Speed @ SNT: ' + speedAtSignal + ' km/h', bold: true, color: spdColor, size: 10.5 });
                panelLines.push({ text: 'RTIS Speed: ' + row.speed + ' km/h', bold: false, color: '#374151', size: 10 });

                // Match quality line
                var mqText = matchResult
                    ? (matchResult.quality === 'exact'
                        ? '✓ Exact match (Δ0.0s)'
                        : '⚠ Closest (Δ' + (matchResult.diffMs/1000).toFixed(1) + 's)')
                    : 'RTIS ping — no per-sec';
                panelLines.push({ text: mqText, bold: false, color: matchResult && matchResult.quality === 'exact' ? '#16a34a' : '#b45309', size: 9.5 });

                // FSD distance
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

                // ±5s speed trend — split into two lines so it doesn't overflow panel width
                if (matchResult && matchResult.point && perSecondData[resultIdx]) {
                    var _pts2 = perSecondData[resultIdx];
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

                // Calculate panel dimensions — wider to fit speed trend
                var PX = 10, PY = 10;          // top-left position
                var PAD2 = 9, LINE_H = 15;
                var panelW = 210, panelH = PAD2 + panelLines.length * LINE_H + PAD2;

                // Keep panel inside canvas
                if (PX + panelW > IW - 10) PX = IW - panelW - 10;
                if (PY + panelH > IH - 25) PY = IH - panelH - 25;

                // Shadow
                ctx.shadowColor = 'rgba(0,0,0,0.35)';
                ctx.shadowBlur = 8; ctx.shadowOffsetX = 2; ctx.shadowOffsetY = 2;

                // Panel background
                ctx.fillStyle = 'rgba(255,255,255,0.97)';
                roundRect(ctx, PX, PY, panelW, panelH, 8);
                ctx.fill();
                ctx.shadowColor = 'transparent'; ctx.shadowBlur = 0; ctx.shadowOffsetX = 0; ctx.shadowOffsetY = 0;

                // Left accent bar
                ctx.fillStyle = isViol ? '#ef4444' : '#22c55e';
                roundRect(ctx, PX, PY, 4, panelH, 4);
                ctx.fill();

                // Border
                ctx.strokeStyle = isViol ? 'rgba(239,68,68,0.35)' : 'rgba(34,197,94,0.35)';
                ctx.lineWidth = 1;
                roundRect(ctx, PX, PY, panelW, panelH, 8);
                ctx.stroke();

                // Draw each line
                var ly2 = PY + PAD2 + 11;
                ctx.textAlign = 'left';
                panelLines.forEach(function(line) {
                    ctx.font = (line.bold ? 'bold ' : '') + line.size + 'px Arial';
                    ctx.fillStyle = line.color;
                    ctx.fillText(line.text, PX + 10, ly2);
                    ly2 += LINE_H;
                });

                resolve(canvas.toDataURL('image/png'));
            };

            satImg.onerror = function() {
                // Esri fetch failed — try OSM Nominatim tiles as fallback
                _fetchOsmFallback(row, violLat, violLon, speedAtSignal, speedLimit,
                    trackPts, localViolIdx, fsdSignals, latLonToPixel, IW, IH, resolve);
            };

            satImg.src = esriUrl;
        });
    }

    // Helper: rounded rectangle path
    function roundRect(ctx, x, y, w, h, r) {
        ctx.beginPath();
        ctx.moveTo(x+r, y);
        ctx.lineTo(x+w-r, y); ctx.arcTo(x+w, y, x+w, y+r, r);
        ctx.lineTo(x+w, y+h-r); ctx.arcTo(x+w, y+h, x+w-r, y+h, r);
        ctx.lineTo(x+r, y+h); ctx.arcTo(x, y+h, x, y+h-r, r);
        ctx.lineTo(x, y+r); ctx.arcTo(x, y, x+r, y, r);
        ctx.closePath();
    }

    // OSM static tiles fallback (when Esri REST fails)
    function _fetchOsmFallback(row, violLat, violLon, speedAtSignal, speedLimit,
                                trackPts, localViolIdx, fsdSignals, latLonToPixel, IW, IH, resolve) {
        // Build a plain dark canvas with info — OSM tiles have CORS restrictions so we just show a clean placeholder
        var canvas = document.createElement('canvas');
        canvas.width = IW; canvas.height = IH;
        var ctx = canvas.getContext('2d');

        // Dark background
        ctx.fillStyle = '#1e293b';
        ctx.fillRect(0, 0, IW, IH);

        // Grid
        ctx.strokeStyle = '#334155'; ctx.lineWidth = 0.8;
        for (var gx = 0; gx < IW; gx += 80) { ctx.beginPath(); ctx.moveTo(gx,0); ctx.lineTo(gx,IH); ctx.stroke(); }
        for (var gy = 0; gy < IH; gy += 80) { ctx.beginPath(); ctx.moveTo(0,gy); ctx.lineTo(IW,gy); ctx.stroke(); }

        // Track polyline
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

        // Violation pin
        var vPx = latLonToPixel(violLat, violLon);
        ctx.beginPath(); ctx.arc(vPx.x, vPx.y, 18, 0, Math.PI*2);
        ctx.fillStyle = '#ef4444'; ctx.fill();
        ctx.strokeStyle = '#fca5a5'; ctx.lineWidth = 3; ctx.stroke();
        ctx.beginPath(); ctx.arc(vPx.x, vPx.y, 6, 0, Math.PI*2);
        ctx.fillStyle = 'white'; ctx.fill();

        // Labels
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


    // Canvas fallback when leaflet-image fails
    function _mapFallbackCanvas(row, lat, lon, speed, speedLimit) {
        var c = document.createElement('canvas');
        c.width = 860; c.height = 420;
        var ctx = c.getContext('2d');
        ctx.fillStyle = '#1e293b';
        ctx.fillRect(0, 0, c.width, c.height);
        // Grid lines
        ctx.strokeStyle = '#334155'; ctx.lineWidth = 1;
        for (var i = 0; i < c.width; i += 60) { ctx.beginPath(); ctx.moveTo(i,0); ctx.lineTo(i,c.height); ctx.stroke(); }
        for (var j = 0; j < c.height; j += 60) { ctx.beginPath(); ctx.moveTo(0,j); ctx.lineTo(c.width,j); ctx.stroke(); }
        // Center dot
        ctx.fillStyle = '#ef4444';
        ctx.beginPath(); ctx.arc(c.width/2, c.height/2, 18, 0, Math.PI*2); ctx.fill();
        ctx.fillStyle = '#fef2f2'; ctx.beginPath(); ctx.arc(c.width/2, c.height/2, 6, 0, Math.PI*2); ctx.fill();
        // Labels
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

    // Render a speed-time chart into #reportChartCanvas and return PNG dataURL
    function _captureChartForRow(row, resultIdx) {
        // Compute braking distance for PDF label and attach to row
        var _pts2 = perSecondData[resultIdx] || [];
        if (_pts2.length && row.sntTimeISO) {
            var _sntMs2 = new Date(row.sntTimeISO).getTime();
            var _vi2 = 0, _bd2 = Infinity;
            _pts2.forEach(function(p, i) { var d = Math.abs(p.time.getTime() - _sntMs2); if (d < _bd2) { _bd2 = d; _vi2 = i; } });
            row._pdfBrakingDist = _calcBrakingDist(_pts2, _sntMs2, _vi2);
        } else {
            row._pdfBrakingDist = null;
        }
        return new Promise(function(resolve) {
            var canvas = document.getElementById('reportChartCanvas');
            canvas.width = 900; canvas.height = 400;
            var ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, 900, 400);

            var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
            var hasPersec  = perSecondData[resultIdx] && perSecondData[resultIdx].length > 0;
            var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
                var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
                var d = new Date(base); var tp = (row.signalTime || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
                if (tp) d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0); return d;
            })();

            var displayPts = [], violIdx = -1;
            if (hasPersec) {
                var allPts = perSecondData[resultIdx];
                var grMatch = row.perSecMatch || matchPerSecToSNT(allPts, sntDateObj);
                var matchIdx = grMatch ? (grMatch.idx || 0) : 0;
                // Try ±3 min time window first
                var violMs = sntDateObj.getTime(), w3 = 3 * 60 * 1000;
                var timePts = allPts.filter(function(p) { return Math.abs(p.time.getTime() - violMs) <= w3; });
                // If too few, use ±120 index window around matched point (covers 2 min of per-sec data)
                if (timePts.length < 5) {
                    displayPts = allPts.slice(Math.max(0, matchIdx - 120), Math.min(allPts.length, matchIdx + 121));
                } else {
                    displayPts = timePts;
                }
                // If still too few, show all data
                if (displayPts.length < 3) displayPts = allPts;
                violIdx = grMatch ? displayPts.findIndex(function(p) { return p === grMatch.point; }) : -1;
                if (violIdx < 0 && grMatch && grMatch.point) {
                    var bestD = Infinity;
                    displayPts.forEach(function(p, i) { var d = Math.abs(p.time - grMatch.point.time); if (d < bestD) { bestD = d; violIdx = i; } });
                }
            } else {
                var trainKey = String(row.trainNo), stnKey = row.stationKey || cleanStationName(row.station.split('→')[0].trim());
                dataRTIS.forEach(function(r) {
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

            var tempChart = new Chart(ctx, {
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
                        // Compute braking distance inline for the PDF chart
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
                        // Shaded region
                        c2.fillStyle = 'rgba(251,146,60,0.13)';
                        c2.fillRect(x1, top2, x2 - x1, bot2 - top2);
                        // Right boundary
                        c2.strokeStyle = 'rgba(234,88,12,0.65)'; c2.lineWidth = 1.5; c2.setLineDash([4,3]);
                        c2.beginPath(); c2.moveTo(x2, top2); c2.lineTo(x2, bot2); c2.stroke();
                        c2.setLineDash([]);
                        // Distance pill label
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

    // Fetch a dataURL image and return ArrayBuffer of PNG bytes
    function _dataUrlToBytes(dataUrl) {
        var b64 = dataUrl.split(',')[1];
        var bin = atob(b64);
        var arr = new Uint8Array(bin.length);
        for (var i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i);
        return arr.buffer;
    }

    // ══════════════════════════════════════════════════════════════
    // PDF REPORT SYSTEM
    // ══════════════════════════════════════════════════════════════

    var _pdfViolations = []; // violations being processed in dialog

    function openPdfDialog() {
        var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;

        // Only rows that: are violations AND have per-sec data AND per-sec speed is OVER limit (red)
        _pdfViolations = analysisResults.filter(function(r) {
            var isViol   = r.resultClass === 'violation' || r.resultClass === 'violation-multi';
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

            // LP input grid — 3 cols: Train No | LP Name | LP HQ / LP CLI Name. | Train Date | Journey Date
            var grid = document.createElement('div');
            grid.style.cssText = 'display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:10px;';

            // Extract SNT date as YYYY-MM-DD for date inputs
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
                inp.type = f[3];           // 'text' or 'date'
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

            // ── Delete button: created with createElement so listener attaches before DOM insert ──
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
            // Append delete button into the header's right-side div
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
                document.getElementById; // placeholder
                hdr.style.borderRadius = '8px';
                // set arrow after DOM insert — use timeout
                setTimeout(function(idx){ return function(){ var a=document.getElementById('pdfArrow_'+idx); if(a) a.style.transform='rotate(-90deg)'; }; }(vi), 0);
            }

            card.appendChild(hdr);
            card.appendChild(body);
            container.appendChild(card);
        });

        // Update modal header count
        document.getElementById('pdfDialogViolCount').textContent = _pdfViolations.length + ' violation' + (_pdfViolations.length!==1?'s':'') + ' — fill LP details below';

        var modal = document.getElementById('pdfDialogModal');
        modal.style.display = 'flex';
        container.scrollTop = 0;
    }

    function closePdfDialog() {
        document.getElementById('pdfDialogModal').style.display = 'none';
    }

    function pdfExpandAll(open) {
        _pdfViolations.forEach(function(_, vi) {
            var body  = document.getElementById('pdfBody_' + vi);
            var arrow = document.getElementById('pdfArrow_' + vi);
            if (!body || !arrow) return;
            body.style.display  = open ? 'block' : 'none';
            arrow.style.transform = open ? 'rotate(0deg)' : 'rotate(-90deg)';
        });
    }

    async function generateAllPdfs() {
        if (!_pdfViolations.length) return;

        // ── Only process cards still present in the DOM (not deleted by user) ──
        var container = document.getElementById('pdfViolationCards');
        var activeVis = Array.from(container.querySelectorAll('[data-vi]'))
                             .map(function(el){ return parseInt(el.getAttribute('data-vi')); });

        if (!activeVis.length) { alert('No violations remaining — all were deleted.'); return; }

        // ── Capture LP field values NOW for active cards only, before dialog is closed ──
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

            // Use pre-captured LP values
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
            await _buildSinglePdf(row, vi, speedLimit, trainNoEdited, lpName, lpHQ, lpCLI, trainDate, journeyDate, remarks, mapDataUrl, chartDataUrl);
        }

        _reportProgress('Done! ' + total + ' PDF(s) generated.', 100);
        await new Promise(function(r){ setTimeout(r, 1800); });
        modal.style.display = 'none';
    }

    async function _buildSinglePdf(row, vi, speedLimit, trainNoEdited, lpName, lpHQ, lpCLI, trainDate, journeyDate, remarks, mapDataUrl, chartDataUrl) {
        
        var doc = new window.jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
        var W = 210, H = 297;
        var mg = 10;   // margin
        var cW = W - mg*2; // content width
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

        // ── Train & Signal Info (4 rows × 6mm + 7mm header = 31mm) ──
        y = _pdfSH(doc, 'TRAIN & SIGNAL INFORMATION', y, mg, W);
        y = _pdfTbl(doc, [
            ['Train No.',    String(trainNoEdited),     'Loco No.',     String(row.loco || '—')],
            ['Station',      String(row.station),      'Signal No.',   String(row.sigNo)],
            ['Signal Type',  sigLabel,                 'Direction',    dirLabel],
            ['SNT Time',     String(row.signalTime),   'RTIS Speed',   String(row.speed) + ' km/h'],
        ], y, mg, W, false, 5);
        y += 2;

        // ── Violation Summary (2 rows × 6mm + 7mm header = 19mm) ──
        y = _pdfSH(doc, 'VIOLATION SUMMARY', y, mg, W);
        y = _pdfTbl(doc, [
            ['Speed at Signal', violSpeed + ' km/h',  'Speed Limit',  speedLimit + ' km/h'],
            ['Excess Speed',   '+' + overSpeed + ' km/h', 'Data Source', 'Per-Sec GPS (verified)'],
        ], y, mg, W, true, 5);
        y += 2;

        // ── LP Details (3 rows × 5mm + 6mm header) ──
        y = _pdfSH(doc, 'LOCO PILOT DETAILS', y, mg, W);
        y = _pdfTbl(doc, [
            ['LP Name',      lpName  || '—',            'LP HQ',        lpHQ    || '—'],
            ['LP CLI Name.', lpCLI   || '—',            'Remarks',      remarks || '—'],
            ['Train Date',   _fmtDate(trainDate),       'Journey Date', _fmtDate(journeyDate)],
        ], y, mg, W, false, 5);
        y += 2;

        // ── Map (section header 7mm + image) ──
        var remaining = H - y - 14; // 14mm footer
        var mapH  = Math.min(Math.round(remaining * 0.52), 78);
        var chartH = remaining - mapH - 16; // 2 section headers

        y = _pdfSH(doc, 'LOCATION MAP', y, mg, W);
        if (mapDataUrl) {
            try {
                doc.addImage(mapDataUrl, 'PNG', mg, y, cW, mapH);
            } catch(e) {
                _pdfNoImage(doc, 'Map unavailable', row.violationLat, row.violationLon, mg, y, cW, mapH);
            }
        } else {
            _pdfNoImage(doc, 'Map unavailable', row.violationLat, row.violationLon, mg, y, cW, mapH);
        }
        y += mapH + 2;

        // ── Speed Graph ──
        y = _pdfSH(doc, 'SPEED PROFILE GRAPH', y, mg, W);
        if (chartDataUrl && chartH > 15) {
            try {
                doc.addImage(chartDataUrl, 'PNG', mg, y, cW, chartH);
            } catch(e) {
                doc.setFontSize(8); doc.setTextColor(150,150,150);
                doc.text('[Speed graph unavailable]', mg, y + 6);
            }
        }

        // ── Footer ──
        doc.setDrawColor(180, 30, 30);
        doc.setLineWidth(0.35);
        doc.line(mg, H - 10, W - mg, H - 10);
        doc.setFontSize(7); doc.setTextColor(140,140,140); doc.setFont('helvetica','normal');
        doc.text('Railway Signal Violation Dashboard  |  Confidential  |  ' + today, mg, H - 5.5);
        doc.text('Page 1 of 1', W - mg, H - 5.5, { align: 'right' });

        // ── Save — Violation_TrainNo_LocoNo_LPName_LPHQ_Station_SignalNo_dateOfViolation ──
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

    function _pdfNoImage(doc, label, lat, lon, x, y, w, h) {
        doc.setFillColor(248, 250, 252);
        doc.rect(x, y, w, h, 'F');
        doc.setDrawColor(203,213,225); doc.setLineWidth(0.3);
        doc.rect(x, y, w, h);
        doc.setFontSize(9); doc.setTextColor(148,163,184); doc.setFont('helvetica','italic');
        doc.text(label + (lat ? '  (GPS: ' + (lat||'') + ', ' + (lon||'') + ')' : ''), x + w/2, y + h/2, { align:'center' });
    }

    // Section header bar — returns new y
    function _pdfSH(doc, text, y, mg, W) {
        doc.setFillColor(30, 58, 95);
        doc.rect(mg, y, W - mg*2, 6, 'F');
        doc.setFont('helvetica', 'bold');
        doc.setFontSize(7.5);
        doc.setTextColor(255, 255, 255);
        doc.text(text, mg + 2.5, y + 4.2);
        return y + 7;
    }

    // Info table — returns new y
    function _pdfTbl(doc, rows, y, mg, W, highlight, rowH) {
        rowH = rowH || 6;
        var colW = (W - mg*2) / 4;
        rows.forEach(function(r) {
            // bg fills
            doc.setFillColor(248, 250, 252);
            doc.rect(mg,           y, colW, rowH, 'F');
            doc.rect(mg + colW*2,  y, colW, rowH, 'F');
            if (highlight) { doc.setFillColor(254, 226, 226); } else { doc.setFillColor(255,255,255); }
            doc.rect(mg + colW,    y, colW, rowH, 'F');
            doc.rect(mg + colW*3,  y, colW, rowH, 'F');
            // border
            doc.setDrawColor(209, 213, 219); doc.setLineWidth(0.18);
            doc.rect(mg, y, W - mg*2, rowH);
            doc.line(mg + colW,   y, mg + colW,   y + rowH);
            doc.line(mg + colW*2, y, mg + colW*2, y + rowH);
            doc.line(mg + colW*3, y, mg + colW*3, y + rowH);
            // label text
            doc.setFont('helvetica', 'normal'); doc.setFontSize(6.8); doc.setTextColor(100,116,139);
            doc.text(String(r[0]), mg + 1.5,        y + rowH - 1.8);
            doc.text(String(r[2]), mg + colW*2 + 1.5, y + rowH - 1.8);
            // value text
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

    // ══════════════════════════════════════════════════════════════
// SPEED-TIME GRAPH (Extracted to src/ui/speed-graph.js)
// ══════════════════════════════════════════════════════════════
import { processSpeedGraph as _openGraph, closeSpeedGraph as _closeGraph } from './ui/speed-graph.js';

function openGraph(resultIdx) {
    var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value) || 63;
    speedChartInstance = _openGraph(resultIdx, analysisResults, perSecondData, dataRTIS, speedLimit, speedChartInstance, _initFilterUI, _msToTimeStr, _buildStnOptions, _drawChart, matchPerSecToSNT);
}

function closeGraph() {
    speedChartInstance = _closeGraph(speedChartInstance);
}

// ══════════════════════════════════════════════════════════════
// STATION MAPPING EDITOR (Extracted to src/ui/mapping-editor.js)
// ══════════════════════════════════════════════════════════════
import { openMapEditor as _openMapEditor, closeMapEditor as _closeMapEditor, getMappingEdits, exportMappingCSV as _exportMappingCSV } from './ui/mapping-editor.js';

function openMapEditor() { _openMapEditor(dataRTIS, allFsdStationNames, stationMappingCache, manualOverrides); }
function closeMapEditor() { _closeMapEditor(); }
function applyMappingEdits() {
    getMappingEdits(manualOverrides, stationMappingCache);
    closeMapEditor();
    log('✏️ Manual overrides saved. Re-running analysis...');
    if (dataRTIS.length && dataSNT.length && dataFSD.length) { setTimeout(function() { executeLogic(); }, 100); }
}
function exportMappingCSV() { _exportMappingCSV(dataRTIS, stationMappingCache, manualOverrides); }

// Backdrop close handled by onclick on backdrops.
// ════════════════════════════════════════════════════════════════
    // MOD 2: DRAGGABLE VIOLATION PANEL
    // ════════════════════════════════════════════════════════════════
    (function(){
        var el, dragging=false, ox=0, oy=0;
        document.addEventListener('mousedown', function(e){
            el = document.getElementById('violPanel');
            if (!el || !el.contains(e.target) || e.target.classList.contains('vp-close')) return;
            dragging = true;
            var r = el.getBoundingClientRect();
            ox = e.clientX - r.left; oy = e.clientY - r.top;
            e.preventDefault();
        });
        document.addEventListener('mousemove', function(e){
            if (!dragging || !el) return;
            var container = document.getElementById('mapContainer');
            if (!container) return;
            var cr = container.getBoundingClientRect();
            var nl = Math.max(0, Math.min(e.clientX - cr.left - ox, cr.width  - el.offsetWidth));
            var nt = Math.max(0, Math.min(e.clientY - cr.top  - oy, cr.height - el.offsetHeight));
            el.style.left  = nl + 'px';
            el.style.top   = nt + 'px';
            el.style.right = 'auto';
        });
        document.addEventListener('mouseup', function(){ dragging=false; });
    })();

    function showViolPanel(row, speedAtSignal, speedLimit, matchQuality, diffSecStr, distToFsdM, nearestFsdSig, distToRtisM) {
        var p = document.getElementById('violPanel');
        var isV = (row.resultClass === 'violation' || row.resultClass === 'violation-multi');
        p.className = isV ? '' : 'complied-panel';
        document.getElementById('vpTitle').innerHTML =
            (isV ? '🚨 ' : '✅ ') + row.result +
            ' <span class="vp-drag-hint">✥ drag</span>';
        var sc  = speedAtSignal > speedLimit ? '#dc2626' : '#16a34a';
        var mLine = matchQuality==='exact'
            ? '<span style="color:#16a34a;font-size:0.72rem">✓ Exact match (Δ'+diffSecStr+')</span>'
            : matchQuality==='closest'
            ? '<span style="color:#b45309;font-size:0.72rem">⚠ Closest (Δ'+diffSecStr+')</span>'
            : '<span style="color:#9ca3af;font-size:0.72rem">RTIS ping — no per-sec</span>';
        var fLine = distToFsdM!=null ? '<br>📍 FSD: <b>'+distToFsdM.toFixed(0)+' m</b>'+(nearestFsdSig?' ('+nearestFsdSig.dir+' S'+(nearestFsdSig.sigNo||'?')+')':'') : '';
        var rLine = (distToRtisM!=null && distToRtisM>1) ? '<br>🚂 RTIS H: <b>'+distToRtisM.toFixed(0)+' m</b>' : '';
        // Build ±5s speed trend from per-sec data if available
        var trendHtml = '';
        if (row.perSecMatch && row.perSecMatch.point) {
            // find resultIdx from analysisResults
            var _ridx = analysisResults.indexOf(row);
            var _pts = _ridx >= 0 && perSecondData[_ridx] ? perSecondData[_ridx] : null;
            if (_pts && _pts.length) {
                // find matched point index
                var _sntT = row.perSecMatch.point.time.getTime();
                var _mi = 0, _md = Infinity;
                _pts.forEach(function(p,i){ var d=Math.abs(p.time.getTime()-_sntT); if(d<_md){_md=d;_mi=i;} });
                var _trend = [], _limit = parseFloat(document.getElementById('inputSpeedLimit').value)||63;
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
        // default position — top right, reset each open
        p.style.right = '14px'; p.style.top = '60px'; p.style.left = 'auto';
    }

    // ════════════════════════════════════════════════════════════════
    // MOD 3+4: SHARED FILTER STATE
    // ════════════════════════════════════════════════════════════════
    var _fc = null;  // { allPts, sntMs, row, resultIdx }
    var _brakingDistResult = null; // { distM, violIdx, stopIdx, stoppedAtZero }

    // ── Braking distance: from violation point → first speed=0 or window end ──
    // Returns null if violation point is outside the pts window.
    // distFromSpeed is a per-second metre increment stored on each point.
    function _calcBrakingDist(pts, sntMs, violIdx) {
        if (!pts || !pts.length || violIdx < 0) return null;
        var winStart = pts[0].time.getTime();
        var winEnd   = pts[pts.length - 1].time.getTime();
        // Violation point must be inside the current window
        if (sntMs < winStart || sntMs > winEnd) return null;

        var totalDist = 0;
        var stopIdx = pts.length - 1; // default: end of window
        var stoppedAtZero = false;

        for (var i = violIdx; i < pts.length; i++) {
            var p = pts[i];
            // Accumulate per-second distance increment
            if (p.distFromSpeed !== null && p.distFromSpeed !== undefined && !isNaN(p.distFromSpeed)) {
                totalDist += p.distFromSpeed;
            }
            // Stop at first point where speed drops below 1 km/h
            // (some RTIS devices never report exact 0 — values like 0.23, 0.89 are effectively stopped)
            if (!isNaN(p.speed) && p.speed < 1) {
                stopIdx = i;
                stoppedAtZero = true;
                break;
            }
        }
        return { distM: Math.round(totalDist), violIdx: violIdx, stopIdx: stopIdx, stoppedAtZero: stoppedAtZero };
    }

    function _msToTimeStr(ms){ return new Date(ms).toTimeString().slice(0,8); }
    function _timeToMs(tStr, refDate){
        var p=tStr.split(':').map(Number), d=new Date(refDate);
        d.setHours(p[0]||0, p[1]||0, p[2]||0, 0); return d.getTime();
    }

    function _buildStnOptions(sel, pts, nearMs) {
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

    function _initFilterUI(pts, sntMs, resultIdx, row) {
        _fc = { allPts:pts, sntMs:sntMs, resultIdx:resultIdx, row:row };
        var f=pts[0].time.getTime(), t=pts[pts.length-1].time.getTime();
        // Map strip
        var ms=document.getElementById('mapFilterStrip');
        ms.style.display='flex';
        document.getElementById('mfFromTime').value=_msToTimeStr(f);
        document.getElementById('mfToTime').value=_msToTimeStr(t);
        _buildStnOptions(document.getElementById('mfFromStn'),pts,f);
        _buildStnOptions(document.getElementById('mfToStn'),pts,t);
        document.getElementById('mfPointCount').textContent='';
        // Graph strip
        var gs=document.getElementById('gfStrip');
        gs.style.display='flex';
        document.getElementById('gfFromTime').value=_msToTimeStr(f);
        document.getElementById('gfToTime').value=_msToTimeStr(t);
        _buildStnOptions(document.getElementById('gfFromStn'),pts,f);
        _buildStnOptions(document.getElementById('gfToStn'),pts,t);
        document.getElementById('gfPointCount').textContent='';
    }

    function _getFiltered(pref) {
        if(!_fc) return [];
        var fMs=_timeToMs(document.getElementById(pref+'FromTime').value, _fc.allPts[0].time);
        var tMs=_timeToMs(document.getElementById(pref+'ToTime').value,   _fc.allPts[0].time);
        return _fc.allPts.filter(function(p){ var m=p.time.getTime(); return m>=fMs&&m<=tMs; });
    }

    // Map filter controls
    function mfSyncTimeToStn(side){
        if(!_fc)return;
        var k=side==='from'?'From':'To';
        var tEl=document.getElementById('mf'+k+'Time'), sEl=document.getElementById('mf'+k+'Stn');
        var ms=_timeToMs(tEl.value, _fc.allPts[0].time);
        var bi=-1,bd=Infinity;
        for(var i=0;i<sEl.options.length;i++){var d=Math.abs(parseInt(sEl.options[i].value)-ms);if(d<bd){bd=d;bi=i;}}
        if(bi>=0) sEl.selectedIndex=bi;
    }
    function mfSyncStnToTime(side){
        if(!_fc)return;
        var k=side==='from'?'From':'To';
        var sEl=document.getElementById('mf'+k+'Stn'), tEl=document.getElementById('mf'+k+'Time');
        if(sEl.value) tEl.value=_msToTimeStr(parseInt(sEl.value));
    }
    function mfApply(){
        if(!_fc||!leafletMap)return;
        var pts=_getFiltered('mf');
        document.getElementById('mfPointCount').textContent=pts.length+' pts';
        _rebuildMapPath(pts);
    }
    function mfReset(){
        if(!_fc)return;
        var pts=_fc.allPts;
        document.getElementById('mfFromTime').value=_msToTimeStr(pts[0].time.getTime());
        document.getElementById('mfToTime').value=_msToTimeStr(pts[pts.length-1].time.getTime());
        _buildStnOptions(document.getElementById('mfFromStn'),pts,pts[0].time.getTime());
        _buildStnOptions(document.getElementById('mfToStn'),pts,pts[pts.length-1].time.getTime());
        mfApply();
    }

    // Graph filter controls
    function gfSyncTimeToStn(side){
        if(!_fc)return;
        var k=side==='from'?'From':'To';
        var tEl=document.getElementById('gf'+k+'Time'), sEl=document.getElementById('gf'+k+'Stn');
        var ms=_timeToMs(tEl.value, _fc.allPts[0].time);
        var bi=-1,bd=Infinity;
        for(var i=0;i<sEl.options.length;i++){var d=Math.abs(parseInt(sEl.options[i].value)-ms);if(d<bd){bd=d;bi=i;}}
        if(bi>=0) sEl.selectedIndex=bi;
    }
    function gfSyncStnToTime(side){
        if(!_fc)return;
        var k=side==='from'?'From':'To';
        var sEl=document.getElementById('gf'+k+'Stn'), tEl=document.getElementById('gf'+k+'Time');
        if(sEl.value) tEl.value=_msToTimeStr(parseInt(sEl.value));
    }
    function gfApply(){
        if(!_fc)return;
        var pts=_getFiltered('gf');
        document.getElementById('gfPointCount').textContent=pts.length+' pts';
        _drawChart(pts, _fc.sntMs, _fc.row);
    }
    function gfReset(){
        if(!_fc)return;
        var pts=_fc.allPts;
        document.getElementById('gfFromTime').value=_msToTimeStr(pts[0].time.getTime());
        document.getElementById('gfToTime').value=_msToTimeStr(pts[pts.length-1].time.getTime());
        _buildStnOptions(document.getElementById('gfFromStn'),pts,pts[0].time.getTime());
        _buildStnOptions(document.getElementById('gfToStn'),pts,pts[pts.length-1].time.getTime());
        gfApply();
    }

    // ── Map path rebuild (keeps violation marker + FSD signals, rebuilds track) ──
    var _trackLayer=null, _startMk=null, _endMk=null;
    function _rebuildMapPath(pts) {
        if(!leafletMap||!pts||pts.length<2)return;
        if(_trackLayer){leafletMap.removeLayer(_trackLayer);_trackLayer=null;}
        if(_startMk)   {leafletMap.removeLayer(_startMk);_startMk=null;}
        if(_endMk)     {leafletMap.removeLayer(_endMk);_endMk=null;}
        var ll=pts.filter(function(p){return p.lat&&p.lon;}).map(function(p){return[p.lat,p.lon];});
        if(ll.length<2)return;
        _trackLayer=L.polyline(ll,{color:'#3b82f6',weight:3,opacity:0.7,dashArray:'7,5'}).addTo(leafletMap);
        var mkS=L.divIcon({html:'<div style="width:11px;height:11px;border-radius:50%;background:#10b981;border:2px solid #065f46;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>',iconSize:[11,11],iconAnchor:[5,5]});
        var mkE=L.divIcon({html:'<div style="width:11px;height:11px;border-radius:50%;background:#f97316;border:2px solid #9a3412;box-shadow:0 1px 4px rgba(0,0,0,0.3)"></div>',iconSize:[11,11],iconAnchor:[5,5]});
        _startMk=L.marker(ll[0],{icon:mkS,zIndexOffset:200}).bindTooltip('▶ Start').addTo(leafletMap);
        _endMk  =L.marker(ll[ll.length-1],{icon:mkE,zIndexOffset:200}).bindTooltip('■ End').addTo(leafletMap);
        leafletMap.fitBounds(_trackLayer.getBounds(),{padding:[40,40]});
    }

    // ── Show KML button only when at least one row has per-sec data ──
    function _updateKmlButton() {
        var btn = document.getElementById('btnExportKML');
        if (!btn) return;
        var hasAny = analysisResults.some(function(_, idx) {
            return perSecondData[idx] && perSecondData[idx].length > 0;
        });
        btn.style.display = hasAny ? 'inline-block' : 'none';
    }

        // ── Chart redraw (used by filter + openGraph) ──
    function _drawChart(pts, sntMs, row) {
        var speedLimit = parseFloat(document.getElementById('inputSpeedLimit').value)||63;
        var labels=[], speeds=[], violIdx=-1, bd=Infinity;
        pts.forEach(function(p,i){
            labels.push(p.time.toLocaleTimeString());
            speeds.push(!isNaN(p.speed)?p.speed:null);
            var d=Math.abs(p.time.getTime()-sntMs);
            if(d<bd){bd=d;violIdx=i;}
        });

        // ── Braking distance calculation (dynamic — recalculates on every filter change) ──
        _brakingDistResult = _calcBrakingDist(pts, sntMs, violIdx);

        var pc=speeds.map(function(s,i){return i===violIdx?'#ef4444':(s!==null&&s>speedLimit?'#f97316':'#3b82f6');});
        var pr=speeds.map(function(s,i){return i===violIdx?8:3;});
        var ll=speeds.map(function(){return speedLimit;});
        var ctx=document.getElementById('speedChart').getContext('2d');
        if(speedChartInstance){speedChartInstance.destroy();speedChartInstance=null;}
        speedChartInstance=new Chart(ctx,{
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
                // ── Braking distance shaded region + label ──
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
                    // Orange-tinted shaded region between violation and stop/window-end
                    c.fillStyle='rgba(251,146,60,0.13)';
                    c.fillRect(x1,top,x2-x1,bot-top);
                    // Right boundary dashed line (stop point or window end)
                    c.strokeStyle='rgba(234,88,12,0.65)';c.lineWidth=1.5;c.setLineDash([4,3]);
                    c.beginPath();c.moveTo(x2,top);c.lineTo(x2,bot);c.stroke();
                    c.setLineDash([]);
                    // Distance label pill
                    var icon=_brakingDistResult.stoppedAtZero?'🛑 Stop':'→ End';
                    var label=icon+': '+_brakingDistResult.distM+' m';
                    c.font='bold 11px Arial';
                    var tw=c.measureText(label).width+16;
                    var midX=(x1+x2)/2, lx=Math.max(x1+2,midX-tw/2);
                    if(lx+tw>chart.chartArea.right-4)lx=chart.chartArea.right-tw-6;
                    var ly=top+56;
                    // Pill background
                    c.fillStyle='rgba(255,255,255,0.96)';
                    c.strokeStyle='rgba(234,88,12,0.8)';c.lineWidth=1.5;
                    c.beginPath();
                    c.moveTo(lx+5,ly-14);c.lineTo(lx+tw-5,ly-14);
                    c.arcTo(lx+tw,ly-14,lx+tw,ly-9,5);c.lineTo(lx+tw,ly+4);
                    c.arcTo(lx+tw,ly+9,lx+tw-5,ly+9,5);c.lineTo(lx+5,ly+9);
                    c.arcTo(lx,ly+9,lx,ly+4,5);c.lineTo(lx,ly-9);
                    c.arcTo(lx,ly-14,lx+5,ly-14,5);c.closePath();
                    c.fill();c.stroke();
                    // Label text
                    c.fillStyle='#c2410c';c.textAlign='left';
                    c.fillText(label,lx+8,ly+2);
                    c.restore();
                }}
            ]
        });
        // Braking distance entry in info bar
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


    // ════════════════════════════════════════════════════════════════
    // KML EXPORT — Google Earth  (async, non-blocking)
    // Three speed bands (green/amber/red), gx:Track for time-slider,
    // colour-grouped LineStrings, no per-point placemark flood.
    // ════════════════════════════════════════════════════════════════

    // ── KML document-level styles (fixed 3 speed bands + pins) ──────
    function _kmlDocStyles() {
        // KML colours are AABBGGRR hex
        return [
            '<Style id="trackGreen"><LineStyle><color>ff5ec522</color><width>3</width></LineStyle></Style>',
            '<Style id="trackAmber"><LineStyle><color>ff0b9ef5</color><width>3</width></LineStyle></Style>',
            '<Style id="trackRed"  ><LineStyle><color>ff4444ef</color><width>4</width></LineStyle></Style>',
            // Translucent blue for the animated gx:Track overlay
            '<Style id="trackAnim"><LineStyle><color>600000ff</color><width>2</width></LineStyle>' +
              '<IconStyle><scale>0.5</scale><Icon><href>http://maps.google.com/mapfiles/kml/shapes/track.png</href></Icon></IconStyle></Style>',
            // Violation pin — red circle paddle
            '<Style id="pinViolation"><IconStyle><color>ff1414ef</color><scale>1.6</scale>' +
              '<Icon><href>http://maps.google.com/mapfiles/kml/paddle/red-circle.png</href></Icon>' +
              '</IconStyle><LabelStyle><scale>1.0</scale><color>ff1414ef</color></LabelStyle></Style>',
            // FSD UP signal — purple triangle
            '<Style id="pinFsdUp"><IconStyle><color>fff65c8b</color><scale>1.2</scale>' +
              '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/triangle.png</href></Icon>' +
              '</IconStyle><LabelStyle><scale>0.8</scale></LabelStyle></Style>',
            // FSD DN signal — amber triangle
            '<Style id="pinFsdDn"><IconStyle><color>ff0b9ef5</color><scale>1.2</scale>' +
              '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/triangle.png</href></Icon>' +
              '</IconStyle><LabelStyle><scale>0.8</scale></LabelStyle></Style>',
            // RTIS H-event ping — blue square
            '<Style id="pinRtis"><IconStyle><color>fff6823b</color><scale>1.0</scale>' +
              '<Icon><href>http://maps.google.com/mapfiles/kml/shapes/square.png</href></Icon>' +
              '</IconStyle><LabelStyle><scale>0.8</scale></LabelStyle></Style>'
        ].join('\n');
    }

    // ── Group pts into consecutive runs by speed band ─────────────
    // Returns [{style, pts[]}] — typically 5-30 runs vs thousands of single-segment placemarks
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
                // Overlap: carry last point of previous run into new run for seamless join
                curStyle = style;
                curPts   = curPts.length ? [curPts[curPts.length - 1], p] : [p];
            } else {
                curPts.push(p);
            }
        }
        if (curPts.length > 1) segments.push({ style: curStyle, pts: curPts });
        return segments;
    }

    // ── Build one KML <Folder> per violation row ──────────────────
    // Uses pre-allocated arrays + single join per large block to avoid
    // O(N²) string growth. No individual push per track point.
    function _kmlViolationFolder(row, ri, pts, speedLimit) {
        var s = [];
        var isViol    = (row.resultClass === 'violation' || row.resultClass === 'violation-multi' || row.perSecPromoted);
        var violSpeed = (row.speedPerSec != null) ? row.speedPerSec : row.speed;
        var excess    = Math.max(0, violSpeed - speedLimit).toFixed(1);

        // Best violation coordinates: prefer exact per-sec SNT-matched point
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

        // ── 1. Animated gx:Track (Google Earth time slider) ──────────
        // Build <when> and <gx:coord> as two pre-sized arrays then join once.
        // This is O(N) but avoids N individual array push calls in the outer loop.
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

        // ── 2. Speed-coded track (colour-grouped runs) ────────────────
        // _buildSpeedSegments returns ~5-30 runs; each run = one Placemark with
        // all its coordinates joined into a single string — no per-point push.
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

        // ── 3. Violation pin ──────────────────────────────────────────
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

        // ── 4. FSD signals ────────────────────────────────────────────
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

        // ── 5. RTIS H-event ping (if meaningfully different from per-sec point) ─
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

    // ── XML-safe string escape ─────────────────────────────────────
    function _kmlEsc(str) {
        return String(str || '')
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&apos;');
    }

    // ── Async KML export — yields to browser between rows to prevent freeze ──
    // ── Open KML row-selection dialog ────────────────────────────
    function openKmlDialog() {
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

            // Row click toggles checkbox
            tr.addEventListener('click', function(e) {
                if (e.target.tagName === 'INPUT') return; // let checkbox handle itself
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

    function _kmlUpdateCount() {
        var checked = document.querySelectorAll('.kml-row-chk:checked').length;
        var total   = document.querySelectorAll('.kml-row-chk').length;
        var label   = document.getElementById('kmlSelCount');
        if (label) label.textContent = checked + ' / ' + total + ' selected';
        var genBtn  = document.getElementById('kmlGenerateBtn');
        if (genBtn) { genBtn.disabled = checked === 0; genBtn.style.opacity = checked === 0 ? '0.45' : '1'; }
    }

    function kmlSelectAll(val) {
        document.querySelectorAll('.kml-row-chk').forEach(function(chk) {
            chk.checked = val;
            var tr = chk.closest('tr');
            if (tr) { tr._sel = val; tr.style.background = val ? '#e0f2fe' : ''; }
        });
        _kmlUpdateCount();
    }

    function closeKmlDialog() {
        document.getElementById('kmlDialogModal').style.display = 'none';
    }

    async function generateSelectedKML() {
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




// Exposing inline handlers to window for Vite module compatibility
window.closeDemotionToast = closeDemotionToast;
window.runAnalysis = runAnalysis;
window.handleBatchPerSec = handleBatchPerSec;
window.clearSession = clearSession;
window.openPdfDialog = openPdfDialog;
window.openKmlDialog = openKmlDialog;
window.downloadCSV = downloadCSV;
window.switchTab = switchTab;
window.closeMap = closeMap;
window.mfSyncTimeToStn = mfSyncTimeToStn;
window.mfSyncStnToTime = mfSyncStnToTime;
window.mfApply = mfApply;
window.mfReset = mfReset;
window.closeGraph = closeGraph;
window.gfSyncTimeToStn = gfSyncTimeToStn;
window.gfSyncStnToTime = gfSyncStnToTime;
window.gfApply = gfApply;
window.gfReset = gfReset;
window.closeMapEditor = closeMapEditor;
window.exportMappingCSV = exportMappingCSV;
window.applyMappingEdits = applyMappingEdits;
window.openMapEditor = openMapEditor;
window.pdfExpandAll = pdfExpandAll;
window.closePdfDialog = closePdfDialog;
window.generateAllPdfs = generateAllPdfs;
window.kmlSelectAll = kmlSelectAll;
window.closeKmlDialog = closeKmlDialog;
window.generateSelectedKML = generateSelectedKML;

