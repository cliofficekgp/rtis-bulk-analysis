import Papa from 'papaparse';
import * as XLSX from 'xlsx';

// --- Detect File Type from CSV Headers ---
export function detectFileType(data) {
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

// --- Check if SNT header row pattern matches ---
function isSntHeaderRow(combinedStr) {
    var _hasStn  = combinedStr.includes('STATION');
    var _hasMsg  = combinedStr.includes('FAULT') || combinedStr.includes('MESSAGE');
    var _hasTime = combinedStr.includes('OCCUR') || combinedStr.includes('TIME') || combinedStr.includes('SHOWN');
    var _hasSpd  = combinedStr.includes('TRAIN SPEED') || combinedStr.includes('KMPH');
    return _hasStn && (_hasMsg || _hasSpd) && _hasTime;
}

// --- XLSX File Handler (SheetJS) — SNT only, always merges ---
// ctx: { log, onSntData(rows, fileName), onValidate(type), onCrossValidate(), onUpdateStatus(type, loaded, fileName, rowCount) }
export function handleXlsxFile(file, ctx) {
    ctx.log("📊 Reading XLSX: " + file.name);
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
                    if (isSntHeaderRow(rowStr)) {
                        headerRowIdx = i;
                        break;
                    }
                }

                if (headerRowIdx === -1) {
                    ctx.log("Sheet [" + sheetName + "] - no SNT header row found, skipping.");
                    return;
                }

                var headers = aoaRaw[headerRowIdx].map(function(h){ return String(h).trim(); });
                ctx.log("Sheet [" + sheetName + "] - header at row " + (headerRowIdx+1) + ", cols: " + headers.filter(Boolean).join(', '));

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
                ctx.log("❌ " + file.name + " — no SNT data rows found in any sheet.");
                alert("⚠️ No SNT data found in " + file.name + ". Please check the file format.");
                return;
            }

            // Confirm it looks like SNT data
            var fileType = detectFileType(allRows);
            if (fileType !== 'snt') {
                ctx.log("File " + file.name + " - detected as [" + (fileType||'unknown') + "], expected SNT.");
                alert("⚠️ " + file.name + " does not appear to be an SNT file. Detected: " + (fileType || 'unknown'));
                return;
            }

            // Deliver rows to main via callback
            ctx.onSntData(allRows, file.name);
        } catch(err) {
            ctx.log("❌ XLSX parse error for " + file.name + ": " + err.message);
            alert("⚠️ Failed to read " + file.name + ": " + err.message);
        }
    };
    reader.readAsArrayBuffer(file);
}

// --- CSV File Loading Logic with Auto-Detection ---
// ctx: { log, filesLoaded, onRtisData(rows), onSntData(rows, fileName), onFsdData(rows),
//        onValidate(type), onCrossValidate(), onUpdateStatus(type, loaded, fileName, rowCount) }
export function handleFile(file, ctx) {
    ctx.log("📁 Reading file: " + file.name);

    Papa.parse(file, {
        header: true,
        skipEmptyLines: true,
        dynamicTyping: true,
        complete: function(results) {
            var rawData = results.data;
            
            // Detect file type
            var fileType = detectFileType(rawData);
            
            if (!fileType) {
                ctx.log("❌ Could not detect file type for: " + file.name);
                alert("⚠️ Invalid CSV format. Please upload valid RTIS, SNT, or FSD file.");
                return;
            }
            
            // For SNT: always merge (no confirm needed). For RTIS/FSD: confirm overwrite.
            if (ctx.filesLoaded[fileType] && fileType !== 'snt') {
                var overwrite = confirm(fileType.toUpperCase() + " file already loaded. Do you want to replace it?");
                if (!overwrite) return;
            }
            
            // --- SPECIAL FIX FOR SNT FILES WITH GARBAGE HEADERS ---
            if (fileType === 'snt') {
                ctx.log("⚠️ Checking for metadata in SNT file...");
                
                var headerRowIndex = -1;
                var foundHeaders = false;
                
                // Find the row that contains actual column headers
                for (var i = 0; i < Math.min(30, rawData.length); i++) {
                    var row = rawData[i];
                    if (!row) continue;
                    
                    var headers = Object.keys(row).map(function(h) { return h.toUpperCase(); });
                    var headerStr = headers.join(' ');
                    var rowValsStr = Object.values(row).map(String).join(' ').toUpperCase();
                    var combinedStr = headerStr + ' ' + rowValsStr;
                    
                    if (isSntHeaderRow(combinedStr)) {
                        headerRowIndex = i;
                        foundHeaders = true;
                        ctx.log("   -> Found SNT header row at position " + (i + 1));
                        break;
                    }
                }
                
                // If metadata found (header row is not at position 0), rebuild the data
                if (foundHeaders && headerRowIndex > 0) {
                    ctx.log("   -> Removing " + headerRowIndex + " metadata row(s)...");
                    
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
                    ctx.log("   -> SNT Repaired: " + rawData.length + " clean data rows ready");
                }
            }
            
            // Store data via callbacks
            if (fileType === 'rtis') {
                ctx.onRtisData(rawData);
            } else if (fileType === 'snt') {
                ctx.onSntData(rawData, file.name);
            } else if (fileType === 'fsd') {
                ctx.onFsdData(rawData);
            }
            
            ctx.log("✅ Loaded " + fileType.toUpperCase() + ": " + rawData.length + " rows");
            ctx.onUpdateStatus(fileType, true, file.name, rawData.length);

            // Perform cross-validation if all files loaded
            if (ctx.checkAllLoaded()) {
                ctx.onCrossValidate();
            }
        }
    });
}

// --- Setup Drag & Drop and File Input ---
// handleFileFn(file): function to call when a file is loaded
// handleXlsxFn(file): function to call when an XLSX file is loaded
// loadSessionFn(): function to call to restore session on load
// logFn(msg): logger
export function initFileHandlers(handleFileFn, handleXlsxFn, loadSessionFn, logFn) {
    document.addEventListener('DOMContentLoaded', function() {
        // Auto-restore session from localStorage/IndexedDB if available
        setTimeout(function() {
            if (typeof loadSessionFn === 'function') {
                var restored = loadSessionFn();
                if (restored) logFn('✅ Session auto-restored. Re-upload per-sec files to restore speed data.');
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
                    handleFileFn(files[i]);
                } else if (fname.endsWith('.xlsx') || fname.endsWith('.xls')) {
                    handleXlsxFn(files[i]);
                }
            }
        });

        // Handle selected files
        fileInput.addEventListener('change', function(e) {
            var files = e.target.files;
            for (var i = 0; i < files.length; i++) {
                var fname = files[i].name.toLowerCase();
                if (fname.endsWith('.csv')) {
                    handleFileFn(files[i]);
                } else if (fname.endsWith('.xlsx') || fname.endsWith('.xls')) {
                    handleXlsxFn(files[i]);
                }
            }
            fileInput.value = ''; // Reset input to allow re-uploading same file
        });
    });
}
