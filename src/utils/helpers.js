// --- GPS Math ---
export function isValidGPS(lat, lon) {
    if (!lat || !lon) return false;
    lat = parseFloat(lat);
    lon = parseFloat(lon);
    return !isNaN(lat) && !isNaN(lon) && 
           Math.abs(lat) <= 90 && 
           Math.abs(lon) <= 180 &&
           !(lat === 0 && lon === 0);
}

export function getDistance(lat1, lon1, lat2, lon2) {
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

// --- Date Formatting ---
export function parseSmartDate(dateStr) {
    if (!dateStr) return null;
    dateStr = String(dateStr).trim();
    dateStr = dateStr.replace(/(\d{2}:\d{2}:\d{2}):\d{1,3}$/, '$1');
    var d = new Date(dateStr);
    if (!isNaN(d.getTime()) && d.getFullYear() > 2000) return d;

    var parts = dateStr.match(/^(\d{1,2})[\/\-\.](\d{1,2})[\/\-\.](\d{2,4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (parts) {
        var year = parseInt(parts[3]);
        if (year < 100) year += 2000;
        var a = parseInt(parts[1]);   
        var b = parseInt(parts[2]);   
        var hh = parseInt(parts[4] || 0);
        var mm = parseInt(parts[5] || 0);
        var ss = parseInt(parts[6] || 0);

        var month, day;
        if (a > 12) { day = a; month = b; }
        else if (b > 12) { day = b; month = a; }
        else { month = a; day = b; }

        var result = new Date(year, month - 1, day, hh, mm, ss);
        if (!isNaN(result.getTime())) return result;
    }
    return null;
}

export function formatDateTime(dateObj) {
    if (!dateObj || isNaN(dateObj.getTime())) return 'Invalid';
    var dd = String(dateObj.getDate()).padStart(2,'0');
    var mm = String(dateObj.getMonth()+1).padStart(2,'0');
    var yyyy = dateObj.getFullYear();
    var hh = String(dateObj.getHours()).padStart(2,'0');
    var min = String(dateObj.getMinutes()).padStart(2,'0');
    var sec = String(dateObj.getSeconds()).padStart(2,'0');
    return dd + '/' + mm + '/' + yyyy + ' ' + hh + ':' + min + ':' + sec;
}

// --- Specific Railways Formatters ---
export function getCol(row, candidates) {
    if (!row) return null;
    for (var i = 0; i < candidates.length; i++) {
        var key = candidates[i];
        if (row.hasOwnProperty(key)) return row[key];
        var rowKeys = Object.keys(row);
        for (var k = 0; k < rowKeys.length; k++) {
            if (rowKeys[k].trim().toUpperCase() === key.toUpperCase()) return row[rowKeys[k]];
        }
    }
    return null;
}

export function extractDirectionFromMessage(message) {
    if (!message) return null;
    var msg = String(message).toUpperCase();
    if (/ ON\s+UP\s+HOME /.test(msg))   return 'UP';
    if (/ ON\s+DN\s+HOME /.test(msg))   return 'DN';
    if (/ ON\s+DOWN\s+HOME /.test(msg)) return 'DN';
    if (/ UP\s+HOME /.test(msg))        return 'UP';
    if (/ DN\s+HOME /.test(msg))        return 'DN';
    if (/ DOWN\s+HOME /.test(msg))      return 'DN';
    if (/ PASSED\s+ON\s+UP /.test(msg)) return 'UP';
    if (/ PASSED\s+ON\s+DN /.test(msg)) return 'DN';
    var upMatch = msg.match(/ (UP|DN|DOWN) /);
    if (upMatch) return upMatch[1] === 'DOWN' ? 'DN' : upMatch[1];
    return null;
}

export function extractSignalNumber(message) {
    if (!message) return null;
    message = String(message).trim();
    var patterns = [ /S-?(\d+)/i, /SA-?(\d+)/i, /AS-?(\d+)/i, /(\d+)S/i ];
    for (var i = 0; i < patterns.length; i++) {
        var match = message.match(patterns[i]);
        if (match && match[1]) return parseInt(match[1]);
    }
    return null;
}

export function getDirectionFromSignal(sigNum) {
    return (sigNum % 2 !== 0) ? 'UP' : 'DN';
}

export function getDistanceLabel(distMeters) {
    if (distMeters < 50) return "Very Close";
    if (distMeters < 150) return "Close";
    return "Far";
}

export function cleanStationName(station) {
    if (!station) return null;
    station = String(station).trim().toUpperCase();
    var match = station.match(/^([A-Z0-9]+)/);
    if (!match || !match[1]) return null;
    return match[1];
}
