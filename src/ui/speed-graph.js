import Chart from 'chart.js/auto';
import { getCol, parseSmartDate, cleanStationName } from '../utils/helpers.js';

export function processSpeedGraph(resultIdx, analysisResults, perSecondData, dataRTIS, speedLimit, speedChartInstance, _initFilterUI, _msToTimeStr, _buildStnOptions, _drawChart, matchPerSecToSNT) {
    var row = analysisResults[resultIdx];
    if (!row) return speedChartInstance;

    var hasPersec  = perSecondData[resultIdx] && perSecondData[resultIdx].length > 0;
    var sntDateObj = row.sntTimeISO ? new Date(row.sntTimeISO) : (function() {
        var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
        var d = new Date(base);
        var tp = (row.signalTime || '').match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
        if (tp) d.setHours(parseInt(tp[1]), parseInt(tp[2]), parseInt(tp[3]||0), 0);
        return d;
    })();

    document.getElementById('graphModal').classList.add('open');
    document.getElementById('graphTitle').innerText = 'Train ' + row.trainNo + ' — ' + row.station + ' — ' + row.sigNo;

    var displayPts = [];

    if (hasPersec) {
        var allPts = perSecondData[resultIdx];
        var grMatch = row.perSecMatch || matchPerSecToSNT(allPts, sntDateObj);

        document.getElementById('graphSubtitle').innerText =
            'Per-sec | SNT: ' + sntDateObj.toLocaleTimeString() +
            (grMatch ? ' [' + grMatch.quality + ', Δ' + (grMatch.diffMs/1000).toFixed(1) + 's]' : '') +
            ' | Limit: ' + speedLimit + ' km/h | Signal: ' + row.signalTime;

        var violMs = sntDateObj.getTime();
        var w3 = 3*60*1000;
        displayPts = allPts.filter(function(p){ return Math.abs(p.time.getTime()-violMs)<=w3; });

        _initFilterUI(allPts, sntDateObj.getTime(), resultIdx, row);
        if(displayPts.length>0){
            document.getElementById('gfFromTime').value = _msToTimeStr(displayPts[0].time.getTime());
            document.getElementById('gfToTime').value   = _msToTimeStr(displayPts[displayPts.length-1].time.getTime());
            _buildStnOptions(document.getElementById('gfFromStn'), allPts, displayPts[0].time.getTime());
            _buildStnOptions(document.getElementById('gfToStn'),   allPts, displayPts[displayPts.length-1].time.getTime());
        }

    } else {
        document.getElementById('graphSubtitle').innerText =
            'RTIS pings at station | Limit: ' + speedLimit + ' km/h | Signal: ' + row.signalTime;
        document.getElementById('gfStrip').style.display = 'none';

        var trainKey   = String(row.trainNo);
        var stationKey = row.stationKey || cleanStationName(row.station.split('→')[0].trim());

        dataRTIS.forEach(function(r) {
            var t   = getCol(r, ['Train Number','Train No.','TRAIN','Train']);
            var stn = cleanStationName(getCol(r, ['Station','STATION','STN_CODE']));
            if (String(t) !== trainKey) return;
            if (stn !== stationKey) return;
            var timeRaw = getCol(r, ['Event Time','TIME','EventTime','Date']);
            var speed   = parseFloat(getCol(r, ['Speed','SPEED']));
            var tm      = parseSmartDate(timeRaw);
            if (tm && !isNaN(speed)) displayPts.push({ time:tm, speed:speed, stn:stn, lat:null, lon:null });
        });
        displayPts.sort(function(a,b){ return a.time - b.time; });

        if (displayPts.length === 0) {
            var base = row.violationTime instanceof Date ? row.violationTime : new Date(row.violationTimeStr);
            displayPts.push({ time:base, speed:parseFloat(row.speed), stn:row.stationKey||'', lat:null, lon:null });
        }
    }

    setTimeout(function() { _drawChart(displayPts, sntDateObj.getTime(), row); }, 80);
    return speedChartInstance;
}

export function closeSpeedGraph(speedChartInstance) {
    document.getElementById('graphModal').classList.remove('open');
    if (speedChartInstance) { speedChartInstance.destroy(); }
    document.getElementById('gfStrip').style.display = 'none';
    return null;
}
