import { cleanStationName, getCol } from '../utils/helpers.js';

export function openMapEditor(dataRTIS, allFsdStationNames, stationMappingCache, manualOverrides) {
    var rtisStations = new Set();
    dataRTIS.forEach(function(r) {
        var s = cleanStationName(getCol(r, ['Station','STATION','STN_CODE']));
        if (s) rtisStations.add(s);
    });

    var tbody = document.getElementById('mappingTableBody');
    tbody.innerHTML = '';

    Array.from(rtisStations).sort().forEach(function(rtis) {
        var cached = stationMappingCache[rtis];
        var manual = manualOverrides[rtis];
        var mapped  = manual || (cached && cached.fsdStation) || null;
        var method  = manual ? 'manual' : (cached ? cached.method : 'unmatched');
        var distStr = (cached && cached.distKm != null) ? (cached.distKm*1000).toFixed(0)+'m' : (method==='exact'?'0m':'—');

        var rowClass = method==='manual' ? 'mapping-row-manual' : (mapped ? 'mapping-row-auto' : 'mapping-row-unmatched');
        var methodBadge = {
            'exact': '✅ Exact', 'name-sim': '🔗 Name-sim', 'proximity': '📍 Proximity',
            'manual': '✏️ Manual', 'unmatched': '❌ No match'
        }[method] || method;

        var opts = '<option value="">— not mapped —</option>';
        allFsdStationNames.forEach(function(fk) {
            opts += '<option value="' + fk + '"' + (fk === mapped ? ' selected' : '') + '>' + fk + '</option>';
        });

        var tr = document.createElement('tr');
        tr.className = rowClass;
        tr.innerHTML =
            '<td class="px-3 py-2 font-mono font-bold text-gray-800">' + rtis + '</td>' +
            '<td class="px-3 py-2 font-mono text-green-700">' + (mapped || '—') + '</td>' +
            '<td class="px-3 py-2">' + methodBadge + '</td>' +
            '<td class="px-3 py-2 text-gray-500">' + distStr + '</td>' +
            '<td class="px-3 py-2"><select class="border rounded text-xs p-1 mapping-override" data-rtis="' + rtis + '">' + opts + '</select></td>';
        tbody.appendChild(tr);
    });

    document.getElementById('mapEditorModal').classList.add('open');
}

export function closeMapEditor() {
    document.getElementById('mapEditorModal').classList.remove('open');
}

export function getMappingEdits(manualOverrides, stationMappingCache) {
    document.querySelectorAll('.mapping-override').forEach(function(sel) {
        var rtis = sel.getAttribute('data-rtis');
        var val  = sel.value;
        if (val) {
            manualOverrides[rtis] = val;
            delete stationMappingCache[rtis]; // manual wins
        } else {
            delete manualOverrides[rtis];
            delete stationMappingCache[rtis];
        }
    });
}

export function exportMappingCSV(dataRTIS, stationMappingCache, manualOverrides) {
    var rtisStations = new Set();
    dataRTIS.forEach(function(r) {
        var s = cleanStationName(getCol(r, ['Station','STATION','STN_CODE']));
        if (s) rtisStations.add(s);
    });
    var rows = ['"RTIS Station","Mapped FSD Station","Method","Distance"'];
    Array.from(rtisStations).sort().forEach(function(rtis) {
        var manual  = manualOverrides[rtis];
        var cached  = stationMappingCache[rtis];
        var mapped  = manual || (cached && cached.fsdStation) || '';
        var method  = manual ? 'manual' : (cached ? cached.method : 'unmatched');
        var distStr = (cached && cached.distKm != null) ? (cached.distKm*1000).toFixed(0)+'m' : '';
        rows.push('"'+rtis+'","'+mapped+'","'+method+'","'+distStr+'"');
    });
    var blob = new Blob([rows.join('\n')], { type: 'text/csv' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'station_mapping.csv';
    a.click();
}
