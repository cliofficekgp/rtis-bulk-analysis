// ══════════════════════════════════════════════════════════════
// GOOGLE DRIVE UPLOAD MODULE
// Uses Google Identity Services (GIS) + Drive API v3
// Uploads PDF files to a specific shared Google Drive folder
// ══════════════════════════════════════════════════════════════

// Target folder ID extracted from the shared link
var FOLDER_ID = '15_nIorwtRdCXUO-OayPFztn07Gmt2dgw';

// OAuth2 config — set your own Client ID from Google Cloud Console
// Instructions: https://console.cloud.google.com/apis/credentials
// 1. Create OAuth2 Client ID (Web application)
// 2. Add your domain to Authorized JavaScript origins
// 3. Enable Google Drive API
var CLIENT_ID = '';   // Will be set via initGoogleDrive()
var SCOPES    = 'https://www.googleapis.com/auth/drive.file';

var _tokenClient = null;
var _accessToken = null;
var _onAuthCallback = null;

// ── Initialize Google Identity Services ──────────────────────
export function initGoogleDrive(clientId) {
    if (clientId) CLIENT_ID = clientId;

    if (!CLIENT_ID) {
        console.warn('[Google Drive] No Client ID configured — upload will be unavailable.');
        return false;
    }

    if (typeof google === 'undefined' || !google.accounts || !google.accounts.oauth2) {
        console.warn('[Google Drive] Google Identity Services not loaded yet.');
        return false;
    }

    _tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: CLIENT_ID,
        scope:     SCOPES,
        callback:  function(tokenResponse) {
            if (tokenResponse && tokenResponse.access_token) {
                _accessToken = tokenResponse.access_token;
                console.log('[Google Drive] Auth success');
                if (_onAuthCallback) {
                    _onAuthCallback(true);
                    _onAuthCallback = null;
                }
            } else {
                console.warn('[Google Drive] Auth failed or cancelled');
                if (_onAuthCallback) {
                    _onAuthCallback(false);
                    _onAuthCallback = null;
                }
            }
        }
    });

    return true;
}

// ── Check if we have a valid token ───────────────────────────
export function isAuthenticated() {
    return !!_accessToken;
}

// ── Check if Google Drive is configured ──────────────────────
export function isDriveConfigured() {
    return !!CLIENT_ID;
}

// ── Request auth token (prompts user if needed) ──────────────
export function authenticate(callback) {
    if (!_tokenClient) {
        if (callback) callback(false);
        return;
    }

    if (_accessToken) {
        if (callback) callback(true);
        return;
    }

    _onAuthCallback = callback;
    _tokenClient.requestAccessToken({ prompt: 'consent' });
}

// ── Upload a single file to Google Drive ─────────────────────
// blob:     File Blob (PDF)
// filename: desired filename
// Returns: Promise<{ id, name, webViewLink } | null>
export function uploadToDrive(blob, filename, onProgress) {
    return new Promise(function(resolve, reject) {
        if (!_accessToken) {
            reject(new Error('Not authenticated with Google'));
            return;
        }

        var metadata = {
            name: filename,
            mimeType: 'application/pdf',
            parents: [FOLDER_ID]
        };

        // Use multipart upload for files < 5MB (typical PDFs)
        var form = new FormData();
        form.append('metadata', new Blob([JSON.stringify(metadata)], { type: 'application/json' }));
        form.append('file', blob);

        var xhr = new XMLHttpRequest();
        xhr.open('POST', 'https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,webViewLink');
        xhr.setRequestHeader('Authorization', 'Bearer ' + _accessToken);

        if (onProgress) {
            xhr.upload.onprogress = function(e) {
                if (e.lengthComputable) {
                    onProgress(Math.round((e.loaded / e.total) * 100));
                }
            };
        }

        xhr.onload = function() {
            if (xhr.status === 200 || xhr.status === 201) {
                try {
                    var result = JSON.parse(xhr.responseText);
                    resolve(result);
                } catch(e) {
                    resolve({ id: 'unknown', name: filename });
                }
            } else if (xhr.status === 401) {
                // Token expired — clear it
                _accessToken = null;
                reject(new Error('Auth token expired — please sign in again'));
            } else {
                var errMsg = 'Upload failed (HTTP ' + xhr.status + ')';
                try {
                    var err = JSON.parse(xhr.responseText);
                    if (err.error && err.error.message) errMsg = err.error.message;
                } catch(e) {}
                reject(new Error(errMsg));
            }
        };

        xhr.onerror = function() {
            reject(new Error('Network error during upload'));
        };

        xhr.send(form);
    });
}

// ── Upload multiple files with progress ──────────────────────
// files: [{ blob, filename }]
// onProgress: function(current, total, filename, status)
// Returns: Promise<[{ filename, success, driveId, error }]>
export async function uploadMultipleToDrive(files, onProgress) {
    var results = [];
    for (var i = 0; i < files.length; i++) {
        var f = files[i];
        if (onProgress) onProgress(i + 1, files.length, f.filename, 'uploading');
        try {
            var result = await uploadToDrive(f.blob, f.filename);
            results.push({ filename: f.filename, success: true, driveId: result.id, webViewLink: result.webViewLink });
            if (onProgress) onProgress(i + 1, files.length, f.filename, 'done');
        } catch(e) {
            results.push({ filename: f.filename, success: false, error: e.message });
            if (onProgress) onProgress(i + 1, files.length, f.filename, 'error');
        }
    }
    return results;
}

// ── Show upload dialog with results ──────────────────────────
export function showUploadDialog(pdfFiles, onUpload) {
    var modal = document.getElementById('driveUploadModal');
    if (!modal) return;

    var listEl = document.getElementById('driveFileList');
    listEl.innerHTML = '';

    var selectAllCb = document.getElementById('driveSelectAll');

    pdfFiles.forEach(function(f, i) {
        var row = document.createElement('div');
        row.style.cssText = 'display:flex;align-items:center;gap:10px;padding:10px 14px;border-bottom:1px solid #f1f5f9;transition:background 0.15s;';
        row.onmouseenter = function() { row.style.background = '#f8fafc'; };
        row.onmouseleave = function() { row.style.background = ''; };

        var cb = document.createElement('input');
        cb.type = 'checkbox';
        cb.id = 'driveFile_' + i;
        cb.checked = true;
        cb.style.cssText = 'width:16px;height:16px;accent-color:#1d4ed8;cursor:pointer;';
        cb.addEventListener('change', _updateSelectAll);

        var label = document.createElement('label');
        label.setAttribute('for', 'driveFile_' + i);
        label.style.cssText = 'flex:1;font-size:0.82rem;color:#1e293b;cursor:pointer;display:flex;align-items:center;gap:8px;';
        label.innerHTML =
            '<span style="font-size:1.1rem;">📄</span>' +
            '<span style="font-weight:600;">' + _escHtml(f.filename) + '</span>';

        var statusSpan = document.createElement('span');
        statusSpan.id = 'driveStatus_' + i;
        statusSpan.style.cssText = 'font-size:0.72rem;color:#94a3b8;min-width:80px;text-align:right;';
        statusSpan.textContent = 'Ready';

        row.appendChild(cb);
        row.appendChild(label);
        row.appendChild(statusSpan);
        listEl.appendChild(row);
    });

    selectAllCb.checked = true;
    selectAllCb.onchange = function() {
        var checkboxes = listEl.querySelectorAll('input[type=checkbox]');
        checkboxes.forEach(function(cb) { cb.checked = selectAllCb.checked; });
        _updateUploadBtn();
    };

    _updateUploadBtn();

    var uploadBtn = document.getElementById('driveUploadBtn');
    uploadBtn.onclick = function() {
        var selected = [];
        pdfFiles.forEach(function(f, i) {
            var cb = document.getElementById('driveFile_' + i);
            if (cb && cb.checked) {
                selected.push({ blob: f.blob, filename: f.filename, index: i });
            }
        });
        if (selected.length === 0) return;

        // Disable controls during upload
        uploadBtn.disabled = true;
        uploadBtn.textContent = '⏳ Uploading...';
        var checkboxes = listEl.querySelectorAll('input[type=checkbox]');
        checkboxes.forEach(function(cb) { cb.disabled = true; });

        if (onUpload) onUpload(selected);
    };

    modal.style.display = 'flex';
}

export function closeUploadDialog() {
    var modal = document.getElementById('driveUploadModal');
    if (modal) modal.style.display = 'none';
}

// Update the status of a specific file row in the dialog
export function updateFileStatus(index, status, message) {
    var el = document.getElementById('driveStatus_' + index);
    if (!el) return;
    if (status === 'uploading') {
        el.style.color = '#1d4ed8';
        el.innerHTML = '⏳ Uploading...';
    } else if (status === 'done') {
        el.style.color = '#16a34a';
        el.innerHTML = '✅ Uploaded';
    } else if (status === 'error') {
        el.style.color = '#dc2626';
        el.innerHTML = '❌ ' + _escHtml(message || 'Failed');
    }
}

// Mark upload complete and re-enable close
export function markUploadComplete(successCount, failCount) {
    var uploadBtn = document.getElementById('driveUploadBtn');
    if (uploadBtn) {
        uploadBtn.disabled = false;
        if (failCount === 0) {
            uploadBtn.textContent = '✅ All Uploaded — Close';
            uploadBtn.onclick = closeUploadDialog;
        } else {
            uploadBtn.textContent = '⚠ ' + successCount + ' uploaded, ' + failCount + ' failed — Close';
            uploadBtn.onclick = closeUploadDialog;
        }
    }
}

function _updateSelectAll() {
    var listEl = document.getElementById('driveFileList');
    var selectAllCb = document.getElementById('driveSelectAll');
    if (!listEl || !selectAllCb) return;
    var checkboxes = listEl.querySelectorAll('input[type=checkbox]');
    var allChecked = true;
    checkboxes.forEach(function(cb) { if (!cb.checked) allChecked = false; });
    selectAllCb.checked = allChecked;
    _updateUploadBtn();
}

function _updateUploadBtn() {
    var listEl = document.getElementById('driveFileList');
    var uploadBtn = document.getElementById('driveUploadBtn');
    if (!listEl || !uploadBtn) return;
    var checkboxes = listEl.querySelectorAll('input[type=checkbox]');
    var anyChecked = false;
    checkboxes.forEach(function(cb) { if (cb.checked) anyChecked = true; });
    uploadBtn.disabled = !anyChecked;
    uploadBtn.style.opacity = anyChecked ? '1' : '0.45';
}

function _escHtml(str) {
    var div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}
