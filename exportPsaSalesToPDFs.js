/***********************
 * CONFIG
 ***********************/
const LABEL_NAME_PDF = 'PSA Sales'; // Gmail label to ingest
const DRIVE_FOLDER_PATH = 'Sales/PSA/Extracted PDFs'; // Path from My Drive root
const MAX_PER_RUN_PDF = 100; // Safety cap per run
const LOG_SHEET = 'ProcessedPDFs'; // Helper sheet for dedupe

/***********************
 * ENTRY POINT
 ***********************/
function exportPsaSalesToPDFs() {
  const label = GmailApp.getUserLabelByName(LABEL_NAME_PDF);
  if (!label) throw new Error(`Label not found: "${LABEL_NAME_PDF}"`);

  const folder = getOrCreateFolderByPath_(DRIVE_FOLDER_PATH);
  const processed = loadProcessedIds_();

  let exported = 0;
  let start = 0,
    pageSize = 50;
  while (exported < MAX_PER_RUN_PDF) {
    const threads = label.getThreads(start, pageSize);
    if (!threads.length) break;
    for (const thread of threads) {
      for (const msg of thread.getMessages()) {
        if (exported >= MAX_PER_RUN_PDF) break;
        const id = msg.getId();
        if (processed.has(id)) continue;

        try {
          const pdfBlob = renderMessageToPDF_(msg);
          const filename = buildFileName_(msg);
          folder.createFile(pdfBlob).setName(filename);

          processed.add(id);
          exported++;
        } catch (e) {
          console.warn(`Failed on message ${id}: ${e && e.message ? e.message : e}`);
        }
      }
    }

    if (threads.length < pageSize) break;
    start += pageSize;
  }

  saveProcessedIds_(processed);
  SpreadsheetApp.getActive().toast(`Exported ${exported} PDFs to: ${DRIVE_FOLDER_PATH}`);
}

/***********************
 * Smarter filename with Certification Number
 ***********************/
function buildFileName_(msg) {
  const date = Utilities.formatDate(msg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const subj = msg.getSubject() || 'No Subject';
  const body = msg.getPlainBody() || '';

  // Attempt to find the certification number to make the filename more useful
  const certRegex = /(?:PSA|CGC) CERT\s*([0-9]+)/i;
  const match = body.match(certRegex);
  const certNumber = match ? match[1] : null;

  const cleanedSubject = subj
    .replace(/[\\/:*?"<>|#]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .substring(0, 120);

  return certNumber ?
    `${date} - PSA Sale Cert ${certNumber}.pdf` :
    `${date} - ${cleanedSubject}.pdf`;
}


/***********************
 * === GENERIC HELPER FUNCTIONS (No changes needed below) ===
 ***********************/

function renderMessageToPDF_(msg) {
  const meta = {
    from: msg.getFrom(),
    to: msg.getTo(),
    cc: msg.getCc(),
    date: Utilities.formatDate(msg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm'),
    subject: msg.getSubject(),
    id: msg.getId()
  };
  let html = msg.getBody() || '';
  html = inlineCidImages_(html, msg);
  html = inlineExternalImages_(html);

  const wrapper = `
    <html><head><meta charset="UTF-8" /><style>@page{size:A4;margin:18mm;}body{font-family:Arial,sans-serif;font-size:12px;color:#222;}.meta{border-bottom:1px solid #ddd;margin-bottom:12px;padding-bottom:8px;}.meta div{margin:2px 0;}.subject{font-size:16px;font-weight:700;margin-bottom:6px;}img{max-width:100%;height:auto;}a{color:#1155cc;text-decoration:none;}table{border-collapse:collapse;}td,th{border:1px solid #e5e5e5;padding:4px 6px;vertical-align:top;}.email-body,p,table,div{page-break-inside:avoid;}</style></head>
    <body><div class="meta"><div class="subject">${escape_(meta.subject)}</div><div><b>From:</b> ${escape_(meta.from)}</div><div><b>To:</b> ${escape_(meta.to||'')}</div>${meta.cc?`<div><b>CC:</b> ${escape_(meta.cc)}</div>`:''}<div><b>Date:</b> ${escape_(meta.date)}</div><div><b>Message ID:</b> ${escape_(meta.id)}</div></div><div class="email-body">${html}</div></body></html>`;
  const htmlBlob = Utilities.newBlob(wrapper, 'text/html', 'email.html');
  return htmlBlob.getAs('application/pdf');
}

function getOrCreateFolderByPath_(path) {
  if (!path) throw new Error('DRIVE_FOLDER_PATH is empty');
  const parts = path.split('/').map(p => p.trim()).filter(Boolean);
  let folder = DriveApp.getRootFolder();
  for (const name of parts) {
    let next = null;
    const it = folder.getFoldersByName(name);
    next = it.hasNext() ? it.next() : folder.createFolder(name);
    folder = next;
  }
  return folder;
}

function inlineCidImages_(html, msg) {
  const atts = msg.getAttachments({
    includeInlineImages: true,
    includeAttachments: false
  }) || [];
  if (!atts.length) return html;
  const cidMap = {};
  for (const a of atts) {
    const cid = (a.getContentId() || '').replace(/[<>]/g, '').trim();
    if (!cid) continue;
    const contentType = a.getContentType() || 'application/octet-stream';
    const base64 = Utilities.base64Encode(a.getBytes());
    cidMap[cid.toLowerCase()] = `data:${contentType};base64,${base64}`;
  }
  html = html.replace(/src\s*=\s*(['"])cid:([^'"]+)\1/gi, (m, q, cid) => {
    const key = (cid || '').replace(/[<>]/g, '').trim().toLowerCase();
    const dataUri = cidMap[key];
    return dataUri ? `src=${q}${dataUri}${q}` : m;
  });
  return html;
}

function inlineExternalImages_(html) {
  if (!html) return html;
  html = html.replace(/\s(data-src|data-original)\s*=\s*(['"])(.*?)\2/gi, (m, attr, q, val) => ` src=${q}${val}${q}`);
  html = html.replace(/\ssrcset\s*=\s*(['"])[\s\S]*?\1/gi, '');
  html = html.replace(/src\s*=\s*(['"])(https?:\/\/[^'"]+)\1/gi, (m, q, rawUrl) => {
    try {
      const url = normalizeGoogleProxyUrl_(rawUrl);
      if (url.length > 2000) return m;
      const resp = UrlFetchApp.fetch(url, {
        followRedirects: true,
        muteHttpExceptions: true,
        headers: {
          'User-Agent': 'Mozilla/5.0 (AppsScript PDF embedder)'
        }
      });
      if (resp.getResponseCode() !== 200) return m;
      let ctype = resp.getHeaders()['Content-Type'] || '';
      if (!ctype) {
        if (/\.(png)(\?|$)/i.test(url)) ctype = 'image/png';
        else if (/\.(jpe?g)(\?|$)/i.test(url)) ctype = 'image/jpeg';
        else if (/\.(gif)(\?|$)/i.test(url)) ctype = 'image/gif';
        else if (/\.(webp)(\?|$)/i.test(url)) ctype = 'image/webp';
        else ctype = 'application/octet-stream';
      }
      const bytes = resp.getContent();
      if (bytes.length > 5 * 1024 * 1024) return m;
      const base64 = Utilities.base64Encode(bytes);
      const dataUri = `data:${ctype};base64,${base64}`;
      return `src=${q}${dataUri}${q}`;
    } catch (e) {
      return m;
    }
  });
  return html;
}

function normalizeGoogleProxyUrl_(u) {
  try {
    if (/googleusercontent\.com\/proxy\//i.test(u)) {
      const hash = u.indexOf('#');
      if (hash > -1) return u.substring(hash + 1);
    }
    return u;
  } catch (_) {
    return u;
  }
}

function loadProcessedIds_() {
  const ss = SpreadsheetApp.getActive();
  let sheet = ss.getSheetByName(LOG_SHEET);
  if (!sheet) {
    sheet = ss.insertSheet(LOG_SHEET);
    sheet.hideSheet();
    sheet.getRange(1, 1).setValue('messageId');
  }
  const vals = sheet.getRange(2, 1, Math.max(0, sheet.getLastRow() - 1), 1).getValues();
  const set = new Set();
  vals.forEach(r => {
    if (r[0]) set.add(String(r[0]));
  });
  return set;
}

function saveProcessedIds_(set) {
  const ss = SpreadsheetApp.getActive();
  const sheet = ss.getSheetByName(LOG_SHEET);
  if (!sheet) return;
  const ids = Array.from(set);
  sheet.clearContents();
  sheet.getRange(1, 1).setValue('messageId');
  if (ids.length) {
    sheet.getRange(2, 1, ids.length, 1).setValues(ids.map(id => [id]));
  }
}

function escape_(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}
