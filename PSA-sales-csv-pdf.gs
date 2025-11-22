/***********************
 * CONFIG
 ***********************/
const LABEL_NAME = 'PSA Sales';
const SHEET_NAME = 'PSA Sales';
const DRIVE_FOLDER_PATH = 'PSA/Extracted PDFs';
const LOG_SHEET = 'ProcessedPDFs';
const MAX_PER_RUN = 100;

/***********************
 * MAIN ENTRY POINT
 ***********************/
function processPsaSales() {
  console.log('=== PSA Sales Processor Started ===');
  console.log(`Config: Label="${LABEL_NAME}", Sheet="${SHEET_NAME}", Folder="${DRIVE_FOLDER_PATH}", Max=${MAX_PER_RUN}`);
  
  const label = GmailApp.getUserLabelByName(LABEL_NAME);
  if (!label) throw new Error(`Label not found: "${LABEL_NAME}"`);
  console.log(`✓ Gmail label found: "${LABEL_NAME}"`);

  const sheet = SpreadsheetApp.getActive().getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error(`Sheet not found: "${SHEET_NAME}"`);
  console.log(`✓ Sheet found: "${SHEET_NAME}"`);

  const folder = getOrCreateFolderByPath_(DRIVE_FOLDER_PATH);
  console.log(`✓ Drive folder ready: "${DRIVE_FOLDER_PATH}"`);
  
  const processedPDFs = loadProcessedIds_();
  console.log(`Loaded ${processedPDFs.size} previously processed PDF message IDs`);
  
  const existingMessageIds = loadExistingMessageIds_(sheet);
  console.log(`Loaded ${existingMessageIds.size} existing sales data message IDs from sheet`);

  let salesProcessed = 0;
  let pdfsExported = 0;
  let emailsScanned = 0;
  let emailsSkipped = 0;
  let start = 0;
  const pageSize = 50;

  console.log('\n--- Beginning Email Processing ---');
  
  while (salesProcessed < MAX_PER_RUN) {
    const threads = label.getThreads(start, pageSize);
    console.log(`Fetching threads: start=${start}, pageSize=${pageSize}, found=${threads.length}`);
    
    if (!threads.length) {
      console.log('No more threads to process');
      break;
    }

    for (const thread of threads) {
      const messages = thread.getMessages();
      console.log(`Thread has ${messages.length} message(s)`);
      
      for (const msg of messages) {
        if (salesProcessed >= MAX_PER_RUN) {
          console.log(`Reached MAX_PER_RUN limit (${MAX_PER_RUN}), stopping`);
          break;
        }

        emailsScanned++;
        const msgId = msg.getId();
        const subject = msg.getSubject();
        const date = Utilities.formatDate(msg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm');
        
        console.log(`\n[Email #${emailsScanned}] ID: ${msgId}`);
        console.log(`  Subject: "${subject}"`);
        console.log(`  Date: ${date}`);
        
        // Check if already processed for sales data
        const hasSalesData = existingMessageIds.has(msgId);
        const hasPDF = processedPDFs.has(msgId);
        
        console.log(`  Sales Data Exists: ${hasSalesData}`);
        console.log(`  PDF Exists: ${hasPDF}`);
        
        if (hasSalesData && hasPDF) {
          console.log(`  → SKIP: Both sales data and PDF already exist`);
          emailsSkipped++;
          continue;
        }

        // Process sales data if needed
        if (!hasSalesData) {
          console.log(`  → ACTION: Parsing sales data...`);
          const parsed = parsePsaEmail_(msg);
          
          if (parsed) {
            console.log(`    ✓ Parsed successfully:`);
            console.log(`      Cert: ${parsed.certNumber}`);
            console.log(`      Card: ${parsed.cardTitle}`);
            console.log(`      Sold: $${parsed.soldAmount}`);
            console.log(`      Fees: $${parsed.feesPaid}`);
            console.log(`      Net: $${parsed.netProceeds}`);
            
            sheet.appendRow([
              parsed.certNumber,
              parsed.saleDate,
              parsed.cardTitle,
              parsed.soldAmount,
              parsed.feesPaid,
              parsed.netProceeds,
              msgId
            ]);
            console.log(`    ✓ Written to sheet`);
            
            existingMessageIds.add(msgId);
            salesProcessed++;
          } else {
            console.log(`    ✗ Failed to parse sales data`);
          }
        } else {
          console.log(`  → SKIP: Sales data already exists`);
        }

        // Export PDF if needed
        if (!hasPDF) {
          console.log(`  → ACTION: Exporting PDF...`);
          const pdfSuccess = exportSinglePDF_(msg, folder, processedPDFs);
          if (pdfSuccess) {
            console.log(`    ✓ PDF exported successfully`);
            pdfsExported++;
          } else {
            console.log(`    ✗ PDF export failed`);
          }
        } else {
          console.log(`  → SKIP: PDF already exists`);
        }
      }
    }

    if (threads.length < pageSize) {
      console.log(`Received fewer threads than pageSize (${threads.length} < ${pageSize}), end of results`);
      break;
    }
    start += pageSize;
  }

  saveProcessedIds_(processedPDFs);
  console.log(`\n✓ Saved PDF tracking state (${processedPDFs.size} total IDs)`);
  
  console.log('\n=== Processing Complete ===');
  console.log(`Emails Scanned: ${emailsScanned}`);
  console.log(`Emails Skipped: ${emailsSkipped}`);
  console.log(`Sales Processed: ${salesProcessed}`);
  console.log(`PDFs Exported: ${pdfsExported}`);
  
  SpreadsheetApp.getActive().toast(
    `Scanned: ${emailsScanned} | Sales: ${salesProcessed} | PDFs: ${pdfsExported}`,
    'PSA Processor Complete',
    10
  );
}

/***********************
 * SALES DATA EXTRACTION
 ***********************/
function parsePsaEmail_(msg) {
  const bodyText = (msg.getPlainBody() || '').replace(/\r/g, '');
  const warnings = [];

  try {
    const certRegex = /(?:PSA|CGC|BGS) CERT\s*([0-9]+)/i;
    const salePriceRegex = /Sale Price\s*\$([0-9.,]+)/i;
    const proceedsRegex = /Proceeds\s*\$([0-9.,]+)/i;

    const certMatch = bodyText.match(certRegex);
    const salePriceMatch = bodyText.match(salePriceRegex);
    const proceedsMatch = bodyText.match(proceedsRegex);

    const certNumber = certMatch ? certMatch[1] : null;
    if (!certNumber) warnings.push('Certification Number');

    const soldAmount = salePriceMatch ? Number(salePriceMatch[1].replace(/,/g, '')) : null;
    if (soldAmount === null) warnings.push('Sale Price');

    const netProceeds = proceedsMatch ? Number(proceedsMatch[1].replace(/,/g, '')) : null;
    if (netProceeds === null) warnings.push('Proceeds');

    const saleDate = Utilities.formatDate(msg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    const feesPaid = (soldAmount !== null && netProceeds !== null) ? round2(soldAmount - netProceeds) : null;

    let cardTitle = null;
    if (certMatch) {
      const titleRegex = new RegExp(`${certMatch[0]}\\s*([\\s\\S]*?)\\s*Sale Price`, 'im');
      const titleMatch = bodyText.match(titleRegex);
      cardTitle = titleMatch ? titleMatch[1].replace(/\s+/g, ' ').trim() : msg.getSubject();
    } else {
      cardTitle = msg.getSubject();
    }

    if (warnings.length > 0) {
      console.warn(`    ⚠ Partial parse - Missing: [${warnings.join(', ')}]`);
    }

    return {
      certNumber,
      saleDate,
      cardTitle,
      soldAmount: soldAmount !== null ? round2(soldAmount) : null,
      feesPaid: feesPaid !== null ? round2(feesPaid) : null,
      netProceeds: netProceeds !== null ? round2(netProceeds) : null
    };
  } catch (e) {
    console.error(`    ✗ Parse exception: ${e.message}`);
    return null;
  }
}

/***********************
 * PDF EXPORT
 ***********************/
function exportSinglePDF_(msg, folder, processedSet) {
  try {
    const pdfBlob = renderMessageToPDF_(msg);
    const filename = buildFileName_(msg);
    console.log(`      Filename: "${filename}"`);
    
    folder.createFile(pdfBlob).setName(filename);
    processedSet.add(msg.getId());
    return true;
  } catch (e) {
    console.error(`      Exception: ${e.message}`);
    return false;
  }
}

function buildFileName_(msg) {
  const date = Utilities.formatDate(msg.getDate(), Session.getScriptTimeZone(), 'yyyy-MM-dd');
  const subj = msg.getSubject() || 'No Subject';
  const body = msg.getPlainBody() || '';

  const certRegex = /(?:PSA|CGC|BGS) CERT\s*([0-9]+)/i;
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

/***********************
 * HELPER FUNCTIONS
 ***********************/
function loadExistingMessageIds_(sheet) {
  const idCol = 7;
  const lastRow = sheet.getLastRow();
  const set = new Set();
  if (lastRow >= 2) {
    sheet.getRange(2, idCol, lastRow - 1, 1).getValues().forEach(r => {
      const v = (r[0] || '').toString().trim();
      if (v) set.add(v);
    });
  }
  return set;
}

function loadProcessedIds_() {
  const ss = SpreadsheetApp.getActive();
  let sheet = ss.getSheetByName(LOG_SHEET);
  if (!sheet) {
    console.log(`Creating hidden sheet: "${LOG_SHEET}"`);
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

function getOrCreateFolderByPath_(path) {
  if (!path) throw new Error('DRIVE_FOLDER_PATH is empty');
  const parts = path.split('/').map(p => p.trim()).filter(Boolean);
  let folder = DriveApp.getRootFolder();
  console.log(`Navigating/creating folder path: ${parts.join(' → ')}`);
  for (const name of parts) {
    let next = null;
    const it = folder.getFoldersByName(name);
    if (it.hasNext()) {
      next = it.next();
      console.log(`  Found existing: "${name}"`);
    } else {
      next = folder.createFolder(name);
      console.log(`  Created new: "${name}"`);
    }
    folder = next;
  }
  return folder;
}

function inlineCidImages_(html, msg) {
  const atts = msg.getAttachments({ includeInlineImages: true, includeAttachments: false }) || [];
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
      const resp = UrlFetchApp.fetch(url, { followRedirects: true, muteHttpExceptions: true, headers: { 'User-Agent': 'Mozilla/5.0 (AppsScript PDF embedder)' } });
      if (resp.getResponseCode() !== 200) return m;
      let ctype = resp.getHeaders()['Content-Type'] || '';
      if (!ctype) {
        if (/\.(png)(\?|$)/i.test(url)) ctype = 'image/png'; else if (/\.(jpe?g)(\?|$)/i.test(url)) ctype = 'image/jpeg'; else if (/\.(gif)(\?|$)/i.test(url)) ctype = 'image/gif'; else if (/\.(webp)(\?|$)/i.test(url)) ctype = 'image/webp'; else ctype = 'application/octet-stream';
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

function escape_(s) {
  return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function round2(n) {
  return Math.round((n + Number.EPSILON) * 100) / 100;
}
