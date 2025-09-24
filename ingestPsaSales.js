/**
 * === CONFIG ===
 * Adjust these to match your Gmail label and Google Sheet tab name.
 */
const LABEL_NAME = 'PSA Sales';   // The Gmail label to ingest from.
const SHEET_NAME = 'PSA Sales';       // The Sheet tab for the output.
const MAX_PER_RUN = 250;              // Safety cap for emails to process per run.

/**
 * Main function to parse labeled PSA sales emails and append one row per sale.
 * De-duplicates by Gmail message ID to prevent processing the same sale twice.
 */
function ingestPsaSales() {
  const sheet = SpreadsheetApp.getActive().getSheetByName(SHEET_NAME);
  if (!sheet) throw new Error(`Missing sheet named "${SHEET_NAME}"`);

  const existingIds = loadExistingMessageIds_(sheet);
  const label = GmailApp.getUserLabelByName(LABEL_NAME);
  if (!label) throw new Error(`Gmail label "${LABEL_NAME}" not found`);

  let appended = 0;
  // Fetch a reasonable number of threads to stay within limits
  const threads = label.getThreads(0, Math.ceil(MAX_PER_RUN / 2)); 
  for (const thread of threads) {
    for (const msg of thread.getMessages()) {
      if (appended >= MAX_PER_RUN) break;

      const msgId = msg.getId();
      if (existingIds.has(msgId)) continue;

      const parsed = parsePsaEmail_(msg);
      if (!parsed) continue; // Only skip if the entire email body is unreadable

      // Append one row for the sale, even if some fields are null
      sheet.appendRow([
        parsed.certNumber,
        parsed.saleDate,
        parsed.cardTitle,
        parsed.soldAmount,
        parsed.feesPaid,
        parsed.netProceeds,
        msgId, // Add message ID for de-duplication
      ]);
      
      existingIds.add(msgId);
      appended++;
    }
    if (appended >= MAX_PER_RUN) break;
  }
  SpreadsheetApp.getActive().toast(`Processed and appended ${appended} new sales.`);
}

/**
 * Extracts sale details from the body of a PSA "Payout Incoming" email.
 * This version logs specific missing fields and processes partial data.
 * @param {GoogleAppsScript.Gmail.GmailMessage} msg The Gmail message object.
 * @return {Object|null} An object with parsed data or null if parsing fails completely.
 */
function parsePsaEmail_(msg) {
  const bodyText = (msg.getPlainBody() || '').replace(/\r/g, '');
  const warnings = [];

  try {
    // MODIFIED: Added 'BGS' to the list of accepted grading companies.
    const certRegex = /(?:PSA|CGC|BGS) CERT\s*([0-9]+)/i;
    const salePriceRegex = /Sale Price\s*\$([0-9.,]+)/i;
    const proceedsRegex = /Proceeds\s*\$([0-9.,]+)/i;
    // MODIFIED: Made date regex more flexible to handle odd line breaks and wording.
    const endedRegex = /(?:Listing\s*)?Ended\s*[\s\S]*?([A-Za-z]{3}\s\d{1,2},?\s\d{1,2}:\d{2}\s[AP]M(?:\s[A-Z]{2,4})?)/i;
    
    // Extract raw values
    const certMatch = bodyText.match(certRegex);
    const salePriceMatch = bodyText.match(salePriceRegex);
    const proceedsMatch = bodyText.match(proceedsRegex);
    const endedMatch = bodyText.match(endedRegex);

    const certNumber = certMatch ? certMatch[1] : null;
    if (!certNumber) warnings.push('Certification Number');

    const soldAmount = salePriceMatch ? Number(salePriceMatch[1].replace(/,/g, '')) : null;
    if (soldAmount === null) warnings.push('Sale Price');

    const netProceeds = proceedsMatch ? Number(proceedsMatch[1].replace(/,/g, '')) : null;
    if (netProceeds === null) warnings.push('Proceeds');
    
    const saleDateStr = endedMatch ? endedMatch[1] : null;
    if (!saleDateStr) warnings.push('Listing Ended Date');

    const feesPaid = (soldAmount !== null && netProceeds !== null) ? round2(soldAmount - netProceeds) : null;
    
    const saleDate = saleDateStr ? Utilities.formatDate(new Date(saleDateStr.replace(',', '')), Session.getScriptTimeZone(), 'yyyy-MM-dd') : null;
    
    let cardTitle = null;
    if (certMatch) {
      const titleRegex = new RegExp(`${certMatch[0]}\\s*([\\s\\S]*?)\\s*Sale Price`, 'im');
      const titleMatch = bodyText.match(titleRegex);
      cardTitle = titleMatch ? titleMatch[1].replace(/\s+/g, ' ').trim() : msg.getSubject();
    } else {
      cardTitle = msg.getSubject();
    }
    
    if (warnings.length > 0) {
      console.warn(`Partially processed message ${msg.getId()}: Missing field(s): [${warnings.join(', ')}]`);
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
    console.error(`Failed to parse message ${msg.getId()}: ${e.message}`);
    return null;
  }
}

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

function round2(n) { 
  return Math.round((n + Number.EPSILON) * 100) / 100; 
}
