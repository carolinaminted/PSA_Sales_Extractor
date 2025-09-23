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
      if (!parsed) continue;

      // Append one row for the sale
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
 * @param {GoogleAppsScript.Gmail.GmailMessage} msg The Gmail message object.
 * @return {Object|null} An object with parsed data or null if parsing fails.
 */
function parsePsaEmail_(msg) {
  const bodyText = (msg.getPlainBody() || '').replace(/\r/g, '');

  try {
    // Regex to find key information
    const certRegex = /(?:PSA|CGC) CERT\s*([0-9]+)/i;
    const salePriceRegex = /Sale Price\s*\$([0-9.,]+)/i;
    const proceedsRegex = /Proceeds\s*\$([0-9.,]+)/i;
    const endedRegex = /Listing Ended\s*([A-Za-z]{3}\s\d{1,2},\s\d{1,2}:\d{2}\s[AP]M\s[A-Z]{3})/i;
    
    // Extract raw values
    const certMatch = bodyText.match(certRegex);
    const salePriceMatch = bodyText.match(salePriceRegex);
    const proceedsMatch = bodyText.match(proceedsRegex);
    const endedMatch = bodyText.match(endedRegex);

    // If any core value is missing, we can't process it.
    if (!certMatch || !salePriceMatch || !proceedsMatch || !endedMatch) {
      console.warn(`Skipping message ${msg.getId()}: Missing one or more key fields.`);
      return null;
    }

    const certNumber = certMatch[1];
    const soldAmount = Number(salePriceMatch[1].replace(/,/g, ''));
    const netProceeds = Number(proceedsMatch[1].replace(/,/g, ''));
    const saleDateStr = endedMatch[1];

    // Calculate fees
    const feesPaid = round2(soldAmount - netProceeds);

    // Format sale date to yyyy-MM-dd
    const saleDate = Utilities.formatDate(new Date(saleDateStr), Session.getScriptTimeZone(), 'yyyy-MM-dd');
    
    // Extract card title, which is between the cert line and the table
    const titleRegex = new RegExp(`${certMatch[0]}\\s*([\\s\\S]*?)\\s*Sale Price`, 'im');
    const titleMatch = bodyText.match(titleRegex);
    const cardTitle = titleMatch ? titleMatch[1].replace(/\s+/g, ' ').trim() : msg.getSubject();

    return {
      certNumber,
      saleDate,
      cardTitle,
      soldAmount: round2(soldAmount),
      feesPaid: round2(feesPaid),
      netProceeds: round2(netProceeds)
    };
  } catch (e) {
    console.error(`Failed to parse message ${msg.getId()}: ${e.message}`);
    return null;
  }
}

/**
 * Loads existing Gmail message IDs from the sheet to prevent duplicates.
 * Assumes the Message ID is in the 7th column (Column G).
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet The target sheet.
 * @return {Set<string>} A Set containing all existing message IDs.
 */
function loadExistingMessageIds_(sheet) {
  const idCol = 7; // Column G
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

/**
 * Utility function to round a number to 2 decimal places.
 * @param {number} n The number to round.
 * @return {number} The rounded number.
 */
function round2(n) { 
  return Math.round((n + Number.EPSILON) * 100) / 100; 
}
