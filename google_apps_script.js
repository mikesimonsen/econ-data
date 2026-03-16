/**
 * Google Apps Script for importing econ-data CSVs from GitHub.
 *
 * Setup:
 * 1. Open your Google Sheet
 * 2. Extensions → Apps Script
 * 3. Paste this entire file
 * 4. Run importAllGroups() manually to test
 * 5. Set up a daily trigger: Edit → Triggers → Add Trigger
 *    - Function: importAllGroups
 *    - Event source: Time-driven
 *    - Type: Day timer
 *    - Time: 8-9 AM (after your 7 AM pipeline run)
 */

// ── Configuration ────────────────────────────────────────────
const GITHUB_OWNER = "mikesimonsen";
const GITHUB_REPO = "econ-data";
const GITHUB_BRANCH = "main";

// Which groups to import, and what to name the Sheet tabs.
// Comment out any you don't want.
const GROUPS = [
  { file: "cpi", tab: "CPI" },
  { file: "cpi-metro", tab: "CPI Metro" },
  { file: "pce", tab: "PCE" },
  { file: "ppi", tab: "PPI" },
  { file: "jolts", tab: "JOLTS" },
  { file: "case-shiller", tab: "Case-Shiller" },
  { file: "existing-home-sales", tab: "Existing Home Sales" },
  { file: "existing-home-sales-nsa", tab: "Existing Sales NSA" },
  { file: "housing-starts", tab: "Housing Starts" },
  { file: "building-permits", tab: "Building Permits" },
  { file: "housing-under-construction", tab: "Under Construction" },
  { file: "housing-completions", tab: "Completions" },
  { file: "new-home-sales", tab: "New Home Sales" },
  { file: "construction-spending", tab: "Construction Spending" },
  { file: "unemployment", tab: "Unemployment" },
  { file: "labor-force", tab: "Labor Force" },
  { file: "jobless-claims", tab: "Jobless Claims" },
  { file: "treasury-yields", tab: "Treasury Yields" },
  { file: "mortgage-rates", tab: "Mortgage Rates" },
  { file: "construction-employment", tab: "Construction Employment" },
];

// ── Main functions ───────────────────────────────────────────

/**
 * Import raw values, period % change, and YoY % change for all groups.
 */
function importAllGroups() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  GROUPS.forEach(function (group) {
    // Raw values
    importOne(ss, "sheets_data", group.file, group.tab);

    // Period % change
    importOne(ss, "sheets_data_calcs/period_pct", group.file, group.tab + " Period%");

    // YoY % change
    importOne(ss, "sheets_data_calcs/yoy_pct", group.file, group.tab + " YoY%");
  });

  updateTimestamp(ss);
}

/**
 * Import only raw values (no calcs).
 */
function importValuesOnly() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  GROUPS.forEach(function (group) {
    importOne(ss, "sheets_data", group.file, group.tab);
  });
  updateTimestamp(ss);
}

/**
 * Import only calculated series (period % and YoY %).
 */
function importCalcsOnly() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  GROUPS.forEach(function (group) {
    importOne(ss, "sheets_data_calcs/period_pct", group.file, group.tab + " Period%");
    importOne(ss, "sheets_data_calcs/yoy_pct", group.file, group.tab + " YoY%");
  });
  updateTimestamp(ss);
}

// ── Helpers ──────────────────────────────────────────────────

function importOne(ss, dataDir, filename, tabName) {
  try {
    var csvText = fetchCSV(dataDir, filename);
    var data = Utilities.parseCsv(csvText);
    writeToSheet(ss, tabName, data);
    Logger.log("Updated: " + tabName + " (" + data.length + " rows)");
  } catch (e) {
    Logger.log("Error updating " + tabName + ": " + e.message);
  }
}

function fetchCSV(dataDir, filename) {
  var url = "https://raw.githubusercontent.com/"
    + GITHUB_OWNER + "/" + GITHUB_REPO + "/"
    + GITHUB_BRANCH + "/" + dataDir + "/" + filename + ".csv";

  var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });

  if (response.getResponseCode() !== 200) {
    throw new Error("HTTP " + response.getResponseCode() + " fetching " + url);
  }

  return response.getContentText();
}

function writeToSheet(ss, tabName, data) {
  var sheet = ss.getSheetByName(tabName);

  if (!sheet) {
    sheet = ss.insertSheet(tabName);
  }

  // Clear existing content
  sheet.clearContents();

  // Write all data at once (much faster than row-by-row)
  if (data.length > 0 && data[0].length > 0) {
    sheet.getRange(1, 1, data.length, data[0].length).setValues(data);

    // Bold header row
    sheet.getRange(1, 1, 1, data[0].length).setFontWeight("bold");

    // Freeze header row
    sheet.setFrozenRows(1);
  }
}

function updateTimestamp(ss) {
  var sheet = ss.getSheetByName("Info");
  if (!sheet) {
    sheet = ss.insertSheet("Info", 0);
  }
  sheet.getRange("A1").setValue("Last updated");
  sheet.getRange("B1").setValue(new Date());
  sheet.getRange("A1").setFontWeight("bold");
}
