/**
 * Google Apps Script for importing econ-data CSVs from GitHub.
 *
 * Setup:
 * 1. Open your Google Sheet
 * 2. Extensions → Apps Script
 * 3. Paste this entire file
 * 4. Run importAllGroups() manually for the initial load
 * 5. Set up a daily trigger: Triggers → Add Trigger
 *    - Function: importUpdatedGroups
 *    - Event source: Time-driven
 *    - Type: Day timer
 *    - Time: 8-9 AM (after your 7 AM pipeline run)
 */

// ── Configuration ────────────────────────────────────────────
var GITHUB_OWNER = "mikesimonsen";
var GITHUB_REPO = "econ-data";
var GITHUB_BRANCH = "main";

// Which groups to import, and what to name the Sheet tabs.
var GROUPS = [
  { file: "cpi", tab: "CPI" },
  { file: "cpi-metro", tab: "CPI Metro" },
  { file: "pce", tab: "PCE" },
  { file: "ppi", tab: "PPI" },
  { file: "labor-headlines", tab: "Labor Headlines" },
  { file: "labor-detail", tab: "Labor Detail" },
  { file: "case-shiller", tab: "Case-Shiller" },
  { file: "existing-home-sales", tab: "Existing Home Sales" },
  { file: "existing-home-sales-nsa", tab: "Existing Sales NSA" },
  { file: "housing-starts", tab: "Housing Starts" },
  { file: "building-permits", tab: "Building Permits" },
  { file: "housing-under-construction", tab: "Under Construction" },
  { file: "housing-completions", tab: "Completions" },
  { file: "new-home-sales", tab: "New Home Sales" },
  { file: "construction-spending", tab: "Construction Spending" },
  { file: "treasury-yields", tab: "Treasury Yields" },
  { file: "oil", tab: "Oil" },
  { file: "sp500", tab: "S&P 500" },
  { file: "mortgage-rates", tab: "Mortgage Rates" },
  { file: "consumer-confidence", tab: "Consumer Confidence" },
  { file: "mba-applications", tab: "MBA Purchase" },
  { file: "mortgage-intent", tab: "Mortgage Intent" },
  { file: "construction-employment", tab: "Construction Employment" },
  { file: "households", tab: "Households" },
  { file: "mortgage-spread", tab: "Mortgage Spread" },
  { file: "altos-inventory", tab: "Altos Inventory" },
  { file: "altos-new-listings", tab: "Altos New Listings" },
  { file: "altos-new-pending", tab: "Altos New Pending" },
];

// ── Main functions ───────────────────────────────────────────

/**
 * Daily trigger: only re-imports groups that have new data since last run.
 * Set this as your daily trigger.
 */
function importUpdatedGroups() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var props = PropertiesService.getScriptProperties();
  var lastRun = props.getProperty("lastManifestCheck") || "";

  // Fetch manifest to see what changed
  var manifest = fetchManifest();
  if (!manifest) {
    Logger.log("Could not fetch manifest — skipping");
    return;
  }

  // Find groups that were updated since our last run
  var updatedFiles = [];
  for (var fileId in manifest) {
    if (manifest[fileId] > lastRun) {
      updatedFiles.push(fileId);
    }
  }

  if (updatedFiles.length === 0) {
    Logger.log("No groups updated since last run");
    logUpdate(ss, []);
    return;
  }

  Logger.log("Groups to update: " + updatedFiles.join(", "));

  // Map file IDs to tab names for logging
  var updatedTabs = [];
  GROUPS.forEach(function (group) {
    if (updatedFiles.indexOf(group.file) === -1) return;

    importOne(ss, "sheets_data", group.file, group.tab);
    importOne(ss, "sheets_data_calcs/period", group.file, group.tab + " Period%");
    importOne(ss, "sheets_data_calcs/yoy", group.file, group.tab + " YoY%");
    updatedTabs.push(group.tab);
  });

  // Remember when we last checked
  props.setProperty("lastManifestCheck", new Date().toISOString());
  logUpdate(ss, updatedTabs);
}

/**
 * Full import of all groups — use for initial setup or manual refresh.
 */
function importAllGroups() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  GROUPS.forEach(function (group) {
    importOne(ss, "sheets_data", group.file, group.tab);
    importOne(ss, "sheets_data_calcs/period", group.file, group.tab + " Period%");
    importOne(ss, "sheets_data_calcs/yoy", group.file, group.tab + " YoY%");
  });

  // Set the checkpoint so importUpdatedGroups knows where to start
  var props = PropertiesService.getScriptProperties();
  props.setProperty("lastManifestCheck", new Date().toISOString());
  var allTabs = GROUPS.map(function (g) { return g.tab; });
  logUpdate(ss, allTabs);
}

/**
 * Import only raw values (no calcs).
 */
function importValuesOnly() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  GROUPS.forEach(function (group) {
    importOne(ss, "sheets_data", group.file, group.tab);
  });
  logUpdate(ss, []);
}

/**
 * Import only calculated series (period and YoY changes).
 */
function importCalcsOnly() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  GROUPS.forEach(function (group) {
    importOne(ss, "sheets_data_calcs/period", group.file, group.tab + " Period%");
    importOne(ss, "sheets_data_calcs/yoy", group.file, group.tab + " YoY%");
  });
  logUpdate(ss, []);
}

// ── Helpers ──────────────────────────────────────────────────

function fetchManifest() {
  var url = "https://raw.githubusercontent.com/"
    + GITHUB_OWNER + "/" + GITHUB_REPO + "/"
    + GITHUB_BRANCH + "/sheets_data/last_updated.json";

  var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });

  if (response.getResponseCode() !== 200) {
    Logger.log("Manifest fetch failed: HTTP " + response.getResponseCode());
    return null;
  }

  return JSON.parse(response.getContentText());
}

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

function logUpdate(ss, updatedTabs) {
  var sheet = ss.getSheetByName("Info");
  if (!sheet) {
    sheet = ss.insertSheet("Info", 0);
  }

  // Set up header if it doesn't exist
  if (sheet.getRange("A1").getValue() !== "Date") {
    sheet.getRange("A1:C1").setValues([["Date", "Status", "Groups Updated"]]);
    sheet.getRange("A1:C1").setFontWeight("bold");
    sheet.setFrozenRows(1);
    sheet.setColumnWidth(1, 160);
    sheet.setColumnWidth(2, 120);
    sheet.setColumnWidth(3, 600);
  }

  // Add today's entry at row 2 (newest on top)
  sheet.insertRowAfter(1);
  var now = new Date();
  var status = updatedTabs.length > 0 ? updatedTabs.length + " groups" : "No new data";
  var groups = updatedTabs.length > 0 ? updatedTabs.join(", ") : "—";
  sheet.getRange("A2:C2").setValues([[now, status, groups]]);
}
