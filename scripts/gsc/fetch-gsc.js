// scripts/gsc/fetch-gsc.js
// Requires: npm i googleapis

'use strict';

const { google } = require('googleapis');

const SEARCH_CONSOLE_SCOPE = 'https://www.googleapis.com/auth/webmasters.readonly';
const SEARCH_ANALYTICS_URL = 'https://www.googleapis.com/webmasters/v3/sites';

function requireEnv(name) {
  const value = process.env[name];
  if (!value || !String(value).trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return String(value).trim();
}

function optionalEnv(name, fallback = null) {
  const value = process.env[name];
  return value == null || String(value).trim() === '' ? fallback : String(value).trim();
}

function parseBoolean(value, fallback = false) {
  if (value == null) return fallback;
  return ['1', 'true', 'yes', 'y', 'on'].includes(String(value).trim().toLowerCase());
}

function parseInteger(value, fallback) {
  const n = Number.parseInt(String(value), 10);
  return Number.isFinite(n) ? n : fallback;
}

function assertIsoDate(value, fieldName) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    throw new Error(`${fieldName} must be YYYY-MM-DD. Received: ${value}`);
  }
}

function normalizePrivateKey(raw) {
  return String(raw || '').replace(/\\n/g, '\n').trim();
}

function buildJwtClient() {
  const rawJson = requireEnv('GSC_SERVICE_ACCOUNT_JSON');

  let serviceAccount;
  try {
    serviceAccount = JSON.parse(rawJson);
  } catch (error) {
    throw new Error(
      `GSC_SERVICE_ACCOUNT_JSON is not valid JSON: ${
        error instanceof Error ? error.message : String(error)
      }`
    );
  }

  const clientEmail = String(serviceAccount.client_email || '').trim();
  const privateKey = normalizePrivateKey(serviceAccount.private_key);

  if (!clientEmail) {
    throw new Error('GSC_SERVICE_ACCOUNT_JSON is missing client_email');
  }

  if (!privateKey) {
    throw new Error('GSC_SERVICE_ACCOUNT_JSON is missing private_key');
  }

  if (!privateKey.includes('BEGIN PRIVATE KEY') || !privateKey.includes('END PRIVATE KEY')) {
    throw new Error('GSC_SERVICE_ACCOUNT_JSON.private_key is not a valid PEM private key');
  }

  return new google.auth.JWT({
    email: clientEmail,
    key: privateKey,
    scopes: [SEARCH_CONSOLE_SCOPE],
  });
}

async function querySearchAnalytics(auth, property, requestBody) {
  const url = `${SEARCH_ANALYTICS_URL}/${encodeURIComponent(property)}/searchAnalytics/query`;

  const response = await auth.request({
    url,
    method: 'POST',
    data: requestBody,
  });

  return response.data || {};
}

async function fetchAllRowsForDimensions({
  auth,
  property,
  startDate,
  endDate,
  dimensions,
  type = 'web',
  rowLimit = 25000,
}) {
  const rows = [];
  let startRow = 0;

  while (true) {
    const body = {
      startDate,
      endDate,
      type,
      dimensions,
      rowLimit,
      startRow,
    };

    const data = await querySearchAnalytics(auth, property, body);
    const batch = Array.isArray(data.rows) ? data.rows : [];

    if (batch.length === 0) {
      break;
    }

    rows.push(...batch);

    if (batch.length < rowLimit) {
      break;
    }

    startRow += rowLimit;
  }

  return rows;
}

function safeNumber(value, fallback = 0) {
  return Number.isFinite(Number(value)) ? Number(value) : fallback;
}

function toIsoNow() {
  return new Date().toISOString();
}

function normalizeQueryRows(rows, siteUrl, snapshotId = null) {
  return rows.map((row) => ({
    site_url: siteUrl,
    snapshot_id: snapshotId,
    date: null,
    query: row.keys?.[0] ?? null,
    page: null,
    country: null,
    device: null,
    clicks: safeNumber(row.clicks),
    impressions: safeNumber(row.impressions),
    ctr: safeNumber(row.ctr),
    position: safeNumber(row.position),
  })).filter((row) => row.query);
}

function normalizePageRows(rows, siteUrl, snapshotId = null) {
  return rows.map((row) => ({
    site_url: siteUrl,
    snapshot_id: snapshotId,
    date: null,
    page: row.keys?.[0] ?? null,
    clicks: safeNumber(row.clicks),
    impressions: safeNumber(row.impressions),
    ctr: safeNumber(row.ctr),
    position: safeNumber(row.position),
  })).filter((row) => row.page);
}

function normalizeCountryRows(rows, siteUrl, snapshotId = null) {
  return rows.map((row) => ({
    site_url: siteUrl,
    snapshot_id: snapshotId,
    date: null,
    country: row.keys?.[0] ?? null,
    clicks: safeNumber(row.clicks),
    impressions: safeNumber(row.impressions),
    ctr: safeNumber(row.ctr),
    position: safeNumber(row.position),
  })).filter((row) => row.country);
}

function normalizeDeviceRows(rows, siteUrl, snapshotId = null) {
  return rows.map((row) => ({
    site_url: siteUrl,
    snapshot_id: snapshotId,
    date: null,
    device: row.keys?.[0] ?? null,
    clicks: safeNumber(row.clicks),
    impressions: safeNumber(row.impressions),
    ctr: safeNumber(row.ctr),
    position: safeNumber(row.position),
  })).filter((row) => row.device);
}

function buildPayload({
  siteUrl,
  gscProperty,
  startDate,
  endDate,
  capturedAt,
  queryMetrics,
  pageMetrics,
  countryMetrics,
  deviceMetrics,
}) {
  return {
    source: 'gsc',
    payload_version: 1,
    site_url: siteUrl,
    gsc_property: gscProperty,
    captured_at: capturedAt,
    period_start: startDate,
    period_end: endDate,
    import_mode: 'api',
    modules: {
      query_metrics: queryMetrics,
      page_metrics: pageMetrics,
      country_metrics: countryMetrics,
      device_metrics: deviceMetrics,
    },
    counts: {
      query_metrics: queryMetrics.length,
      page_metrics: pageMetrics.length,
      country_metrics: countryMetrics.length,
      device_metrics: deviceMetrics.length,
    },
  };
}

async function postToWorker(payload) {
  const ingestUrl = requireEnv('GSC_INGEST_URL');
  const ingestSecret = requireEnv('GSC_INGEST_SECRET');

  if (typeof fetch !== 'function') {
    throw new Error('Global fetch is not available. Use Node.js 18+ in GitHub Actions.');
  }

  const response = await fetch(ingestUrl, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-ingest-secret': ingestSecret,
    },
    body: JSON.stringify(payload),
  });

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(`Worker ingest failed: ${response.status} ${response.statusText} - ${responseText}`);
  }

  try {
    return JSON.parse(responseText);
  } catch {
    return { ok: true, raw: responseText };
  }
}

function printSummary(payload) {
  console.log(JSON.stringify({
    site_url: payload.site_url,
    gsc_property: payload.gsc_property,
    captured_at: payload.captured_at,
    period_start: payload.period_start,
    period_end: payload.period_end,
    counts: payload.counts,
    samples: {
      query_metrics: payload.modules.query_metrics.slice(0, 2),
      page_metrics: payload.modules.page_metrics.slice(0, 2),
      country_metrics: payload.modules.country_metrics.slice(0, 2),
      device_metrics: payload.modules.device_metrics.slice(0, 2),
    },
  }, null, 2));
}

async function main() {
  const siteUrl = requireEnv('SITE_URL');
  const gscProperty = requireEnv('GSC_PROPERTY');
  const startDate = requireEnv('START_DATE');
  const endDate = requireEnv('END_DATE');
  const searchType = optionalEnv('SEARCH_TYPE', 'web');
  const rowLimit = parseInteger(optionalEnv('ROW_LIMIT', '25000'), 25000);
  const dryRun = parseBoolean(optionalEnv('DRY_RUN', 'false'), false);

  assertIsoDate(startDate, 'START_DATE');
  assertIsoDate(endDate, 'END_DATE');

  const auth = buildJwtClient();
  await auth.authorize();

  const capturedAt = toIsoNow();

  console.log(`Fetching GSC data for ${siteUrl} using property ${gscProperty}`);
  console.log(`Date range: ${startDate} -> ${endDate}`);
  console.log(`Search type: ${searchType}`);

  const [queryRows, pageRows, countryRows, deviceRows] = await Promise.all([
    fetchAllRowsForDimensions({
      auth,
      property: gscProperty,
      startDate,
      endDate,
      dimensions: ['query'],
      type: searchType,
      rowLimit,
    }),
    fetchAllRowsForDimensions({
      auth,
      property: gscProperty,
      startDate,
      endDate,
      dimensions: ['page'],
      type: searchType,
      rowLimit,
    }),
    fetchAllRowsForDimensions({
      auth,
      property: gscProperty,
      startDate,
      endDate,
      dimensions: ['country'],
      type: searchType,
      rowLimit,
    }),
    fetchAllRowsForDimensions({
      auth,
      property: gscProperty,
      startDate,
      endDate,
      dimensions: ['device'],
      type: searchType,
      rowLimit,
    }),
  ]);

  const queryMetrics = normalizeQueryRows(queryRows, siteUrl);
  const pageMetrics = normalizePageRows(pageRows, siteUrl);
  const countryMetrics = normalizeCountryRows(countryRows, siteUrl);
  const deviceMetrics = normalizeDeviceRows(deviceRows, siteUrl);

  const payload = buildPayload({
    siteUrl,
    gscProperty,
    startDate,
    endDate,
    capturedAt,
    queryMetrics,
    pageMetrics,
    countryMetrics,
    deviceMetrics,
  });

  printSummary(payload);

  if (dryRun) {
    console.log('DRY_RUN=true, skipping Worker ingest.');
    return;
  }

  const ingestResult = await postToWorker(payload);
  console.log('Worker ingest response:');
  console.log(JSON.stringify(ingestResult, null, 2));
}

main().catch((error) => {
  console.error('GSC fetch failed.');
  console.error(error?.stack || error?.message || String(error));
  process.exit(1);
});
