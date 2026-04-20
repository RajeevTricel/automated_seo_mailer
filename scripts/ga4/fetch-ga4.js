#!/usr/bin/env node
 
/**
 * scripts/ga4/fetch-ga4.js
 * Fetch GA4 data from Windsor API and ingest into staging Worker.
 * Same pattern as scripts/gsc/fetch-gsc.js
 */
 
const https = require('https');
 
// ── Config ──────────────────────────────────────────────────────────────────
const WINDSOR_API_KEY  = process.env.WINDSOR_API_KEY;
const INGEST_URL       = process.env.SHADOW_GA4_INGEST_URL;   // POST /api/ingest-ga4
const INGEST_SECRET    = process.env.SHADOW_INGEST_SECRET;
const MAPPINGS_URL     = process.env.SHADOW_MAPPINGS_URL;      // GET /api/site-source-mappings?source=ga4
 
// Manual overrides (workflow_dispatch inputs)
const SINGLE_SITE      = process.env.INPUT_SITE_URL   || null;
const GA4_PROPERTY_ID  = process.env.INPUT_GA4_PROPERTY_ID || null;
const START_DATE       = process.env.INPUT_START_DATE || daysAgo(28);
const END_DATE         = process.env.INPUT_END_DATE   || daysAgo(1);
const DRY_RUN          = process.env.INPUT_DRY_RUN === 'true';
 
// ── Helpers ──────────────────────────────────────────────────────────────────
function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d.toISOString().slice(0, 10);
}
 
function fetchJson(url, opts = {}) {
  return new Promise((resolve, reject) => {
    const req = https.request(url, opts, (res) => {
      let body = '';
      res.on('data', c => body += c);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(body) }); }
        catch (e) { reject(new Error(`JSON parse failed: ${body.slice(0, 200)}`)); }
      });
    });
    req.on('error', reject);
    if (opts.body) req.write(opts.body);
    req.end();
  });
}
 
async function postJson(url, payload, secret) {
  const body = JSON.stringify(payload);
  const u = new URL(url);
  const res = await fetchJson(u.toString(), {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-ingest-secret': secret,
      'Content-Length': Buffer.byteLength(body),
    },
    body,
  });
  return res;
}
 
// ── Windsor fetch ─────────────────────────────────────────────────────────────
async function fetchWindsorGA4(propertyId, startDate, endDate) {
  const fields = [
  'date',
  'sessions',
  'active_users',
  'engaged_sessions',
  'engagement_rate',
  'bounce_rate',
  'average_session_duration',
  'devicecategory',
  'source',
  'medium',
  'landing_page',
  ].join(',');
 
  const url = new URL('https://connectors.windsor.ai/googleanalytics4');
  url.searchParams.set('api_key',   WINDSOR_API_KEY);
  url.searchParams.set('date_from', startDate);
  url.searchParams.set('date_to',   endDate);
  url.searchParams.set('fields',    fields);
  url.searchParams.set('account_id', propertyId);
 
  console.log(`  Windsor fetch: account_id=${propertyId} ${startDate} → ${endDate}`);
 
  const res = await fetchJson(url.toString(), { method: 'GET' });
  if (res.status !== 200) {
    throw new Error(`Windsor returned ${res.status}: ${JSON.stringify(res.data).slice(0, 300)}`);
  }
  return res.data.data || [];
  console.log('Sample row:', JSON.stringify(rows[0])); 
  return rows;
}
 
// ── Normalize ─────────────────────────────────────────────────────────────────
function normalizeRows(rows, siteUrl, propertyId, startDate, endDate) {
  const capturedAt = new Date().toISOString();
 
  // Site-level daily aggregates
  const dailyMap = {};
  for (const r of rows) {
    const key = r.date || 'unknown';
    if (!dailyMap[key]) {
      dailyMap[key] = {
        date: key,
        sessions: 0, users: 0,
        engaged_sessions: 0,
        engagement_rate_sum: 0, bounce_rate_sum: 0,
        avg_session_duration_sum: 0, row_count: 0,
      };
    }
    const d = dailyMap[key];
    d.sessions             += Number(r.sessions)             || 0;
    d.users                += Number(r.active_users)                || 0;
    d.engaged_sessions     += Number(r.engaged_sessions)     || 0;
    d.engagement_rate_sum  += Number(r.engagement_rate)      || 0;
    d.bounce_rate_sum      += Number(r.bounce_rate)          || 0;
    d.device_category      += Number(r.devicecategory)       || 0;
    d.avg_session_duration_sum += Number(r.average_session_duration) || 0;
    d.row_count++;
  }
 
  const siteMetrics = Object.values(dailyMap).map(d => ({
    site_url:             siteUrl,
    ga4_property_id:      propertyId,
    date:                 d.date,
    sessions:             d.sessions,
    users:                d.users,
    engaged_sessions:     d.engaged_sessions,
    engagement_rate:      d.row_count ? d.engagement_rate_sum / d.row_count : null,
    bounce_rate:          d.row_count ? d.bounce_rate_sum     / d.row_count : null,
    avg_session_duration: d.row_count ? d.avg_session_duration_sum / d.row_count : null,
    captured_at:          capturedAt,
    period_start:         startDate,
    period_end:           endDate,
  }));
 
  // Landing page aggregates
  const pageMap = {};
  for (const r of rows) {
    const key = r.landing_page || '(not set)';
    if (!pageMap[key]) {
      pageMap[key] = {
        landing_page: key,
        sessions: 0, users: 0,
        engaged_sessions: 0, bounce_rate_sum: 0, row_count: 0,
      };
    }
    const p = pageMap[key];
    p.sessions         += Number(r.sessions)         || 0;
    p.users            += Number(r.active_users)            || 0;
    p.engaged_sessions += Number(r.engaged_sessions) || 0;
    p.bounce_rate_sum  += Number(r.bounce_rate)      || 0;
    p.row_count++;
  }
 
  const landingPageMetrics = Object.values(pageMap).map(p => ({
    site_url:         siteUrl,
    ga4_property_id:  propertyId,
    landing_page:     p.landing_page,
    sessions:         p.sessions,
    users:            p.users,
    engaged_sessions: p.engaged_sessions,
    bounce_rate:      p.row_count ? p.bounce_rate_sum / p.row_count : null,
    captured_at:      capturedAt,
    period_start:     startDate,
    period_end:       endDate,
  }));
 
  return { siteMetrics, landingPageMetrics, capturedAt };
}
 
// ── Fetch mappings ────────────────────────────────────────────────────────────
async function fetchMappings() {
  const res = await fetchJson(MAPPINGS_URL, { method: 'GET' });
  if (res.status !== 200) throw new Error(`Mappings fetch failed: ${res.status}`);
  return res.data.mappings || [];
}
 
// ── Process one site ──────────────────────────────────────────────────────────
async function processSite(siteUrl, propertyId, startDate, endDate) {
  siteUrl = siteUrl.replace(/\/$/, '');
  console.log(`\n[${siteUrl}] ga4_property_id=${propertyId}`);
 
  const rows = await fetchWindsorGA4(propertyId, startDate, endDate);
  console.log(`  Raw rows: ${rows.length}`);
 
  if (rows.length === 0) {
    console.log('  No data returned — skipping ingest');
    return { siteUrl, status: 'skipped', rows: 0 };
  }
 
  const { siteMetrics, landingPageMetrics, capturedAt } = normalizeRows(rows, siteUrl, propertyId, startDate, endDate);
 
  console.log(`  site_metrics: ${siteMetrics.length} rows`);
  console.log(`  landing_page_metrics: ${landingPageMetrics.length} rows`);
 
  const payload = {
    site_url:             siteUrl,
    ga4_property_id:      propertyId,
    captured_at:          capturedAt,
    period_start:         startDate,
    period_end:           endDate,
    site_metrics:         siteMetrics,
    landing_page_metrics: landingPageMetrics,
  };
 
  if (DRY_RUN) {
    console.log('  DRY RUN — skipping POST');
    return { siteUrl, status: 'dry_run', site_metrics: siteMetrics.length, landing_pages: landingPageMetrics.length };
  }
 
  const res = await postJson(INGEST_URL, payload, INGEST_SECRET);
  console.log(`  Ingest response: ${res.status}`);
  if (res.status !== 200) {
    throw new Error(`Ingest failed ${res.status}: ${JSON.stringify(res.data).slice(0, 300)}`);
  }
  return { siteUrl, status: 'ok', site_metrics: siteMetrics.length, landing_pages: landingPageMetrics.length };
}
 
// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  if (!WINDSOR_API_KEY) throw new Error('WINDSOR_API_KEY not set');
  if (!INGEST_SECRET)   throw new Error('SHADOW_INGEST_SECRET not set');
 
  console.log(`GA4 fetch | ${START_DATE} → ${END_DATE} | dry_run=${DRY_RUN}`);
 
  let sites = [];
 
  if (SINGLE_SITE && GA4_PROPERTY_ID) {
    // Manual single-site mode
    sites = [{ site_url: SINGLE_SITE, ga4_property_id: GA4_PROPERTY_ID }];
  } else {
    // All-sites mode — fetch mappings from Worker
    if (!MAPPINGS_URL) throw new Error('SHADOW_MAPPINGS_URL not set');
    const mappings = await fetchMappings();
    sites = mappings.filter(m => m.ga4_property_id);
    console.log(`Found ${sites.length} sites with ga4_property_id`);
  }
 
  const results = [];
  const failures = [];
 
  for (const site of sites) {
    try {
      const r = await processSite(site.site_url, site.ga4_property_id, START_DATE, END_DATE);
      results.push(r);
    } catch (err) {
      console.error(`  ERROR [${site.site_url}]: ${err.message}`);
      failures.push({ siteUrl: site.site_url, error: err.message });
      results.push({ siteUrl: site.site_url, status: 'failed', error: err.message });
    }
  }
 
  console.log('\n── Summary ──────────────────────────────────');
  for (const r of results) {
    const tag = r.status === 'ok' ? '✅' : r.status === 'dry_run' ? '🔍' : r.status === 'skipped' ? '⏭' : '❌';
    console.log(`${tag} ${r.siteUrl} — ${r.status}`);
  }
 
  if (failures.length > 0) {
    console.error(`\n${failures.length} site(s) failed`);
    process.exit(1);
  }
}
 
main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
