#!/usr/bin/env node
// scripts/summaries/compute-summaries.js
//
// For each active site:
//   1. Fetches /api/site-summary  (PageSpeed + GSC + GA4 + freshness)
//   2. Fetches /api/ga4           (daily rows for trend/change detection)
//   3. Computes health scores, risk flags, opportunity flags, priority actions, detected changes
//   4. POSTs structured payload to POST /api/ingest-summaries
//
// Env vars:
//   SHADOW_WORKER_BASE       — base URL of staging Worker (no trailing slash)
//   SHADOW_INGEST_SECRET     — shared secret sent as x-ingest-secret header
//   DRY_RUN                  — 'true' to skip POST and log payload instead
//   SITE_URL                 — optional: process one site only
 
const WORKER_BASE    = (process.env.SHADOW_WORKER_BASE || '').replace(/\/$/, '');
const INGEST_SECRET  = process.env.SHADOW_INGEST_SECRET;
const DRY_RUN        = process.env.DRY_RUN === 'true';
const TARGET_SITE    = process.env.SITE_URL || null;
 
if (!WORKER_BASE)   { console.error('SHADOW_WORKER_BASE not set'); process.exit(1); }
if (!INGEST_SECRET) { console.error('SHADOW_INGEST_SECRET not set'); process.exit(1); }
 
// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------
async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.json();
}
 
// ---------------------------------------------------------------------------
// Score helpers
// ---------------------------------------------------------------------------
 
// Accepts either 0-1 floats (PSI native) or 0-100 integers (already normalised)
function normalizeScore(val) {
  if (val == null || isNaN(val)) return null;
  const n = Number(val);
  return n > 1 ? Math.round(n) : Math.round(n * 100);
}
 
function avg(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
 
// Pull strategy block from pagespeed — handles both flat and nested shapes
function getStrategyScores(pagespeed, strategy) {
  if (!pagespeed) return null;
  // site-summary shape: pagespeed.desktop / pagespeed.mobile
  const s = pagespeed[strategy];
  if (!s) return null;
  // scores might be top-level or nested under .scores
  return {
    performance:   normalizeScore(s.performance   ?? s.scores?.performance),
    seo:           normalizeScore(s.seo           ?? s.scores?.seo),
    accessibility: normalizeScore(s.accessibility ?? s.scores?.accessibility),
    best_practices: normalizeScore(s.best_practices ?? s.scores?.best_practices)
  };
}
 
// ---------------------------------------------------------------------------
// Health score computation  (each returns 0-100 integer or null)
// ---------------------------------------------------------------------------
 
// Technical health: weighted average of performance (50%), SEO (30%), accessibility (20%)
// Uses worst of desktop/mobile for performance to reflect real user experience
function computeTechnicalHealth(pagespeed) {
  const desktop = getStrategyScores(pagespeed, 'desktop');
  const mobile  = getStrategyScores(pagespeed, 'mobile');
 
  const perfScores = [desktop?.performance, mobile?.performance].filter(v => v != null);
  const seoScores  = [desktop?.seo,         mobile?.seo        ].filter(v => v != null);
  const a11yScores = [desktop?.accessibility, mobile?.accessibility].filter(v => v != null);
 
  if (!perfScores.length) return null;
 
  // Use average of desktop + mobile (both matter)
  const avgPerf = avg(perfScores);
  const avgSeo  = seoScores.length  ? avg(seoScores)  : avgPerf;
  const avgA11y = a11yScores.length ? avg(a11yScores) : avgPerf;
 
  return Math.round(avgPerf * 0.5 + avgSeo * 0.3 + avgA11y * 0.2);
}
 
// Search performance: CTR quality (60pts) + impression volume (40pts)
// CTR benchmark: 5% = full CTR score
// Volume benchmark: 100k impressions = full volume score
function computeSearchPerformanceScore(gsc) {
  const totals = gsc?.totals;
  if (!totals) return null;
 
  const { ctr, impressions } = totals;
  if (ctr == null && impressions == null) return null;
 
  const ctrScore = ctr != null
    ? Math.min((ctr / 0.05) * 60, 60)
    : 0;
 
  const volScore = impressions > 0
    ? Math.min((Math.log10(Math.max(impressions, 1)) / Math.log10(100000)) * 40, 40)
    : 0;
 
  return Math.max(0, Math.round(ctrScore + volScore));
}
 
// Traffic health: engagement quality (60pts) + session volume (40pts)
// Engagement benchmark: 70% engagement rate = full engagement score
// Volume benchmark: 10k sessions = full volume score
function computeTrafficHealth(ga4) {
  const totals = ga4?.totals;
  if (!totals) return null;
 
  const { sessions, engagement_rate, bounce_rate } = totals;
  if (sessions == null) return null;
 
  // Derive engagement from engagement_rate if present, else invert bounce_rate
  let engRatio = engagement_rate;
  if (engRatio == null && bounce_rate != null) engRatio = 1 - bounce_rate;
 
  const engScore = engRatio != null
    ? Math.min((engRatio / 0.70) * 60, 60)
    : 30; // neutral fallback
 
  const volScore = sessions > 0
    ? Math.min((Math.log10(Math.max(sessions, 1)) / Math.log10(10000)) * 40, 40)
    : 0;
 
  return Math.max(0, Math.round(engScore + volScore));
}
 
// ---------------------------------------------------------------------------
// Risk flags
// ---------------------------------------------------------------------------
function computeRisks(summary) {
  const risks = [];
  const { pagespeed, gsc, ga4, freshness } = summary;
 
  // -- PageSpeed: low performance --
  const desktop = getStrategyScores(pagespeed, 'desktop');
  const mobile  = getStrategyScores(pagespeed, 'mobile');
 
  if (desktop?.performance != null && desktop.performance < 50) {
    risks.push({
      risk_type:    'low_performance_desktop',
      severity:     desktop.performance < 25 ? 'critical' : 'high',
      source:       'pagespeed',
      description:  `Desktop performance score is ${desktop.performance} — below threshold of 50`,
      metric_value: desktop.performance,
      threshold:    50
    });
  }
 
  if (mobile?.performance != null && mobile.performance < 50) {
    risks.push({
      risk_type:    'low_performance_mobile',
      severity:     mobile.performance < 25 ? 'critical' : 'high',
      source:       'pagespeed',
      description:  `Mobile performance score is ${mobile.performance} — below threshold of 50`,
      metric_value: mobile.performance,
      threshold:    50
    });
  }
 
  // -- GSC: very low CTR with meaningful impressions --
  if (gsc?.totals) {
    const { ctr, impressions } = gsc.totals;
    if (ctr != null && impressions > 1000 && ctr < 0.01) {
      risks.push({
        risk_type:    'low_gsc_ctr',
        severity:     'medium',
        source:       'gsc',
        description:  `Overall CTR is ${(ctr * 100).toFixed(2)}% across ${impressions.toLocaleString()} impressions — well below 1%`,
        metric_value: ctr,
        threshold:    0.01
      });
    }
  }
 
  // -- GA4: high bounce rate --
  if (ga4?.totals) {
    const { bounce_rate, sessions } = ga4.totals;
    if (bounce_rate != null && bounce_rate > 0.70 && (sessions ?? 0) > 100) {
      risks.push({
        risk_type:    'high_bounce_rate',
        severity:     bounce_rate > 0.85 ? 'high' : 'medium',
        source:       'ga4',
        description:  `Bounce rate is ${(bounce_rate * 100).toFixed(1)}% — above threshold of 70%`,
        metric_value: bounce_rate,
        threshold:    0.70
      });
    }
  }
 
  // -- Stale data --
  const now          = Date.now();
  const sevenDaysMs  = 7 * 24 * 60 * 60 * 1000;
 
  const freshnessMap = [
    { key: 'pagespeed_last_updated_at', source: 'pagespeed', label: 'PageSpeed' },
    { key: 'gsc_last_updated_at',       source: 'gsc',       label: 'GSC'       },
    { key: 'ga4_last_updated_at',       source: 'ga4',       label: 'GA4'       }
  ];
 
  for (const { key, source, label } of freshnessMap) {
    const ts = freshness?.[key];
    if (!ts) continue;
    const ageMs = now - new Date(ts).getTime();
    if (ageMs > sevenDaysMs) {
      const days = Math.round(ageMs / (24 * 60 * 60 * 1000));
      risks.push({
        risk_type:    'stale_data',
        severity:     days > 14 ? 'high' : 'medium',
        source,
        description:  `${label} data is ${days} days old — freshness threshold is 7 days`,
        metric_value: days,
        threshold:    7
      });
    }
  }
 
  return risks;
}
 
// ---------------------------------------------------------------------------
// Opportunity flags
// ---------------------------------------------------------------------------
function computeOpportunities(summary) {
  const opportunities = [];
  const { pagespeed, gsc, ga4 } = summary;
 
  // -- GSC: high impressions / low CTR queries (top 3) --
  if (Array.isArray(gsc?.top_queries)) {
    const lowCtr = gsc.top_queries
      .filter(q => q.impressions > 1000 && q.ctr < 0.02)
      .slice(0, 3);
 
    for (const q of lowCtr) {
      opportunities.push({
        opportunity_type: 'high_impressions_low_ctr',
        source:           'gsc',
        description:      `Query "${q.query}" has ${q.impressions.toLocaleString()} impressions but ${(q.ctr * 100).toFixed(1)}% CTR — title/meta optimisation could lift clicks`,
        metric_value:     q.ctr,
        potential_impact: q.impressions > 10000 ? 'high' : 'medium'
      });
    }
  }
 
  // -- PageSpeed: accessibility or SEO score < 80 --
  const desktop = getStrategyScores(pagespeed, 'desktop');
 
  if (desktop?.accessibility != null && desktop.accessibility < 80) {
    opportunities.push({
      opportunity_type: 'low_accessibility',
      source:           'pagespeed',
      description:      `Desktop accessibility score is ${desktop.accessibility} — improvements can expand audience reach`,
      metric_value:     desktop.accessibility,
      potential_impact: 'medium'
    });
  }
 
  if (desktop?.seo != null && desktop.seo < 80) {
    opportunities.push({
      opportunity_type: 'low_seo_score',
      source:           'pagespeed',
      description:      `Desktop SEO score is ${desktop.seo} — technical fixes available that may improve ranking`,
      metric_value:     desktop.seo,
      potential_impact: 'high'
    });
  }
 
  // -- GA4: high-traffic pages with low engagement (top 2) --
  if (Array.isArray(ga4?.top_pages)) {
    const lowEng = ga4.top_pages
      .filter(p => (p.sessions ?? 0) > 200 && p.engagement_rate != null && p.engagement_rate < 0.40)
      .slice(0, 2);
 
    for (const p of lowEng) {
      opportunities.push({
        opportunity_type: 'high_traffic_low_engagement',
        source:           'ga4',
        description:      `Page "${p.landing_page}" has ${(p.sessions ?? 0).toLocaleString()} sessions but only ${(p.engagement_rate * 100).toFixed(0)}% engagement — content or CTA improvements may increase conversions`,
        metric_value:     p.engagement_rate,
        potential_impact: (p.sessions ?? 0) > 1000 ? 'high' : 'medium'
      });
    }
  }
 
  return opportunities;
}
 
// ---------------------------------------------------------------------------
// Priority actions — top 3 derived from risks + opportunities
// ---------------------------------------------------------------------------
const SEVERITY_RANK = { critical: 40, high: 30, medium: 20, low: 10 };
const IMPACT_RANK   = { high: 28, medium: 18, low: 8 };
 
function computePriorityActions(risks, opportunities) {
  const candidates = [];
 
  for (const r of risks) {
    candidates.push({
      score:       SEVERITY_RANK[r.severity] ?? 10,
      action_type: r.risk_type,
      description: `Fix: ${r.description}`,
      source:      r.source,
      severity:    r.severity
    });
  }
 
  for (const o of opportunities) {
    candidates.push({
      score:       IMPACT_RANK[o.potential_impact] ?? 8,
      action_type: o.opportunity_type,
      description: `Opportunity: ${o.description}`,
      source:      o.source,
      severity:    o.potential_impact
    });
  }
 
  return candidates
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((a, i) => ({ ...a, priority: i + 1 }));
}
 
// ---------------------------------------------------------------------------
// Detected changes — compares first half vs second half of GA4 daily trend
// Requires at least 14 daily rows to be meaningful
// ---------------------------------------------------------------------------
function computeChanges(ga4) {
  const daily = ga4?.daily;
  if (!Array.isArray(daily) || daily.length < 14) return [];
 
  // Sort ascending by date
  const sorted = [...daily].sort((a, b) => (a.date > b.date ? 1 : -1));
  const half   = Math.floor(sorted.length / 2);
  const prev   = sorted.slice(0, half);
  const curr   = sorted.slice(sorted.length - half);
 
  const sumField = (rows, field) =>
    rows.reduce((acc, r) => acc + (Number(r[field]) || 0), 0);
 
  const metrics = ['sessions', 'active_users', 'engaged_sessions'];
  const changes = [];
 
  for (const metric of metrics) {
    const prevVal = sumField(prev, metric);
    const currVal = sumField(curr, metric);
    if (prevVal === 0) continue;
 
    const deltaPct = ((currVal - prevVal) / prevVal) * 100;
    if (Math.abs(deltaPct) < 20) continue; // not material
 
    changes.push({
      metric,
      source:         'ga4',
      previous_value: prevVal,
      current_value:  currVal,
      delta_pct:      Math.round(deltaPct * 10) / 10,
      direction:      deltaPct > 0 ? 'up' : 'down',
      is_material:    true
    });
  }
 
  return changes;
}
 
// ---------------------------------------------------------------------------
// Per-site processing
// ---------------------------------------------------------------------------
async function processSite(siteUrl) {
  console.log(`\n--- ${siteUrl} ---`);
 
  // 1. Fetch merged site-summary (PageSpeed + GSC + GA4 + freshness)
  let summary;
  try {
    const res = await fetchJSON(`${WORKER_BASE}/api/site-summary?site=${encodeURIComponent(siteUrl)}`);
    if (!res.ok) {
      console.warn(`  site-summary ok=false: ${res.error || 'unknown'}`);
      return null;
    }
    summary = res;
  } catch (err) {
    console.error(`  site-summary fetch failed: ${err.message}`);
    return null;
  }
 
  // 2. Fetch GA4 daily data for change detection (richer than site-summary GA4 block)
  try {
    const ga4Res = await fetchJSON(`${WORKER_BASE}/api/ga4?site=${encodeURIComponent(siteUrl)}`);
    if (ga4Res.ok && Array.isArray(ga4Res.daily)) {
      summary.ga4 = summary.ga4 || {};
      summary.ga4.daily = ga4Res.daily;
    }
  } catch (_) {
    // optional — proceed without daily data
  }
 
  const computedAt = new Date().toISOString();
 
  // 3. Compute scores
  const technicalHealth  = computeTechnicalHealth(summary.pagespeed);
  const searchPerf       = computeSearchPerformanceScore(summary.gsc);
  const trafficHealth    = computeTrafficHealth(summary.ga4);
 
  const nonNull          = [technicalHealth, searchPerf, trafficHealth].filter(v => v != null);
  const overallHealth    = nonNull.length
    ? Math.round(nonNull.reduce((a, b) => a + b, 0) / nonNull.length)
    : null;
 
  console.log(`  Health → technical=${technicalHealth ?? 'n/a'} search=${searchPerf ?? 'n/a'} traffic=${trafficHealth ?? 'n/a'} overall=${overallHealth ?? 'n/a'}`);
 
  // 4. Compute flags
  const risks          = computeRisks(summary);
  const opportunities  = computeOpportunities(summary);
  const priorityActions = computePriorityActions(risks, opportunities);
  const changes        = computeChanges(summary.ga4);
 
  console.log(`  Flags → risks=${risks.length} opp=${opportunities.length} actions=${priorityActions.length} changes=${changes.length}`);
 
  if (risks.length)         risks.forEach(r          => console.log(`    RISK [${r.severity}] ${r.risk_type}`));
  if (opportunities.length) opportunities.forEach(o  => console.log(`    OPP  [${o.potential_impact}] ${o.opportunity_type}`));
  if (priorityActions.length) priorityActions.forEach(a => console.log(`    ACT  #${a.priority} ${a.action_type}`));
  if (changes.length)       changes.forEach(c         => console.log(`    CHG  ${c.metric} ${c.direction} ${c.delta_pct}%`));
 
  return {
    site_url:   siteUrl,
    computed_at: computedAt,
    health: {
      technical_health_score:   technicalHealth,
      search_performance_score: searchPerf,
      traffic_health_score:     trafficHealth,
      overall_health_score:     overallHealth
    },
    risks,
    opportunities,
    priority_actions: priorityActions,
    changes
  };
}
 
// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
async function main() {
  console.log('=== compute-summaries.js ===');
  console.log(`DRY_RUN:     ${DRY_RUN}`);
  console.log(`TARGET_SITE: ${TARGET_SITE || 'all sites'}`);
  console.log(`WORKER_BASE: ${WORKER_BASE}`);
  console.log('');
 
  // Resolve site list
  let sites;
  if (TARGET_SITE) {
    sites = [TARGET_SITE.replace(/\/$/, '')];
  } else {
    // Use GA4 mappings endpoint — all 17 active sites have GA4 populated
    const mappingsUrl = `${WORKER_BASE}/api/site-source-mappings?source=ga4`;
    const mappingsRes = await fetchJSON(mappingsUrl);
    if (!mappingsRes.ok) {
      throw new Error(`Failed to fetch site mappings: ${mappingsRes.error}`);
    }
    // Worker returns { ok, count, sites: [...] }
    sites = (mappingsRes.sites || []).map(s => s.site_url.replace(/\/$/, ''));
    console.log(`Resolved ${sites.length} sites from GA4 mappings`);
  }
 
  const results = { success: [], failed: [] };
 
  for (const siteUrl of sites) {
    try {
      const payload = await processSite(siteUrl);
 
      if (!payload) {
        results.failed.push({ site_url: siteUrl, error: 'No summary data' });
        continue;
      }
 
      if (DRY_RUN) {
        console.log(`  [DRY RUN] payload ready — skipping POST`);
        if (process.env.DRY_RUN_VERBOSE === 'true') {
          console.log(JSON.stringify(payload, null, 2));
        }
        results.success.push(siteUrl);
        continue;
      }
 
      // POST to Worker
      const postRes = await fetch(`${WORKER_BASE}/api/ingest-summaries`, {
        method:  'POST',
        headers: {
          'Content-Type':    'application/json',
          'x-ingest-secret': INGEST_SECRET
        },
        body: JSON.stringify(payload)
      });
 
      const postBody = await postRes.json();
      if (!postBody.ok) throw new Error(postBody.error || `HTTP ${postRes.status}`);
 
      console.log(`  ✓ written → risks=${postBody.risks_written} opp=${postBody.opportunities_written} actions=${postBody.actions_written} changes=${postBody.changes_written}`);
      results.success.push(siteUrl);
 
    } catch (err) {
      console.error(`  ✗ ${siteUrl}: ${err.message}`);
      results.failed.push({ site_url: siteUrl, error: err.message });
    }
 
    // Polite pause between sites
    await new Promise(r => setTimeout(r, 300));
  }
 
  // Summary
  console.log('\n=== Results ===');
  console.log(`Success: ${results.success.length} / ${sites.length}`);
  if (results.failed.length) {
    console.error(`\nFailed sites:`);
    results.failed.forEach(f => console.error(`  ${f.site_url} — ${f.error}`));
    process.exit(1);
  }
}
 
main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
