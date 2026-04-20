#!/usr/bin/env node

const WORKER_BASE   = (process.env.SHADOW_WORKER_BASE || '').replace(/\/$/, '');
const INGEST_SECRET = process.env.SHADOW_INGEST_SECRET;
const DRY_RUN       = process.env.DRY_RUN === 'true';
const TARGET_SITE   = process.env.SITE_URL || null;

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
// Accepts 0-1 floats (PSI native) or 0-100 integers — always returns 0-100
function normalizeScore(val) {
  if (val == null || isNaN(val)) return null;
  const n = Number(val);
  return n > 1 ? Math.round(n) : Math.round(n * 100);
}

function avg(arr) {
  if (!arr.length) return null;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

// Pull desktop or mobile strategy block from pagespeed section of site-summary
function getStrategyScores(pagespeed, strategy) {
  if (!pagespeed) return null;
  const s = pagespeed[strategy]; // pagespeed.desktop / pagespeed.mobile
  if (!s) return null;
  return {
    performance:    normalizeScore(s.performance    ?? s.scores?.performance),
    seo:            normalizeScore(s.seo            ?? s.scores?.seo),
    accessibility:  normalizeScore(s.accessibility  ?? s.scores?.accessibility),
    best_practices: normalizeScore(s.best_practices ?? s.scores?.best_practices)
  };
}

// ---------------------------------------------------------------------------
// Health score computation — all return 0-100 integer or null
// ---------------------------------------------------------------------------

// Technical: performance 50% + SEO 30% + accessibility 20% (avg desktop+mobile)
function computeTechnicalHealth(pagespeed) {
  const desktop = getStrategyScores(pagespeed, 'desktop');
  const mobile  = getStrategyScores(pagespeed, 'mobile');

  const perfScores = [desktop?.performance, mobile?.performance].filter(v => v != null);
  const seoScores  = [desktop?.seo,         mobile?.seo        ].filter(v => v != null);
  const a11yScores = [desktop?.accessibility, mobile?.accessibility].filter(v => v != null);

  if (!perfScores.length) return null;

  const avgPerf = avg(perfScores);
  const avgSeo  = seoScores.length  ? avg(seoScores)  : avgPerf;
  const avgA11y = a11yScores.length ? avg(a11yScores) : avgPerf;

  return Math.round(avgPerf * 0.5 + avgSeo * 0.3 + avgA11y * 0.2);
}

// Search performance: CTR quality (60pts) + impression volume (40pts)
// Benchmark: 5% CTR = 60pts, 100k impressions = 40pts
function computeSearchPerformanceHealth(gsc) {
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
// Benchmark: 70% engagement = 60pts, 10k sessions = 40pts
function computeTrafficHealth(ga4) {
  const totals = ga4?.totals;
  if (!totals) return null;
  const { sessions, engagement_rate, bounce_rate } = totals;
  if (sessions == null) return null;

  let engRatio = engagement_rate;
  if (engRatio == null && bounce_rate != null) engRatio = 1 - bounce_rate;

  const engScore = engRatio != null
    ? Math.min((engRatio / 0.70) * 60, 60)
    : 30;
  const volScore = sessions > 0
    ? Math.min((Math.log10(Math.max(sessions, 1)) / Math.log10(10000)) * 40, 40)
    : 0;

  return Math.max(0, Math.round(engScore + volScore));
}

// Indexing health: placeholder — will improve when IndexNow data available
// For now: derived from PageSpeed SEO score as best proxy
function computeIndexingHealth(pagespeed) {
  const desktop = getStrategyScores(pagespeed, 'desktop');
  const mobile  = getStrategyScores(pagespeed, 'mobile');
  const seoScores = [desktop?.seo, mobile?.seo].filter(v => v != null);
  return seoScores.length ? Math.round(avg(seoScores)) : null;
}

// Trend signal: placeholder — will improve when Trends data available
// For now: null (no data source yet)
function computeTrendSignal() {
  return null;
}

// Summary text — one sentence overview
function buildSummaryText(health, risks, opportunities) {
  const score = health.overall_health_score;
  const riskCount = risks.length;
  const oppCount  = opportunities.length;
  const level = score == null ? 'unknown' : score >= 75 ? 'good' : score >= 50 ? 'fair' : 'poor';

  let text = `Overall health is ${level}`;
  if (score != null) text += ` (${score}/100)`;
  if (riskCount > 0) text += ` with ${riskCount} risk${riskCount > 1 ? 's' : ''} flagged`;
  if (oppCount  > 0) text += ` and ${oppCount} improvement opportunit${oppCount > 1 ? 'ies' : 'y'} identified`;
  text += '.';
  return text;
}

// ---------------------------------------------------------------------------
// Risk flags — output matches site_risk_flags schema
// ---------------------------------------------------------------------------
function computeRisks(summary, asOfDate) {
  const risks = [];
  const { pagespeed, gsc, ga4, freshness } = summary;

  const desktop = getStrategyScores(pagespeed, 'desktop');
  const mobile  = getStrategyScores(pagespeed, 'mobile');

  // Low desktop performance
  if (desktop?.performance != null && desktop.performance < 50) {
    risks.push({
      risk_type:    'low_performance_desktop',
      severity:     desktop.performance < 25 ? 'critical' : 'high',
      title:        `Desktop performance score is ${desktop.performance}`,
      description:  `Desktop PageSpeed performance score of ${desktop.performance} is below the 50-point threshold, indicating significant loading or rendering issues for desktop users.`,
      evidence_json: JSON.stringify({ score: desktop.performance, threshold: 50, source: 'pagespeed' })
    });
  }

  // Low mobile performance
  if (mobile?.performance != null && mobile.performance < 50) {
    risks.push({
      risk_type:    'low_performance_mobile',
      severity:     mobile.performance < 25 ? 'critical' : 'high',
      title:        `Mobile performance score is ${mobile.performance}`,
      description:  `Mobile PageSpeed performance score of ${mobile.performance} is below the 50-point threshold. Mobile performance directly impacts Core Web Vitals ranking signals.`,
      evidence_json: JSON.stringify({ score: mobile.performance, threshold: 50, source: 'pagespeed' })
    });
  }

  // Low GSC CTR with meaningful impressions
  if (gsc?.totals) {
    const { ctr, impressions, clicks } = gsc.totals;
    if (ctr != null && impressions > 1000 && ctr < 0.01) {
      risks.push({
        risk_type:    'low_gsc_ctr',
        severity:     'medium',
        title:        `Search CTR is ${(ctr * 100).toFixed(2)}% across ${(impressions || 0).toLocaleString()} impressions`,
        description:  `The site is appearing in search results frequently but not attracting clicks. A CTR below 1% with this impression volume suggests title tags and meta descriptions need optimisation.`,
        evidence_json: JSON.stringify({ ctr, impressions, clicks, threshold_ctr: 0.01, source: 'gsc' })
      });
    }
  }

  // High bounce rate
  if (ga4?.totals) {
    const { bounce_rate, sessions } = ga4.totals;
    if (bounce_rate != null && bounce_rate > 0.70 && (sessions ?? 0) > 100) {
      risks.push({
        risk_type:    'high_bounce_rate',
        severity:     bounce_rate > 0.85 ? 'high' : 'medium',
        title:        `Bounce rate is ${(bounce_rate * 100).toFixed(1)}%`,
        description:  `${(bounce_rate * 100).toFixed(1)}% of sessions end without engagement. This may indicate a mismatch between search intent and landing page content, or page speed issues driving early exits.`,
        evidence_json: JSON.stringify({ bounce_rate, sessions, threshold: 0.70, source: 'ga4' })
      });
    }
  }

  // Stale data
  const now         = Date.now();
  const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
  const freshnessMap = [
    { key: 'pagespeed_last_updated_at', label: 'PageSpeed', source: 'pagespeed' },
    { key: 'gsc_last_updated_at',       label: 'GSC',       source: 'gsc'       },
    { key: 'ga4_last_updated_at',       label: 'GA4',       source: 'ga4'       }
  ];

  for (const { key, label, source } of freshnessMap) {
    const ts = freshness?.[key];
    if (!ts) continue;
    const ageMs = now - new Date(ts).getTime();
    if (ageMs > sevenDaysMs) {
      const days = Math.round(ageMs / (24 * 60 * 60 * 1000));
      risks.push({
        risk_type:    'stale_data',
        severity:     days > 14 ? 'high' : 'medium',
        title:        `${label} data is ${days} days old`,
        description:  `${label} data was last updated ${days} days ago. Data older than 7 days reduces confidence in health scores and recommendations.`,
        evidence_json: JSON.stringify({ days_old: days, last_updated: ts, threshold_days: 7, source })
      });
    }
  }

  return risks;
}

// ---------------------------------------------------------------------------
// Opportunity flags — output matches site_opportunity_flags schema
// ---------------------------------------------------------------------------
function computeOpportunities(summary) {
  const opportunities = [];
  const { pagespeed, gsc, ga4 } = summary;

  // High impressions / low CTR queries (top 3)
  if (Array.isArray(gsc?.top_queries)) {
    const lowCtr = gsc.top_queries
      .filter(q => q.impressions > 1000 && q.ctr < 0.02)
      .slice(0, 3);

    for (const q of lowCtr) {
      opportunities.push({
        opportunity_type: 'high_impressions_low_ctr',
        impact_level:     q.impressions > 10000 ? 'high' : 'medium',
        title:            `"${q.query}" — ${(q.impressions).toLocaleString()} impressions, ${(q.ctr * 100).toFixed(1)}% CTR`,
        description:      `This query generates significant search impressions but has a low click-through rate. Improving the title tag or meta description for the ranking page could increase organic clicks without needing to improve rankings.`,
        evidence_json:    JSON.stringify({ query: q.query, impressions: q.impressions, clicks: q.clicks, ctr: q.ctr, source: 'gsc' })
      });
    }
  }

  // Low accessibility score
  const desktop = getStrategyScores(pagespeed, 'desktop');
  if (desktop?.accessibility != null && desktop.accessibility < 80) {
    opportunities.push({
      opportunity_type: 'low_accessibility',
      impact_level:     'medium',
      title:            `Desktop accessibility score is ${desktop.accessibility}`,
      description:      `Accessibility improvements can expand audience reach, reduce legal risk, and positively influence search ranking signals. Scores below 80 typically have actionable quick wins.`,
      evidence_json:    JSON.stringify({ score: desktop.accessibility, threshold: 80, source: 'pagespeed' })
    });
  }

  // Low SEO score
  if (desktop?.seo != null && desktop.seo < 80) {
    opportunities.push({
      opportunity_type: 'low_seo_score',
      impact_level:     'high',
      title:            `Desktop SEO score is ${desktop.seo}`,
      description:      `PageSpeed SEO audit identifies technical on-page issues. Scores below 80 typically include missing meta tags, crawlability issues, or structured data gaps that directly affect search ranking.`,
      evidence_json:    JSON.stringify({ score: desktop.seo, threshold: 80, source: 'pagespeed' })
    });
  }

  // High-traffic pages with low engagement (top 2)
  if (Array.isArray(ga4?.top_pages)) {
    const lowEng = ga4.top_pages
      .filter(p => (p.sessions ?? 0) > 200 && p.engagement_rate != null && p.engagement_rate < 0.40)
      .slice(0, 2);

    for (const p of lowEng) {
      opportunities.push({
        opportunity_type: 'high_traffic_low_engagement',
        impact_level:     (p.sessions ?? 0) > 1000 ? 'high' : 'medium',
        title:            `${p.landing_page} — ${(p.sessions ?? 0).toLocaleString()} sessions, ${(p.engagement_rate * 100).toFixed(0)}% engagement`,
        description:      `This landing page attracts significant traffic but has low engagement. Content improvements, clearer calls to action, or better intent alignment could increase conversions without needing more traffic.`,
        evidence_json:    JSON.stringify({ page: p.landing_page, sessions: p.sessions, engagement_rate: p.engagement_rate, threshold: 0.40, source: 'ga4' })
      });
    }
  }

  return opportunities;
}


const SEVERITY_SCORE = { critical: 40, high: 30, medium: 20, low: 10 };
const IMPACT_SCORE   = { high: 28, medium: 18, low: 8 };

const ACTION_META = {
  // Risks
  low_performance_desktop:    { type: 'improve_page_speed',    owner: 'developer',  due: 'this sprint',  level_map: { critical: 'critical', high: 'high', medium: 'high' } },
  low_performance_mobile:     { type: 'improve_mobile_speed',  owner: 'developer',  due: 'this sprint',  level_map: { critical: 'critical', high: 'high' } },
  low_gsc_ctr:                { type: 'optimise_meta_content', owner: 'content',    due: 'this week',    level_map: { medium: 'medium' } },
  high_bounce_rate:           { type: 'improve_landing_pages', owner: 'content',    due: 'this week',    level_map: { high: 'high', medium: 'medium' } },
  stale_data:                 { type: 'fix_data_pipeline',     owner: 'developer',  due: 'today',        level_map: { high: 'high', medium: 'medium' } },
  // Opportunities
  high_impressions_low_ctr:   { type: 'optimise_meta_content', owner: 'content',    due: 'this week',    level_map: { high: 'high', medium: 'medium' } },
  low_accessibility:          { type: 'fix_accessibility',     owner: 'developer',  due: 'next sprint',  level_map: { medium: 'medium' } },
  low_seo_score:              { type: 'fix_technical_seo',     owner: 'developer',  due: 'this sprint',  level_map: { high: 'high', medium: 'medium' } },
  high_traffic_low_engagement:{ type: 'improve_landing_pages', owner: 'content',    due: 'this week',    level_map: { high: 'high', medium: 'medium' } }
};

function computePriorityActions(risks, opportunities) {
  const candidates = [];

  for (const r of risks) {
    const meta = ACTION_META[r.risk_type] || {};
    candidates.push({
      score:         SEVERITY_SCORE[r.severity] ?? 10,
      action_type:   meta.type    || r.risk_type,
      priority_level: meta.level_map?.[r.severity] || r.severity,
      title:         r.title,
      rationale:     r.description,
      owner_hint:    meta.owner   || null,
      due_hint:      meta.due     || null,
      recommended_steps_json: buildStepsJson(r.risk_type, 'risk'),
      status:        'open'
    });
  }

  for (const o of opportunities) {
    const meta = ACTION_META[o.opportunity_type] || {};
    candidates.push({
      score:         IMPACT_SCORE[o.impact_level] ?? 8,
      action_type:   meta.type    || o.opportunity_type,
      priority_level: o.impact_level,
      title:         o.title,
      rationale:     o.description,
      owner_hint:    meta.owner   || null,
      due_hint:      meta.due     || null,
      recommended_steps_json: buildStepsJson(o.opportunity_type, 'opportunity'),
      status:        'open'
    });
  }

  return candidates
    .sort((a, b) => b.score - a.score)
    .slice(0, 3)
    .map((a, i) => ({ ...a, action_rank: i + 1 }));
}

function buildStepsJson(flagType, category) {
  const steps = {
    low_performance_desktop:     ['Run PageSpeed audit', 'Identify largest contentful paint blockers', 'Compress images and defer non-critical JS', 'Retest after changes'],
    low_performance_mobile:      ['Run mobile PageSpeed audit', 'Check viewport configuration', 'Optimise images for mobile', 'Review render-blocking resources'],
    low_gsc_ctr:                 ['Export low-CTR pages from Search Console', 'Review title tags and meta descriptions', 'Test new copy variants', 'Monitor CTR changes over 2 weeks'],
    high_bounce_rate:            ['Identify top bounce pages in GA4', 'Review landing page content vs search intent', 'Improve page load speed', 'Add clear calls to action'],
    stale_data:                  ['Check GitHub Actions workflow logs', 'Verify API credentials are valid', 'Re-run failed ingestion job', 'Confirm data appears in D1'],
    high_impressions_low_ctr:    ['Open Search Console for this query', 'Identify which page ranks for the query', 'Rewrite title tag to be more compelling', 'Consider adding a structured data snippet'],
    low_accessibility:           ['Run Lighthouse accessibility audit', 'Fix contrast ratio issues', 'Add alt text to images', 'Ensure keyboard navigation works'],
    low_seo_score:               ['Run Lighthouse SEO audit', 'Fix missing meta descriptions', 'Resolve crawlability issues', 'Add canonical tags where missing'],
    high_traffic_low_engagement: ['Review heatmaps for this page', 'Check bounce rate in GA4', 'Improve above-the-fold content', 'Add internal links to related content']
  };
  const s = steps[flagType] || ['Review relevant metrics', 'Identify root cause', 'Implement fix', 'Monitor results'];
  return JSON.stringify(s);
}

function computeChanges(ga4) {
  const daily = ga4?.daily;
  if (!Array.isArray(daily) || daily.length < 14) return [];

  const sorted = [...daily].sort((a, b) => (a.date > b.date ? 1 : -1));
  const half   = Math.floor(sorted.length / 2);
  const prev   = sorted.slice(0, half);
  const curr   = sorted.slice(sorted.length - half);

  const sumField = (rows, field) => rows.reduce((acc, r) => acc + (Number(r[field]) || 0), 0);

  const metricDefs = [
    { field: 'sessions',         label: 'Sessions',         change_type: 'traffic_change'    },
    { field: 'active_users',     label: 'Active users',     change_type: 'users_change'      },
    { field: 'engaged_sessions', label: 'Engaged sessions', change_type: 'engagement_change' }
  ];

  const changes = [];

  for (const { field, label, change_type } of metricDefs) {
    const prevVal = sumField(prev, field);
    const currVal = sumField(curr, field);
    if (prevVal === 0) continue;

    const deltaPct = ((currVal - prevVal) / prevVal) * 100;
    if (Math.abs(deltaPct) < 20) continue;

    const direction     = deltaPct > 0 ? 'up' : 'down';
    const magnitudeScore = Math.min(Math.abs(deltaPct) / 100, 1); // 0-1 scale
    const absPct        = Math.abs(Math.round(deltaPct * 10) / 10);

    changes.push({
      change_type,
      direction,
      magnitude_score: Math.round(magnitudeScore * 100) / 100,
      title:           `${label} ${direction} ${absPct}% vs previous period`,
      description:     `${label} changed from ${prevVal.toLocaleString()} to ${currVal.toLocaleString()} (${direction === 'up' ? '+' : '-'}${absPct}%) comparing the two halves of the current data window.`,
      current_value:   currVal,
      previous_value:  prevVal,
      delta_pct:       Math.round(deltaPct * 10) / 10,
      evidence_json:   JSON.stringify({ field, prev_period_rows: prev.length, curr_period_rows: curr.length, source: 'ga4' })
    });
  }

  return changes;
}

// ---------------------------------------------------------------------------
// Per-site processing
// ---------------------------------------------------------------------------
async function processSite(siteUrl) {
  console.log(`\n--- ${siteUrl} ---`);

  // 1. Fetch merged site-summary
  let summary;
  try {
    const res = await fetchJSON(`${WORKER_BASE}/api/site-summary?site=${encodeURIComponent(siteUrl)}`);
    if (!res.ok) { console.warn(`  site-summary error: ${res.error}`); return null; }
    summary = res;
  } catch (err) {
    console.error(`  site-summary fetch failed: ${err.message}`);
    return null;
  }

  // 2. Fetch GA4 daily data for change detection
  try {
    const ga4Res = await fetchJSON(`${WORKER_BASE}/api/ga4?site=${encodeURIComponent(siteUrl)}`);
    if (ga4Res.ok && Array.isArray(ga4Res.daily)) {
      summary.ga4        = summary.ga4 || {};
      summary.ga4.daily  = ga4Res.daily;
    }
  } catch (_) { /* optional */ }

  const asOfDate = new Date().toISOString();

  // 3. Compute health scores
  const technicalHealth  = computeTechnicalHealth(summary.pagespeed);
  const searchHealth     = computeSearchPerformanceHealth(summary.gsc);
  const trafficHealth    = computeTrafficHealth(summary.ga4);
  const indexingHealth   = computeIndexingHealth(summary.pagespeed);
  const trendSignal      = computeTrendSignal();

  const nonNull          = [technicalHealth, searchHealth, trafficHealth, indexingHealth].filter(v => v != null);
  const overallHealth    = nonNull.length
    ? Math.round(nonNull.reduce((a, b) => a + b, 0) / nonNull.length)
    : null;

  console.log(`  Health → technical=${technicalHealth ?? 'n/a'} search=${searchHealth ?? 'n/a'} traffic=${trafficHealth ?? 'n/a'} indexing=${indexingHealth ?? 'n/a'} overall=${overallHealth ?? 'n/a'}`);

  // 4. Compute flags
  const risks           = computeRisks(summary, asOfDate);
  const opportunities   = computeOpportunities(summary);
  const priorityActions = computePriorityActions(risks, opportunities);
  const changes         = computeChanges(summary.ga4);

  console.log(`  Flags → risks=${risks.length} opp=${opportunities.length} actions=${priorityActions.length} changes=${changes.length}`);
  risks.forEach(r          => console.log(`    RISK [${r.severity}] ${r.risk_type}`));
  opportunities.forEach(o  => console.log(`    OPP  [${o.impact_level}] ${o.opportunity_type}`));
  priorityActions.forEach(a => console.log(`    ACT  #${a.action_rank} ${a.action_type} [${a.priority_level}]`));
  changes.forEach(c        => console.log(`    CHG  ${c.change_type} ${c.direction} ${c.delta_pct}%`));

  // 5. Build summary text
  const health = {
    technical_health_score:          technicalHealth,
    search_performance_health_score: searchHealth,
    traffic_health_score:            trafficHealth,
    indexing_health_score:           indexingHealth,
    trend_signal_score:              trendSignal,
    overall_health_score:            overallHealth,
    summary_text:                    null,
    supporting_metrics_json:         null
  };
  health.summary_text = buildSummaryText(health, risks, opportunities);
  health.supporting_metrics_json = JSON.stringify({
    pagespeed: {
      desktop_perf: getStrategyScores(summary.pagespeed, 'desktop')?.performance,
      mobile_perf:  getStrategyScores(summary.pagespeed, 'mobile')?.performance
    },
    gsc: {
      clicks:      summary.gsc?.totals?.clicks,
      impressions: summary.gsc?.totals?.impressions,
      ctr:         summary.gsc?.totals?.ctr
    },
    ga4: {
      sessions:        summary.ga4?.totals?.sessions,
      engagement_rate: summary.ga4?.totals?.engagement_rate,
      bounce_rate:     summary.ga4?.totals?.bounce_rate
    }
  });

  return {
    site_url:   siteUrl,
    as_of_date: asOfDate,
    health,
    risks,
    opportunities,
    priority_actions: priorityActions,
    changes
  };
}

async function main() {
  console.log('=== compute-summaries.js v2 ===');
  console.log(`DRY_RUN:     ${DRY_RUN}`);
  console.log(`TARGET_SITE: ${TARGET_SITE || 'all sites'}`);
  console.log(`WORKER_BASE: ${WORKER_BASE}\n`);

  let sites;
  if (TARGET_SITE) {
    sites = [TARGET_SITE.replace(/\/$/, '')];
  } else {
    const mappingsRes = await fetch(`${WORKER_BASE}/api/site-source-mappings?source=ga4`, {
      headers: { 'x-ingest-secret': INGEST_SECRET }
    });
    const res = await mappingsRes.json();
    if (!res.ok) throw new Error(`Failed to fetch mappings: ${res.error}`);
    sites = (res.sites || []).map(s => s.site_url.replace(/\/$/, ''));
    console.log(`Resolved ${sites.length} sites from GA4 mappings`);
  }

  const results = { success: [], failed: [] };

  for (const siteUrl of sites) {
    try {
      const payload = await processSite(siteUrl);

      if (!payload) {
        results.failed.push({ site_url: siteUrl, error: 'No summary data returned' });
        continue;
      }

      if (DRY_RUN) {
        console.log(`  [DRY RUN] payload ready, skipping POST`);
        if (process.env.DRY_RUN_VERBOSE === 'true') console.log(JSON.stringify(payload, null, 2));
        results.success.push(siteUrl);
        continue;
      }

      const postRes = await fetch(`${WORKER_BASE}/api/ingest-summaries`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'x-ingest-secret': INGEST_SECRET },
        body:    JSON.stringify(payload)
      });

      const postBody = await postRes.json();
      if (!postBody.ok) throw new Error(postBody.error || `HTTP ${postRes.status}`);

      console.log(`  ✓ risks=${postBody.risks_written} opp=${postBody.opportunities_written} actions=${postBody.actions_written} changes=${postBody.changes_written}`);
      results.success.push(siteUrl);

    } catch (err) {
      console.error(`  ✗ ${siteUrl}: ${err.message}`);
      results.failed.push({ site_url: siteUrl, error: err.message });
    }

    await new Promise(r => setTimeout(r, 300));
  }

  console.log(`\n=== Results: ${results.success.length} success / ${results.failed.length} failed ===`);
  if (results.failed.length) {
    results.failed.forEach(f => console.error(`  FAILED: ${f.site_url} — ${f.error}`));
    process.exit(1);
  }
}

main().catch(err => { console.error('Fatal:', err.message); process.exit(1); });
