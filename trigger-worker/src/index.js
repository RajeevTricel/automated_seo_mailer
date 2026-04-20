// trigger-worker/src/index.js

export default {
  async fetch(request, env) {
    const corsHeaders = buildCorsHeaders(request, env);

    try {
      if (request.method === 'OPTIONS') {
        return new Response(null, {
          status: 204,
          headers: corsHeaders
        });
      }

      const url = new URL(request.url);

      if (url.pathname === '/trigger') {
        return await handleTrigger(request, env, corsHeaders);
      }

      if (url.pathname === '/api/ingest-run') {
        return await handleIngestRun(request, env, corsHeaders);
      }

      if (url.pathname === '/api/ingest-gsc') {
        return await handleIngestGsc(request, env, corsHeaders);
      }

      if (url.pathname === '/api/latest-run') {
        return await handleLatestRun(request, env, corsHeaders);
      }

      if (url.pathname === '/api/sites') {
        return await handleSites(request, env, corsHeaders);
      }

      if (url.pathname === '/api/site-overview') {
        return await handleSiteOverview(request, env, corsHeaders);
      }

      if (url.pathname === '/api/site-pagespeed') {
        return await handleSitePagespeed(request, env, corsHeaders);
      }

      if (url.pathname === '/api/site-extractions') {
        return await handleSiteExtractions(request, env, corsHeaders);
      }
      if (url.pathname === '/api/site-source-mappings') {
        return await handleSiteSourceMappings(request, env, corsHeaders);
      }
      if (url.pathname === '/api/gsc') {
        return await handleSiteGsc(request, env, corsHeaders);
      }
      if (url.pathname === '/api/site-summary') {
        return await handleSiteSummary(request, env, corsHeaders);
      }
      if (url.pathname === '/api/ingest-ga4' && request.method === 'POST') return await handleIngestGa4(request, env, corsHeaders);
      if (url.pathname === '/api/ga4' && request.method === 'GET') return await handleSiteGa4(request, env, corsHeaders);

      return json({ ok: false, message: 'Not found' }, 404, corsHeaders);
    } catch (error) {
      return json(
        {
          ok: false,
          message: 'Worker exception',
          error: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : null
        },
        500,
        corsHeaders
      );
    }
  }
};

async function handleTrigger(request, env, corsHeaders) {
  if (request.method !== 'POST') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  const origin = request.headers.get('Origin') || '';

  if (env.ALLOWED_ORIGIN && env.ALLOWED_ORIGIN !== '*' && origin !== env.ALLOWED_ORIGIN) {
    return json({ ok: false, message: 'Origin not allowed' }, 403, corsHeaders);
  }

  if (env.ADMIN_KEY) {
    const providedKey = request.headers.get('x-admin-key') || '';

    if (!providedKey || providedKey !== env.ADMIN_KEY) {
      return json({ ok: false, message: 'Invalid admin key' }, 401, corsHeaders);
    }
  }

  if (!env.GITHUB_TOKEN) {
    return json({ ok: false, message: 'Missing GITHUB_TOKEN secret' }, 500, corsHeaders);
  }

  const workflowId = env.GITHUB_WORKFLOW_ID || 'daily_report.yml';
  const ref = env.GITHUB_REF || 'main';

  const githubResponse = await fetch(
    `https://api.github.com/repos/${encodeURIComponent(env.GITHUB_OWNER)}/${encodeURIComponent(env.GITHUB_REPO)}/actions/workflows/${encodeURIComponent(workflowId)}/dispatches`,
    {
      method: 'POST',
      headers: {
        Accept: 'application/vnd.github+json',
        Authorization: `Bearer ${env.GITHUB_TOKEN}`,
        'Content-Type': 'application/json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'tricel-report-trigger'
      },
      body: JSON.stringify({
        ref,
        inputs: {
          send_email: 'false'
        }
      })
    }
  );

  if (githubResponse.status === 204) {
    return json(
      {
        ok: true,
        message: 'Fresh check queued successfully. Email will be skipped for this refresh.'
      },
      200,
      corsHeaders
    );
  }

  const errorText = await githubResponse.text();

  return json(
    { ok: false, message: `GitHub API ${githubResponse.status}: ${errorText}` },
    githubResponse.status,
    corsHeaders
  );
}

async function handleSiteSummary(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const url = new URL(request.url);
  const site = asNullableString(url.searchParams.get('site'));

  if (!site) {
    return json({ ok: false, message: 'Missing required query param: site' }, 400, corsHeaders);
  }

  // Step 1: get current PageSpeed run
  const run = await getCurrentOrLatestSuccessfulRun(env.DB);

  // Step 2: batch - PageSpeed scores + GSC snapshots + freshness in one round-trip
  const [psResult, qSnapResult, pSnapResult, dSnapResult, freshnessResult] =
    await env.DB.batch([
      env.DB.prepare(
        `SELECT strategy, performance_score, accessibility_score, best_practices_score, seo_score, created_at
         FROM site_results
         WHERE run_id = ? AND site_url = ?
         ORDER BY CASE strategy WHEN 'desktop' THEN 1 WHEN 'mobile' THEN 2 ELSE 99 END`
      ).bind(run ? run.id : '', site),
      env.DB.prepare(
        `SELECT id, captured_at, period_start, period_end, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'query_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT id, captured_at, period_start, period_end, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'page_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT id, captured_at, period_start, period_end, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'device_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT pagespeed_last_updated_at, gsc_last_updated_at, ga4_last_updated_at, overall_freshness_status, freshness_confidence_score, updated_at
         FROM site_freshness_summaries
         WHERE site_url = ? LIMIT 1`
      ).bind(site),
    ]);

  const psRows     = Array.isArray(psResult?.results)      ? psResult.results      : [];
  const querySnap  = qSnapResult?.results?.[0]             || null;
  const pageSnap   = pSnapResult?.results?.[0]             || null;
  const deviceSnap = dSnapResult?.results?.[0]             || null;
  const freshness  = freshnessResult?.results?.[0]         || null;

  // Build PageSpeed payload
  const pagespeedStrategies = {};
  for (const row of psRows) {
    pagespeedStrategies[row.strategy] = {
      scores: {
        performance:    row.performance_score,
        accessibility:  row.accessibility_score,
        best_practices: row.best_practices_score,
        seo:            row.seo_score
      },
      captured_at: row.created_at
    };
  }
  const ga4Snap = await env.DB.prepare(
    `SELECT module, captured_at, period_start, period_end
     FROM source_snapshots
     WHERE site_url = ? AND source = 'ga4' AND module = 'site_metrics'
     ORDER BY captured_at DESC LIMIT 1`
  ).bind(site).first();
  
  let ga4Totals = null;
  let ga4TopPages = [];
  
  if (ga4Snap) {
    ga4Totals = await env.DB.prepare(
      `SELECT
         SUM(sessions) as total_sessions,
         SUM(active_users) as total_active_users,
         SUM(engaged_sessions) as total_engaged_sessions,
         ROUND(AVG(bounce_rate), 4) as avg_bounce_rate,
         ROUND(AVG(engagement_rate), 4) as avg_engagement_rate,
         ROUND(AVG(average_session_duration), 2) as avg_session_duration
       FROM ga4_site_metrics
       WHERE site_url = ? AND period_start = ? AND period_end = ?`
    ).bind(site, ga4Snap.period_start, ga4Snap.period_end).first();
  
    const ga4Pages = await env.DB.prepare(
      `SELECT landing_page, SUM(sessions) as sessions, SUM(active_users) as active_users
       FROM ga4_landing_page_metrics
       WHERE site_url = ? AND period_start = ? AND period_end = ?
       GROUP BY landing_page ORDER BY sessions DESC LIMIT 5`
    ).bind(site, ga4Snap.period_start, ga4Snap.period_end).all();
  
    ga4TopPages = ga4Pages.results || [];
  }
  // Step 3: if GSC snapshots exist, fetch summarised metric rows
  let gscSummary = null;
  const refSnap = querySnap || pageSnap || deviceSnap;

  if (refSnap) {
    const periodStart = refSnap.period_start;
    const periodEnd   = refSnap.period_end;

    const [topQueriesResult, topPagesResult, devicesResult] = await env.DB.batch([
      env.DB.prepare(
        `SELECT query,
                SUM(clicks)      AS clicks,
                SUM(impressions) AS impressions,
                ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
                ROUND(AVG(position), 1) AS position
         FROM gsc_query_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY query
         ORDER BY impressions DESC
         LIMIT 5`
      ).bind(site, periodStart, periodEnd),
      env.DB.prepare(
        `SELECT page,
                SUM(clicks)      AS clicks,
                SUM(impressions) AS impressions,
                ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
                ROUND(AVG(position), 1) AS position
         FROM gsc_page_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY page
         ORDER BY impressions DESC
         LIMIT 5`
      ).bind(site, periodStart, periodEnd),
      env.DB.prepare(
        `SELECT device,
                SUM(clicks)      AS clicks,
                SUM(impressions) AS impressions,
                ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
                ROUND(AVG(position), 1) AS position
         FROM gsc_device_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY device
         ORDER BY impressions DESC`
      ).bind(site, periodStart, periodEnd)
    ]);

    const topQueries = Array.isArray(topQueriesResult?.results) ? topQueriesResult.results : [];
    const topPages   = Array.isArray(topPagesResult?.results)   ? topPagesResult.results   : [];
    const devices    = Array.isArray(devicesResult?.results)    ? devicesResult.results    : [];

    const totalClicks      = devices.reduce((sum, d) => sum + (d.clicks || 0), 0);
    const totalImpressions = devices.reduce((sum, d) => sum + (d.impressions || 0), 0);

    gscSummary = {
      period:      { start: periodStart, end: periodEnd },
      captured_at: refSnap.captured_at,
      totals: {
        clicks:      totalClicks,
        impressions: totalImpressions,
        ctr:         totalImpressions > 0
                       ? Math.round((totalClicks / totalImpressions) * 10000) / 10000
                       : 0
      },
      top_queries: topQueries,
      top_pages:   topPages,
      devices
    };
  }

  return json(
    {
      ok: true,
      site,
      freshness: freshness
        ? {
            pagespeed_last_updated_at:  freshness.pagespeed_last_updated_at,
            gsc_last_updated_at:        freshness.gsc_last_updated_at,
            ga4_last_updated_at:        freshness.ga4_last_updated_at, 
            overall_freshness_status:   freshness.overall_freshness_status,
            freshness_confidence_score: freshness.freshness_confidence_score,
            updated_at:                 freshness.updated_at
          }
        : null,
      pagespeed: run
        ? {
            run_id:                run.id,
            snapshot_generated_at: run.snapshot_generated_at,
            strategies:            pagespeedStrategies
          }
        : null,
      gsc: gscSummary,
      ga4: ga4Totals ? {
        period: { start: ga4Snap.period_start, end: ga4Snap.period_end },
        totals: ga4Totals,
        top_pages: ga4TopPages,
      } : null,
    },
    200,
    corsHeaders
  );
}


async function handleIngestGa4(request, env, corsHeaders) {
  if (!env.INGEST_SHARED_SECRET) {
    return json({ ok: false, message: 'Missing INGEST_SHARED_SECRET secret' }, 500, corsHeaders);
  }

  const providedSecret = request.headers.get('x-ingest-secret') || '';
  if (!providedSecret || providedSecret !== env.INGEST_SHARED_SECRET) {
    return json({ ok: false, message: 'Invalid ingest secret' }, 401, corsHeaders);
  }

  let body;
  try { body = await request.json(); }
  catch { return json({ error: 'invalid JSON' }, 400, corsHeaders); }

  const {
    site_url, ga4_property_id, captured_at,
    period_start, period_end,
    site_metrics = [], landing_page_metrics = [],
  } = body;

  if (!site_url || !ga4_property_id) {
    return json({ error: 'site_url and ga4_property_id required' }, 400, corsHeaders);
  }

  // Register snapshot
  const snapshotSite = await env.DB.prepare(
    `INSERT INTO source_snapshots
       (site_url, source, module, captured_at, period_start, period_end,
        import_mode, parse_status, normalized_row_count)
     VALUES (?, 'ga4', 'site_metrics', ?, ?, ?, 'api', 'success', ?)`
  ).bind(site_url, captured_at, period_start, period_end, site_metrics.length).run();

  const snapshotPage = await env.DB.prepare(
    `INSERT INTO source_snapshots
       (site_url, source, module, captured_at, period_start, period_end,
        import_mode, parse_status, normalized_row_count)
     VALUES (?, 'ga4', 'landing_page_metrics', ?, ?, ?, 'api', 'success', ?)`
  ).bind(site_url, captured_at, period_start, period_end, landing_page_metrics.length).run();

   // Insert site_metrics
  const siteBatches = [];
  for (const r of site_metrics) {
    siteBatches.push(
      env.DB.prepare(
        `INSERT INTO ga4_site_metrics
           (site_url, ga4_property_id, date, sessions, active_users,
            engaged_sessions, engagement_rate, bounce_rate,
            average_session_duration, captured_at, period_start, period_end)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(
        r.site_url, r.ga4_property_id, r.date,
        r.sessions != null ? r.sessions : null,
        r.active_users != null ? r.active_users : null,
        r.engaged_sessions != null ? r.engaged_sessions : null,
        r.engagement_rate != null ? r.engagement_rate : null,
        r.bounce_rate != null ? r.bounce_rate : null,
        r.average_session_duration != null ? r.average_session_duration : null,
        r.captured_at, r.period_start, r.period_end
      )
    );
  }
  await executeBatches(env.DB, siteBatches, 50);
  
  // Insert landing_page_metrics
  const pageBatches = [];
  for (const r of landing_page_metrics) {
    pageBatches.push(
      env.DB.prepare(
        `INSERT INTO ga4_landing_page_metrics
           (site_url, ga4_property_id, landing_page, sessions, active_users,
            engaged_sessions, bounce_rate, captured_at, period_start, period_end)
         VALUES (?,?,?,?,?,?,?,?,?,?)`
      ).bind(
        r.site_url, r.ga4_property_id, r.landing_page,
        r.sessions != null ? r.sessions : null,
        r.active_users != null ? r.active_users : null,
        r.engaged_sessions != null ? r.engaged_sessions : null,
        r.bounce_rate != null ? r.bounce_rate : null,
        r.captured_at, r.period_start, r.period_end
      )
    );
  }
  await executeBatches(env.DB, pageBatches, 50);

  // Update freshness
  await upsertGa4FreshnessSummary(env.DB, site_url, captured_at);

  return json({
    ok: true,
    site_url,
    site_metrics_inserted: site_metrics.length,
    landing_pages_inserted: landing_page_metrics.length,
  });
}

async function upsertGa4FreshnessSummary(db, siteUrl, capturedAt) {
  await db.prepare(
    `INSERT INTO site_freshness_summaries (site_url, ga4_last_updated_at, updated_at)
     VALUES (?, ?, datetime('now'))
     ON CONFLICT(site_url) DO UPDATE SET
       ga4_last_updated_at = excluded.ga4_last_updated_at,
       updated_at = datetime('now')`
  ).bind(siteUrl, capturedAt).run();
}

async function handleSiteGa4(request, env) {
  const url = new URL(request.url);
  const siteUrl = url.searchParams.get('site');
  if (!siteUrl) return json({ error: 'site param required' }, 400, corsHeaders);

  // Latest snapshot info
  const snapshots = await env.DB.prepare(
    `SELECT module, captured_at, period_start, period_end, normalized_row_count
     FROM source_snapshots
     WHERE site_url = ? AND source = 'ga4'
     ORDER BY captured_at DESC LIMIT 10`
  ).bind(siteUrl).all();

  // Freshness
  const freshness = await env.DB.prepare(
    `SELECT ga4_last_updated_at FROM site_freshness_summaries WHERE site_url = ?`
  ).bind(siteUrl).first();

  // Period from most recent site_metrics snapshot
  const latestSnap = (snapshots.results || []).find(s => s.module === 'site_metrics');
  const periodStart = latestSnap?.period_start;
  const periodEnd   = latestSnap?.period_end;

  // Totals
  const totals = await env.DB.prepare(
    `SELECT
       SUM(sessions) as total_sessions,
       SUM(active_users) as total_active_users,
       SUM(engaged_sessions) as total_engaged_sessions,
       AVG(bounce_rate) as avg_bounce_rate,
       AVG(engagement_rate) as avg_engagement_rate,
       AVG(average_session_duration) as avg_session_duration
     FROM ga4_site_metrics
     WHERE site_url = ? AND period_start = ? AND period_end = ?`
  ).bind(siteUrl, periodStart, periodEnd).first();

  // Top 20 landing pages by sessions
  const topPages = await env.DB.prepare(
    `SELECT landing_page, SUM(sessions) as sessions, SUM(active_users) as active_users, AVG(bounce_rate) as avg_bounce_rate
     FROM ga4_landing_page_metrics
     WHERE site_url = ? AND period_start = ? AND period_end = ?
     GROUP BY landing_page
     ORDER BY sessions DESC LIMIT 20`
  ).bind(siteUrl, periodStart, periodEnd).all();

  // Daily trend (last 28 rows)
  const dailyTrend = await env.DB.prepare(
    `SELECT date, SUM(sessions) as sessions, SUM(active_users) as active_users
     FROM ga4_site_metrics
     WHERE site_url = ? AND period_start = ? AND period_end = ?
     GROUP BY date ORDER BY date ASC`
  ).bind(siteUrl, periodStart, periodEnd).all();

  return json({
    site_url:    siteUrl,
    freshness:   freshness?.ga4_last_updated_at || null,
    period:      { start: periodStart, end: periodEnd },
    totals,
    top_landing_pages: topPages.results || [],
    daily_trend:       dailyTrend.results || [],
    snapshots:         snapshots.results || [],
  });
}

async function handleSiteGsc(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const url = new URL(request.url);
  const site = asNullableString(url.searchParams.get('site'));

  if (!site) {
    return json({ ok: false, message: 'Missing required query param: site' }, 400, corsHeaders);
  }

  // Batch 1: latest snapshot metadata per module + freshness summary
  const [qSnapResult, pSnapResult, cSnapResult, dSnapResult, freshnessResult] =
    await env.DB.batch([
      env.DB.prepare(
        `SELECT id, module, captured_at, period_start, period_end, normalized_row_count, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'query_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT id, module, captured_at, period_start, period_end, normalized_row_count, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'page_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT id, module, captured_at, period_start, period_end, normalized_row_count, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'country_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT id, module, captured_at, period_start, period_end, normalized_row_count, freshness_status
         FROM source_snapshots
         WHERE site_url = ? AND source = 'gsc' AND module = 'device_metrics'
         ORDER BY captured_at DESC LIMIT 1`
      ).bind(site),
      env.DB.prepare(
        `SELECT gsc_last_updated_at, overall_freshness_status, freshness_confidence_score, updated_at
         FROM site_freshness_summaries
         WHERE site_url = ? LIMIT 1`
      ).bind(site)
    ]);

  const querySnap   = qSnapResult?.results?.[0]    || null;
  const pageSnap    = pSnapResult?.results?.[0]    || null;
  const countrySnap = cSnapResult?.results?.[0]    || null;
  const deviceSnap  = dSnapResult?.results?.[0]    || null;
  const freshness   = freshnessResult?.results?.[0] || null;

  if (!querySnap && !pageSnap && !countrySnap && !deviceSnap) {
    return json({ ok: false, message: 'No GSC data found for site', site }, 404, corsHeaders);
  }

  // Use the most recent snapshot's period as the filter window
  const refSnap = querySnap || pageSnap || countrySnap || deviceSnap;
  const periodStart = refSnap.period_start;
  const periodEnd   = refSnap.period_end;

  // Batch 2: aggregated metric rows for that period
  // CTR recomputed from SUM(clicks)/SUM(impressions) to avoid averaging per-row CTRs
  const [topQueriesResult, topPagesResult, countriesResult, devicesResult] =
    await env.DB.batch([
      env.DB.prepare(
        `SELECT
           query,
           SUM(clicks)      AS clicks,
           SUM(impressions) AS impressions,
           ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
           ROUND(AVG(position), 1) AS position
         FROM gsc_query_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY query
         ORDER BY impressions DESC
         LIMIT 20`
      ).bind(site, periodStart, periodEnd),
      env.DB.prepare(
        `SELECT
           page,
           SUM(clicks)      AS clicks,
           SUM(impressions) AS impressions,
           ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
           ROUND(AVG(position), 1) AS position
         FROM gsc_page_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY page
         ORDER BY impressions DESC
         LIMIT 20`
      ).bind(site, periodStart, periodEnd),
      env.DB.prepare(
        `SELECT
           country,
           SUM(clicks)      AS clicks,
           SUM(impressions) AS impressions,
           ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
           ROUND(AVG(position), 1) AS position
         FROM gsc_country_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY country
         ORDER BY impressions DESC`
      ).bind(site, periodStart, periodEnd),
      env.DB.prepare(
        `SELECT
           device,
           SUM(clicks)      AS clicks,
           SUM(impressions) AS impressions,
           ROUND(CAST(SUM(clicks) AS REAL) / NULLIF(SUM(impressions), 0), 4) AS ctr,
           ROUND(AVG(position), 1) AS position
         FROM gsc_device_metrics
         WHERE site_url = ? AND date >= ? AND date <= ?
         GROUP BY device
         ORDER BY impressions DESC`
      ).bind(site, periodStart, periodEnd)
    ]);

  return json(
    {
      ok: true,
      site,
      period: {
        start: periodStart,
        end: periodEnd
      },
      snapshots: {
        query_metrics:   querySnap,
        page_metrics:    pageSnap,
        country_metrics: countrySnap,
        device_metrics:  deviceSnap
      },
      freshness: freshness
        ? {
            gsc_last_updated_at:        freshness.gsc_last_updated_at,
            overall_freshness_status:   freshness.overall_freshness_status,
            freshness_confidence_score: freshness.freshness_confidence_score,
            updated_at:                 freshness.updated_at
          }
        : null,
      top_queries: Array.isArray(topQueriesResult?.results) ? topQueriesResult.results : [],
      top_pages:   Array.isArray(topPagesResult?.results)   ? topPagesResult.results   : [],
      countries:   Array.isArray(countriesResult?.results)  ? countriesResult.results  : [],
      devices:     Array.isArray(devicesResult?.results)    ? devicesResult.results    : []
    },
    200,
    corsHeaders
  );
}

async function handleSiteSourceMappings(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const expectedSecret =
    asNullableString(env.SHADOW_INGEST_SECRET) ||
    asNullableString(env.INGEST_SHARED_SECRET);

  if (!expectedSecret) {
    return json(
      { ok: false, message: 'Missing GSC_INGEST_SHARED_SECRET or INGEST_SHARED_SECRET secret' },
      500,
      corsHeaders
    );
  }

  const providedSecret = request.headers.get('x-ingest-secret') || '';
  if (!providedSecret || providedSecret !== expectedSecret) {
    return json({ ok: false, message: 'Invalid ingest secret' }, 401, corsHeaders);
  }

  const url = new URL(request.url);
  const source = asNullableString(url.searchParams.get('source')) || 'all';

  let whereClause = `WHERE is_active = 1`;
  if (source === 'gsc') {
    whereClause += ` AND gsc_property IS NOT NULL AND TRIM(gsc_property) != ''`;
  } else if (source === 'ga4') {
    whereClause += ` AND ga4_property_id IS NOT NULL AND TRIM(ga4_property_id) != ''`;
  }

  const result = await env.DB.prepare(
    `
      SELECT
        id,
        site_url,
        display_name,
        group_name,
        gsc_property,
        ga4_property_id,
        trends_topic_label,
        indexnow_host_key,
        is_active,
        created_at,
        updated_at
      FROM site_source_mappings
      ${whereClause}
      ORDER BY group_name ASC, display_name ASC, site_url ASC
    `
  ).all();

  const sites = Array.isArray(result?.results) ? result.results : [];

  return json(
    {
      ok: true,
      source,
      count: sites.length,
      sites
    },
    200,
    corsHeaders
  );
}
async function handleLatestRun(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const run = await getCurrentOrLatestSuccessfulRun(env.DB);

  if (!run) {
    return json({ ok: false, message: 'No runs found' }, 404, corsHeaders);
  }

  return json(
    {
      ok: true,
      run
    },
    200,
    corsHeaders
  );
}

async function handleIngestRun(request, env, corsHeaders) {
  if (request.method !== 'POST') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  if (!env.INGEST_SHARED_SECRET) {
    return json({ ok: false, message: 'Missing INGEST_SHARED_SECRET secret' }, 500, corsHeaders);
  }

  const providedSecret = request.headers.get('x-ingest-secret') || '';
  if (!providedSecret || providedSecret !== env.INGEST_SHARED_SECRET) {
    return json({ ok: false, message: 'Invalid ingest secret' }, 401, corsHeaders);
  }

  let payload;
  try {
    payload = await request.json();
  } catch {
    return json({ ok: false, message: 'Invalid JSON body' }, 400, corsHeaders);
  }

  const snapshotEnvelope = isObject(payload?.snapshot) ? payload.snapshot : payload;
  const snapshot = isObject(snapshotEnvelope?.report) ? snapshotEnvelope.report : snapshotEnvelope;

  const createdAt = new Date().toISOString();
  const runId = buildRunId(payload);
  const source = asNonEmptyString(payload?.source) || 'shadow_site_intelligence';
  const triggerType = asNonEmptyString(payload?.trigger_type) || 'workflow_dispatch';
  const reportUrl =
    asNullableString(payload?.report_url) ||
    asNullableString(snapshotEnvelope?.report_url) ||
    asNullableString(snapshotEnvelope?.reportUrl) ||
    asNullableString(snapshot?.report_url) ||
    asNullableString(snapshot?.reportUrl);
  const snapshotGeneratedAt = extractSnapshotGeneratedAt(payload, snapshotEnvelope, snapshot);

  const existingRun = await env.DB.prepare('SELECT id, status FROM runs WHERE id = ?')
    .bind(runId)
    .first();

  if (existingRun) {
    return json(
      {
        ok: true,
        message: 'Run already ingested',
        run_id: existingRun.id,
        status: existingRun.status
      },
      200,
      corsHeaders
    );
  }

  await env.DB.prepare(
    `
      INSERT INTO runs (
        id,
        source,
        trigger_type,
        status,
        is_current,
        report_url,
        snapshot_generated_at,
        created_at,
        completed_at,
        site_count,
        strategy_count,
        raw_snapshot_json
      )
      VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, NULL, 0, 0, ?)
    `
  )
    .bind(
      runId,
      source,
      triggerType,
      reportUrl,
      snapshotGeneratedAt,
      createdAt,
      safeJsonStringify(snapshotEnvelope)
    )
    .run();

  let normalized;
  try {
    normalized = normalizeSnapshot(runId, snapshot, createdAt);
  } catch (error) {
    await markRunFailed(env.DB, runId);
    return json(
      {
        ok: false,
        message: 'Snapshot normalization failed',
        error: error instanceof Error ? error.message : String(error)
      },
      400,
      corsHeaders
    );
  }

  if (normalized.siteResults.length === 0) {
    await markRunFailed(env.DB, runId);
    return json(
      {
        ok: false,
        message: 'No site result rows could be extracted from snapshot',
        run_id: runId,
        snapshot_envelope_keys: Object.keys(snapshotEnvelope || {}),
        snapshot_keys: Object.keys(snapshot || {})
      },
      400,
      corsHeaders
    );
  }

  try {
    await insertSiteResults(env.DB, normalized.siteResults);
    await insertSiteExtractions(env.DB, normalized.siteExtractions);

    await env.DB.prepare('UPDATE runs SET is_current = 0 WHERE is_current = 1 AND id != ?')
      .bind(runId)
      .run();

    await env.DB.prepare(
      `
        UPDATE runs
        SET
          status = 'success',
          is_current = 1,
          completed_at = ?,
          site_count = ?,
          strategy_count = ?
        WHERE id = ?
      `
    )
      .bind(new Date().toISOString(), normalized.siteCount, normalized.strategyCount, runId)
      .run();

    const uniqueSiteUrls = [...new Set(normalized.siteResults.map((r) => r.site_url))];
    const freshnessAt = snapshotGeneratedAt || createdAt;
    for (const siteUrl of uniqueSiteUrls) {
      await upsertPagespeedFreshnessSummary(env.DB, siteUrl, freshnessAt);
    }

    return json(
      {
        ok: true,
        run_id: runId,
        site_count: normalized.siteCount,
        strategy_count: normalized.strategyCount,
        inserted_site_results: normalized.siteResults.length,
        inserted_site_extractions: normalized.siteExtractions.length
      },
      200,
      corsHeaders
    );
  } catch (error) {
    await markRunFailed(env.DB, runId);
    return json(
      {
        ok: false,
        message: 'Database ingest failed',
        run_id: runId,
        error: error instanceof Error ? error.message : String(error)
      },
      500,
      corsHeaders
    );
  }
}

async function handleIngestGsc(request, env, corsHeaders) {
  if (request.method !== 'POST') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const expectedSecret =
    asNullableString(env.GSC_INGEST_SHARED_SECRET) ||
    asNullableString(env.INGEST_SHARED_SECRET);

  if (!expectedSecret) {
    return json(
      { ok: false, message: 'Missing GSC_INGEST_SHARED_SECRET or INGEST_SHARED_SECRET secret' },
      500,
      corsHeaders
    );
  }

  const providedSecret = request.headers.get('x-ingest-secret') || '';
  if (!providedSecret || providedSecret !== expectedSecret) {
    return json({ ok: false, message: 'Invalid ingest secret' }, 401, corsHeaders);
  }

  let payload;
  try {
    payload = await request.json();
  } catch {
    return json({ ok: false, message: 'Invalid JSON body' }, 400, corsHeaders);
  }

  const siteUrl = asNullableString(payload?.site_url);
  const gscProperty = asNullableString(payload?.gsc_property);
  const capturedAt = asNullableString(payload?.captured_at) || new Date().toISOString();
  const periodStart = asNullableString(payload?.period_start);
  const periodEnd = asNullableString(payload?.period_end);
  const importMode = asNullableString(payload?.import_mode) || 'api';
  const source = 'gsc';

  if (!siteUrl || !gscProperty || !periodStart || !periodEnd) {
    return json(
      {
        ok: false,
        message: 'Missing required fields',
        required: ['site_url', 'gsc_property', 'period_start', 'period_end']
      },
      400,
      corsHeaders
    );
  }

  const modules = isObject(payload?.modules) ? payload.modules : {};

  const queryMetrics = normalizeInboundGscQueryMetrics(
    Array.isArray(modules.query_metrics) ? modules.query_metrics : [],
    siteUrl,
    periodEnd
  );
  const pageMetrics = normalizeInboundGscPageMetrics(
    Array.isArray(modules.page_metrics) ? modules.page_metrics : [],
    siteUrl,
    periodEnd
  );
  const countryMetrics = normalizeInboundGscCountryMetrics(
    Array.isArray(modules.country_metrics) ? modules.country_metrics : [],
    siteUrl,
    periodEnd
  );
  const deviceMetrics = normalizeInboundGscDeviceMetrics(
    Array.isArray(modules.device_metrics) ? modules.device_metrics : [],
    siteUrl,
    periodEnd
  );

  const totalRowCount =
    queryMetrics.length +
    pageMetrics.length +
    countryMetrics.length +
    deviceMetrics.length;

  try {
    const snapshotIds = {};

    if (queryMetrics.length) {
      snapshotIds.query_metrics = await insertSourceSnapshot(env.DB, {
        siteUrl,
        source,
        module: 'query_metrics',
        capturedAt,
        periodStart,
        periodEnd,
        importMode,
        parseStatus: 'success',
        normalizedRowCount: queryMetrics.length,
        freshnessStatus: 'fresh',
        notes: `Property: ${gscProperty}`
      });

      await insertGscQueryMetrics(env.DB, queryMetrics, snapshotIds.query_metrics);
    }

    if (pageMetrics.length) {
      snapshotIds.page_metrics = await insertSourceSnapshot(env.DB, {
        siteUrl,
        source,
        module: 'page_metrics',
        capturedAt,
        periodStart,
        periodEnd,
        importMode,
        parseStatus: 'success',
        normalizedRowCount: pageMetrics.length,
        freshnessStatus: 'fresh',
        notes: `Property: ${gscProperty}`
      });

      await insertGscPageMetrics(env.DB, pageMetrics, snapshotIds.page_metrics);
    }

    if (countryMetrics.length) {
      snapshotIds.country_metrics = await insertSourceSnapshot(env.DB, {
        siteUrl,
        source,
        module: 'country_metrics',
        capturedAt,
        periodStart,
        periodEnd,
        importMode,
        parseStatus: 'success',
        normalizedRowCount: countryMetrics.length,
        freshnessStatus: 'fresh',
        notes: `Property: ${gscProperty}`
      });

      await insertGscCountryMetrics(env.DB, countryMetrics, snapshotIds.country_metrics);
    }

    if (deviceMetrics.length) {
      snapshotIds.device_metrics = await insertSourceSnapshot(env.DB, {
        siteUrl,
        source,
        module: 'device_metrics',
        capturedAt,
        periodStart,
        periodEnd,
        importMode,
        parseStatus: 'success',
        normalizedRowCount: deviceMetrics.length,
        freshnessStatus: 'fresh',
        notes: `Property: ${gscProperty}`
      });

      await insertGscDeviceMetrics(env.DB, deviceMetrics, snapshotIds.device_metrics);
    }

    await upsertGscFreshnessSummary(env.DB, {
      siteUrl,
      gscLastUpdatedAt: capturedAt
    });

    return json(
      {
        ok: true,
        source: 'gsc',
        site_url: siteUrl,
        gsc_property: gscProperty,
        captured_at: capturedAt,
        period_start: periodStart,
        period_end: periodEnd,
        snapshot_ids: snapshotIds,
        inserted: {
          query_metrics: queryMetrics.length,
          page_metrics: pageMetrics.length,
          country_metrics: countryMetrics.length,
          device_metrics: deviceMetrics.length,
          total: totalRowCount
        }
      },
      200,
      corsHeaders
    );
  } catch (error) {
    return json(
      {
        ok: false,
        message: 'GSC ingest failed',
        error: error instanceof Error ? error.message : String(error)
      },
      500,
      corsHeaders
    );
  }
}

async function upsertPagespeedFreshnessSummary(db, siteUrl, capturedAt) {
  const now = new Date().toISOString();

  const existing = await db.prepare(
    `SELECT id FROM site_freshness_summaries WHERE site_url = ? LIMIT 1`
  ).bind(siteUrl).first();

  if (existing?.id) {
    await db.prepare(
      `UPDATE site_freshness_summaries
       SET pagespeed_last_updated_at = ?,
           overall_freshness_status = 'fresh',
           updated_at = ?
       WHERE site_url = ?`
    ).bind(capturedAt, now, siteUrl).run();
    return;
  }

  await db.prepare(
    `INSERT INTO site_freshness_summaries (
       site_url, pagespeed_last_updated_at, gsc_last_updated_at,
       ga4_last_updated_at, trends_last_updated_at, indexnow_last_updated_at,
       overall_freshness_status, freshness_confidence_score,
       summary_json, created_at, updated_at
     )
     VALUES (?, ?, NULL, NULL, NULL, NULL, 'fresh', 0.5, ?, ?, ?)`
  ).bind(
    siteUrl,
    capturedAt,
    JSON.stringify({ sources: { pagespeed: { last_updated_at: capturedAt, status: 'fresh' } } }),
    now,
    now
  ).run();
}

async function upsertGscFreshnessSummary(db, input) {
  const now = new Date().toISOString();

  const existing = await db.prepare(
    `
      SELECT id
      FROM site_freshness_summaries
      WHERE site_url = ?
      LIMIT 1
    `
  )
    .bind(input.siteUrl)
    .first();

  if (existing?.id) {
    await db.prepare(
      `
        UPDATE site_freshness_summaries
        SET
          gsc_last_updated_at = ?,
          overall_freshness_status = 'fresh',
          freshness_confidence_score = CASE
            WHEN freshness_confidence_score < 0.5 THEN 0.5
            ELSE freshness_confidence_score
          END,
          summary_json = ?,
          updated_at = ?
        WHERE site_url = ?
      `
    )
      .bind(
        input.gscLastUpdatedAt,
        safeJsonStringify({
          sources: {
            gsc: {
              last_updated_at: input.gscLastUpdatedAt,
              status: 'fresh'
            }
          }
        }),
        now,
        input.siteUrl
      )
      .run();

    return;
  }

  await db.prepare(
    `
      INSERT INTO site_freshness_summaries (
        site_url,
        pagespeed_last_updated_at,
        gsc_last_updated_at,
        ga4_last_updated_at,
        trends_last_updated_at,
        indexnow_last_updated_at,
        overall_freshness_status,
        freshness_confidence_score,
        summary_json,
        created_at,
        updated_at
      )
      VALUES (?, NULL, ?, NULL, NULL, NULL, 'fresh', 0.5, ?, ?, ?)
    `
  )
    .bind(
      input.siteUrl,
      input.gscLastUpdatedAt,
      safeJsonStringify({
        sources: {
          gsc: {
            last_updated_at: input.gscLastUpdatedAt,
            status: 'fresh'
          }
        }
      }),
      now,
      now
    )
    .run();
}
async function insertGscCountryMetrics(db, rows, snapshotId) {
  if (!rows.length) {
    return;
  }

  const statements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO gsc_country_metrics (
          site_url,
          snapshot_id,
          date,
          country,
          clicks,
          impressions,
          ctr,
          position,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_url, date, country)
        DO UPDATE SET
          snapshot_id = excluded.snapshot_id,
          clicks = excluded.clicks,
          impressions = excluded.impressions,
          ctr = excluded.ctr,
          position = excluded.position,
          created_at = excluded.created_at
      `
    ).bind(
      row.site_url,
      snapshotId,
      row.date,
      row.country,
      row.clicks,
      row.impressions,
      row.ctr,
      row.position,
      new Date().toISOString()
    )
  );

  await executeBatches(db, statements, 50);
}

async function insertGscDeviceMetrics(db, rows, snapshotId) {
  if (!rows.length) {
    return;
  }

  const statements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO gsc_device_metrics (
          site_url,
          snapshot_id,
          date,
          device,
          clicks,
          impressions,
          ctr,
          position,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_url, date, device)
        DO UPDATE SET
          snapshot_id = excluded.snapshot_id,
          clicks = excluded.clicks,
          impressions = excluded.impressions,
          ctr = excluded.ctr,
          position = excluded.position,
          created_at = excluded.created_at
      `
    ).bind(
      row.site_url,
      snapshotId,
      row.date,
      row.device,
      row.clicks,
      row.impressions,
      row.ctr,
      row.position,
      new Date().toISOString()
    )
  );

  await executeBatches(db, statements, 50);
}

async function insertSourceSnapshot(db, snapshot) {
  const result = await db.prepare(
    `
      INSERT INTO source_snapshots (
        site_url,
        source,
        module,
        captured_at,
        period_start,
        period_end,
        import_mode,
        parse_status,
        normalized_row_count,
        freshness_status,
        notes,
        created_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `
  )
    .bind(
      snapshot.siteUrl,
      snapshot.source,
      snapshot.module,
      snapshot.capturedAt,
      snapshot.periodStart,
      snapshot.periodEnd,
      snapshot.importMode,
      snapshot.parseStatus,
      snapshot.normalizedRowCount,
      snapshot.freshnessStatus,
      snapshot.notes,
      new Date().toISOString()
    )
    .run();

  const snapshotId = result?.meta?.last_row_id;
  if (!snapshotId) {
    throw new Error(`Failed to create source snapshot for module: ${snapshot.module}`);
  }

  return snapshotId;
}
function normalizeDevice(value) {
  const normalized = asNullableString(value)?.toUpperCase() || null;

  if (!normalized) {
    return null;
  }

  if (normalized === 'DESKTOP' || normalized === 'MOBILE' || normalized === 'TABLET') {
    return normalized;
  }

  return normalized;
}
function toNumber(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : 0;
}
function normalizeInboundGscQueryMetrics(rows, siteUrl, fallbackDate) {
  return rows
    .map((row) => ({
      site_url: siteUrl,
      date: asNullableString(row?.date) || fallbackDate,
      query: asNullableString(row?.query),
      page: asNullableString(row?.page),
      country: asNullableString(row?.country),
      device: normalizeDevice(row?.device),
      clicks: toNumber(row?.clicks),
      impressions: toNumber(row?.impressions),
      ctr: toNumber(row?.ctr),
      position: toNumber(row?.position)
    }))
    .filter((row) => row.query && row.date);
}

function normalizeInboundGscPageMetrics(rows, siteUrl, fallbackDate) {
  return rows
    .map((row) => ({
      site_url: siteUrl,
      date: asNullableString(row?.date) || fallbackDate,
      page: asNullableString(row?.page),
      clicks: toNumber(row?.clicks),
      impressions: toNumber(row?.impressions),
      ctr: toNumber(row?.ctr),
      position: toNumber(row?.position)
    }))
    .filter((row) => row.page && row.date);
}

function normalizeInboundGscCountryMetrics(rows, siteUrl, fallbackDate) {
  return rows
    .map((row) => ({
      site_url: siteUrl,
      date: asNullableString(row?.date) || fallbackDate,
      country: asNullableString(row?.country),
      clicks: toNumber(row?.clicks),
      impressions: toNumber(row?.impressions),
      ctr: toNumber(row?.ctr),
      position: toNumber(row?.position)
    }))
    .filter((row) => row.country && row.date);
}

function normalizeInboundGscDeviceMetrics(rows, siteUrl, fallbackDate) {
  return rows
    .map((row) => ({
      site_url: siteUrl,
      date: asNullableString(row?.date) || fallbackDate,
      device: normalizeDevice(row?.device),
      clicks: toNumber(row?.clicks),
      impressions: toNumber(row?.impressions),
      ctr: toNumber(row?.ctr),
      position: toNumber(row?.position)
    }))
    .filter((row) => row.device && row.date);
}

async function insertGscQueryMetrics(db, rows, snapshotId) {
  if (!rows.length) {
    return;
  }

  const deleteStatements = rows.map((row) =>
    db.prepare(
      `
        DELETE FROM gsc_query_metrics
        WHERE site_url = ?
          AND date = ?
          AND query = ?
          AND COALESCE(page, '') = COALESCE(?, '')
          AND COALESCE(country, '') = COALESCE(?, '')
          AND COALESCE(device, '') = COALESCE(?, '')
      `
    ).bind(
      row.site_url,
      row.date,
      row.query,
      row.page,
      row.country,
      row.device
    )
  );

  await executeBatches(db, deleteStatements, 50);

  const insertStatements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO gsc_query_metrics (
          site_url,
          snapshot_id,
          date,
          query,
          page,
          country,
          device,
          clicks,
          impressions,
          ctr,
          position,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).bind(
      row.site_url,
      snapshotId,
      row.date,
      row.query,
      row.page,
      row.country,
      row.device,
      row.clicks,
      row.impressions,
      row.ctr,
      row.position,
      new Date().toISOString()
    )
  );

  await executeBatches(db, insertStatements, 50);
}

async function insertGscPageMetrics(db, rows, snapshotId) {
  if (!rows.length) {
    return;
  }

  const statements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO gsc_page_metrics (
          site_url,
          snapshot_id,
          date,
          page,
          clicks,
          impressions,
          ctr,
          position,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_url, date, page)
        DO UPDATE SET
          snapshot_id = excluded.snapshot_id,
          clicks = excluded.clicks,
          impressions = excluded.impressions,
          ctr = excluded.ctr,
          position = excluded.position,
          created_at = excluded.created_at
      `
    ).bind(
      row.site_url,
      snapshotId,
      row.date,
      row.page,
      row.clicks,
      row.impressions,
      row.ctr,
      row.position,
      new Date().toISOString()
    )
  );

  await executeBatches(db, statements, 50);
}


// trigger-worker/src/index.js
async function insertSiteResults(db, rows) {
  const statements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO site_results (
          run_id,
          site_url,
          strategy,
          display_name,
          group_name,
          raw_url,
          target_url,
          error,
          performance_score,
          accessibility_score,
          best_practices_score,
          seo_score,
          metrics_json,
          categories_json,
          audits_json,
          raw_result_json,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).bind(
      row.run_id,
      row.site_url,
      row.strategy,
      row.display_name,
      row.group_name,
      row.raw_url,
      row.target_url,
      row.error,
      row.performance_score,
      row.accessibility_score,
      row.best_practices_score,
      row.seo_score,
      row.metrics_json,
      row.categories_json,
      row.audits_json,
      row.raw_result_json,
      row.created_at
    )
  );

  await executeBatches(db, statements, 50);
}
async function insertSiteExtractions(db, rows) {
  if (!rows.length) {
    return;
  }

  const statements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO site_extractions (
          run_id,
          site_url,
          strategy,
          title,
          meta_description,
          canonical_url,
          robots_directives,
          schema_summary_json,
          heading_summary_json,
          entity_summary_json,
          answer_readiness_json,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).bind(
      row.run_id,
      row.site_url,
      row.strategy,
      row.title,
      row.meta_description,
      row.canonical_url,
      row.robots_directives,
      row.schema_summary_json,
      row.heading_summary_json,
      row.entity_summary_json,
      row.answer_readiness_json,
      row.created_at
    )
  );

  await executeBatches(db, statements, 50);
}


async function executeBatches(db, statements, batchSize) {
  for (let index = 0; index < statements.length; index += batchSize) {
    await db.batch(statements.slice(index, index + batchSize));
  }
}

async function markRunFailed(db, runId) {
  await db.prepare(
    `
      UPDATE runs
      SET
        status = 'failed',
        completed_at = ?
      WHERE id = ?
    `
  )
    .bind(new Date().toISOString(), runId)
    .run();
}
async function handleSites(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const currentRun = await getCurrentOrLatestSuccessfulRun(env.DB);

  if (!currentRun) {
    return json({ ok: false, message: 'No runs found' }, 404, corsHeaders);
  }

  const result = await env.DB.prepare(
    `
      SELECT
        site_url,
        MAX(display_name) AS display_name,
        MAX(group_name) AS group_name,
        MAX(raw_url) AS raw_url,
        MAX(target_url) AS target_url,
        MAX(CASE WHEN strategy = 'desktop' THEN performance_score END) AS desktop_performance_score,
        MAX(CASE WHEN strategy = 'desktop' THEN accessibility_score END) AS desktop_accessibility_score,
        MAX(CASE WHEN strategy = 'desktop' THEN best_practices_score END) AS desktop_best_practices_score,
        MAX(CASE WHEN strategy = 'desktop' THEN seo_score END) AS desktop_seo_score,
        MAX(CASE WHEN strategy = 'mobile' THEN performance_score END) AS mobile_performance_score,
        MAX(CASE WHEN strategy = 'mobile' THEN accessibility_score END) AS mobile_accessibility_score,
        MAX(CASE WHEN strategy = 'mobile' THEN best_practices_score END) AS mobile_best_practices_score,
        MAX(CASE WHEN strategy = 'mobile' THEN seo_score END) AS mobile_seo_score,
        COUNT(*) AS strategy_rows
      FROM site_results
      WHERE run_id = ?
      GROUP BY site_url
      ORDER BY group_name ASC, display_name ASC, site_url ASC
    `
  )
    .bind(currentRun.id)
    .all();

  const sites = Array.isArray(result?.results) ? result.results : [];

  return json(
    {
      ok: true,
      run: {
        id: currentRun.id,
        created_at: currentRun.created_at,
        snapshot_generated_at: currentRun.snapshot_generated_at,
        site_count: currentRun.site_count,
        strategy_count: currentRun.strategy_count
      },
      sites
    },
    200,
    corsHeaders
  );
}
async function handleSiteOverview(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const url = new URL(request.url);
  const site = asNullableString(url.searchParams.get('site'));

  if (!site) {
    return json({ ok: false, message: 'Missing required query param: site' }, 400, corsHeaders);
  }

  const run = await getCurrentOrLatestSuccessfulRun(env.DB);

  if (!run) {
    return json({ ok: false, message: 'No runs found' }, 404, corsHeaders);
  }

  const result = await env.DB.prepare(
    `
      SELECT
        sr.run_id,
        sr.site_url,
        sr.strategy,
        sr.performance_score,
        sr.accessibility_score,
        sr.best_practices_score,
        sr.seo_score,
        sr.metrics_json,
        sr.categories_json,
        sr.audits_json,
        sr.raw_result_json,
        sx.title,
        sx.meta_description,
        sx.canonical_url,
        sx.robots_directives,
        sx.schema_summary_json,
        sx.heading_summary_json,
        sx.entity_summary_json,
        sx.answer_readiness_json
      FROM site_results sr
      LEFT JOIN site_extractions sx
        ON sx.run_id = sr.run_id
       AND sx.site_url = sr.site_url
       AND sx.strategy = sr.strategy
      WHERE sr.run_id = ?
        AND sr.site_url = ?
      ORDER BY CASE sr.strategy
        WHEN 'desktop' THEN 1
        WHEN 'mobile' THEN 2
        ELSE 99
      END
    `
  )
    .bind(run.id, site)
    .all();

  const rows = Array.isArray(result?.results) ? result.results : [];

  if (!rows.length) {
    return json(
      {
        ok: false,
        message: 'Site not found in current run',
        run_id: run.id,
        site
      },
      404,
      corsHeaders
    );
  }

  const strategies = {};

  for (const row of rows) {
    strategies[row.strategy] = {
      scores: {
        performance: row.performance_score,
        accessibility: row.accessibility_score,
        best_practices: row.best_practices_score,
        seo: row.seo_score
      },
      metrics: parseJsonField(row.metrics_json, {}),
      categories: parseJsonField(row.categories_json, {}),
      audits: parseJsonField(row.audits_json, {}),
      raw_result: parseJsonField(row.raw_result_json, {}),
      extractions: {
        title: row.title,
        meta_description: row.meta_description,
        canonical_url: row.canonical_url,
        robots_directives: row.robots_directives,
        schema_summary: parseJsonField(row.schema_summary_json, {}),
        heading_summary: parseJsonField(row.heading_summary_json, {}),
        entity_summary: parseJsonField(row.entity_summary_json, {}),
        answer_readiness: parseJsonField(row.answer_readiness_json, {})
      }
    };
  }

  return json(
    {
      ok: true,
      run: {
        id: run.id,
        created_at: run.created_at,
        snapshot_generated_at: run.snapshot_generated_at,
        site_count: run.site_count,
        strategy_count: run.strategy_count
      },
      site: {
        site_url: site,
        strategies
      }
    },
    200,
    corsHeaders
  );
}
async function getCurrentOrLatestSuccessfulRun(db) {
  const currentRun = await db.prepare(
    `
      SELECT
        id,
        source,
        trigger_type,
        status,
        is_current,
        report_url,
        snapshot_generated_at,
        created_at,
        completed_at,
        site_count,
        strategy_count
      FROM runs
      WHERE is_current = 1
      ORDER BY created_at DESC
      LIMIT 1
    `
  ).first();

  if (currentRun) {
    return currentRun;
  }

  const latestSuccessfulRun = await db.prepare(
    `
      SELECT
        id,
        source,
        trigger_type,
        status,
        is_current,
        report_url,
        snapshot_generated_at,
        created_at,
        completed_at,
        site_count,
        strategy_count
      FROM runs
      WHERE status = 'success'
      ORDER BY created_at DESC
      LIMIT 1
    `
  ).first();

  return latestSuccessfulRun || null;
}


async function handleSitePagespeed(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const url = new URL(request.url);
  const site = asNullableString(url.searchParams.get('site'));

  if (!site) {
    return json({ ok: false, message: 'Missing required query param: site' }, 400, corsHeaders);
  }

  const run = await getCurrentOrLatestSuccessfulRun(env.DB);

  if (!run) {
    return json({ ok: false, message: 'No runs found' }, 404, corsHeaders);
  }

  const result = await env.DB.prepare(
    `
      SELECT
        run_id,
        site_url,
        strategy,
        display_name,
        group_name,
        raw_url,
        target_url,
        error,
        performance_score,
        accessibility_score,
        best_practices_score,
        seo_score,
        metrics_json,
        categories_json,
        audits_json,
        raw_result_json,
        created_at
      FROM site_results
      WHERE run_id = ?
        AND site_url = ?
      ORDER BY CASE strategy
        WHEN 'desktop' THEN 1
        WHEN 'mobile' THEN 2
        ELSE 99
      END
    `
  )
    .bind(run.id, site)
    .all();

  const rows = Array.isArray(result?.results) ? result.results : [];

  if (!rows.length) {
    return json(
      {
        ok: false,
        message: 'Site not found in current run',
        run_id: run.id,
        site
      },
      404,
      corsHeaders
    );
  }

  const strategies = {};

  for (const row of rows) {
    strategies[row.strategy] = {
      strategy: row.strategy,
      site_url: row.site_url,
      display_name: row.display_name,
      group_name: row.group_name,
      raw_url: row.raw_url,
      target_url: row.target_url,
      error: row.error,
      created_at: row.created_at,
      scores: {
        performance: row.performance_score,
        accessibility: row.accessibility_score,
        best_practices: row.best_practices_score,
        seo: row.seo_score
      },
      metrics: parseJsonField(row.metrics_json, {}),
      categories: parseJsonField(row.categories_json, {}),
      audits: parseJsonField(row.audits_json, {}),
      raw_result: parseJsonField(row.raw_result_json, {})
    };
  }

  return json(
    {
      ok: true,
      run: {
        id: run.id,
        created_at: run.created_at,
        snapshot_generated_at: run.snapshot_generated_at,
        site_count: run.site_count,
        strategy_count: run.strategy_count
      },
      site: {
        site_url: site
      },
      pagespeed: strategies
    },
    200,
    corsHeaders
  );
}
async function handleSiteExtractions(request, env, corsHeaders) {
  if (request.method !== 'GET') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  if (!env.DB) {
    return json({ ok: false, message: 'Missing DB binding' }, 500, corsHeaders);
  }

  const url = new URL(request.url);
  const site = asNullableString(url.searchParams.get('site'));

  if (!site) {
    return json({ ok: false, message: 'Missing required query param: site' }, 400, corsHeaders);
  }

  const run = await getCurrentOrLatestSuccessfulRun(env.DB);

  if (!run) {
    return json({ ok: false, message: 'No runs found' }, 404, corsHeaders);
  }

  const result = await env.DB.prepare(
    `
      SELECT
        run_id,
        site_url,
        strategy,
        title,
        meta_description,
        canonical_url,
        robots_directives,
        schema_summary_json,
        heading_summary_json,
        entity_summary_json,
        answer_readiness_json,
        created_at
      FROM site_extractions
      WHERE run_id = ?
        AND site_url = ?
      ORDER BY CASE strategy
        WHEN 'desktop' THEN 1
        WHEN 'mobile' THEN 2
        ELSE 99
      END
    `
  )
    .bind(run.id, site)
    .all();

  const rows = Array.isArray(result?.results) ? result.results : [];

  if (!rows.length) {
    return json(
      {
        ok: false,
        message: 'Site extractions not found in current run',
        run_id: run.id,
        site
      },
      404,
      corsHeaders
    );
  }

  const extractions = {};

  for (const row of rows) {
    extractions[row.strategy] = {
      strategy: row.strategy,
      site_url: row.site_url,
      created_at: row.created_at,
      title: row.title,
      meta_description: row.meta_description,
      canonical_url: row.canonical_url,
      robots_directives: row.robots_directives,
      schema_summary: parseJsonField(row.schema_summary_json, {}),
      heading_summary: parseJsonField(row.heading_summary_json, {}),
      entity_summary: parseJsonField(row.entity_summary_json, {}),
      answer_readiness: parseJsonField(row.answer_readiness_json, {})
    };
  }

  return json(
    {
      ok: true,
      run: {
        id: run.id,
        created_at: run.created_at,
        snapshot_generated_at: run.snapshot_generated_at,
        site_count: run.site_count,
        strategy_count: run.strategy_count
      },
      site: {
        site_url: site
      },
      extractions
    },
    200,
    corsHeaders
  );
}

function normalizeSnapshot(runId, snapshot, createdAt) {
  const candidates = collectResultCandidates(snapshot);
  const siteResults = [];
  const siteExtractions = [];
  const uniqueSites = new Set();

  for (const candidate of candidates) {
    const siteUrl = extractSiteUrl(candidate.siteWrapper, candidate.result);
    const strategy = extractStrategy(candidate.strategyHint, candidate.result);

    if (!siteUrl || !strategy) {
      continue;
    }

    uniqueSites.add(siteUrl);

    const categories = extractCategories(candidate.result);
    const audits = extractAudits(candidate.result);
    const metrics = extractMetrics(candidate.result, audits);
    const extractions = extractExtractions(candidate.result, audits);

    siteResults.push({
      run_id: runId,
      site_url: siteUrl,
      strategy,
      display_name:
        asNullableString(candidate.result?.displayName) ||
        asNullableString(candidate.siteWrapper?.displayName),
      group_name:
        asNullableString(candidate.result?.group_name) ||
        asNullableString(candidate.result?.groupName) ||
        asNullableString(candidate.siteWrapper?.group_name) ||
        asNullableString(candidate.siteWrapper?.groupName),
      raw_url:
        asNullableString(candidate.result?.raw_url) ||
        asNullableString(candidate.result?.rawUrl) ||
        asNullableString(candidate.siteWrapper?.raw_url) ||
        asNullableString(candidate.siteWrapper?.rawUrl),
      target_url:
        asNullableString(candidate.result?.target_url) ||
        asNullableString(candidate.result?.targetUrl) ||
        asNullableString(candidate.siteWrapper?.target_url) ||
        asNullableString(candidate.siteWrapper?.targetUrl),
      error:
        asNullableString(candidate.result?.error) ||
        asNullableString(candidate.siteWrapper?.error),
      performance_score: extractCategoryScore(categories, 'performance'),
      accessibility_score: extractCategoryScore(categories, 'accessibility'),
      best_practices_score:
        extractCategoryScore(categories, 'best-practices') ??
        extractCategoryScore(categories, 'bestPractices'),
      seo_score: extractCategoryScore(categories, 'seo'),
      metrics_json: safeJsonStringify(metrics),
      categories_json: safeJsonStringify(categories),
      audits_json: safeJsonStringify(audits),
      raw_result_json: safeJsonStringify(candidate.result),
      created_at: createdAt
    });

    siteExtractions.push({
      run_id: runId,
      site_url: siteUrl,
      strategy,
      title: asNullableString(extractions.title),
      meta_description:
        asNullableString(extractions.meta_description) ||
        asNullableString(extractions.metaDescription),
      canonical_url:
        asNullableString(extractions.canonical_url) ||
        asNullableString(extractions.canonicalUrl),
      robots_directives:
        asNullableString(extractions.robots_directives) ||
        asNullableString(extractions.robotsDirectives),
      schema_summary_json: safeJsonStringify(
        extractions.schema_summary ?? extractions.schemaSummary ?? {}
      ),
      heading_summary_json: safeJsonStringify(
        extractions.heading_summary ?? extractions.headingSummary ?? {}
      ),
      entity_summary_json: safeJsonStringify(
        extractions.entity_summary ?? extractions.entitySummary ?? {}
      ),
      answer_readiness_json: safeJsonStringify(
        extractions.answer_readiness ?? extractions.answerReadiness ?? {}
      ),
      created_at: createdAt
    });
  }

  const uniqueStrategyPairs = new Set(
    siteResults.map((row) => `${row.site_url}::${row.strategy}`)
  );

  return {
    siteResults,
    siteExtractions,
    siteCount: uniqueSites.size,
    strategyCount: uniqueStrategyPairs.size
  };
}


function collectResultCandidates(snapshot) {
  const candidates = [];

  if (Array.isArray(snapshot?.strategies)) {
    for (const strategyWrapper of snapshot.strategies) {
      if (!isObject(strategyWrapper) || !Array.isArray(strategyWrapper.groups)) {
        continue;
      }

      for (const groupWrapper of strategyWrapper.groups) {
        if (!isObject(groupWrapper) || !Array.isArray(groupWrapper.entries)) {
          continue;
        }

        for (const entry of groupWrapper.entries) {
          if (!isObject(entry)) {
            continue;
          }

          candidates.push({
            payload: snapshot,
            siteWrapper: entry,
            result: buildReportEntryResult(entry, strategyWrapper, groupWrapper),
            strategyHint: strategyWrapper?.strategy
          });
        }
      }
    }

    return dedupeCandidates(candidates);
  }

  if (Array.isArray(snapshot?.sites)) {
    for (const siteWrapper of snapshot.sites) {
      candidates.push(...collectFromSiteWrapper(siteWrapper, snapshot));
    }
    return dedupeCandidates(candidates);
  }

  if (Array.isArray(snapshot?.results)) {
    for (const result of snapshot.results) {
      candidates.push({
        payload: snapshot,
        siteWrapper: result,
        result,
        strategyHint: result?.strategy
      });
    }
    return dedupeCandidates(candidates);
  }

  candidates.push(...collectFromSiteWrapper(snapshot, snapshot));
  return dedupeCandidates(candidates);
}

function buildReportEntryResult(entry, strategyWrapper, groupWrapper) {
  return {
    ...entry,
    strategy:
      asNullableString(strategyWrapper?.strategy) ||
      asNullableString(entry?.strategy),
    site_url:
      asNullableString(entry?.targetUrl) ||
      asNullableString(entry?.site_url) ||
      asNullableString(entry?.siteUrl) ||
      asNullableString(entry?.url) ||
      asNullableString(entry?.rawUrl),
    group_name:
      asNullableString(entry?.groupName) ||
      asNullableString(groupWrapper?.groupName),
    categories: mapReportScoresToCategories(entry?.scores),
    audits: {},
    metrics: {},
    extractions: {}
  };
}

function collectFromSiteWrapper(siteWrapper, payload) {
  const candidates = [];

  if (!isObject(siteWrapper)) {
    return candidates;
  }

  if (Array.isArray(siteWrapper.results)) {
    for (const result of siteWrapper.results) {
      candidates.push({
        payload,
        siteWrapper,
        result,
        strategyHint: result?.strategy
      });
    }
  }

  const strategiesObject = isObject(siteWrapper.strategies) ? siteWrapper.strategies : null;

  if (strategiesObject) {
    for (const strategy of ['desktop', 'mobile']) {
      if (isObject(strategiesObject[strategy])) {
        candidates.push({
          payload,
          siteWrapper,
          result: strategiesObject[strategy],
          strategyHint: strategy
        });
      }
    }
  }

  for (const strategy of ['desktop', 'mobile']) {
    if (isObject(siteWrapper[strategy])) {
      candidates.push({
        payload,
        siteWrapper,
        result: siteWrapper[strategy],
        strategyHint: strategy
      });
    }
  }

  if (siteWrapper.strategy && isObject(siteWrapper)) {
    candidates.push({
      payload,
      siteWrapper,
      result: siteWrapper,
      strategyHint: siteWrapper.strategy
    });
  }

  return dedupeCandidates(candidates);
}

function dedupeCandidates(candidates) {
  const seen = new Set();
  const output = [];

  for (const candidate of candidates) {
    const siteUrl = extractSiteUrl(candidate.siteWrapper, candidate.result) || '';
    const strategy = extractStrategy(candidate.strategyHint, candidate.result) || '';
    const rawKey = `${siteUrl}::${strategy}::${safeJsonStringify(candidate.result)}`;

    if (seen.has(rawKey)) {
      continue;
    }

    seen.add(rawKey);
    output.push(candidate);
  }

  return output;
}

function mapReportScoresToCategories(scores) {
  if (!isObject(scores)) {
    return {};
  }

  const categories = {};

  if (scores.performance !== undefined && scores.performance !== null) {
    categories.performance = { score: Number(scores.performance) };
  }

  if (scores.accessibility !== undefined && scores.accessibility !== null) {
    categories.accessibility = { score: Number(scores.accessibility) };
  }

  const bestPractices = scores.bestPractices ?? scores['best-practices'] ?? null;
  if (bestPractices !== null && bestPractices !== undefined) {
    categories['best-practices'] = { score: Number(bestPractices) };
  }

  if (scores.seo !== undefined && scores.seo !== null) {
    categories.seo = { score: Number(scores.seo) };
  }

  return categories;
}

function extractSiteUrl(siteWrapper, result) {
  const values = [
    result?.site_url,
    result?.siteUrl,
    result?.targetUrl,
    result?.url,
    result?.site,
    result?.website,
    result?.rawUrl,
    siteWrapper?.site_url,
    siteWrapper?.siteUrl,
    siteWrapper?.targetUrl,
    siteWrapper?.url,
    siteWrapper?.site,
    siteWrapper?.website,
    siteWrapper?.rawUrl,
    siteWrapper?.domain
  ];

  for (const value of values) {
    const stringValue = asNullableString(value);
    if (stringValue) {
      return stringValue;
    }
  }

  return null;
}

function extractStrategy(strategyHint, result) {
  const value = String(strategyHint || result?.strategy || result?.formFactor || '').toLowerCase();

  if (value.includes('desktop')) {
    return 'desktop';
  }

  if (value.includes('mobile')) {
    return 'mobile';
  }

  return null;
}

function extractCategories(result) {
  if (isObject(result?.scores)) {
    return mapReportScoresToCategories(result.scores);
  }

  return (
    result?.categories ||
    result?.categoryScores ||
    result?.lighthouseResult?.categories ||
    result?.result?.lighthouseResult?.categories ||
    {}
  );
}

function extractAudits(result) {
  return (
    result?.audits ||
    result?.lighthouseResult?.audits ||
    result?.result?.lighthouseResult?.audits ||
    {}
  );
}

function extractMetrics(result, audits) {
  if (isObject(result?.metrics)) {
    return result.metrics;
  }

  const metricMap = {
    first_contentful_paint: audits?.['first-contentful-paint']?.numericValue,
    largest_contentful_paint: audits?.['largest-contentful-paint']?.numericValue,
    speed_index: audits?.['speed-index']?.numericValue,
    total_blocking_time: audits?.['total-blocking-time']?.numericValue,
    cumulative_layout_shift: audits?.['cumulative-layout-shift']?.numericValue,
    interactive: audits?.interactive?.numericValue
  };

  return Object.fromEntries(
    Object.entries(metricMap).filter(([, value]) => value !== undefined && value !== null)
  );
}

function extractExtractions(result, audits) {
  if (isObject(result?.extractions)) {
    return result.extractions;
  }

  if (isObject(result?.site_extractions)) {
    return result.site_extractions;
  }

  if (isObject(result?.page_metadata)) {
    return result.page_metadata;
  }

  return {
    title: audits?.['document-title']?.title || null,
    meta_description: audits?.['meta-description']?.title || null,
    canonical_url: null,
    robots_directives: null,
    schema_summary: {},
    heading_summary: {},
    entity_summary: {},
    answer_readiness: {}
  };
}

function extractCategoryScore(categories, key) {
  const category = categories?.[key];
  if (!isObject(category)) {
    return null;
  }

  return normalizeScore(category.score);
}

function normalizeScore(score) {
  if (score === null || score === undefined || score === '') {
    return null;
  }

  const numeric = Number(score);
  if (!Number.isFinite(numeric)) {
    return null;
  }

  return numeric <= 1 ? Math.round(numeric * 100) : Math.round(numeric);
}

function extractSnapshotGeneratedAt(payload, snapshotEnvelope, snapshot) {
  const candidates = [
    payload?.snapshot_generated_at,
    snapshotEnvelope?.snapshot_generated_at,
    snapshotEnvelope?.generated_at,
    snapshotEnvelope?.generatedAt,
    snapshotEnvelope?.created_at,
    snapshotEnvelope?.createdAt,
    snapshot?.snapshot_generated_at,
    snapshot?.generated_at,
    snapshot?.generatedAt,
    snapshot?.created_at,
    snapshot?.createdAt
  ];

  for (const value of candidates) {
    const stringValue = asNullableString(value);
    if (stringValue) {
      return stringValue;
    }
  }

  return null;
}

function buildRunId(payload) {
  if (payload?.workflow_run_id) {
    const workflowRunId = String(payload.workflow_run_id).trim();
    const attempt = String(payload?.workflow_run_attempt || '1').trim();
    return `gha-${workflowRunId}-${attempt}`;
  }

  if (payload?.run_id) {
    return String(payload.run_id).trim();
  }

  return crypto.randomUUID();
}

function buildCorsHeaders(request, env) {
  const origin = request.headers.get('Origin') || '';
  const allowOrigin =
    env.ALLOWED_ORIGIN === '*'
      ? '*'
      : env.ALLOWED_ORIGIN && origin === env.ALLOWED_ORIGIN
        ? origin
        : env.ALLOWED_ORIGIN || '*';

  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, x-admin-key, x-ingest-secret',
    'Access-Control-Max-Age': '86400',
    Vary: 'Origin'
  };
}

function json(payload, status, headers = {}) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'Content-Type': 'application/json; charset=utf-8',
      ...headers
    }
  });
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value ?? {});
  } catch {
    return JSON.stringify({ serialization_error: true });
  }
}
function parseJsonField(value, fallback = null) {
  if (value === null || value === undefined || value === '') {
    return fallback;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function asNonEmptyString(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed || null;
}

function asNullableString(value) {
  if (value === null || value === undefined) {
    return null;
  }

  const stringValue = String(value).trim();
  return stringValue || null;
}

function isObject(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
