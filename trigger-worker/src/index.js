// trigger-worker/src/index.js
export default {
  async fetch(request, env) {
    const corsHeaders = buildCorsHeaders(request, env);

    if (request.method === 'OPTIONS') {
      return new Response(null, {
        status: 204,
        headers: corsHeaders
      });
    }

    const url = new URL(request.url);

    if (url.pathname === '/trigger') {
      return handleTrigger(request, env, corsHeaders);
    }

    if (url.pathname === '/api/ingest-run') {
      return handleIngestRun(request, env, corsHeaders);
    }

    return json({ ok: false, message: 'Not found' }, 404, corsHeaders);
  }
};

async function handleTrigger(request, env, corsHeaders) {
  if (request.method !== 'POST') {
    return json({ ok: false, message: 'Method not allowed' }, 405, corsHeaders);
  }

  const origin = request.headers.get('Origin') || '';

  if (env.ALLOWED_ORIGIN && origin !== env.ALLOWED_ORIGIN) {
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

  const snapshot = isObject(payload?.snapshot) ? payload.snapshot : payload;
  const createdAt = new Date().toISOString();
  const runId = buildRunId(payload);
  const source = asNonEmptyString(payload?.source) || 'shadow_site_intelligence';
  const triggerType = asNonEmptyString(payload?.trigger_type) || 'workflow_dispatch';
  const reportUrl =
    asNullableString(payload?.report_url) ||
    asNullableString(snapshot?.report_url) ||
    asNullableString(snapshot?.reportUrl);
  const snapshotGeneratedAt = extractSnapshotGeneratedAt(payload, snapshot);

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
      safeJsonStringify(snapshot)
    )
    .run();

  let normalized;
  try {
    normalized = normalizeSnapshot(snapshot, createdAt);
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
      .bind(
        new Date().toISOString(),
        normalized.siteCount,
        normalized.strategyCount,
        runId
      )
      .run();

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

async function insertSiteResults(db, rows) {
  const statements = rows.map((row) =>
    db.prepare(
      `
        INSERT INTO site_results (
          run_id,
          site_url,
          strategy,
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    ).bind(
      row.run_id,
      row.site_url,
      row.strategy,
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
    const chunk = statements.slice(index, index + batchSize);
    await db.batch(chunk);
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

function normalizeSnapshot(runId, snapshot, createdAt) {
  const runId = runId;
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

  if (Array.isArray(snapshot?.sites)) {
    for (const siteWrapper of snapshot.sites) {
      candidates.push(...collectFromSiteWrapper(siteWrapper, snapshot));
    }
    return candidates;
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
    return candidates;
  }

  candidates.push(...collectFromSiteWrapper(snapshot, snapshot));
  return candidates;
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

  const strategiesObject =
    isObject(siteWrapper.strategies) ? siteWrapper.strategies : null;

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

function extractSiteUrl(siteWrapper, result) {
  const values = [
    result?.site_url,
    result?.siteUrl,
    result?.url,
    result?.site,
    result?.website,
    siteWrapper?.site_url,
    siteWrapper?.siteUrl,
    siteWrapper?.url,
    siteWrapper?.site,
    siteWrapper?.website,
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
  const value = String(
    strategyHint || result?.strategy || result?.formFactor || ''
  ).toLowerCase();

  if (value.includes('desktop')) {
    return 'desktop';
  }

  if (value.includes('mobile')) {
    return 'mobile';
  }

  return null;
}

function extractCategories(result) {
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

function extractSnapshotGeneratedAt(payload, snapshot) {
  const candidates = [
    payload?.snapshot_generated_at,
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
    const runId = String(payload.workflow_run_id).trim();
    const attempt = String(payload?.workflow_run_attempt || '1').trim();
    return `gha-${runId}-${attempt}`;
  }

  if (payload?.run_id) {
    return String(payload.run_id).trim();
  }

  return crypto.randomUUID();
}

function buildCorsHeaders(request, env) {
  const origin = request.headers.get('Origin') || '';
  const allowOrigin =
    env.ALLOWED_ORIGIN && origin === env.ALLOWED_ORIGIN
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

function asNonEmptyString(value) {
  if (typeof value !== 'string') {
    return null;
  }

  const trimmed = value.trim();
  return trimmed ? trimmed : null;
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
