const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8"
};

export default {
  async fetch(request, env, ctx) {
    try {
      const url = new URL(request.url);
      const method = request.method.toUpperCase();

      if (method === "OPTIONS") {
        return handleCors(request, env);
      }

      if (url.pathname === "/trigger" && method === "POST") {
        return handleTrigger(request, env);
      }

      if (url.pathname === "/api/ingest-run" && method === "POST") {
        return handleIngestRun(request, env);
      }

      if (url.pathname === "/api/latest-run" && method === "GET") {
        return handleLatestRun(request, env);
      }

      if (url.pathname === "/api/sites" && method === "GET") {
        return handleSites(request, env);
      }

      if (url.pathname === "/api/site" && method === "GET") {
        return handleSite(request, env);
      }

      if (url.pathname === "/health" && method === "GET") {
        return jsonResponse(200, { ok: true, service: "tricel-report-trigger" });
      }

      return jsonResponse(404, { ok: false, error: "Not found" });
    } catch (error) {
      return jsonResponse(500, {
        ok: false,
        error: "Unhandled worker error",
        details: error instanceof Error ? error.message : String(error)
      });
    }
  }
};

async function handleTrigger(request, env) {
  assertAllowedOrigin(request, env);
  assertAdminKey(request, env);

  const response = await fetch(
    `https://api.github.com/repos/${env.GITHUB_OWNER}/${env.GITHUB_REPO}/actions/workflows/${env.GITHUB_WORKFLOW_ID}/dispatches`,
    {
      method: "POST",
      headers: {
        "authorization": `Bearer ${env.GITHUB_TOKEN}`,
        "accept": "application/vnd.github+json",
        "content-type": "application/json",
        "user-agent": "tricel-report-trigger"
      },
      body: JSON.stringify({
        ref: env.GITHUB_REF,
        inputs: {
          send_email: "false"
        }
      })
    }
  );

  if (!response.ok) {
    const body = await safeText(response);
    return jsonResponse(response.status, {
      ok: false,
      error: "GitHub workflow dispatch failed",
      details: body
    });
  }

  return withCors(
    request,
    env,
    jsonResponse(202, {
      ok: true,
      message: "Refresh workflow dispatched"
    })
  );
}

async function handleIngestRun(request, env) {
  assertIngestSecret(request, env);

  const payload = await request.json();
  validateIngestPayload(payload);

  const normalized = normalizeReportPayload(payload);
  const now = new Date().toISOString();
  const runId = payload.run_id || crypto.randomUUID();
  const source = payload.source || "github_actions";
  const triggerType = payload.trigger_type || "unknown";
  const reportUrl = payload.report_url || null;
  const snapshotGeneratedAt = payload.snapshot_generated_at || normalized.snapshotGeneratedAt || now;
  const rawSnapshotJson = JSON.stringify(payload.report_data);

  await env.DB.prepare(
    `
      INSERT INTO runs (
        id, source, trigger_type, status, is_current, report_url,
        snapshot_generated_at, created_at, completed_at, site_count,
        strategy_count, raw_snapshot_json
      )
      VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, NULL, ?, ?, ?)
    `
  )
    .bind(
      runId,
      source,
      triggerType,
      reportUrl,
      snapshotGeneratedAt,
      now,
      normalized.uniqueSiteCount,
      normalized.strategyRowCount,
      rawSnapshotJson
    )
    .run();

  const siteStatements = [];
  const extractionStatements = [];

  for (const row of normalized.rows) {
    siteStatements.push(
      env.DB.prepare(
        `
          INSERT INTO site_results (
            run_id, site_url, strategy, performance_score, accessibility_score,
            best_practices_score, seo_score, metrics_json, categories_json,
            audits_json, raw_result_json, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
      ).bind(
        runId,
        row.siteUrl,
        row.strategy,
        row.scores.performance,
        row.scores.accessibility,
        row.scores.bestPractices,
        row.scores.seo,
        JSON.stringify(row.metrics),
        JSON.stringify(row.categories),
        JSON.stringify(row.audits),
        JSON.stringify(row.raw),
        now
      )
    );

    extractionStatements.push(
      env.DB.prepare(
        `
          INSERT INTO site_extractions (
            run_id, site_url, strategy, title, meta_description, canonical_url,
            robots_directives, schema_summary_json, heading_summary_json,
            entity_summary_json, answer_readiness_json, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
      ).bind(
        runId,
        row.siteUrl,
        row.strategy,
        row.extractions.title,
        row.extractions.metaDescription,
        row.extractions.canonicalUrl,
        row.extractions.robotsDirectives,
        JSON.stringify(row.extractions.schemaSummary),
        JSON.stringify(row.extractions.headingSummary),
        JSON.stringify(row.extractions.entitySummary),
        JSON.stringify(row.extractions.answerReadiness),
        now
      )
    );
  }

  for (const chunk of chunked(siteStatements, 50)) {
    await env.DB.batch(chunk);
  }

  for (const chunk of chunked(extractionStatements, 50)) {
    await env.DB.batch(chunk);
  }

  await env.DB.prepare(`UPDATE runs SET is_current = 0 WHERE is_current = 1 AND id != ?`)
    .bind(runId)
    .run();

  await env.DB.prepare(
    `
      UPDATE runs
      SET status = 'success',
          is_current = 1,
          completed_at = ?
      WHERE id = ?
    `
  )
    .bind(now, runId)
    .run();

  await pruneOldRuns(env);

  return jsonResponse(200, {
    ok: true,
    run_id: runId,
    site_count: normalized.uniqueSiteCount,
    strategy_count: normalized.strategyRowCount
  });
}

async function handleLatestRun(request, env) {
  const run = await env.DB.prepare(
    `
      SELECT id, source, trigger_type, status, is_current, report_url,
             snapshot_generated_at, created_at, completed_at,
             site_count, strategy_count
      FROM runs
      WHERE is_current = 1
      ORDER BY created_at DESC
      LIMIT 1
    `
  ).first();

  if (!run) {
    return withCors(request, env, jsonResponse(404, { ok: false, error: "No current run found" }));
  }

  return withCors(request, env, jsonResponse(200, { ok: true, run }));
}

async function handleSites(request, env) {
  const currentRun = await getCurrentRun(env);

  if (!currentRun) {
    return withCors(request, env, jsonResponse(404, { ok: false, error: "No current run found" }));
  }

  const rows = await env.DB.prepare(
    `
      SELECT
        sr.site_url,
        sr.strategy,
        sr.performance_score,
        sr.accessibility_score,
        sr.best_practices_score,
        sr.seo_score,
        se.title,
        se.meta_description,
        se.canonical_url,
        se.robots_directives
      FROM site_results sr
      LEFT JOIN site_extractions se
        ON se.run_id = sr.run_id
       AND se.site_url = sr.site_url
       AND se.strategy = sr.strategy
      WHERE sr.run_id = ?
      ORDER BY sr.site_url ASC, sr.strategy ASC
    `
  )
    .bind(currentRun.id)
    .all();

  return withCors(
    request,
    env,
    jsonResponse(200, {
      ok: true,
      run_id: currentRun.id,
      sites: rows.results || []
    })
  );
}

async function handleSite(request, env) {
  const url = new URL(request.url);
  const siteUrl = url.searchParams.get("site_url");
  const strategy = url.searchParams.get("strategy");

  if (!siteUrl || !strategy) {
    return withCors(
      request,
      env,
      jsonResponse(400, {
        ok: false,
        error: "site_url and strategy are required"
      })
    );
  }

  const currentRun = await getCurrentRun(env);

  if (!currentRun) {
    return withCors(request, env, jsonResponse(404, { ok: false, error: "No current run found" }));
  }

  const row = await env.DB.prepare(
    `
      SELECT
        sr.site_url,
        sr.strategy,
        sr.performance_score,
        sr.accessibility_score,
        sr.best_practices_score,
        sr.seo_score,
        sr.metrics_json,
        sr.categories_json,
        sr.audits_json,
        se.title,
        se.meta_description,
        se.canonical_url,
        se.robots_directives,
        se.schema_summary_json,
        se.heading_summary_json,
        se.entity_summary_json,
        se.answer_readiness_json
      FROM site_results sr
      LEFT JOIN site_extractions se
        ON se.run_id = sr.run_id
       AND se.site_url = sr.site_url
       AND se.strategy = sr.strategy
      WHERE sr.run_id = ?
        AND sr.site_url = ?
        AND sr.strategy = ?
      LIMIT 1
    `
  )
    .bind(currentRun.id, siteUrl, strategy)
    .first();

  if (!row) {
    return withCors(request, env, jsonResponse(404, { ok: false, error: "Site result not found" }));
  }

  return withCors(
    request,
    env,
    jsonResponse(200, {
      ok: true,
      run_id: currentRun.id,
      site: {
        ...row,
        metrics_json: safeParseJson(row.metrics_json),
        categories_json: safeParseJson(row.categories_json),
        audits_json: safeParseJson(row.audits_json),
        schema_summary_json: safeParseJson(row.schema_summary_json),
        heading_summary_json: safeParseJson(row.heading_summary_json),
        entity_summary_json: safeParseJson(row.entity_summary_json),
        answer_readiness_json: safeParseJson(row.answer_readiness_json)
      }
    })
  );
}

function validateIngestPayload(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Invalid JSON payload");
  }

  if (!payload.report_data || typeof payload.report_data !== "object") {
    throw new Error("report_data is required");
  }
}

function normalizeReportPayload(payload) {
  const reportData = payload.report_data;
  const rows = [];

  const candidates = collectCandidateRows(reportData);

  for (const candidate of candidates) {
    const siteUrl = readFirstString(candidate, [
      "site_url",
      "siteUrl",
      "url",
      "origin",
      "site",
      "page_url"
    ]);

    if (!siteUrl) {
      continue;
    }

    const strategy = normalizeStrategy(
      readFirstString(candidate, ["strategy", "device", "formFactor"])
    );

    if (strategy) {
      rows.push(buildNormalizedRow(siteUrl, strategy, candidate));
      continue;
    }

    const desktopCandidate = candidate.desktop || candidate.desktopResult || candidate.results?.desktop;
    const mobileCandidate = candidate.mobile || candidate.mobileResult || candidate.results?.mobile;

    if (desktopCandidate) {
      rows.push(buildNormalizedRow(siteUrl, "desktop", desktopCandidate));
    }

    if (mobileCandidate) {
      rows.push(buildNormalizedRow(siteUrl, "mobile", mobileCandidate));
    }
  }

  if (rows.length === 0) {
    throw new Error("Could not normalize any site results from report_data");
  }

  const uniqueSites = new Set(rows.map((row) => row.siteUrl));

  return {
    rows,
    uniqueSiteCount: uniqueSites.size,
    strategyRowCount: rows.length,
    snapshotGeneratedAt:
      readFirstString(reportData, ["generated_at", "generatedAt", "created_at", "createdAt"]) || null
  };
}

function collectCandidateRows(reportData) {
  if (Array.isArray(reportData)) {
    return reportData;
  }

  const arrayKeys = ["results", "sites", "entries", "pages", "items"];
  for (const key of arrayKeys) {
    if (Array.isArray(reportData[key])) {
      return reportData[key];
    }
  }

  if (reportData.sites && typeof reportData.sites === "object" && !Array.isArray(reportData.sites)) {
    return Object.entries(reportData.sites).map(([siteUrl, value]) => ({
      site_url: siteUrl,
      ...value
    }));
  }

  return [];
}

function buildNormalizedRow(siteUrl, strategy, source) {
  const categories = source.categories || source.categoryScores || source.lighthouseResult?.categories || {};
  const audits = source.audits || source.lighthouseResult?.audits || {};
  const metrics = source.metrics || source.loadingExperience || source.originLoadingExperience || {};

  const title =
    readFirstString(source, ["title"]) ||
    readFirstString(source.page, ["title"]) ||
    readFirstString(source.meta, ["title"]);

  const metaDescription =
    readFirstString(source, ["meta_description", "metaDescription"]) ||
    readFirstString(source.meta, ["description", "meta_description"]);

  const canonicalUrl =
    readFirstString(source, ["canonical_url", "canonicalUrl"]) ||
    readFirstString(source.meta, ["canonical", "canonical_url"]);

  const robotsDirectives =
    readFirstString(source, ["robots_directives", "robotsDirectives"]) ||
    readFirstString(source.meta, ["robots"]);

  return {
    siteUrl,
    strategy,
    scores: {
      performance: normalizeScore(readCategoryScore(categories, "performance")),
      accessibility: normalizeScore(readCategoryScore(categories, "accessibility")),
      bestPractices: normalizeScore(
        readCategoryScore(categories, "best-practices") ?? readCategoryScore(categories, "bestPractices")
      ),
      seo: normalizeScore(readCategoryScore(categories, "seo"))
    },
    metrics,
    categories,
    audits,
    raw: source,
    extractions: {
      title: title || null,
      metaDescription: metaDescription || null,
      canonicalUrl: canonicalUrl || null,
      robotsDirectives: robotsDirectives || null,
      schemaSummary: normalizeObject(source.schema_summary || source.schemaSummary),
      headingSummary: normalizeObject(source.heading_summary || source.headingSummary),
      entitySummary: normalizeObject(source.entity_summary || source.entitySummary),
      answerReadiness: normalizeObject(source.answer_readiness || source.answerReadiness)
    }
  };
}

function readCategoryScore(categories, key) {
  const value = categories?.[key];
  if (typeof value === "number") {
    return value;
  }
  if (value && typeof value.score === "number") {
    return value.score;
  }
  return null;
}

function normalizeScore(value) {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return null;
  }
  return value <= 1 ? Math.round(value * 100) : Math.round(value);
}

function normalizeStrategy(value) {
  if (!value) {
    return null;
  }

  const normalized = String(value).trim().toLowerCase();

  if (normalized.includes("desktop")) {
    return "desktop";
  }

  if (normalized.includes("mobile")) {
    return "mobile";
  }

  return null;
}

function normalizeObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value;
}

function readFirstString(source, keys) {
  if (!source || typeof source !== "object") {
    return null;
  }

  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return null;
}

async function getCurrentRun(env) {
  return env.DB.prepare(
    `
      SELECT id, created_at
      FROM runs
      WHERE is_current = 1
      ORDER BY created_at DESC
      LIMIT 1
    `
  ).first();
}

async function pruneOldRuns(env) {
  const retentionRuns = Math.max(1, Number.parseInt(env.INGEST_RETENTION_RUNS || "14", 10));

  const oldRuns = await env.DB.prepare(
    `
      SELECT id
      FROM runs
      WHERE is_current = 0
      ORDER BY created_at DESC
      LIMIT -1 OFFSET ?
    `
  )
    .bind(retentionRuns)
    .all();

  const ids = (oldRuns.results || []).map((row) => row.id);

  if (ids.length === 0) {
    return;
  }

  const statements = ids.map((id) =>
    env.DB.prepare(`DELETE FROM runs WHERE id = ?`).bind(id)
  );

  for (const chunk of chunked(statements, 50)) {
    await env.DB.batch(chunk);
  }
}

function assertAllowedOrigin(request, env) {
  const origin = request.headers.get("origin");
  if (!origin || origin !== env.ALLOWED_ORIGIN) {
    throw new HttpError(403, "Origin not allowed");
  }
}

function assertAdminKey(request, env) {
  const adminKey = request.headers.get("x-admin-key");
  if (!adminKey || adminKey !== env.ADMIN_KEY) {
    throw new HttpError(401, "Invalid admin key");
  }
}

function assertIngestSecret(request, env) {
  const authHeader = request.headers.get("authorization");
  const expected = `Bearer ${env.INGEST_SHARED_SECRET}`;
  if (!authHeader || authHeader !== expected) {
    throw new HttpError(401, "Invalid ingest secret");
  }
}

function handleCors(request, env) {
  return new Response(null, {
    status: 204,
    headers: corsHeaders(request, env)
  });
}

function withCors(request, env, response) {
  const headers = new Headers(response.headers);
  const entries = corsHeaders(request, env);

  for (const [key, value] of Object.entries(entries)) {
    headers.set(key, value);
  }

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers
  });
}

function corsHeaders(request, env) {
  const origin = request.headers.get("origin");
  const allowedOrigin = origin === env.ALLOWED_ORIGIN ? origin : env.ALLOWED_ORIGIN;

  return {
    "access-control-allow-origin": allowedOrigin,
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,authorization,x-admin-key",
    "access-control-max-age": "86400"
  };
}

function jsonResponse(status, payload) {
  return new Response(JSON.stringify(payload, null, 2), {
    status,
    headers: JSON_HEADERS
  });
}

function safeParseJson(value) {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
}

function chunked(items, size) {
  const output = [];
  for (let index = 0; index < items.length; index += size) {
    output.push(items.slice(index, index + size));
  }
  return output;
}

async function safeText(response) {
  try {
    return await response.text();
  } catch {
    return "";
  }
}

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.name = "HttpError";
    this.status = status;
  }
}
