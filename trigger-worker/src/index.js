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

    if (url.pathname !== '/trigger') {
      return json({ ok: false, message: 'Not found' }, 404, corsHeaders);
    }

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

    const workflowId = env.GITHUB_WORKFLOW_ID || 'daily-report.yml';
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
};

function buildCorsHeaders(request, env) {
  const origin = request.headers.get('Origin') || '';
  const allowOrigin =
    env.ALLOWED_ORIGIN && origin === env.ALLOWED_ORIGIN
      ? origin
      : env.ALLOWED_ORIGIN || '*';

  return {
    'Access-Control-Allow-Origin': allowOrigin,
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, x-admin-key',
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
