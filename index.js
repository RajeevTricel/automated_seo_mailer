// /index.js
'use strict';

require('dotenv').config();

const fs = require('node:fs/promises');
const path = require('node:path');
const { SITE_GROUPS } = require('./sites');

const DRY_RUN = process.argv.includes('--dry-run');
const EMAILJS_SEND_URL = 'https://api.emailjs.com/api/v1.0/email/send';

function getRequiredEnv(name) {
  const value = process.env[name];

  if (!value || !value.trim()) {
    throw new Error(`Missing required environment variable: ${name}`);
  }

  return value.trim();
}

function getEnv(name, fallback = '') {
  const value = process.env[name];
  return value && value.trim() ? value.trim() : fallback;
}

function getBooleanEnv(name, fallback = false) {
  const value = process.env[name];

  if (value == null || value === '') {
    return fallback;
  }

  return ['1', 'true', 'yes', 'on'].includes(value.trim().toLowerCase());
}

function getNumberEnv(name, fallback) {
  const value = process.env[name];

  if (value == null || value === '') {
    return fallback;
  }

  const parsed = Number(value);

  if (Number.isNaN(parsed)) {
    throw new Error(`Environment variable ${name} must be a number`);
  }

  return parsed;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeUrl(value) {
  return /^https?:\/\//i.test(value) ? value : `https://${value}`;
}

function displayUrl(value) {
  return normalizeUrl(value)
    .replace(/^https?:\/\//i, '')
    .replace(/^www\./i, '')
    .replace(/\/$/, '');
}

function htmlEscape(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatTimestamp(date, timeZone) {
  return new Intl.DateTimeFormat('en-IE', {
    dateStyle: 'full',
    timeStyle: 'medium',
    timeZone
  }).format(date);
}

function scoreColor(score) {
  if (score >= 90) {
    return '#16a34a';
  }

  if (score >= 70) {
    return '#d97706';
  }

  return '#dc2626';
}

function scoreLabel(score) {
  if (score >= 90) {
    return 'Good';
  }

  if (score >= 70) {
    return 'Warning';
  }

  return 'Poor';
}

function summarize(entries) {
  const successful = entries.filter((entry) => !entry.error && entry.scores);
  const failed = entries.length - successful.length;

  if (successful.length === 0) {
    return {
      averagePerformance: null,
      good: 0,
      warning: 0,
      poor: 0,
      failed,
      total: entries.length
    };
  }

  const performanceScores = successful.map((entry) => entry.scores.performance);
  const averagePerformance = Math.round(
    performanceScores.reduce((sum, value) => sum + value, 0) / successful.length
  );

  let good = 0;
  let warning = 0;
  let poor = 0;

  for (const score of performanceScores) {
    if (score >= 90) {
      good += 1;
    } else if (score >= 70) {
      warning += 1;
    } else {
      poor += 1;
    }
  }

  return {
    averagePerformance,
    good,
    warning,
    poor,
    failed,
    total: entries.length
  };
}

function buildPageSpeedUrl(targetUrl, apiKey, strategy) {
  const endpoint = new URL('https://www.googleapis.com/pagespeedonline/v5/runPagespeed');
  endpoint.searchParams.set('url', targetUrl);
  endpoint.searchParams.set('key', apiKey);
  endpoint.searchParams.set('strategy', strategy);

  for (const category of ['performance', 'accessibility', 'best-practices', 'seo']) {
    endpoint.searchParams.append('category', category);
  }

  return endpoint.toString();
}

function parsePageSpeedScores(payload) {
  const categories = payload?.lighthouseResult?.categories;

  if (!categories) {
    throw new Error('PageSpeed response did not include Lighthouse categories');
  }

  const readScore = (key) => {
    const value = categories[key]?.score;

    if (typeof value !== 'number') {
      throw new Error(`Missing score for category: ${key}`);
    }

    return Math.round(value * 100);
  };

  return {
    performance: readScore('performance'),
    accessibility: readScore('accessibility'),
    bestPractices: readScore('best-practices'),
    seo: readScore('seo')
  };
}

async function fetchPageSpeedScores(targetUrl, apiKey, strategy) {
  const requestUrl = buildPageSpeedUrl(targetUrl, apiKey, strategy);
  const response = await fetch(requestUrl, {
    headers: {
      Accept: 'application/json',
      'User-Agent': 'automated-seo-mailer/1.0'
    }
  });

  if (!response.ok) {
    const details = await response.text();
    const clipped = details.length > 500 ? `${details.slice(0, 500)}...` : details;
    throw new Error(`PageSpeed API ${response.status}: ${clipped}`);
  }

  const payload = await response.json();
  return parsePageSpeedScores(payload);
}

async function fetchWithRetry(targetUrl, apiKey, strategy, attempts = 2) {
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fetchPageSpeedScores(targetUrl, apiKey, strategy);
    } catch (error) {
      lastError = error;

      if (attempt < attempts) {
        await sleep(1500 * attempt);
      }
    }
  }

  throw lastError;
}

async function collectStrategyResults(strategy, apiKey, delayMs) {
  const groups = [];
  const groupNames = Object.keys(SITE_GROUPS);

  for (let groupIndex = 0; groupIndex < groupNames.length; groupIndex += 1) {
    const groupName = groupNames[groupIndex];
    const urls = SITE_GROUPS[groupName];
    const entries = [];

    for (let index = 0; index < urls.length; index += 1) {
      const rawUrl = urls[index];
      const targetUrl = normalizeUrl(rawUrl);

      process.stdout.write(`[${strategy}] ${groupName} :: ${displayUrl(rawUrl)}\n`);

      try {
        const scores = await fetchWithRetry(targetUrl, apiKey, strategy);

        entries.push({
          groupName,
          rawUrl,
          targetUrl,
          displayName: displayUrl(rawUrl),
          scores,
          error: null
        });
      } catch (error) {
        entries.push({
          groupName,
          rawUrl,
          targetUrl,
          displayName: displayUrl(rawUrl),
          scores: null,
          error: error.message
        });
      }

      const isLastRequest =
        groupIndex === groupNames.length - 1 && index === urls.length - 1;

      if (!isLastRequest) {
        await sleep(delayMs);
      }
    }

    groups.push({
      groupName,
      entries,
      summary: summarize(entries)
    });
  }

  const flatEntries = groups.flatMap((group) => group.entries);

  return {
    strategy,
    groups,
    summary: summarize(flatEntries)
  };
}

function renderSummaryCards(summary) {
  const average = summary.averagePerformance == null ? '—' : `${summary.averagePerformance}%`;

  const cards = [
    { label: 'Average performance', value: average, color: '#2563eb' },
    { label: 'Good', value: String(summary.good), color: '#16a34a' },
    { label: 'Warning', value: String(summary.warning), color: '#d97706' },
    { label: 'Poor', value: String(summary.poor), color: '#dc2626' },
    { label: 'Failed', value: String(summary.failed), color: '#64748b' }
  ];

  return cards
    .map(
      (card) => `
        <td style="padding: 0 6px 12px 6px;">
          <div style="border: 1px solid #e2e8f0; border-radius: 14px; padding: 14px; background: #ffffff;">
            <div style="font-size: 12px; color: #64748b; margin-bottom: 8px;">${htmlEscape(card.label)}</div>
            <div style="font-size: 24px; font-weight: 800; color: ${card.color};">${htmlEscape(card.value)}</div>
          </div>
        </td>
      `
    )
    .join('');
}

function renderScoreCell(score, shortLabel) {
  const color = scoreColor(score);

  return `
    <td style="padding: 12px 10px; text-align: center; border-bottom: 1px solid #e2e8f0;">
      <div style="font-weight: 800; color: ${color}; font-size: 16px;">${score}%</div>
      <div style="font-size: 11px; color: #64748b;">${htmlEscape(shortLabel)}</div>
    </td>
  `;
}

function renderEntryRow(entry) {
  if (entry.error) {
    return `
      <tr>
        <td style="padding: 12px 14px; border-bottom: 1px solid #e2e8f0; font-weight: 700; color: #0f172a;">
          ${htmlEscape(entry.displayName)}
        </td>
        <td colspan="5" style="padding: 12px 14px; border-bottom: 1px solid #e2e8f0; color: #dc2626; font-size: 13px;">
          Failed: ${htmlEscape(entry.error)}
        </td>
      </tr>
    `;
  }

  return `
    <tr>
      <td style="padding: 12px 14px; border-bottom: 1px solid #e2e8f0; font-weight: 700; color: #0f172a;">
        ${htmlEscape(entry.displayName)}
      </td>
      ${renderScoreCell(entry.scores.performance, 'Performance')}
      ${renderScoreCell(entry.scores.accessibility, 'Accessibility')}
      ${renderScoreCell(entry.scores.bestPractices, 'Best')}
      ${renderScoreCell(entry.scores.seo, 'SEO')}
      <td style="padding: 12px 10px; text-align: center; border-bottom: 1px solid #e2e8f0; font-weight: 800; color: ${scoreColor(entry.scores.performance)};">
        ${htmlEscape(scoreLabel(entry.scores.performance))}
      </td>
    </tr>
  `;
}

function buildStrategySections(report) {
  return report.strategies
    .map((strategyBlock) => {
      const groupSections = strategyBlock.groups
        .map(
          (group) => `
            <div style="margin: 0 0 28px 0;">
              <div style="font-size: 13px; letter-spacing: 0.12em; text-transform: uppercase; color: #64748b; font-weight: 800; margin-bottom: 12px;">
                ${htmlEscape(group.groupName)}
              </div>
              <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse: collapse; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 16px; overflow: hidden;">
                <thead>
                  <tr style="background: #f8fafc;">
                    <th align="left" style="padding: 14px; color: #334155; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;">Site</th>
                    <th style="padding: 14px; color: #334155; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;">Perf</th>
                    <th style="padding: 14px; color: #334155; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;">Acc</th>
                    <th style="padding: 14px; color: #334155; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;">Best</th>
                    <th style="padding: 14px; color: #334155; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;">SEO</th>
                    <th style="padding: 14px; color: #334155; font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em;">Status</th>
                  </tr>
                </thead>
                <tbody>
                  ${group.entries.map(renderEntryRow).join('')}
                </tbody>
              </table>
            </div>
          `
        )
        .join('');

      return `
        <section style="margin-bottom: 44px;">
          <div style="display: inline-block; padding: 8px 12px; border-radius: 999px; background: #dbeafe; color: #1d4ed8; font-size: 12px; font-weight: 800; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 18px;">
            ${htmlEscape(strategyBlock.strategy)}
          </div>
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom: 16px;">
            <tr>
              ${renderSummaryCards(strategyBlock.summary)}
            </tr>
          </table>
          ${groupSections}
        </section>
      `;
    })
    .join('');
}

function buildCompactRows(report) {
  const rows = [];

  for (const strategyBlock of report.strategies) {
    for (const group of strategyBlock.groups) {
      for (const entry of group.entries) {
        if (entry.error) {
          rows.push(`
            <tr>
              <td style="padding:8px;border-bottom:1px solid #e2e8f0;">${htmlEscape(strategyBlock.strategy.toUpperCase())}</td>
              <td style="padding:8px;border-bottom:1px solid #e2e8f0;">${htmlEscape(group.groupName)}</td>
              <td style="padding:8px;border-bottom:1px solid #e2e8f0;">${htmlEscape(entry.displayName)}</td>
              <td colspan="5" style="padding:8px;border-bottom:1px solid #e2e8f0;color:#dc2626;">FAILED</td>
            </tr>
          `);
          continue;
        }

        rows.push(`
          <tr>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;">${htmlEscape(strategyBlock.strategy.toUpperCase())}</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;">${htmlEscape(group.groupName)}</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;">${htmlEscape(entry.displayName)}</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;text-align:center;color:${scoreColor(entry.scores.performance)};">${entry.scores.performance}%</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;text-align:center;">${entry.scores.accessibility}%</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;text-align:center;">${entry.scores.bestPractices}%</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;text-align:center;">${entry.scores.seo}%</td>
            <td style="padding:8px;border-bottom:1px solid #e2e8f0;text-align:center;color:${scoreColor(entry.scores.performance)};">${htmlEscape(scoreLabel(entry.scores.performance))}</td>
          </tr>
        `);
      }
    }
  }

  return rows.join('');
}

function buildOverviewText(report) {
  const lines = [];
  const overallAverage =
    report.summary.averagePerformance == null ? 'N/A' : `${report.summary.averagePerformance}%`;

  lines.push(
    `Overall average performance: ${overallAverage}. Good: ${report.summary.good}, Warning: ${report.summary.warning}, Poor: ${report.summary.poor}, Failed: ${report.summary.failed}.`
  );

  for (const strategyBlock of report.strategies) {
    const average =
      strategyBlock.summary.averagePerformance == null
        ? 'N/A'
        : `${strategyBlock.summary.averagePerformance}%`;

    lines.push(
      `${strategyBlock.strategy.toUpperCase()}: Avg ${average}, Good ${strategyBlock.summary.good}, Warning ${strategyBlock.summary.warning}, Poor ${strategyBlock.summary.poor}, Failed ${strategyBlock.summary.failed}.`
    );
  }

  return lines.join('\n');
}

function buildStrategyLabel(report) {
  return report.strategies
    .map((strategyBlock) => strategyBlock.strategy.toUpperCase())
    .join(' + ');
}

function buildAverageDisplay(summary) {
  return summary.averagePerformance == null ? 'N/A' : `${summary.averagePerformance}%`;
}

function buildEmailDetailHtml(report) {
  return `
    <div style="font-size:14px;color:#334155;margin-bottom:14px;">
      Full report summary for all monitored sites:
    </div>
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;background:#ffffff;border:1px solid #e2e8f0;">
      <thead>
        <tr style="background:#f8fafc;">
          <th align="left" style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Strategy</th>
          <th align="left" style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Group</th>
          <th align="left" style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Site</th>
          <th style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Perf</th>
          <th style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Acc</th>
          <th style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Best</th>
          <th style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">SEO</th>
          <th style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:12px;">Status</th>
        </tr>
      </thead>
      <tbody>
        ${buildCompactRows(report)}
      </tbody>
    </table>
  `.trim();
}

function buildHtmlReport(report, generatedAt, timeZone) {
  const overviewText = buildOverviewText(report);
  const strategySections = buildStrategySections(report);

  return `
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Tricel PageSpeed Report</title>
  </head>
  <body style="margin: 0; padding: 24px; background: #f1f5f9; font-family: Arial, Helvetica, sans-serif; color: #0f172a;">
    <div style="max-width: 1180px; margin: 0 auto;">
      <div style="background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%); border-radius: 24px; padding: 28px 32px; margin-bottom: 24px;">
        <div style="font-size: 34px; font-weight: 900; color: #ffffff; margin-bottom: 8px;">Tricel PageSpeed Report</div>
        <div style="font-size: 14px; color: #cbd5e1;">
          Generated ${htmlEscape(formatTimestamp(generatedAt, timeZone))} · Time zone ${htmlEscape(timeZone)}
        </div>
      </div>

      <div style="background: #ffffff; border: 1px solid #e2e8f0; border-radius: 16px; padding: 24px; margin-bottom: 24px;">
        <div style="font-size: 22px; font-weight: 800; color: #0f172a; margin-bottom: 16px;">Overview</div>
        <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="margin-bottom: 16px;">
          <tr>
            ${renderSummaryCards(report.summary)}
          </tr>
        </table>
        <div style="font-size: 15px; line-height: 1.7; color: #334155; white-space: pre-line;">
          ${htmlEscape(overviewText)}
        </div>
      </div>

      ${strategySections}
    </div>
  </body>
</html>
  `.trim();
}

function buildTextReport(report, generatedAt, timeZone) {
  const lines = [
    'TRICEL PAGESPEED REPORT',
    `Generated: ${formatTimestamp(generatedAt, timeZone)}`,
    `Timezone: ${timeZone}`,
    '',
    buildOverviewText(report),
    ''
  ];

  for (const strategyBlock of report.strategies) {
    lines.push(`[${strategyBlock.strategy.toUpperCase()}]`);
    lines.push(`Average Performance: ${buildAverageDisplay(strategyBlock.summary)}`);
    lines.push(
      `Good: ${strategyBlock.summary.good} | Warning: ${strategyBlock.summary.warning} | Poor: ${strategyBlock.summary.poor} | Failed: ${strategyBlock.summary.failed}`
    );
    lines.push('');

    for (const group of strategyBlock.groups) {
      lines.push(`  ${group.groupName}`);

      for (const entry of group.entries) {
        if (entry.error) {
          lines.push(`    - ${entry.displayName}: FAILED (${entry.error})`);
          continue;
        }

        const { performance, accessibility, bestPractices, seo } = entry.scores;

        lines.push(
          `    - ${entry.displayName}: P ${performance}% | A ${accessibility}% | B ${bestPractices}% | S ${seo}% | ${scoreLabel(performance)}`
        );
      }

      lines.push('');
    }

    lines.push('');
  }

  return lines.join('\n').trim();
}

function buildSubject(report, generatedAt, timeZone) {
  const prefix = getEnv('SUBJECT_PREFIX', 'Tricel PageSpeed');
  const date = new Intl.DateTimeFormat('en-IE', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    timeZone
  }).format(generatedAt);

  return `${prefix} Report - ${date} - Avg ${buildAverageDisplay(report.summary)}`;
}

async function sendEmail(report, generatedAt, timeZone, text) {
  const payload = {
    service_id: getRequiredEnv('EMAILJS_SERVICE_ID'),
    template_id: getRequiredEnv('EMAILJS_TEMPLATE_ID'),
    user_id: getRequiredEnv('EMAILJS_PUBLIC_KEY'),
    template_params: {
      subject: buildSubject(report, generatedAt, timeZone),
      to_email: getRequiredEnv('EMAIL_TO'),
      reply_to: getEnv('EMAIL_REPLY_TO', getRequiredEnv('EMAIL_TO')),
      from_name: getEnv('EMAIL_FROM_NAME', 'Tricel PageSpeed Reports'),
      generated_at: formatTimestamp(generatedAt, timeZone),
      strategy_label: buildStrategyLabel(report),
      overview_text: buildOverviewText(report),
      avg_perf: buildAverageDisplay(report.summary),
      good_count: String(report.summary.good),
      warning_count: String(report.summary.warning),
      poor_count: String(report.summary.poor),
      failed_count: String(report.summary.failed),
      email_html: buildEmailDetailHtml(report),
      text_report: text
    }
  };

  const privateKey = getEnv('EMAILJS_PRIVATE_KEY');

  if (privateKey) {
    payload.accessToken = privateKey;
  }

  const response = await fetch(EMAILJS_SEND_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Accept: 'text/plain'
    },
    body: JSON.stringify(payload)
  });

  const responseText = await response.text();

  if (!response.ok) {
    throw new Error(`EmailJS ${response.status}: ${responseText}`);
  }

  return responseText;
}

function aggregateReport(strategyResults) {
  const allEntries = strategyResults.flatMap((strategyBlock) =>
    strategyBlock.groups.flatMap((group) => group.entries)
  );

  return {
    strategies: strategyResults,
    summary: summarize(allEntries)
  };
}

async function writePreview(html) {
  const outputPath = path.join(process.cwd(), 'report-preview.html');
  await fs.writeFile(outputPath, html, 'utf8');
  return outputPath;
}

async function main() {
  const apiKey = getRequiredEnv('PAGESPEED_API_KEY');
  const timeZone = getEnv('TIMEZONE', 'Europe/Dublin');
  const delayMs = getNumberEnv('REQUEST_DELAY_MS', 6000);
  const strictMode = getBooleanEnv('STRICT_MODE', false);

  const strategies = getEnv('REPORT_STRATEGIES', 'mobile,desktop')
    .split(',')
    .map((value) => value.trim().toLowerCase())
    .filter(Boolean);

  if (strategies.length === 0) {
    throw new Error('REPORT_STRATEGIES must include at least one strategy');
  }

  for (const strategy of strategies) {
    if (!['mobile', 'desktop'].includes(strategy)) {
      throw new Error(`Unsupported strategy: ${strategy}`);
    }
  }

  const generatedAt = new Date();
  const strategyResults = [];

  for (let index = 0; index < strategies.length; index += 1) {
    const strategy = strategies[index];
    const result = await collectStrategyResults(strategy, apiKey, delayMs);
    strategyResults.push(result);

    if (index < strategies.length - 1) {
      await sleep(delayMs);
    }
  }

  const report = aggregateReport(strategyResults);
  const html = buildHtmlReport(report, generatedAt, timeZone);
  const text = buildTextReport(report, generatedAt, timeZone);

  if (DRY_RUN) {
    const previewPath = await writePreview(html);
    process.stdout.write(`Preview written to ${previewPath}\n`);
    process.stdout.write(`${text}\n`);
    return;
  }

  const result = await sendEmail(report, generatedAt, timeZone, text);
  process.stdout.write(`EmailJS response: ${result}\n`);

  if (report.summary.failed > 0 && strictMode) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
