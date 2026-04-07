'use strict';

require('dotenv').config();

const fs = require('node:fs/promises');
const path = require('node:path');
const { SITE_GROUPS } = require('./sites');

const DRY_RUN = process.argv.includes('--dry-run');
const BUILD_ONLY = process.argv.includes('--build-only');
const SEND_ONLY = process.argv.includes('--send-only');

const EMAILJS_SEND_URL = 'https://api.emailjs.com/api/v1.0/email/send';
const REPORT_HTML_PATH = path.join(process.cwd(), 'report-preview.html');
const REPORT_DATA_PATH = path.join(process.cwd(), 'report-data.json');

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
    return 'GOOD';
  }

  if (score >= 70) {
    return 'WARNING';
  }

  return 'POOR';
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

function buildAverageDisplay(summary) {
  return summary.averagePerformance == null ? 'N/A' : `${summary.averagePerformance}%`;
}
function getEmailStrategyBlock(report) {
  return (
    report.strategies.find((strategyBlock) => strategyBlock.strategy === 'desktop') ||
    report.strategies[0]
  );
}

function buildAverageDisplay(summary) {
  return summary.averagePerformance == null ? 'N/A' : `${summary.averagePerformance}%`;
}

function buildEmailOverviewText(report) {
  const strategyBlock = getEmailStrategyBlock(report);

  return [
    `${strategyBlock.strategy.toUpperCase()} average performance: ${buildAverageDisplay(strategyBlock.summary)}.`,
    `Good: ${strategyBlock.summary.good}, Warning: ${strategyBlock.summary.warning}, Poor: ${strategyBlock.summary.poor}, Failed: ${strategyBlock.summary.failed}.`,
    'Open the full report link below for complete MOBILE + DESKTOP analysis.'
  ].join(' ');
}

function buildEmailStrategyLabel(report) {
  return getEmailStrategyBlock(report).strategy.toUpperCase();
}

function renderDesktopDetailRow(entry) {
  if (entry.error) {
    return `
      <tr>
        <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-weight:600;color:#0f172a;">
          ${htmlEscape(entry.displayName)}
        </td>
        <td colspan="5" style="padding:8px 10px;border-bottom:1px solid #e2e8f0;color:#dc2626;font-weight:700;">
          FAILED
        </td>
      </tr>
    `;
  }

  const perfColor = scoreColor(entry.scores.performance);

  return `
    <tr>
      <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-weight:600;color:#0f172a;">
        ${htmlEscape(entry.displayName)}
      </td>
      <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;color:${perfColor};font-weight:700;">
        ${entry.scores.performance}%
      </td>
      <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;">
        ${entry.scores.accessibility}%
      </td>
      <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;">
        ${entry.scores.bestPractices}%
      </td>
      <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;">
        ${entry.scores.seo}%
      </td>
      <td style="padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:center;color:${perfColor};font-weight:700;">
        ${htmlEscape(scoreLabel(entry.scores.performance))}
      </td>
    </tr>
  `;
}

function buildDesktopDetailHtml(report) {
  const strategyBlock = getEmailStrategyBlock(report);

  return strategyBlock.groups
    .map(
      (group) => `
        <div style="margin:0 0 18px 0;">
          <div style="margin:0 0 8px 0;font-size:13px;font-weight:800;letter-spacing:0.08em;text-transform:uppercase;color:#0f172a;">
            ${htmlEscape(group.groupName)}
          </div>
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;overflow:hidden;">
            <thead>
              <tr style="background:#f8fafc;">
                <th align="left" style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569;">Site</th>
                <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569;">P</th>
                <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569;">A</th>
                <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569;">B</th>
                <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569;">S</th>
                <th style="padding:8px 10px;border-bottom:1px solid #e2e8f0;font-size:12px;color:#475569;">Status</th>
              </tr>
            </thead>
            <tbody>
              ${group.entries.map(renderDesktopDetailRow).join('')}
            </tbody>
          </table>
        </div>
      `
    )
    .join('');
}

function buildTextReport(report, generatedAt, timeZone, reportUrl) {
  const strategyBlock = getEmailStrategyBlock(report);
  const lines = [
    `TRICEL PAGESPEED REPORT - ${strategyBlock.strategy.toUpperCase()}`,
    `Generated: ${formatTimestamp(generatedAt, timeZone)}`,
    '',
    buildEmailOverviewText(report),
    ''
  ];

  for (const group of strategyBlock.groups) {
    lines.push(`[${group.groupName.toUpperCase()}]`);

    for (const entry of group.entries) {
      if (entry.error) {
        lines.push(`${entry.displayName} | FAILED`);
        continue;
      }

      lines.push(
        `${entry.displayName} | P: ${entry.scores.performance}% | A: ${entry.scores.accessibility}% | B: ${entry.scores.bestPractices}% | S: ${entry.scores.seo}% | ${scoreLabel(entry.scores.performance)}`
      );
    }

    lines.push('');
  }

  if (reportUrl) {
    lines.push(`Full report: ${reportUrl}`);
  }

  return lines.join('\n').trim();
}

function buildSubject(report, generatedAt, timeZone) {
  const prefix = getEnv('SUBJECT_PREFIX', 'Tricel PageSpeed');
  const strategyBlock = getEmailStrategyBlock(report);
  const date = new Intl.DateTimeFormat('en-IE', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    timeZone
  }).format(generatedAt);

  return `${prefix} Report - ${strategyBlock.strategy.toUpperCase()} - ${date} - Avg ${buildAverageDisplay(strategyBlock.summary)}`;
}

async function sendEmail(report, generatedAt, timeZone, reportUrl) {
  const strategyBlock = getEmailStrategyBlock(report);

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
      strategy_label: buildEmailStrategyLabel(report),
      overview_text: buildEmailOverviewText(report),
      avg_perf: buildAverageDisplay(strategyBlock.summary),
      good_count: String(strategyBlock.summary.good),
      warning_count: String(strategyBlock.summary.warning),
      poor_count: String(strategyBlock.summary.poor),
      failed_count: String(strategyBlock.summary.failed),
      desktop_detail_html: buildDesktopDetailHtml(report),
      report_url: reportUrl
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

async function main() {
  const timeZone = getEnv('TIMEZONE', 'Europe/Dublin');
  const strictMode = getBooleanEnv('STRICT_MODE', false);

  if (SEND_ONLY) {
    const snapshot = await readSnapshot();
    const reportUrl = getRequiredEnv('REPORT_URL');
    const text = buildTextReport(snapshot.report, snapshot.generatedAt, snapshot.timeZone, reportUrl);

    process.stdout.write(`${text}\n`);

    const result = await sendEmail(snapshot.report, snapshot.generatedAt, snapshot.timeZone, reportUrl);
    process.stdout.write(`EmailJS response: ${result}\n`);

    if (snapshot.report.summary.failed > 0 && strictMode) {
      process.exitCode = 1;
    }

    return;
  }

  const apiKey = getRequiredEnv('PAGESPEED_API_KEY');
  const delayMs = getNumberEnv('REQUEST_DELAY_MS', 6000);
  const snapshot = await buildFreshReport(apiKey, timeZone, delayMs);
  const written = await writeSnapshot(snapshot.report, snapshot.generatedAt, snapshot.timeZone);

  process.stdout.write(`Preview written to ${written.htmlPath}\n`);
  process.stdout.write(`Snapshot written to ${written.dataPath}\n`);

  const text = buildTextReport(
    snapshot.report,
    snapshot.generatedAt,
    snapshot.timeZone,
    getEnv('REPORT_URL')
  );

  if (DRY_RUN || BUILD_ONLY) {
    process.stdout.write(`${text}\n`);
    return;
  }

  const reportUrl = getRequiredEnv('REPORT_URL');
  const result = await sendEmail(snapshot.report, snapshot.generatedAt, snapshot.timeZone, reportUrl);
  process.stdout.write(`EmailJS response: ${result}\n`);

  if (snapshot.report.summary.failed > 0 && strictMode) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
