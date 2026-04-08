#!/usr/bin/env node

import fs from "node:fs/promises";
import process from "node:process";

async function main() {
  const workerUrl = requiredEnv("REPORT_INGEST_URL");
  const ingestSecret = requiredEnv("REPORT_INGEST_SECRET");
  const reportPath = process.env.REPORT_DATA_PATH || "report-data.json";
  const reportUrl = process.env.REPORT_PUBLIC_URL || "https://pagespeed.tricel.eu";
  const source = process.env.REPORT_SOURCE || "github_actions";
  const triggerType = process.env.REPORT_TRIGGER_TYPE || "unknown";
  const runId =
    process.env.GITHUB_RUN_ID && process.env.GITHUB_RUN_ATTEMPT
      ? `${process.env.GITHUB_RUN_ID}-${process.env.GITHUB_RUN_ATTEMPT}`
      : crypto.randomUUID();

  const raw = await fs.readFile(reportPath, "utf8");
  const reportData = JSON.parse(raw);

  const payload = {
    run_id: runId,
    source,
    trigger_type: triggerType,
    report_url: reportUrl,
    snapshot_generated_at:
      reportData.generated_at ||
      reportData.generatedAt ||
      new Date().toISOString(),
    report_data: reportData
  };

  const response = await fetch(workerUrl, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "authorization": `Bearer ${ingestSecret}`
    },
    body: JSON.stringify(payload)
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(`Ingest failed (${response.status}): ${text}`);
  }

  process.stdout.write(`${text}\n`);
}

function requiredEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
