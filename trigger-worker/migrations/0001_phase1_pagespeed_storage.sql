-- trigger-worker/migrations/0001_phase1_pagespeed_storage.sql

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'success', 'failed')),
  is_current INTEGER NOT NULL DEFAULT 0 CHECK (is_current IN (0, 1)),
  report_url TEXT,
  snapshot_generated_at TEXT,
  created_at TEXT NOT NULL,
  completed_at TEXT,
  site_count INTEGER NOT NULL DEFAULT 0,
  strategy_count INTEGER NOT NULL DEFAULT 0,
  raw_snapshot_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_status_created_at
  ON runs(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_runs_is_current_created_at
  ON runs(is_current, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_only_one_current
  ON runs(is_current)
  WHERE is_current = 1;

CREATE TABLE IF NOT EXISTS site_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  site_url TEXT NOT NULL,
  strategy TEXT NOT NULL CHECK (strategy IN ('desktop', 'mobile')),
  performance_score REAL,
  accessibility_score REAL,
  best_practices_score REAL,
  seo_score REAL,
  metrics_json TEXT NOT NULL,
  categories_json TEXT NOT NULL,
  audits_json TEXT NOT NULL,
  raw_result_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_results_run_id
  ON site_results(run_id);

CREATE INDEX IF NOT EXISTS idx_site_results_site_strategy
  ON site_results(site_url, strategy);

CREATE UNIQUE INDEX IF NOT EXISTS idx_site_results_run_site_strategy_unique
  ON site_results(run_id, site_url, strategy);

CREATE TABLE IF NOT EXISTS site_extractions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  site_url TEXT NOT NULL,
  strategy TEXT NOT NULL CHECK (strategy IN ('desktop', 'mobile')),
  title TEXT,
  meta_description TEXT,
  canonical_url TEXT,
  robots_directives TEXT,
  schema_summary_json TEXT NOT NULL,
  heading_summary_json TEXT NOT NULL,
  entity_summary_json TEXT NOT NULL,
  answer_readiness_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_site_extractions_run_id
  ON site_extractions(run_id);

CREATE INDEX IF NOT EXISTS idx_site_extractions_site_strategy
  ON site_extractions(site_url, strategy);

CREATE UNIQUE INDEX IF NOT EXISTS idx_site_extractions_run_site_strategy_unique
  ON site_extractions(run_id, site_url, strategy);
