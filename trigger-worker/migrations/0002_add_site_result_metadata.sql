ALTER TABLE site_results ADD COLUMN display_name TEXT;
ALTER TABLE site_results ADD COLUMN group_name TEXT;
ALTER TABLE site_results ADD COLUMN raw_url TEXT;
ALTER TABLE site_results ADD COLUMN target_url TEXT;
ALTER TABLE site_results ADD COLUMN error TEXT;

CREATE INDEX IF NOT EXISTS idx_site_results_group_name
  ON site_results(group_name);

CREATE INDEX IF NOT EXISTS idx_site_results_display_name
  ON site_results(display_name);
