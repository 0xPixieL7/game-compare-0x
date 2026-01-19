-- =========================================================
-- TABLE-LEVEL AUTOVACUUM TUNING (HOT TABLES)
-- =========================================================
-- Apply to reduce bloat and keep stats fresh on write-heavy tables.
-- Run with a privileged role that can ALTER TABLE.

ALTER TABLE video_games SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02,
  fillfactor = 90
);

ALTER TABLE video_game_title_sources SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 90
);

ALTER TABLE video_game_prices SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02,
  fillfactor = 90
);

ALTER TABLE video_game_sources SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 95
);

ALTER TABLE products SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 95
);

ALTER TABLE price_charting_igdb_mappings SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 95
);

ALTER TABLE media SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 95
);

ALTER TABLE images SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 95
);

ALTER TABLE videos SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.05,
  fillfactor = 95
);

ALTER TABLE telescope_entries SET (
  autovacuum_vacuum_scale_factor = 0.1,
  autovacuum_analyze_scale_factor = 0.1,
  fillfactor = 90
);

ALTER TABLE telescope_entries_tags SET (
  autovacuum_vacuum_scale_factor = 0.1,
  autovacuum_analyze_scale_factor = 0.1,
  fillfactor = 90
);

ALTER TABLE telescope_monitoring SET (
  autovacuum_vacuum_scale_factor = 0.1,
  autovacuum_analyze_scale_factor = 0.1,
  fillfactor = 90
);

ALTER TABLE sessions SET (
  autovacuum_vacuum_scale_factor = 0.1,
  autovacuum_analyze_scale_factor = 0.1,
  fillfactor = 90
);

-- Verify:
-- SELECT relname, reloptions FROM pg_class WHERE relname IN (
--   'video_games','video_game_title_sources','video_game_prices','video_game_sources',
--   'products','price_charting_igdb_mappings','media','images','videos',
--   'telescope_entries','telescope_entries_tags','telescope_monitoring','sessions'
-- );
