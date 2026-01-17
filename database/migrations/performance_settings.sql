-- =========================================================
-- DATABASE-LEVEL PERFORMANCE SETTINGS (MICRO)
-- =========================================================
-- Supabase Micro: 1GB RAM, 2-core ARM
-- These settings enable parallel query processing
-- =========================================================

-- PERFORMANCE TRICK: Parallel Query Tuning (4x faster aggregations)
-- =========================================================
-- Enable parallel workers (adjust based on CPU cores)
-- Recommendation: max_parallel_workers = (CPU cores - 1)
-- Supabase often blocks ALTER DATABASE; fall back to ALTER ROLE for current_user.
DO $$
DECLARE
  dbname text := current_database();
  username text := current_user;
BEGIN
  BEGIN
    EXECUTE format('alter database %I set max_parallel_workers = 1', dbname);
    EXECUTE format('alter database %I set max_parallel_workers_per_gather = 1', dbname);
    EXECUTE format('alter database %I set parallel_tuple_cost = 0.1', dbname);
    EXECUTE format('alter database %I set parallel_setup_cost = 200', dbname);

    -- Lower thresholds to trigger parallel execution
    EXECUTE format('alter database %I set min_parallel_table_scan_size = ''32MB''', dbname);
    EXECUTE format('alter database %I set min_parallel_index_scan_size = ''1MB''', dbname);

    -- PERFORMANCE TRICK: Query Planner Tuning
    -- =========================================================
    -- Bias toward index scans (we have good indexes)
    EXECUTE format('alter database %I set random_page_cost = 1.1', dbname);
    EXECUTE format('alter database %I set effective_cache_size = ''512MB''', dbname);
    EXECUTE format('alter database %I set work_mem = ''8MB''', dbname);
    EXECUTE format('alter database %I set maintenance_work_mem = ''64MB''', dbname);
    EXECUTE format('alter database %I set constraint_exclusion = ''partition''', dbname);

  EXCEPTION WHEN insufficient_privilege THEN
    EXECUTE format('alter role %I set max_parallel_workers = 1', username);
    EXECUTE format('alter role %I set max_parallel_workers_per_gather = 1', username);
    EXECUTE format('alter role %I set parallel_tuple_cost = 0.1', username);
    EXECUTE format('alter role %I set parallel_setup_cost = 200', username);

    EXECUTE format('alter role %I set min_parallel_table_scan_size = ''32MB''', username);
    EXECUTE format('alter role %I set min_parallel_index_scan_size = ''1MB''', username);

    EXECUTE format('alter role %I set random_page_cost = 1.1', username);
    EXECUTE format('alter role %I set effective_cache_size = ''512MB''', username);
    EXECUTE format('alter role %I set work_mem = ''8MB''', username);
    EXECUTE format('alter role %I set maintenance_work_mem = ''64MB''', username);
    EXECUTE format('alter role %I set constraint_exclusion = ''partition''', username);
  END;
END $$;

-- PERFORMANCE TRICK: Connection Pooling
-- =========================================================
-- Already handled by your Rust sqlx::PgPool
-- Keep pool size modest: (CPU cores * 1.5) for mixed workload

-- PERFORMANCE TRICK: Maintenance Settings
-- =========================================================
-- Aggressive autovacuum for hot tables (prevents bloat)
-- Note: These are table-specific, not database-level
-- Run after schema creation:
/*
alter table prices set (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_analyze_scale_factor = 0.01,
  fillfactor = 90
);

alter table current_price set (
  autovacuum_vacuum_scale_factor = 0.02,
  fillfactor = 100  -- Read-heavy, pack tight
);

alter table offers set (
  autovacuum_vacuum_scale_factor = 0.05,
  fillfactor = 95
);
*/

-- =========================================================
-- VERIFICATION QUERIES
-- =========================================================
-- Check if parallel workers are enabled:
-- show max_parallel_workers_per_gather;

-- Test parallel execution on large table:
-- explain (analyze, buffers) 
-- select count(*) from prices_2025_11 
-- where recorded_at > now() - interval '30 days';
-- Look for "Parallel Seq Scan" or "Parallel Index Scan" in output

-- Check index usage:
-- select schemaname, tablename, indexname, idx_scan, idx_tup_read
-- from pg_stat_user_indexes
-- where schemaname = 'public'
-- order by idx_scan desc;

-- Check table sizes:
-- select schemaname, tablename, 
--        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
--        pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
--        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as indexes_size
-- from pg_tables
-- where schemaname = 'public'
-- order by pg_total_relation_size(schemaname||'.'||tablename) desc;
