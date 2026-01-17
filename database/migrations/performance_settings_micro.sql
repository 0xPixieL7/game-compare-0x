-- =========================================================
-- DATABASE-LEVEL PERFORMANCE SETTINGS (MICRO)
-- =========================================================
-- Supabase Micro: 1GB RAM, 2-core ARM
-- Applies settings to current database, fallback to ALTER ROLE.

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

    EXECUTE format('alter database %I set min_parallel_table_scan_size = ''32MB''', dbname);
    EXECUTE format('alter database %I set min_parallel_index_scan_size = ''1MB''', dbname);

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
