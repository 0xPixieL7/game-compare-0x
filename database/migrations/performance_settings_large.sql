-- =========================================================
-- DATABASE-LEVEL PERFORMANCE SETTINGS (LARGE)
-- =========================================================
-- Supabase Large: 8GB RAM, 2-core ARM
-- Applies settings to current database, fallback to ALTER ROLE.

DO $$
DECLARE
  dbname text := current_database();
  username text := current_user;
BEGIN
  BEGIN
    EXECUTE format('alter database %I set max_parallel_workers = 2', dbname);
    EXECUTE format('alter database %I set max_parallel_workers_per_gather = 2', dbname);
    EXECUTE format('alter database %I set parallel_tuple_cost = 0.05', dbname);
    EXECUTE format('alter database %I set parallel_setup_cost = 100', dbname);

    EXECUTE format('alter database %I set min_parallel_table_scan_size = ''8MB''', dbname);
    EXECUTE format('alter database %I set min_parallel_index_scan_size = ''512kB''', dbname);

    EXECUTE format('alter database %I set random_page_cost = 1.1', dbname);
    EXECUTE format('alter database %I set effective_cache_size = ''4GB''', dbname);
    EXECUTE format('alter database %I set work_mem = ''32MB''', dbname);
    EXECUTE format('alter database %I set maintenance_work_mem = ''256MB''', dbname);
    EXECUTE format('alter database %I set constraint_exclusion = ''partition''', dbname);
  EXCEPTION WHEN insufficient_privilege THEN
    EXECUTE format('alter role %I set max_parallel_workers = 2', username);
    EXECUTE format('alter role %I set max_parallel_workers_per_gather = 2', username);
    EXECUTE format('alter role %I set parallel_tuple_cost = 0.05', username);
    EXECUTE format('alter role %I set parallel_setup_cost = 100', username);

    EXECUTE format('alter role %I set min_parallel_table_scan_size = ''8MB''', username);
    EXECUTE format('alter role %I set min_parallel_index_scan_size = ''512kB''', username);

    EXECUTE format('alter role %I set random_page_cost = 1.1', username);
    EXECUTE format('alter role %I set effective_cache_size = ''4GB''', username);
    EXECUTE format('alter role %I set work_mem = ''32MB''', username);
    EXECUTE format('alter role %I set maintenance_work_mem = ''256MB''', username);
    EXECUTE format('alter role %I set constraint_exclusion = ''partition''', username);
  END;
END $$;
