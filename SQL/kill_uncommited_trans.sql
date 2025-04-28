SELECT pid, usename, state, query, xact_start, query_start, backend_start
FROM pg_stat_activity
WHERE state = 'idle in transaction';


SELECT pg_terminate_backend(670694);