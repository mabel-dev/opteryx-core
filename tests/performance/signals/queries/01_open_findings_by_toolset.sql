-- PLACEHOLDER. Replace/extend with real customer-shaped queries as they're
-- identified -- this suite is meant to grow into complex, realistic queries
-- against the synthetic signals dataset (dev/generate_signals_data.py),
-- one query per file.
SELECT
    toolset,
    priority,
    COUNT(*) AS finding_count,
    COUNT(DISTINCT ip) AS distinct_hosts
FROM {DATASET}
WHERE is_open = TRUE
  AND priority IN ('High', 'Medium')
GROUP BY toolset, priority
ORDER BY finding_count DESC;
