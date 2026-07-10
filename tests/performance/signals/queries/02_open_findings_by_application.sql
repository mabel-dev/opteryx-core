-- derived from a real user query
SELECT
  uniqueapp,
  finding_ref,
  is_risk_accepted
FROM
  scratch.signals_synthetic AS o1
  CROSS JOIN UNNEST(application) AS uniqueapp
  INNER JOIN (
    SELECT
      finding_ref AS uvid,
      MAX(first_seen_at) AS lfd
    FROM
      scratch.signals_synthetic AS o2
    GROUP BY
      finding_ref
  ) AS lastest ON finding_ref = uvid
  AND first_seen_at = lfd
WHERE
  uniqueapp IN ('Cascade Gateway', 'Zenith Hub')
  AND first_seen_at::TIMESTAMP[s] >= '2026-01-01'::date
  AND host_support_group NOT LIKE '%F5%'
