"""
Shared query bodies for the odata_dashboard benchmark suite.

Query shapes mined from the odata.opteryx.app query log (a live OData dashboard
serving geopolitics.gdelt_events and 4 security tables). The dashboard polls the
same handful of shapes over and over with only a sliding timestamp / clicked
country / clicked vendor changing - this suite captures each distinct SHAPE
once rather than replaying literal duplicates.

Table references are left as `{GDELT}` / `{NVD}` / `{EXPLOITED}` / `{VPW}` /
`{EXPLOITDB}` placeholders so both the Opteryx runner (testdata.public.<table>)
and the DuckDB baseline runner (read_parquet('testdata/public/<table>/*.parquet'))
can format the SAME query bodies against their own table syntax - one set of
queries, no drift between the two.

gdelt_events is a 1,000,000-row KEYSET-PAGINATED SAMPLE (oldest rows by
global_event_id, not a random sample) - its date_added column only spans
2026-01-01..2026-01-11, so its query constants are chosen from that window,
not the literal "last 24h" timestamps seen in the live log. The 4 security
tables are pulled in FULL (367k/1.6k/367k/47k rows) so their constants are the
literal ones observed in the log.
"""

# fmt: off
QUERIES = [

    # --- gdelt_events: "since last poll" heartbeat --------------------------
    ("01", "SELECT COUNT(*) AS n, SUM(num_articles) AS arts FROM {GDELT} WHERE date_added >= '2026-01-06T00:00:00+00:00'::TIMESTAMP;"),

    # --- gdelt_events: low/med-cardinality GROUP BY, all filtered the same way
    ("02", "SELECT quad_class, SUM(num_articles) AS arts, SUM(goldstein_scale) AS g_sum, COUNT(*) AS n FROM {GDELT} WHERE date_added >= '2026-01-06T00:00:00+00:00'::TIMESTAMP AND quad_class IS NOT NULL GROUP BY quad_class;"),
    ("03", "SELECT event_root_code, SUM(num_articles) AS arts, COUNT(*) AS n FROM {GDELT} WHERE date_added >= '2026-01-06T00:00:00+00:00'::TIMESTAMP AND event_root_code IS NOT NULL GROUP BY event_root_code;"),
    ("04", "SELECT action_geo_country_code, SUM(avg_tone) AS tone_sum, SUM(num_articles) AS arts, COUNT(*) AS n FROM {GDELT} WHERE date_added >= '2026-01-06T00:00:00+00:00'::TIMESTAMP AND action_geo_country_code IS NOT NULL GROUP BY action_geo_country_code;"),
    ("05", "SELECT date_added, SUM(num_articles) AS arts, COUNT(*) AS n FROM {GDELT} WHERE date_added >= '2026-01-06T00:00:00+00:00'::TIMESTAMP GROUP BY date_added;"),

    # --- gdelt_events: top-40 leaderboard (ORDER BY 2 cols + wide projection)
    ("06", "SELECT actor1_name, actor2_name, event_root_code, goldstein_scale, avg_tone, num_mentions, num_sources, num_articles, action_geo_full_name, source_url FROM {GDELT} WHERE date_added >= '2026-01-06T00:00:00+00:00'::TIMESTAMP AND source_url IS NOT NULL ORDER BY num_mentions DESC, num_articles DESC LIMIT 40;"),

    # --- gdelt_events: bilateral drill-down (user clicked country = USA) ----
    ("07", "SELECT actor1_country_code, SUM(goldstein_scale) AS goldstein_sum, SUM(avg_tone) AS tone_sum, COUNT(*) AS n FROM {GDELT} WHERE date_added >= '2026-01-01T00:00:00+00:00'::TIMESTAMP AND actor2_country_code = 'USA' AND actor1_country_code IS NOT NULL AND actor1_country_code != 'USA' AND goldstein_scale IS NOT NULL GROUP BY actor1_country_code;"),
    ("08", "SELECT event_date, SUM(goldstein_scale) AS g_sum, COUNT(*) AS n FROM {GDELT} WHERE date_added >= '2026-01-01T00:00:00+00:00'::TIMESTAMP AND actor1_country_code = 'USA' AND actor2_country_code IS NOT NULL AND actor2_country_code != 'USA' AND goldstein_scale IS NOT NULL GROUP BY event_date;"),
    ("09", "SELECT actor1_name, actor2_name, event_root_code, goldstein_scale, avg_tone, num_mentions, num_sources, num_articles, action_geo_full_name, source_url FROM {GDELT} WHERE date_added >= '2026-01-01T00:00:00+00:00'::TIMESTAMP AND (actor1_country_code = 'GBR' OR actor2_country_code = 'GBR') AND source_url IS NOT NULL ORDER BY num_mentions DESC, num_articles DESC LIMIT 40;"),

    # --- gdelt_events: range discovery, pathological filter, schema probe ---
    ("10", "SELECT MIN(event_date) AS mn, MAX(event_date) AS mx FROM {GDELT};"),
    ("11", "SELECT COUNT(*) AS count FROM {GDELT} WHERE event_date >= '3000-01-01'::TIMESTAMP;"),
    ("12", "SELECT * FROM {GDELT} LIMIT 2;"),

    # --- gdelt_events: unfiltered ("all time" view) variants of 02/04/01 ----
    ("13", "SELECT quad_class, SUM(num_articles) AS arts, SUM(goldstein_scale) AS g_sum, COUNT(*) AS n FROM {GDELT} WHERE quad_class IS NOT NULL GROUP BY quad_class;"),
    ("14", "SELECT action_geo_country_code, COUNT(*) AS n, SUM(num_articles) AS arts, SUM(avg_tone) AS tsum, SUM(goldstein_scale) AS gsum FROM {GDELT} WHERE action_geo_country_code IS NOT NULL GROUP BY action_geo_country_code;"),
    ("15", "SELECT SUM(num_articles) AS arts, SUM(num_mentions) AS ment, SUM(num_sources) AS src FROM {GDELT};"),

    # --- nvd_vulnerabilities: count with time filter ------------------------
    ("16", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP;"),

    # --- nvd_vulnerabilities: CVSS severity histogram sent as 5 separate
    # round trips (this is real traffic - the app does not batch these)
    ("17", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score >= 9.0;"),
    ("18", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score >= 7.0 AND cvss_score < 9.0;"),
    ("19", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score >= 4.0 AND cvss_score < 7.0;"),
    ("20", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score < 4.0;"),
    ("21", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score >= 7.0;"),
    # ... and the single-query GROUP BY CASE this *could* have been - same
    # answer, one round trip instead of five. `GROUP BY 1` (positional) groups by
    # the CASE at select position 1 — the convention every peer engine uses.
    ("22", "SELECT CASE WHEN cvss_score >= 9.0 THEN '9.0+' WHEN cvss_score >= 7.0 THEN '7.0-9.0' WHEN cvss_score >= 4.0 THEN '4.0-7.0' WHEN cvss_score IS NOT NULL THEN '<4.0' ELSE 'unscored' END AS bucket, COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP GROUP BY 1;"),

    # --- nvd_vulnerabilities: null-check counts, sum+count for an average ---
    ("23", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score IS NOT NULL;"),
    ("24", "SELECT COUNT(*) AS count FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score IS NULL;"),
    ("25", "SELECT SUM(cvss_score) AS total, COUNT(*) AS n FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP;"),

    # --- nvd_vulnerabilities: vendor GROUP BY (~36k distinct - genuinely
    # high cardinality, unlike gdelt's low/med-card groups above)
    ("26", "SELECT vendor, COUNT(*) AS count FROM {NVD} WHERE vendor IS NOT NULL AND published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP GROUP BY vendor;"),
    ("27", "SELECT vendor, MIN(published_at) AS first_seen FROM {NVD} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP GROUP BY vendor;"),

    # --- nvd_vulnerabilities: substring filter (contains(vendor, ...)) ------
    ("28", "SELECT COUNT(*) AS count FROM {NVD} WHERE vendor LIKE '%microsoft%';"),
    ("29", "SELECT COUNT(*) AS count FROM {NVD} WHERE vendor LIKE '%microsoft%' AND cvss_score >= 7.0;"),

    # --- nvd_vulnerabilities: unfiltered baseline ---------------------------
    ("30", "SELECT COUNT(*) AS count FROM {NVD};"),

    # --- exploited_vulnerabilities: projection scan (cisa_kev x nvd join) ---
    ("31", "SELECT cve_id, cvss_score, cvss_vector FROM {EXPLOITED} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP;"),
    ("32", "SELECT cve_id, cvss_score, cvss_vector FROM {EXPLOITED};"),

    # --- vulnerabilities_per_week: week GROUP BY ----------------------------
    ("33", "SELECT week, COUNT(*) AS scored FROM {VPW} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP AND cvss_score IS NOT NULL GROUP BY week;"),
    ("34", "SELECT week, COUNT(*) AS total FROM {VPW} WHERE published_at >= '2023-07-19T11:02:46+00:00'::TIMESTAMP GROUP BY week;"),

    # --- exploit_db: projection scan, count, unfiltered count --------------
    ("35", "SELECT date_published FROM {EXPLOITDB} WHERE date_published >= '2023-07-19T11:02:46+00:00'::TIMESTAMP;"),
    ("36", "SELECT COUNT(*) AS count FROM {EXPLOITDB} WHERE date_published >= '2023-07-19T11:02:46+00:00'::TIMESTAMP;"),
    ("37", "SELECT COUNT(*) AS count FROM {EXPLOITDB};"),
]
# fmt: on
