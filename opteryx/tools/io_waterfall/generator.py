# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
HTML waterfall chart generator for IO traces.

Creates an interactive ECharts-based visualization showing file operations
on a timeline (like Chrome DevTools Network tab).
"""

import json
from pathlib import Path
from typing import Optional

from opteryx.tools.io_waterfall.reader import TraceReader


def generate_waterfall_html(trace_file: str, output_file: Optional[str] = None) -> str:
    """
    Generate an interactive HTML waterfall chart from a trace file.

    Args:
        trace_file: Path to .jsonl trace file
        output_file: Output HTML path (default: trace_file.html)

    Returns:
        Path to the generated HTML file
    """
    trace_path = Path(trace_file)
    output_path = Path(output_file or str(trace_path).replace(".jsonl", ".html"))

    reader = TraceReader(trace_file)
    metadata = reader.metadata()
    operations = reader.operation_timelines()
    stats = reader.statistics()

    # keep raw events for drill-down support
    all_events = list(reader.events())
    # group events by file_id for quick lookup in JS
    events_by_file: dict[str, list] = {}
    for ev in all_events:
        fid = ev.get("file_id")
        if fid:
            events_by_file.setdefault(fid, []).append(ev)

    # row order (needed for click drill-down)
    ordered_file_ids = [op.get("file_id") for op in operations]

    # build chart configuration using helper
    echarts_config = _build_echarts_config(operations, metadata)

    # render HTML page
    html = _render_html_template(echarts_config, metadata, stats, events_by_file, ordered_file_ids)

    # write to disk
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    return str(output_path)


def _build_echarts_config(operations: list, metadata: dict) -> dict:
    """Build ECharts configuration for waterfall chart.

    This logic was previously part of ``generate_waterfall_html`` but is
    factored out so the CLI can validate the configuration independently and
    to keep the main function concise.  The layout is identical to the
    earlier implementation, including phases and zoom behaviour.
    """
    # Find time boundaries (seconds)
    all_times = []
    for timeline in operations:
        for key in ["download_start", "download_complete", "decode_start", "decode_complete"]:
            if timeline.get(key) is not None:
                all_times.append(timeline[key])

    min_time = min(all_times) if all_times else 0
    max_time = max(all_times) if all_times else 1

    def t(v):
        """Normalize a timestamp to seconds relative to first event."""
        return (v - min_time) if v is not None else None

    # Short names for y-axis labels
    short_names = [op.get("label", "unknown") for op in operations]

    # Data format (array per operation):
    # [catIndex, dl_start, dl_end, buf_start, buf_end, dec_start, dec_end, rows, name,
    #  component, rg_idx, bytes]
    series_data = []
    for i, tl in enumerate(operations):
        dl_start = t(tl.get("download_start"))
        dl_end = t(tl.get("download_complete"))
        dec_start = t(tl.get("decode_start"))
        dec_end = t(tl.get("decode_complete"))
        buf_start = dl_end
        buf_end = dec_start
        rows = tl.get("rows_decoded", 0) or 0
        series_data.append(
            [
                i,
                dl_start,
                dl_end,
                buf_start,
                buf_end,
                dec_start,
                dec_end,
                rows,
                short_names[i],
                tl.get("component"),
                tl.get("rg_idx"),
                tl.get("bytes_received", 0) or 0,
            ]
        )

    return {
        "title": {
            "text": "IO Waterfall Chart",
            "subtext": f"Query: {metadata.get('query', 'Unknown')[:120]}",
        },
        "tooltip": {
            "trigger": "item",
            "confine": True,
        },
        "grid": {
            "left": "5px",
            "right": "30px",
            "top": "60px",
            "bottom": "60px",
            "containLabel": True,
        },
        "xAxis": {
            "type": "value",
            "name": "Time (s)",
            "nameLocation": "end",
            "min": 0,
            "max": round(max_time - min_time, 3),
        },
        "yAxis": {
            "type": "category",
            "data": short_names,
            "inverse": True,
            "axisLabel": {"fontSize": 11},
        },
        "series": [
            {
                "type": "custom",
                "renderItem": "RENDER_ITEM_PLACEHOLDER",
                "data": series_data,
                "encode": {"y": 0, "x": [1, 2]},
                "tooltip": {"formatter": "TOOLTIP_PLACEHOLDER"},
            }
        ],
        "dataZoom": [
            {
                "type": "slider",
                "yAxisIndex": [0],
                "start": 0,
                "end": min(100, max(20, round(1200 / max(len(short_names), 1)))),
                "width": 20,
                "right": 8,
            },
            {"type": "slider", "xAxisIndex": [0], "start": 0, "end": 100, "bottom": 10},
        ],
    }


def _render_html_template(
    echarts_config: dict,
    metadata: dict,
    stats: dict,
    events_by_file: dict,
    ordered_file_ids: list,
) -> str:
    """Render the HTML template with embedded chart configuration.

    ``events_by_file`` is a mapping file_id→list-of-events used by the
    embedded drill-down click handler.  ``ordered_file_ids`` must correspond
    to the order of rows in the waterfall chart so we can index into the map.
    """

    query_text = metadata.get("query", "No query text available")
    session_id = metadata.get("session_id", "Unknown")

    stats_html = _format_stats(stats)

    # JSON-serialize the auxiliary data for embedding in the page
    events_json = json.dumps(events_by_file)
    file_id_list_json = json.dumps(ordered_file_ids)

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>IO Waterfall Chart</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            padding: 20px;
        }}
        h1 {{ color: #333; margin-top: 0; }}
        .metadata {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            border-left: 4px solid #4A90E2;
        }}
        .metadata-item {{ margin: 5px 0; font-size: 14px; color: #666; }}
        .metadata-label {{ font-weight: bold; color: #333; display: inline-block; width: 150px; }}
        .query-text {{
            background-color: #f0f0f0;
            padding: 10px;
            border-radius: 4px;
            font-family: monospace;
            font-size: 12px;
            word-break: break-all;
            margin-top: 10px;
        }}
        #chart {{ width: 100%; height: 640px; margin-bottom: 20px; }}
        .stats-section {{
            background-color: #f9f9f9;
            padding: 15px;
            border-radius: 4px;
            margin-top: 20px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-top: 15px;
        }}
        .stat-card {{
            background: white;
            padding: 15px;
            border-radius: 4px;
            border: 1px solid #e0e0e0;
        }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #4A90E2; margin: 8px 0 4px; }}
        .stat-label {{ font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 1px; }}
        .legend {{
            display: flex;
            gap: 30px;
            margin-top: 10px;
            font-size: 14px;
        }}
        .legend-item {{ display: flex; align-items: center; gap: 8px; }}
        .legend-color {{ width: 18px; height: 14px; border-radius: 2px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>IO Waterfall Chart</h1>
        <div class="metadata">
            <div class="metadata-item">
                <span class="metadata-label">Session ID:</span>
                <code>{session_id}</code>
            </div>
            <div class="metadata-item">
                <span class="metadata-label">Query:</span>
                <div class="query-text">{query_text}</div>
            </div>
        </div>

        <div id="chart"></div>

        <div class="legend">
            <div class="legend-item">
                <div class="legend-color" style="background:#4A90E2"></div>
                <span>Download Phase</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background:#F5A623"></div>
                <span>Buffer Phase</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background:#7ED321"></div>
                <span>Decode Phase</span>
            </div>
        </div>

        <div class="stats-section">
            <h3>Summary Statistics</h3>
            {stats_html}
        </div>
    </div>

    <script>
        var chart = echarts.init(document.getElementById('chart'));

        // embed raw events for debugging/drill-down
        var traceEvents = {events_json};

        chart.on('click', function(params) {{
            // params.dataIndex corresponds to the row index
            var idx = params.dataIndex;
            var fileId = {file_id_list_json}[idx];
            console.log('Events for', fileId, traceEvents[fileId] || []);
        }});

        // Data layout (per operation):
        // [catIndex, dl_start, dl_end, buf_start, buf_end, dec_start, dec_end,
        //  rows, name, component, rg_idx, bytes]

        function renderItem(params, api) {{
            var cat      = api.value(0);   // category index
            var dlStart  = api.value(1);
            var dlEnd    = api.value(2);
            var bufStart = api.value(3);
            var bufEnd   = api.value(4);
            var decStart = api.value(5);
            var decEnd   = api.value(6);

            var barH = Math.max(api.size([0, 1])[1] * 0.55, 4);

            function makeBar(xStart, xEnd, color) {{
                if (xStart == null || xEnd == null || xEnd <= xStart) return null;
                var p0 = api.coord([xStart, cat]);
                var p1 = api.coord([xEnd,   cat]);
                var w  = Math.max(p1[0] - p0[0], 1);
                return {{
                    type: 'rect',
                    shape: {{ x: p0[0], y: p0[1] - barH / 2, width: w, height: barH }},
                    style: api.style({{ fill: color, opacity: 0.88 }})
                }};
            }}

            var items = [
                makeBar(dlStart,  dlEnd,  '#4A90E2'),
                makeBar(bufStart, bufEnd, '#F5A623'),
                makeBar(decStart, decEnd, '#7ED321'),
            ].filter(Boolean);

            return {{ type: 'group', children: items }};
        }}

        function tooltipFmt(params) {{
            var d = params.data;
            var name = d[8];
            var dl  = d[2] != null && d[1] != null ? ((d[2]-d[1])*1000).toFixed(0)+'ms' : '—';
            var buf = d[4] != null && d[3] != null ? ((d[4]-d[3])*1000).toFixed(0)+'ms' : '—';
            var dec = d[6] != null && d[5] != null ? ((d[6]-d[5])*1000).toFixed(0)+'ms' : '—';
            var rows = d[7] ? d[7].toLocaleString() : '—';
            var component = d[9] || 'file';
            var rg = d[10] != null ? d[10] : '—';
            var bytes = d[11] || 0;
            return '<b>' + name + '</b><br/>'
                 + 'Component: ' + component + '<br/>'
                 + 'Row Group: ' + rg + '<br/>'
                 + 'Download: ' + dl + '<br/>'
                 + 'Buffer: '   + buf + '<br/>'
                 + 'Decode: '   + dec + '<br/>'
                 + 'Bytes: ' + bytes.toLocaleString() + '<br/>'
                 + 'Rows: '     + rows;
        }}

        var option = {json.dumps(echarts_config)};
        option.series[0].renderItem = renderItem;
        option.series[0].tooltip = {{ formatter: tooltipFmt }};

        chart.setOption(option);
        window.addEventListener('resize', function() {{ chart.resize(); }});
    </script>
</body>
</html>"""

    return html


def _format_stats(stats: dict) -> str:
    """Format statistics as HTML."""

    def format_bytes(b):
        for unit in ["B", "KB", "MB", "GB"]:
            if b < 1024:
                return f"{b:.1f} {unit}"
            b /= 1024
        return f"{b:.1f} TB"

    def format_time(ms):
        if ms < 1000:
            return f"{ms:.0f} ms"
        return f"{ms / 1000:.2f} s"

    html = '<div class="stats-grid">'

    stats_items = [
        ("Total Files", str(stats["total_files"]), ""),
        ("Total Ops", str(stats.get("total_operations", 0)), ""),
        ("Download Ops", str(stats.get("total_download_ops", 0)), ""),
        ("Decode Ops", str(stats.get("total_decode_ops", 0)), ""),
        ("Footer Downloads", str(stats.get("footer_download_ops", 0)), ""),
        ("Rowgroup Downloads", str(stats.get("rowgroup_download_ops", 0)), ""),
        ("Rowgroup Decodes", str(stats.get("rowgroup_decode_ops", 0)), ""),
        ("Total Data", format_bytes(stats["total_bytes"]), ""),
        ("Total Rows", f"{stats['total_rows']:,}", ""),
        ("Query Duration", format_time(stats["query_duration_ms"]), ""),
        ("Download Phase", format_time(stats["download_phase_duration_ms"]), ""),
        ("Decode Phase", format_time(stats["decode_phase_duration_ms"]), ""),
        ("Avg Download/Op", format_time(stats["avg_download_time_ms"]), ""),
        ("Avg Decode/Op", format_time(stats["avg_decode_time_ms"]), ""),
        ("Max Concurrent", str(stats["max_concurrent_downloads"]), "downloads"),
    ]

    for label, value, unit in stats_items:
        html += f"""
        <div class="stat-card">
            <div class="stat-label">{label}</div>
            <div class="stat-value">{value}</div>
            {f'<div style="font-size: 12px; color: #999;">{unit}</div>' if unit else ""}
        </div>
        """

    html += "</div>"
    return html
