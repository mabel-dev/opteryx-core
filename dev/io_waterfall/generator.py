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

from .reader import TraceReader


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

    # collect operator execution timeline for second chart
    exec_ops, _t0, total_duration = reader.exec_timelines()
    exec_config = _build_exec_echarts_config(exec_ops, total_duration)

    # collect per-operator profile totals for third chart
    profiles = reader.operator_profiles()
    profile_config = _build_profile_echarts_config(profiles)

    # render HTML page
    html = _render_html_template(
        echarts_config,
        metadata,
        stats,
        events_by_file,
        ordered_file_ids,
        exec_config=exec_config,
        profile_config=profile_config,
    )

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
        for key in [
            "download_start",
            "download_complete",
            "buffer_start",
            "buffer_complete",
            "decode_start",
            "decode_complete",
        ]:
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
        buf_start = t(tl.get("buffer_start")) if tl.get("buffer_start") is not None else dl_end
        buf_end = (
            t(tl.get("buffer_complete")) if tl.get("buffer_complete") is not None else buf_start
        )
        dec_start = t(tl.get("decode_start")) if tl.get("decode_start") is not None else buf_end
        dec_end = (
            t(tl.get("decode_complete")) if tl.get("decode_complete") is not None else dec_start
        )
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


# Fixed palette for operator execution chart — 15 visually distinct colours.
_OP_PALETTE = [
    "#4A90E2",
    "#E24A4A",
    "#7ED321",
    "#F5A623",
    "#9B59B6",
    "#1ABC9C",
    "#E67E22",
    "#C0392B",
    "#2ECC71",
    "#3498DB",
    "#F39C12",
    "#8E44AD",
    "#16A085",
    "#D35400",
    "#27AE60",
]


def _build_exec_echarts_config(
    exec_ops: list,
    total_duration: Optional[float],
) -> Optional[dict]:
    """Build ECharts config for the operator execution waterfall.

    Returns None when there are no operator_execute events to display.

    Lanes are per operator *instance* (keyed by operator_id).  When multiple
    instances share the same name they get a numeric suffix: "Parquet Read [1]",
    "Parquet Read [2]".

    Non-producing calls (hash-build phase, zero-row outputs) are rendered at
    reduced opacity so they are visually distinct from probe/producing bars.
    """
    if not exec_ops:
        return None

    # Determine lane order from first-appearance of each operator_id.
    id_to_name: dict = {}
    id_order: list = []  # operator_id strings in first-appearance order
    for op in exec_ops:
        oid = op["operator_id"]
        if oid not in id_to_name:
            id_to_name[oid] = op["operator_name"]
            id_order.append(oid)

    # Build labels: deduplicate per name with numeric suffix.
    name_to_ids: dict = {}
    for oid in id_order:
        name = id_to_name[oid]
        name_to_ids.setdefault(name, []).append(oid)

    id_to_label: dict = {}
    for name, ids in name_to_ids.items():
        if len(ids) == 1:
            id_to_label[ids[0]] = name
        else:
            for idx, oid in enumerate(ids, 1):
                id_to_label[oid] = f"{name} [{idx}]"

    categories = [id_to_label[oid] for oid in id_order]
    id_to_cat: dict = {oid: i for i, oid in enumerate(id_order)}

    # Assign one colour per label (i.e. per instance).
    color_map = {oid: _OP_PALETTE[i % len(_OP_PALETTE)] for i, oid in enumerate(id_order)}

    max_time = total_duration or max(op["wall_end"] for op in exec_ops)

    series_data = [
        [
            id_to_cat[op["operator_id"]],  # 0: catIndex
            op["wall_start"],  # 1: start_s
            op["wall_end"],  # 2: end_s
            op["rows_out"],  # 3: rows_out
            id_to_label[op["operator_id"]],  # 4: label
            round(op["duration_ns"] / 1e6, 3),  # 5: duration_ms
            color_map[op["operator_id"]],  # 6: color
            1 if op.get("produced_rows", True) else 0,  # 7: 1=probe 0=build
        ]
        for op in exec_ops
    ]

    return {
        "categories": categories,
        "max_time": round(max_time, 3),
        "data": series_data,
    }


def _build_profile_echarts_config(profiles: list) -> Optional[dict]:
    """Build config for the operator profile (EXPLAIN ANALYZE) bar chart.

    Returns a dict with a ``data`` list where each entry has:
        label, total_ms, rows_in, rows_out, calls, color, selectivity.
    Returns None when there are no operator_execute events.
    """
    if not profiles:
        return None

    # Deduplicate labels exactly as the exec chart does.
    name_to_ids: dict = {}
    for p in profiles:
        name_to_ids.setdefault(p["operator_name"], []).append(p["operator_id"])

    id_to_label: dict = {}
    for name, ids in name_to_ids.items():
        if len(ids) == 1:
            id_to_label[ids[0]] = name
        else:
            for idx, oid in enumerate(ids, 1):
                id_to_label[oid] = f"{name} [{idx}]"

    data = []
    for i, p in enumerate(profiles):
        oid = p["operator_id"]
        data.append(
            {
                "label": id_to_label[oid],
                "total_ms": round(p["total_duration_ns"] / 1e6, 1),
                "rows_in": p["total_rows_in"],
                "rows_out": p["total_rows_out"],
                "calls": p["call_count"],
                "color": _OP_PALETTE[i % len(_OP_PALETTE)],
                "selectivity": p["selectivity"],
            }
        )

    return {"data": data}


def _render_html_template(
    echarts_config: dict,
    metadata: dict,
    stats: dict,
    events_by_file: dict,
    ordered_file_ids: list,
    exec_config: Optional[dict] = None,
    profile_config: Optional[dict] = None,
) -> str:
    """Render the HTML template with embedded chart configuration.

    ``events_by_file`` is a mapping file_id→list-of-events used by the
    embedded drill-down click handler.  ``ordered_file_ids`` must correspond
    to the order of rows in the waterfall chart so we can index into the map.
    """

    query_text = metadata.get("query", "No query text available")
    session_id = metadata.get("session_id", "Unknown")

    stats_html = _format_stats(stats)

    # Build exec chart assets (empty string / null when no exec events).
    exec_chart_height = 0
    exec_section_html = ""
    exec_config_json = "null"
    if exec_config and exec_config.get("data"):
        exec_chart_height = max(220, len(exec_config.get("categories", [])) * 40 + 90)
        exec_config_json = json.dumps(exec_config)
        seen_ops: dict = {}
        for d in exec_config["data"]:
            if d[4] not in seen_ops:
                seen_ops[d[4]] = d[6]
        legend_items = "".join(
            f'<div class="legend-item">'
            f'<div class="legend-color" style="background:{color}"></div>'
            f"<span>{name}</span></div>"
            for name, color in seen_ops.items()
        )
        exec_section_html = (
            f'<div style="margin-top:30px;border-top:1px solid #e0e0e0;padding-top:20px;">'
            f'<h2 style="color:#333;margin:0 0 12px 0;font-size:18px;">'
            f"Operator Execution Waterfall</h2>"
            f'<div id="exec-chart" style="width:100%;height:{exec_chart_height}px;'
            f'margin-bottom:10px;"></div>'
            f'<div class="legend">{legend_items}</div>'
            f"</div>"
        )

    # Build operator profile (EXPLAIN ANALYZE) chart assets.
    profile_section_html = ""
    profile_config_json = "null"
    if profile_config and profile_config.get("data"):
        profile_chart_height = max(160, len(profile_config["data"]) * 52 + 70)
        profile_config_json = json.dumps(profile_config)
        profile_section_html = (
            f'<div style="margin-top:30px;border-top:1px solid #e0e0e0;padding-top:20px;">'
            f'<h2 style="color:#333;margin:0 0 4px 0;font-size:18px;">'
            f"Operator Profile</h2>"
            f'<p style="color:#888;font-size:12px;margin:0 0 12px 0;">'
            f"Cumulative CPU time per operator. "
            f"Labels show rows&nbsp;in&nbsp;→&nbsp;out and selectivity.</p>"
            f'<div id="profile-chart" style="width:100%;height:{profile_chart_height}px;">'
            f"</div></div>"
        )

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
        #io-chart {{ width: 100%; height: 640px; margin-bottom: 20px; }}
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

        <div id="io-chart"></div>

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

        {exec_section_html}

        {profile_section_html}

        <div class="stats-section">
            <h3>Summary Statistics</h3>
            {stats_html}
        </div>
    </div>

    <script>
        var ioChart = echarts.init(document.getElementById('io-chart'));

        // embed raw events for debugging/drill-down
        var traceEvents = {events_json};

        ioChart.on('click', function(params) {{
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

        ioChart.setOption(option);
        window.addEventListener('resize', function() {{ ioChart.resize(); }});

        // ── Operator Execution Waterfall ──────────────────────────────────
        var execConfig = {exec_config_json};
        if (execConfig && execConfig.data && execConfig.data.length > 0) {{
            var execChart = echarts.init(document.getElementById('exec-chart'));

            function renderExecItem(params, api) {{
                var cat    = api.value(0);
                var start  = api.value(1);
                var end    = api.value(2);
                var color  = api.value(6);
                var isProbe = api.value(7) === 1;
                var barH   = Math.max(api.size([0, 1])[1] * 0.45, 4);
                var p0 = api.coord([start, cat]);
                var p1 = api.coord([end,   cat]);
                var w  = Math.max(p1[0] - p0[0], 1);
                return {{
                    type: 'rect',
                    shape: {{ x: p0[0], y: p0[1] - barH / 2, width: w, height: barH }},
                    style: api.style({{ fill: color, opacity: isProbe ? 0.85 : 0.28 }})
                }};
            }}

            execChart.setOption({{
                grid: {{ left: '5px', right: '30px', top: '20px', bottom: '50px', containLabel: true }},
                tooltip: {{ trigger: 'item', confine: true }},
                xAxis: {{
                    type: 'value', name: 'Time (s)', nameLocation: 'end',
                    min: 0, max: execConfig.max_time,
                }},
                yAxis: {{
                    type: 'category', data: execConfig.categories,
                    inverse: true, axisLabel: {{ fontSize: 11 }},
                }},
                series: [{{
                    type: 'custom',
                    renderItem: renderExecItem,
                    data: execConfig.data,
                    encode: {{ y: 0, x: [1, 2] }},
                    tooltip: {{
                        formatter: function(params) {{
                            var d = params.data;
                            var phase = d[7] === 1 ? 'probe / emit' : 'build / no rows';
                            var rowsLine = d[7] === 1
                                ? 'Rows out: ' + (d[3] || 0).toLocaleString() + '<br/>'
                                : '';
                            return '<b>' + d[4] + '</b><br/>'
                                 + 'Phase: ' + phase + '<br/>'
                                 + 'Duration: ' + d[5].toFixed(1) + 'ms<br/>'
                                 + rowsLine;
                        }}
                    }}
                }}],
                dataZoom: [
                    {{ type: 'slider', xAxisIndex: [0], start: 0, end: 100, bottom: 10 }},
                ],
            }});
            window.addEventListener('resize', function() {{ execChart.resize(); }});
        }}
        // ── Operator Profile (EXPLAIN ANALYZE) ─────────────────────────
        var profileConfig = {profile_config_json};
        if (profileConfig && profileConfig.data && profileConfig.data.length > 0) {{
            var profileChart = echarts.init(document.getElementById('profile-chart'));
            var pLabels = profileConfig.data.map(function(d) {{ return d.label; }});
            profileChart.setOption({{
                grid: {{ left: '5px', right: '220px', top: '10px', bottom: '30px', containLabel: true }},
                tooltip: {{
                    trigger: 'axis',
                    axisPointer: {{ type: 'shadow' }},
                    formatter: function(params) {{
                        var d = profileConfig.data[params[0].dataIndex];
                        var rowsIn  = d.rows_in  > 0 ? d.rows_in.toLocaleString()  : '\u2014';
                        var rowsOut = d.rows_out > 0 ? d.rows_out.toLocaleString() : '\u2014';
                        var sel = d.selectivity !== null
                            ? d.selectivity.toFixed(3) + '%' : 'N/A';
                        return '<b>' + d.label + '</b><br/>'
                             + 'CPU: ' + d.total_ms.toLocaleString() + ' ms<br/>'
                             + 'Calls: ' + d.calls.toLocaleString() + '<br/>'
                             + 'Rows in: ' + rowsIn + '<br/>'
                             + 'Rows out: ' + rowsOut + '<br/>'
                             + 'Selectivity: ' + sel;
                    }}
                }},
                xAxis: {{ type: 'value', name: 'CPU time (ms)', nameLocation: 'end' }},
                yAxis: {{
                    type: 'category',
                    data: pLabels,
                    inverse: true,
                    axisLabel: {{ fontSize: 12 }},
                }},
                series: [{{
                    type: 'bar',
                    data: profileConfig.data.map(function(d) {{
                        return {{ value: d.total_ms, itemStyle: {{ color: d.color }} }};
                    }}),
                    label: {{
                        show: true,
                        position: 'right',
                        formatter: function(params) {{
                            var d = profileConfig.data[params.dataIndex];
                            var out = d.rows_out.toLocaleString();
                            if (d.rows_in > 0) {{
                                var inp = d.rows_in.toLocaleString();
                                var sel = d.selectivity !== null
                                    ? ' (' + d.selectivity.toFixed(1) + '%)' : '';
                                return inp + ' \u2192 ' + out + sel;
                            }}
                            return out + ' rows';
                        }},
                        fontSize: 11,
                        color: '#444',
                    }},
                    barMaxWidth: 40,
                }}],
            }});
            window.addEventListener('resize', function() {{ profileChart.resize(); }});
        }}
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
