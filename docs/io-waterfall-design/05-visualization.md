# IO Waterfall Design - Visualization

## Output: Interactive HTML Waterfall Chart

Similar to Chrome DevTools Network tab, showing each file as a horizontal bar with phases colored differently.

### Visual Design

```
IO Waterfall Chart - "SELECT * FROM table"

Timeline (ms):
0        500        1000       1500       2000       2500

file.parquet.1   ██░░░░░░░░██░░░░░░░░░░██████████
file.parquet.2      ██░░░░░░░░██░░░░░░░░░░██████████
file.parquet.3         ██░░░░░░░░██░░░░░░░███████████
file.parquet.4            ██░░░░░░░░██████████
file.parquet.5               ██░░░░░████████

Legend:
██ = Downloading (Blue)
░░ = Buffering (Yellow)
██ = Decoding (Green)
```

### Interactive Features

1. **Hover Details**
   ```
   Hovering over bar for file.parquet.1:
   
   File: s3://bucket/year=2024/file.parquet.1
   Size: 1.0 MB
   
   Download: 234.5 ms
     Started: 2026-02-17 14:30:22.156
     Ended:   2026-02-17 14:30:22.391
     Speed: 4.3 MB/s
   
   Buffering: 12.3 ms
     (waiting for decode)
   
   Decode: 89.2 ms
     Rows: 12,345
     Batches: 5
     Throughput: 138.4K rows/s
   ```

2. **Click to Expand**
   - Click file name → zoom into that file's timeline
   - Click phase → highlight all phases of that type across files
   - (implementation note) In the current tool a click will also log the
     underlying trace events for that file to the browser console, allowing
     developers to inspect footer/row-group/column activity and build a
     richer drill-down UI later.

3. **Filtering/Sorting**
   ```
   Sort by:
     - Download duration (longest first)
     - Total duration
     - File size
     - Alphabetical
   
   Filter by:
     - Size range (1-10 MB, 10-100 MB, etc.)
     - Duration range
     - Connector type (S3, Local, etc.)
   ```

4. **Timeline Controls**
   ```
   [Zoom In] [Zoom Out] [Reset]
   
   Show grid lines every: 100ms | 500ms | 1s | 5s | 10s
   ```

5. **Statistics Panel**
   ```
   ┌─────────────────────────────────────┐
   │ Summary Statistics                  │
   ├─────────────────────────────────────┤
   │ Total Files: 42                     │
   │ Total Size: 52.4 MB                 │
   │ Query Duration: 2.45s               │
   │                                     │
   │ Download Phase:                     │
   │   Avg: 234.5 ms (min: 45, max: 892) │
   │   Total Time: 1.2s                  │
   │   Avg Bandwidth: 516 MB/s           │
   │                                     │
   │ Decode Phase:                       │
   │   Avg: 89.2 ms (min: 12, max: 345)  │
   │   Total Time: 3.7s                  │
   │   Avg Throughput: 512K rows/s       │
   │                                     │
   │ Max Concurrency:                    │
   │   Downloads: 4 simultaneous         │
   │   Decodes: 4 simultaneous           │
   │   Both: 2 simultaneous              │
   └─────────────────────────────────────┘
   ```

6. **Timeline Markers**
   ```
   Mark query start, end, major phase transitions
   
   Query Start ────────────────────────────── Query End
   │                                          │
   0ms                                     2450ms
   
   File discovery: 0-5ms
   Download phase: 5-1200ms
   Decode phase: 1200-2450ms
   ```

## Implementation Technology Stack

### Format: HTML5 + Canvas/SVG + JavaScript

**Canvas**: Better for rendering 1000s of bars
**SVG**: Better for interactive elements, text

Hybrid approach:
- Canvas for bars (performance)
- SVG overlays for labels, grid, interactive zones
- Canvas for animation when zooming

### Library Options

#### Option A: Plotly.js (Recommended for MVP)
```javascript
import Plotly from 'plotly.js-dist'

// Use a gantt-like configuration
const bars = [
  {
    name: 'file.parquet.1',
    x: [[start_download, end_download], 
        [end_download, start_decode],
        [start_decode, end_decode]],
    y: [['download'], ['buffer'], ['decode']],
    marker: {color: ['#4A90E2', '#F5D76E', '#7ED321']},
    type: 'bar',
    orientation: 'h'
  }
  // ... more files
]

Plotly.newPlot('waterfall', bars, layout, {responsive: true})
```

**Pros**: 
- Built-in interactivity
- Responsive
- Minimal custom code
- Good hover/click handling

**Cons**:
- Larger bundle size
- Less custom control

#### Option B: D3.js (More Control)
```javascript
import * as d3 from 'd3'

const scale = d3.scaleLinear()
  .domain([minTime, maxTime])
  .range([0, width])

const svg = d3.select('#waterfall')
  .append('svg')
  .attr('width', width)
  .attr('height', height)

// Render bars with custom logic
svg.selectAll('.file')
  .data(files)
  .enter()
  .append('g')
  .attr('class', 'file')
  .attr('transform', (d, i) => `translate(0, ${i * barHeight})`)
  .each(function(file) {
    // Draw download, buffer, decode phases
  })
```

**Pros**:
- Complete control over rendering
- Smaller core library
- Excellent for custom visualization

**Cons**:
- More boilerplate code
- Steeper learning curve
- Manual interactivity handling

#### Option C: Apache ECharts (Good Balance)
```javascript
import * as echarts from 'echarts'

const option = {
  tooltip: { trigger: 'axis' },
  grid: { top: 80, bottom: 60, left: 200 },
  xAxis: {
    type: 'time',
    min: minTime,
    max: maxTime
  },
  yAxis: {
    type: 'category',
    data: fileNames
  },
  series: [
    {
      type: 'custom',
      renderItem: (params, api) => {
        // Render download phase
        // Render buffer phase
        // Render decode phase
      },
      data: formattedData
    }
  ]
}

const chart = echarts.init(document.getElementById('waterfall'))
chart.setOption(option)
```

**Pros**:
- Good balance of features and control
- Excellent performance
- Rich interactivity
- Good TypeScript support

**Cons**:
- Smaller ecosystem than D3/Plotly
- Learning curve for custom renderItem

### Recommendation: **ECharts** for initial implementation

- Handles large datasets efficiently
- Custom renderItem allows exact visual we need
- Good hover/tooltip support
- Responsive by default

## Python Tool: Chart Generation

```python
# opteryx/tools/io_waterfall/generate.py

from opteryx.tools.io_waterfall import TraceReader
import json

def generate_waterfall_html(trace_file, output_file=None):
    """
    Generate interactive HTML waterfall chart from trace data
    
    Args:
        trace_file: Path to io_trace_*.jsonl file
        output_file: Output HTML path (default: trace_file.html)
    
    Returns:
        HTML file path
    """
    
    reader = TraceReader(trace_file)
    metadata = reader.metadata()
    
    # Parse events into file timelines
    files = {}
    for event in reader.events():
        file_id = event.get('file_id')
        if file_id not in files:
            files[file_id] = {}
        
        if event['type'] == 'download_start':
            files[file_id]['download_start'] = event['timestamp']
        elif event['type'] == 'download_complete':
            files[file_id]['download_end'] = event['timestamp']
            files[file_id]['bytes_received'] = event.get('bytes_received')
        elif event['type'] == 'decode_start':
            files[file_id]['decode_start'] = event['timestamp']
        elif event['type'] == 'decode_complete':
            files[file_id]['decode_end'] = event['timestamp']
            files[file_id]['rows'] = event.get('rows_decoded')
            files[file_id]['batches'] = event.get('batches')
    
    # Compute metrics
    start_time = min(f['download_start'] for f in files.values() 
                     if 'download_start' in f)
    end_time = max(f['decode_end'] for f in files.values() 
                   if 'decode_end' in f)
    
    # Generate ECharts config
    echarts_config = build_echarts_config(files, start_time, end_time, metadata)
    
    # Render HTML
    html = render_html_template(echarts_config, metadata)
    
    output_file = output_file or trace_file.replace('.jsonl', '.html')
    with open(output_file, 'w') as f:
        f.write(html)
    
    return output_file
```

## Chart Data Format for ECharts

```python
def build_echarts_config(files, start_time, end_time, metadata):
    return {
        'title': {
            'text': f"IO Waterfall - {metadata['query'][:60]}",
            'subtext': f"Session: {metadata['session_id']}"
        },
        'tooltip': {
            'trigger': 'axis',
            'confine': True,
            'formatter': tooltip_formatter
        },
        'timeAxis': {
            'type': 'time',
            'min': int(start_time * 1000),
            'max': int(end_time * 1000),
            'axisLabel': {'formatter': '{HH}:{mm}:{ss}.{SSS}'}
        },
        'yAxis': {
            'type': 'category',
            'data': sorted(files.keys()),
            'axisLabel': {'width': 200, 'overflow': 'truncate'}
        },
        'series': [{
            'type': 'custom',
            'renderItem': render_phase_rectangles,
            'data': [
                {
                    'file_id': fid,
                    'download_start': f.get('download_start'),
                    'download_end': f.get('download_end'),
                    'decode_start': f.get('decode_start'),
                    'decode_end': f.get('decode_end'),
                    'size_bytes': f.get('bytes_received'),
                    'rows': f.get('rows')
                }
                for fid, f in files.items()
            ]
        }],
        'dataZoom': [{
            'type': 'slider',
            'show': True,
            'yAxisIndex': [0],
            'start': 0,
            'end': min(100, 5000 / len(files))  # Show ~50 files at a time
        }]
    }
```

## Custom ECharts renderItem Function

```javascript
function render_phase_rectangles(params, api) {
    const yIndex = api.value(0);  // File index
    const item = params.data;
    
    const results = [];
    
    // Download phase (blue)
    if (item.download_start && item.download_end) {
        const rect = api.rect({
            x: api.coord([item.download_start * 1000, yIndex])[0],
            y: api.coord([0, yIndex])[1],
            width: api.coord([item.download_end * 1000, yIndex])[0] - 
                   api.coord([item.download_start * 1000, yIndex])[0],
            height: api.size([0, 1])[1]
        });
        
        results.push({
            type: 'rect',
            shape: rect,
            style: api.style({
                fill: '#4A90E2',
                opacity: 0.8
            }),
            emphasis: {
                style: { opacity: 1, lineWidth: 2 }
            }
        });
    }
    
    // Buffer phase (yellow)
    if (item.download_end && item.decode_start) {
        // Similar rect rendering
    }
    
    // Decode phase (green)
    if (item.decode_start && item.decode_end) {
        // Similar rect rendering
    }
    
    return results;
}
```

## Export Options

```python
def export_waterfall(trace_file, format='html', output_file=None):
    """
    Export waterfall in different formats
    
    Args:
        format: 'html', 'png', 'svg', 'csv', 'json'
    
    Returns:
        Path to exported file
    """
    
    if format == 'html':
        return generate_waterfall_html(trace_file, output_file)
    
    elif format == 'png':
        # Use Playwright/Selenium to render HTML as PNG
        from playwright.async_api import async_playwright
        async def screenshot():
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page = await browser.new_page()
                await page.goto(f'file://{html_file}')
                await page.screenshot(path=output_file)
                await browser.close()
        
        html_file = generate_waterfall_html(trace_file)
        asyncio.run(screenshot())
        return output_file
    
    elif format == 'csv':
        # Export as timeline CSV for Excel/etc
        reader = TraceReader(trace_file)
        df = pd.DataFrame([
            {
                'file': event['file_id'],
                'phase': phase_from_event(event),
                'timestamp': event['timestamp'],
                'duration_ms': ...,
            }
            for event in reader.events()
        ])
        return df.to_csv(output_file or trace_file.replace('.jsonl', '.csv'))
    
    elif format == 'json':
        # Structured JSON for programmatic access
        reader = TraceReader(trace_file)
        output = {
            'metadata': reader.metadata(),
            'files': {} # organized file timeline data
        }
        return json.dump(output, output_file or trace_file.replace('.jsonl', '.json'))
```

## CLI Usage

```bash
# Generate from trace file
python -m opteryx.tools.io_waterfall trace /tmp/io_trace.jsonl

# Output to specific file
python -m opteryx.tools.io_waterfall trace /tmp/io_trace.jsonl -o /tmp/waterfall.html

# Various formats
python -m opteryx.tools.io_waterfall trace /tmp/io_trace.jsonl --format png --output waterfall.png
python -m opteryx.tools.io_waterfall trace /tmp/io_trace.jsonl --format csv --output data.csv

# Statistics only (no chart)
python -m opteryx.tools.io_waterfall stats /tmp/io_trace.jsonl
```

## Performance for Large Datasets

Expected traces:
- 1000 files → ~5000 events → 500KB JSONLines → <1MB HTML
- 10000 files → ~50000 events → 5MB JSONLines → ~10MB HTML

Rendering:
- ECharts can render 10,000 bars smoothly with dataZoom
- Consider rendering blocks (e.g., 100 files per view) for very large traces

Optimization strategies if needed:
1. Server-side aggregation (group files into buckets)
2. Streaming visualization (load trace incrementally)
3. Binary format for faster parsing (later iteration)
