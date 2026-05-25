import os
import sys
import json
import logging
import duckdb
from datetime import datetime

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/afternoon_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- UNIFIED ANALYTICS FILE ---
UNIFIED_DB = "environmental_data.db"
OUTPUT_HTML = "index.html"

COMPASS_NODES = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
NODE_COLORS = {
    "N": "#f87171",   # Red
    "NE": "#fb923c",  # Orange
    "E": "#facc15",   # Yellow
    "SE": "#4ade80",  # Green
    "S": "#2dd4bf",   # Teal
    "SW": "#38bdf8",  # Sky
    "W": "#818cf8",   # Indigo
    "NW": "#c084fc"   # Purple
}

def extract_dashboard_datasets():
    """ Extracts forecast, telemetry, and observed metrics from the unified database. """
    logging.info(f"Opening unified environmental data warehouse: {UNIFIED_DB}")
    
    if not os.path.exists(UNIFIED_DB):
        logging.error(f"CRITICAL ERROR: Unified database file '{UNIFIED_DB}' not found.")
        sys.exit(1)

    conn = duckdb.connect(UNIFIED_DB, read_only=True)
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    
    # 1. Fetch latest morning forecast metrics
    try:
        forecast_res = conn.execute("""
            SELECT 
                forecast_date, 
                predicted_avg_temp_f, 
                predicted_avg_wind_deg, 
                upwind_cardinal, 
                downwind_cardinal,
                predicted_avg_humidity_pct
            FROM daily_shift_forecasts 
            ORDER BY forecast_date DESC LIMIT 1;
        """).fetchone()
        
        if not forecast_res:
            forecast_res = (current_date_str, "72", "0", "N/A", "N/A", "50")
    except Exception as f_err:
        logging.warning(f"Failed to query shift forecasts: {f_err}. Using baselines.")
        forecast_res = (current_date_str, "72", "0", "N/A", "N/A", "50")

    # 2. Fetch the afternoon observed metrics
    try:
        observed_res = conn.execute("""
            SELECT 
                observed_avg_temp_f, 
                observed_avg_wind_deg, 
                upwind_cardinal, 
                downwind_cardinal,
                observed_avg_humidity_pct
            FROM daily_shift_weather_observed 
            ORDER BY forecast_date DESC LIMIT 1;
        """).fetchone()
        
        if not observed_res:
            observed_res = ("N/A", "N/A", "N/A", "N/A", "N/A")
    except Exception as o_err:
        logging.warning(f"Failed to query observed weather realities: {o_err}. Using baselines.")
        observed_res = ("N/A", "N/A", "N/A", "N/A", "N/A")

    # 3. Extract intraday time-series streams across the 8 dynamic fact tables
    chart_payloads = {}

    try:
        for node in COMPASS_NODES:
            table_name = f"fact_pid_{node.lower()}"
            
            # Verify the dynamic fact table structure exists before parsing
            table_check = conn.execute(
                f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}';"
            ).fetchone()[0]

            if table_check == 0:
                logging.warning(f"Fact table '{table_name}' does not exist yet. Rendering baseline.")
                chart_payloads[node] = []
                continue

            # Slicing the standard 'HH:MM:SS' string format down to 'HH:MM'
            query = f"""
                SELECT substring(measurement_time::VARCHAR, 1, 5) as t_str, CAST(tvoc_ppb AS DOUBLE)
                FROM {table_name}
                WHERE measurement_date = '{current_date_str}'
                ORDER BY measurement_time ASC;
            """
            rows = conn.execute(query).fetchall()
            
            # Format cleanly for Chart.js timeline injection
            chart_payloads[node] = [{"x": r[0], "y": r[1]} for r in rows]
            logging.info(f"Node-{node} timeline compiled. Datapoints: {len(rows)}")

    except Exception as db_err:
        logging.error(f"Error parsing high-frequency telemetry streams: {db_err}")
        chart_payloads = {node: [] for node in COMPASS_NODES}
    finally:
        conn.close()

    return forecast_res, observed_res, chart_payloads

def build_afternoon_dashboard():
    logging.info("Starting dashboard generation cycle...")
    
    # Extract structural details out of the single data warehouse
    forecast_data, observed_data, telemetry_data = extract_dashboard_datasets()
    f_date, f_temp, f_wind_deg, f_upwind, f_downwind, f_humid = forecast_data
    o_temp, o_wind_deg, o_upwind, o_downwind, o_humid = observed_data
    
    # Build core HTML boilerplate framework
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Perimeter Air Monitoring Network</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns"></script>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans">

    <div class="max-w-7xl mx-auto px-4 py-8">
        
        <div class="flex flex-col md:flex-row md:items-center md:justify-between border-b border-slate-800 pb-6 mb-8">
            <div>
                <h1 class="text-3xl font-extrabold tracking-tight bg-gradient-to-r from-sky-400 to-indigo-500 bg-clip-text text-transparent">
                    Perimeter PID Telemetry Network
                </h1>
                <p class="text-sm text-slate-400 mt-1">Real-Time Fence-Line Micro-Climate & VOC Ingestion Node</p>
            </div>
            <div class="mt-4 md:mt-0 text-left md:text-right bg-slate-900 border border-slate-800 px-4 py-2 rounded-lg">
                <span class="text-xs uppercase font-semibold text-slate-500 block tracking-wider">Shift Date</span>
                <span class="text-sm font-mono text-indigo-400 font-bold">{f_date}</span>
            </div>
        </div>

        <div class="mb-4">
            <h2 class="text-lg font-bold text-slate-300 tracking-tight">Morning Shift Forecast</h2>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-10">
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-md">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Shift Temp</span>
                <div class="text-3xl font-bold mt-1 text-amber-400">{f_temp}°F</div>
            </div>
            
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-md">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Wind Path</span>
                <div class="text-3xl font-bold mt-1 text-sky-400">{f_wind_deg}° ({f_upwind})</div>
                <div class="text-xs text-slate-500 mt-0.5">Downwind Exposure Path: {f_downwind}</div>
            </div>
            
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-md">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Avg Humidity</span>
                <div class="text-3xl font-bold mt-1 text-indigo-400">{f_humid}%</div>
            </div>
        </div>

        <div class="mb-6">
            <h2 class="text-xl font-bold text-slate-200 tracking-tight">Active Sensor Boundary Timelines</h2>
            <p class="text-xs text-slate-500 mt-0.5">Continuous PID micro-volt log entries mapped across active 07:00 to 17:00 core shift windows</p>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-12">
    """

    # Inject layout grid wrappers for the 8 perimeter points
    for node in COMPASS_NODES:
        color = NODE_COLORS[node]
        html_content += f"""
            <div class="bg-slate-900 p-4 rounded-xl border border-slate-800 shadow-sm flex flex-col justify-between">
                <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-3">
                    <span class="font-bold text-sm text-slate-200 tracking-wide">Boundary Node: {node}</span>
                    <span class="w-2.5 h-2.5 rounded-full shadow-sm" style="background-color: {color};"></span>
                </div>
                <div class="relative w-full h-44">
                    <canvas id="chart_{node}"></canvas>
                </div>
            </div>
        """

    # Close Section 2 grid, and append Section 3: Observed Weather Realities
    html_content += f"""
        </div>

        <div class="mb-4 border-t border-slate-900 pt-8">
            <h2 class="text-lg font-bold text-slate-300 tracking-tight">Observed Shift Reality (Verified Meteorological Actuals)</h2>
            <p class="text-xs text-slate-500 mt-0.5">Aggregated actual parameters parsed directly from high-precision intraday weather observation payloads</p>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-md">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Observed Avg Temp</span>
                <div class="text-3xl font-bold mt-1 text-emerald-400">{o_temp}{'°F' if o_temp != 'N/A' else ''}</div>
            </div>
            
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-md">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Observed Wind Vector</span>
                <div class="text-3xl font-bold mt-1 text-teal-400">{o_wind_deg}{'°' if o_wind_deg != 'N/A' else ''} ({o_upwind})</div>
                <div class="text-xs text-slate-500 mt-0.5">Verified Exposure Path: {o_downwind}</div>
            </div>
            
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800 shadow-md">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Observed Avg Humidity</span>
                <div class="text-3xl font-bold mt-1 text-cyan-400">{o_humid}{'%' if o_humid != 'N/A' else ''}</div>
            </div>
        </div>

    </div>

    <script>
        const telemetryData = {json.dumps(telemetry_data)};
        const nodeColors = {json.dumps(NODE_COLORS)};

        Object.keys(telemetryData).forEach(node => {{
            const ctx = document.getElementById(`chart_${{node}}`).getContext('2d');
            
            new Chart(ctx, {{
                type: 'line',
                data: {{
                    datasets: [{{
                        label: `${{node}} Node Ingestion (ppb)`,
                        data: telemetryData[node],
                        borderColor: nodeColors[node],
                        borderWidth: 2,
                        pointRadius: 0,
                        pointHoverRadius: 4,
                        fill: false,
                        tension: 0.15
                    }}]
                }},
                options: {{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {{
                        legend: {{ display: false }},
                        tooltip: {{
                            mode: 'index',
                            intersect: false,
                            backgroundColor: '#1e293b',
                            titleColor: '#94a3b8',
                            bodyColor: '#f1f5f9',
                            borderColor: '#334155',
                            borderWidth: 1
                        }}
                    }},
                    scales: {{
                        x: {{
                            type: 'time',
                            time: {{
                                parser: 'HH:mm',
                                unit: 'hour',
                                displayFormats: {{ hour: 'HH:mm' }}
                            }},
                            min: '07:00',
                            max: '17:00',
                            grid: {{ color: '#1e293b' }},
                            ticks: {{ color: '#64748b', font: {{ size: 9 }} }}
                        }},
                        y: {{
                            beginAtZero: true,
                            suggestedMax: 150,
                            grid: {{ color: '#1e293b' }},
                            ticks: {{ color: '#64748b', font: {{ size: 9 }} }}
                        }}
                    }}
                }}
            }});
        }});
    </script>
</body>
</html>
"""

    # Flush the updated web layout to root space
    try:
        with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info(f"Dashboard rewrite finalized. Output target flushed to: {OUTPUT_HTML}")
    except Exception as io_err:
        logging.error(f"Failed to execute static file write transaction: {io_err}")

if __name__ == "__main__":
    build_afternoon_dashboard()