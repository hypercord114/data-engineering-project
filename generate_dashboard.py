import os
import sys
import logging
import duckdb
from datetime import datetime

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline-logs/morning_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- CONFIGURATION ---
UNIFIED_DB = "environmental_data.db"
OUTPUT_HTML = "index.html"
COMPASS_NODES = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

def build_dashboard():
    logging.info("Starting static dashboard generation (Template)...")
    
    if not os.path.exists(UNIFIED_DB):
        logging.error(f"Database '{UNIFIED_DB}' not found.")
        sys.exit(1)

    # Connect and pull latest forecast data
    conn = duckdb.connect(UNIFIED_DB, read_only=True)
    forecast = conn.execute("""
        SELECT forecast_date, predicted_avg_temp_f, predicted_avg_wind_deg, 
               upwind_cardinal, downwind_cardinal, predicted_avg_humidity_pct
        FROM daily_shift_forecasts 
        ORDER BY forecast_date DESC LIMIT 1;
    """).fetchone()
    conn.close()

    # Fallback if DB is empty
    if not forecast:
        f_date, temp, wind_deg, upwind, downwind, humid = datetime.now().strftime("%Y-%m-%d"), "--", "--", "N/A", "N/A", "--"
    else:
        f_date, temp, wind_deg, upwind, downwind, humid = forecast

    # HTML Boilerplate mirroring the afternoon structure
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Perimeter Air Monitoring Network</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans">

    <div class="max-w-7xl mx-auto px-4 py-8">
        <div class="flex flex-col md:flex-row md:items-center md:justify-between border-b border-slate-800 pb-6 mb-8">
            <div>
                <h1 class="text-3xl font-extrabold tracking-tight bg-gradient-to-r from-slate-400 to-slate-600 bg-clip-text text-transparent">
                    Perimeter PID Telemetry Network
                </h1>
                <p class="text-sm text-slate-400 mt-1">Status: Initialized / Pending Ingestion</p>
            </div>
            <div class="bg-slate-900 border border-slate-800 px-4 py-2 rounded-lg">
                <span class="text-xs uppercase font-semibold text-slate-500 block">Shift Date</span>
                <span class="text-sm font-mono text-slate-400 font-bold">{f_date}</span>
            </div>
        </div>

        <div class="mb-4"><h2 class="text-lg font-bold text-slate-300">Morning Shift Forecast</h2></div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-10">
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800"><span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Shift Temp</span><div class="text-3xl font-bold mt-1 text-amber-400">{temp}°F</div></div>
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800"><span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Wind Path</span><div class="text-3xl font-bold mt-1 text-sky-400">{wind_deg}° ({upwind})</div><div class="text-xs text-slate-500 mt-0.5">Downwind Exposure Path: {downwind}</div></div>
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800"><span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Avg Humidity</span><div class="text-3xl font-bold mt-1 text-indigo-400">{humid}%</div></div>
        </div>

        <div class="mb-6"><h2 class="text-xl font-bold text-slate-200">Active Sensor Boundary Timelines</h2></div>
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-12">
            {''.join([f'''
            <div class="bg-slate-900 p-4 rounded-xl border border-slate-800 flex flex-col justify-between">
                <div class="flex items-center justify-between border-b border-slate-800 pb-2 mb-3">
                    <span class="font-bold text-sm text-slate-500">Node: {node}</span>
                </div>
                <div class="relative w-full h-44 flex items-center justify-center border-t border-slate-800 mt-2">
                    <span class="text-slate-700 text-xs italic">Awaiting Telemetry...</span>
                </div>
            </div>''' for node in COMPASS_NODES])}
        </div>

        <div class="mb-4 border-t border-slate-900 pt-8">
            <h2 class="text-lg font-bold text-slate-300">Observed Shift Reality</h2>
        </div>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800"><span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Observed Avg Temp</span><div class="text-3xl font-bold mt-1 text-slate-700">--</div></div>
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800"><span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Observed Wind Vector</span><div class="text-3xl font-bold mt-1 text-slate-700">--</div></div>
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800"><span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Observed Avg Humidity</span><div class="text-3xl font-bold mt-1 text-slate-700">--</div></div>
        </div>
    </div>
</body>
</html>
"""

    try:
        with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info(f"Dashboard template finalized. Output: {OUTPUT_HTML}")
    except Exception as io_err:
        logging.error(f"Failed to write template: {io_err}")

if __name__ == "__main__":
    build_dashboard()