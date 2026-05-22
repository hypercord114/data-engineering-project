import os
import sys
import logging
import duckdb
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- LOGGING CONFIGURATION ---
# Appends logs to the central pipeline log file rather than separate ones
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("morning_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

def build_dashboard():
    db_file = "environmental_data.db"
    output_html = "index.html"
    
    logging.info("--- Launching Dashboard Regeneration Step ---")
    
    if not os.path.exists(db_file):
        logging.error(f"DASHBOARD FAILURE: Database file '{db_file}' not found. Aborting step.")
        sys.exit(1)

    # 1. Fetch the latest forecast details from DuckDB
    logging.info(f"Querying latest environmental profile from {db_file}...")
    connection = duckdb.connect(db_file, read_only=True)
    try:
        query = """
            SELECT 
                forecast_date, 
                predicted_avg_temp_f, 
                predicted_avg_wind_deg, 
                predicted_avg_humidity_pct,
                upwind_cardinal, 
                downwind_cardinal
            FROM daily_shift_forecasts 
            ORDER BY forecast_date DESC 
            LIMIT 1;
        """
        row = connection.execute(query).fetchone()
    except Exception as query_err:
        logging.error(f"DASHBOARD FAILURE: Could not query database. Error: {query_err}")
        sys.exit(1)
    finally:
        connection.close()

    if not row:
        logging.warning("Database table empty. Dashboard generation skipped.")
        return

    f_date, temp, wind_deg, humid, upwind, downwind = row

    # 2. INTRADAY FIELD DATA (Placeholder arrays for afternoon update)
    actual_times = ["07:00", "09:00", "11:00", "13:00", "15:00", "17:00"]
    tvoc_values = [110, 135, 290, 240, 180, 125]
    observed_wind = [35, 38, 42, 36, 32, 34]

    # 3. Create interactive Dual-Axis Graph using Plotly
    logging.info("Assembling interactive multi-axis Plotly telemetry graph...")
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(x=actual_times, y=tvoc_values, name="TVOC Levels (ppb)",
                   line=dict(color="#34d399", width=3), mode='lines+markers'),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(x=actual_times, y=observed_wind, name="Observed Wind (°)",
                   line=dict(color="#38bdf8", width=2, dash='dash'), mode='lines+markers'),
        secondary_y=True
    )

    fig.update_layout(
        title=f"Intraday Operations Metrics — Shift Date: {f_date}",
        title_font=dict(size=18, color="#e2e8f0"),
        paper_bgcolor="#1e293b",
        plot_bgcolor="#0f172a",
        legend=dict(font=dict(color="#f1f5f9")),
        xaxis=dict(gridcolor="#334155", tickfont=dict(color="#94a3b8")),
        margin=dict(l=40, r=40, t=60, b=40)
    )

    fig.update_yaxes(title_text="TVOC Ingestion (ppb)", color="#34d399", gridcolor="#334155", secondary_y=False)
    fig.update_yaxes(title_text="Wind Angle (Degrees)", color="#38bdf8", range=[0, 360], secondary_y=True)

    # 4. Generate the dashboard static HTML payload string
    graph_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

    dashboard_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Operations Hub</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 p-8">
    <div class="max-w-5xl mx-auto">
        <header class="border-b border-slate-800 pb-4 mb-8">
            <h1 class="text-3xl font-bold text-emerald-400">Shift Environmental Dashboard</h1>
            <p class="text-slate-400 text-sm">Target Tracking Date: <span class="text-slate-200 font-medium">{f_date}</span></p>
        </header>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Shift Temp</span>
                <div class="text-3xl font-bold mt-1 text-amber-400">{temp}°F</div>
            </div>
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Predicted Wind Path</span>
                <div class="text-3xl font-bold mt-1 text-sky-400">{wind_deg}° ({upwind})</div>
                <div class="text-xs text-slate-500 mt-0.5">Downwind Exposure Path: {downwind}</div>
            </div>
            <div class="bg-slate-900 p-6 rounded-xl border border-slate-800">
                <span class="text-xs uppercase tracking-wider text-slate-500 font-semibold">Avg Shift Humidity</span>
                <div class="text-3xl font-bold mt-1 text-indigo-400">{humid}%</div>
            </div>
        </div>

        <div class="bg-slate-900 p-4 rounded-xl border border-slate-800 shadow-xl">
            {graph_html}
        </div>
    </div>
</body>
</html>
"""

    try:
        with open(output_html, "w", encoding="utf-8") as f:
            f.write(dashboard_template)
        logging.info(f"Dashboard HTML content successfully bound and written to: {output_html}")
        logging.info("--- Dashboard Generation Phase Finalized ---")
    except Exception as file_err:
        logging.error(f"DASHBOARD FAILURE: Could not compile file template out to disk. Error: {file_err}")
        sys.exit(1)

if __name__ == "__main__":
    build_dashboard()
