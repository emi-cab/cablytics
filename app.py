"""
CABlytics — main Flask application.

V1 routes (/analyze, /analyze-with-ai, /health) are preserved in ga4_api.py
and imported here unchanged.

V2 adds:
  - Blueprint registered at /v2/
  - APScheduler started on app startup
"""

from flask import Flask
from ga4_api import app as v1_app

# Import and register V2 blueprint
from v2.routes import v2 as v2_blueprint
from v2.scheduler import start_scheduler

# Register V2 blueprint on the existing V1 app
v1_app.register_blueprint(v2_blueprint)

# Start the scheduler (runs in background thread, survives across requests)
start_scheduler()

# Export as `app` so Gunicorn picks it up via `app:app` in the Procfile
app = v1_app

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
