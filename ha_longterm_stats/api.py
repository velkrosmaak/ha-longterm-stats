import io
import csv
import logging
from typing import Optional, List
from fastapi import FastAPI, Query, HTTPException, Response, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from ha_longterm_stats.config import config
from ha_longterm_stats.db import Database
from ha_longterm_stats.ha_client import HomeAssistantClient

logger = logging.getLogger("ha_longterm_stats.api")

app = FastAPI(title="Home Assistant Long-Term Stats", version="0.1.0")

# Shared instances set on startup
db: Optional[Database] = None
ha_client: Optional[HomeAssistantClient] = None
templates: Optional[Jinja2Templates] = None

class EntityToggleRequest(BaseModel):
    enabled: bool

def init_app(database_instance: Database, client_instance: HomeAssistantClient, template_dir: str):
    global db, ha_client, templates
    db = database_instance
    ha_client = client_instance
    templates = Jinja2Templates(directory=template_dir)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    if not templates:
        return HTMLResponse("Templates not initialized", status_code=500)
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/api/status")
async def get_status():
    if not db or not ha_client:
        raise HTTPException(status_code=500, detail="Database or HA client not initialized")
    
    db_status = db.get_db_status()
    return {
        "status": "ok",
        "ha_connected": ha_client.is_connected,
        "ha_last_connected": ha_client.last_connected_at,
        "ha_last_error": ha_client.last_error,
        "ha_url": config.ha_url,
        "db": db_status
    }

@app.get("/api/sensors")
async def get_sensors(
    enabled_only: bool = Query(False),
    device_class: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("name", description="name, last_updated_desc, last_updated_asc"),
    is_static: Optional[bool] = Query(None, description="Filter for static value sensors (true/false)")
):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    return db.get_entities(enabled_only=enabled_only, device_class=device_class, sort_by=sort_by, is_static_filter=is_static)



@app.post("/api/sensors/{entity_id:path}/toggle")
async def toggle_sensor(entity_id: str, payload: EntityToggleRequest):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    db.set_entity_enabled(entity_id, payload.enabled)
    return {"status": "success", "entity_id": entity_id, "enabled": payload.enabled}

@app.get("/api/stats")
async def get_stats(
    entity_ids: str = Query(..., description="Comma-separated entity IDs"),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None),
    interval: str = Query("day", description="raw, hour, day, week, month, year"),
    metric: str = Query("avg", description="avg, min, max, sum")
):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    
    entities = [e.strip() for e in entity_ids.split(",") if e.strip()]
    results = {}
    for eid in entities:
        results[eid] = db.get_time_series(
            entity_id=eid,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
            metric=metric
        )
    return {
        "interval": interval,
        "metric": metric,
        "data": results
    }

@app.get("/api/stats/summary")
async def get_summary(entity_id: str = Query(...)):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    res = db.get_sensor_summary(entity_id)
    if not res:
        raise HTTPException(status_code=404, detail="Entity not found")
    return res

@app.get("/api/stats/heatmap")
async def get_heatmap(
    entity_id: str = Query(...),
    year: Optional[int] = Query(None)
):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    heatmap_data = db.get_heatmap_data(entity_id=entity_id, year=year)
    return {
        "entity_id": entity_id,
        "year": year,
        "data": heatmap_data
    }

@app.get("/api/stats/compare")
async def get_compare(
    entity_id: str = Query(...),
    period_type: str = Query("month", description="month"),
    metric: str = Query("avg", description="avg, min, max, sum")
):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    return db.get_period_comparison(entity_id=entity_id, period_type=period_type, metric=metric)

@app.post("/api/trigger-rollup")
async def trigger_rollup():
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    db.calculate_rollups()
    return {"status": "success", "message": "Rollups calculated successfully"}

@app.get("/api/export")
async def export_csv(
    entity_id: str = Query(...),
    interval: str = Query("day"),
    start_time: Optional[str] = Query(None),
    end_time: Optional[str] = Query(None)
):
    if not db:
        raise HTTPException(status_code=500, detail="Database not initialized")
    
    data = db.get_time_series(
        entity_id=entity_id,
        start_time=start_time,
        end_time=end_time,
        interval=interval,
        metric="avg"
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["time", "value", "min_val", "max_val", "avg_val"])
    for row in data:
        writer.writerow([
            row.get("time", ""),
            row.get("value", ""),
            row.get("min_val", ""),
            row.get("max_val", ""),
            row.get("avg_val", "")
        ])

    output.seek(0)
    filename = f"{entity_id.replace('.', '_')}_{interval}_export.csv"
    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
