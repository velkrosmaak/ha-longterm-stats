import pytest
from pathlib import Path
from fastapi.testclient import TestClient
from ha_longterm_stats.db import Database
from ha_longterm_stats.ha_client import HomeAssistantClient
from ha_longterm_stats.api import app, init_app

@pytest.fixture
def client(tmp_path):
    db_path = str(tmp_path / "test_api.db")
    database = Database(db_path)
    ha_client = HomeAssistantClient("http://localhost:8123", "test_token", database)
    
    templates_dir = str(Path(__file__).parent.parent / "ha_longterm_stats" / "templates")
    init_app(database_instance=database, client_instance=ha_client, template_dir=templates_dir)

    # Insert mock sensor
    database.upsert_entity("sensor.test_temp", "Test Temperature", "°C", "temperature")
    database.insert_reading("sensor.test_temp", "2026-01-10T12:00:00Z", 22.5, "22.5")
    database.calculate_rollups()

    return TestClient(app)

def test_dashboard_endpoint(client):
    res = client.get("/")
    assert res.status_code == 200
    assert "HA Long-Term Stats" in res.text

def test_status_endpoint(client):
    res = client.get("/api/status")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "ok"
    assert "db" in data
    assert data["db"]["total_entities"] >= 1

def test_sensors_endpoint(client):
    res = client.get("/api/sensors")
    assert res.status_code == 200
    sensors = res.json()
    assert len(sensors) == 1
    assert sensors[0]["entity_id"] == "sensor.test_temp"

def test_stats_endpoint(client):
    res = client.get("/api/stats?entity_ids=sensor.test_temp&interval=day&metric=avg")
    assert res.status_code == 200
    data = res.json()
    assert "sensor.test_temp" in data["data"]
    assert len(data["data"]["sensor.test_temp"]) == 1

def test_export_csv_endpoint(client):
    res = client.get("/api/export?entity_id=sensor.test_temp&interval=day")
    assert res.status_code == 200
    assert "text/csv" in res.headers["content-type"]
    assert "sensor_test_temp" in res.headers["content-disposition"]
