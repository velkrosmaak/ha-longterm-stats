import os
import pytest
import datetime
from ha_longterm_stats.db import Database

@pytest.fixture
def temp_db(tmp_path):
    db_path = str(tmp_path / "test_ha_stats.db")
    return Database(db_path)

def test_entity_upsert_and_toggle(temp_db):
    temp_db.upsert_entity(
        entity_id="sensor.living_room_temperature",
        friendly_name="Living Room Temperature",
        unit_of_measurement="°C",
        device_class="temperature",
        state_class="measurement"
    )

    entities = temp_db.get_entities()
    assert len(entities) == 1
    assert entities[0]["entity_id"] == "sensor.living_room_temperature"
    assert entities[0]["friendly_name"] == "Living Room Temperature"
    assert entities[0]["enabled"] == 1

    temp_db.set_entity_enabled("sensor.living_room_temperature", False)
    entities = temp_db.get_entities()
    assert entities[0]["enabled"] == 0

def test_entity_sorting(temp_db):
    temp_db.upsert_entity("sensor.alpha", "Alpha Sensor")
    temp_db.upsert_entity("sensor.beta", "Beta Sensor")
    
    # Update last_seen for beta later
    temp_db.upsert_entity("sensor.beta", "Beta Sensor")

    sorted_by_name = temp_db.get_entities(sort_by="name")
    assert sorted_by_name[0]["entity_id"] == "sensor.alpha"

    sorted_by_newest = temp_db.get_entities(sort_by="last_updated_desc")
    assert sorted_by_newest[0]["entity_id"] == "sensor.beta"

def test_static_sensor_detection(temp_db):
    temp_db.upsert_entity("sensor.constant", "Constant Sensor")
    temp_db.upsert_entity("sensor.dynamic", "Dynamic Sensor")

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    later = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)).isoformat()

    # Constant sensor: same value twice
    temp_db.insert_reading("sensor.constant", now, 10.0, "10.0")
    temp_db.insert_reading("sensor.constant", later, 10.0, "10.0")

    # Dynamic sensor: different values
    temp_db.insert_reading("sensor.dynamic", now, 10.0, "10.0")
    temp_db.insert_reading("sensor.dynamic", later, 12.0, "12.0")

    entities = temp_db.get_entities()
    const_ent = [e for e in entities if e["entity_id"] == "sensor.constant"][0]
    dyn_ent = [e for e in entities if e["entity_id"] == "sensor.dynamic"][0]

    assert const_ent["is_static"] is True
    assert dyn_ent["is_static"] is False

    # Test filtering
    static_only = temp_db.get_entities(is_static_filter=True)
    assert len(static_only) == 1
    assert static_only[0]["entity_id"] == "sensor.constant"



def test_sensor_readings_and_rollups(temp_db):
    entity_id = "sensor.outdoor_temp"
    temp_db.upsert_entity(entity_id=entity_id, friendly_name="Outdoor Temp", unit_of_measurement="°C", device_class="temperature")

    # Insert sample readings over a 2 day span
    base_time = datetime.datetime(2026, 1, 15, 10, 0, 0, tzinfo=datetime.timezone.utc)
    readings = []
    
    # Day 1: 10:00 to 12:00
    readings.append((entity_id, (base_time).isoformat(), 15.0, "15.0"))
    readings.append((entity_id, (base_time + datetime.timedelta(minutes=30)).isoformat(), 17.0, "17.0"))
    readings.append((entity_id, (base_time + datetime.timedelta(hours=1)).isoformat(), 20.0, "20.0"))
    
    # Day 2: 10:00 to 11:00
    day2_time = base_time + datetime.timedelta(days=1)
    readings.append((entity_id, (day2_time).isoformat(), 5.0, "5.0"))
    readings.append((entity_id, (day2_time + datetime.timedelta(minutes=30)).isoformat(), 9.0, "9.0"))

    temp_db.insert_readings_batch(readings)

    # Compute rollups
    temp_db.calculate_rollups()

    # Query hourly stats
    hourly = temp_db.get_time_series(entity_id=entity_id, interval="hour", metric="avg")
    assert len(hourly) >= 2

    # Query daily stats
    daily = temp_db.get_time_series(entity_id=entity_id, interval="day", metric="avg")
    assert len(daily) == 2
    
    # Check Day 1 daily min / max / avg
    day1_stat = [d for d in daily if d["time"] == "2026-01-15"][0]
    assert day1_stat["min_val"] == 15.0
    assert day1_stat["max_val"] == 20.0
    assert day1_stat["avg_val"] == pytest.approx(17.333, abs=0.01)

    # Query Monthly stats
    monthly = temp_db.get_time_series(entity_id=entity_id, interval="month", metric="avg")
    assert len(monthly) == 1
    assert monthly[0]["time"] == "2026-01"

    # Query Summary
    summary = temp_db.get_sensor_summary(entity_id)
    assert summary["latest"]["value"] == 9.0
    assert summary["stats_all_time"]["min"] == 5.0
    assert summary["stats_all_time"]["max"] == 20.0
