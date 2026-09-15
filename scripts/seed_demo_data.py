import os
import random
import datetime
from ha_longterm_stats.db import Database

def seed_demo_data(db_path: str = "data/ha_stats.db"):
    print(f"Seeding demo multi-year data into {db_path}...")
    db = Database(db_path)

    sensors = [
        ("sensor.outdoor_temperature", "Outdoor Temperature", "°C", "temperature"),
        ("sensor.living_room_temperature", "Living Room Temperature", "°C", "temperature"),
        ("sensor.outdoor_humidity", "Outdoor Humidity", "%", "humidity"),
        ("sensor.solar_power", "Solar Power Output", "W", "power"),
        ("sensor.daily_energy_consumption", "Daily Energy Consumption", "kWh", "energy"),
        ("sensor.calibration_offset", "Calibration Offset Constant", "°C", "temperature"),
    ]


    for entity_id, name, unit, device_class in sensors:
        db.upsert_entity(
            entity_id=entity_id,
            friendly_name=name,
            unit_of_measurement=unit,
            device_class=device_class,
            state_class="measurement"
        )

    # Generate readings over the last 365 days
    now = datetime.datetime.now(datetime.timezone.utc)
    start_date = now - datetime.timedelta(days=365)
    
    current_time = start_date
    batch = []
    
    # 6-hour interval steps to generate realistic trends without huge overhead
    step = datetime.timedelta(hours=6)

    print("Generating simulated historical sensor states...")
    while current_time <= now:
        day_of_year = current_time.timetuple().tm_yday
        hour = current_time.hour
        ts_str = current_time.isoformat()

        # Seasonal temperature curve (min in Jan, max in July)
        seasonal_temp = 12 + 10 * (-1 * (0.5 - abs((day_of_year - 180) / 180.0)))
        diurnal = 4 * (hour / 24.0)

        # Outdoor Temp
        out_temp = round(seasonal_temp + diurnal + random.uniform(-2.5, 2.5), 1)
        batch.append(("sensor.outdoor_temperature", ts_str, out_temp, str(out_temp)))

        # Indoor Temp (buffered around 21°C)
        in_temp = round(20.5 + 0.2 * (out_temp - 15) + random.uniform(-0.8, 0.8), 1)
        batch.append(("sensor.living_room_temperature", ts_str, in_temp, str(in_temp)))

        # Humidity
        hum = round(max(30.0, min(95.0, 70.0 - out_temp + random.uniform(-10, 10))), 1)
        batch.append(("sensor.outdoor_humidity", ts_str, hum, str(hum)))

        # Solar Power
        solar = round(max(0.0, 3000 * (1 - abs(hour - 13) / 6.0) + random.uniform(-200, 200)), 1) if 7 <= hour <= 19 else 0.0
        batch.append(("sensor.solar_power", ts_str, solar, str(solar)))

        # Energy
        energy = round(15.0 + diurnal * 2 + random.uniform(-2, 4), 2)
        batch.append(("sensor.daily_energy_consumption", ts_str, energy, str(energy)))

        # Static sensor constant reading
        batch.append(("sensor.calibration_offset", ts_str, 0.5, "0.5"))

        current_time += step


    db.insert_readings_batch(batch)
    print(f"Inserted {len(batch)} raw sensor readings. Computing hourly and daily rollups...")
    db.calculate_rollups()
    print("✨ Demo data seeding complete! Database is ready.")

if __name__ == "__main__":
    seed_demo_data()
