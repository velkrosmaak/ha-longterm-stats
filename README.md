# Home Assistant Long-Term Sensor Statistics & Dashboard

A lightweight, standalone Python application that records Home Assistant sensor states long-term in an optimized SQLite database with automatic hourly/daily rollup aggregations, and provides a modern web interface for analyzing yearly, monthly, weekly, and daily statistics.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg)
![SQLite](https://img.shields.io/badge/SQLite-WAL-blue.svg)
![ECharts](https://img.shields.io/badge/ECharts-5.5-red.svg)

---

## Key Features

- **⚡ Real-Time Home Assistant Ingestion**: Async WebSocket connection (`/api/websocket`) with automatic snapshot ingestion, state change listening, and auto-reconnection.
- **📊 Long-Term Data Storage & Rollups**:
  - SQLite with Write-Ahead Logging (WAL) for zero-dependency high-throughput writing.
  - Automated continuous rollup calculations (`hourly_stats` and `daily_stats`) allowing instant queries across multi-year datasets.
  - Optional raw data pruning while preserving long-term rollup statistics indefinitely.
- **🎨 Interactive Modern Dashboard**:
  - **Time Series & Range Bands**: Visualizing temperature, humidity, energy, and power metrics with min/max envelope bands.
  - **Yearly Calendar Heatmap**: GitHub-style daily calendar heatmap for temperature & environmental metrics across full years.
  - **Period-Over-Period Comparison**: Compare month-by-month performance across multiple years.
  - **Sensor Summary Cards**: Quick view of 24h, 7d, 30d, 1y, and all-time record highs/lows.
  - **Sensor Management**: Search, filter by category/device class, and toggle logging on/off per sensor.
  - **CSV Export**: One-click CSV export for any sensor time series.

---

## Quick Start

### 1. Installation

Clone the repository and install requirements:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configuration

Copy `.env.example` to `.env` and set your Home Assistant URL and Long-Lived Access Token:

```bash
cp .env.example .env
```

Edit `.env`:

```env
HA_URL=http://homeassistant.local:8123
HA_TOKEN=your_long_lived_access_token_here
DB_PATH=data/ha_stats.db
PORT=8080
```

> **How to get a Long-Lived Access Token in Home Assistant**:
> 1. Log into your Home Assistant UI.
> 2. Click on your profile icon (bottom left).
> 3. Scroll down to **Security** -> **Long-Lived Access Tokens**.
> 4. Click **Create Token**, give it a name (e.g. `HA Long-Term Stats`), copy the token into your `.env`.

---

### 3. Run the Dashboard

```bash
PYTHONPATH=. python main.py
```

Open your browser at `http://localhost:8080`.

---

## Demo Mode (No Home Assistant Needed for Testing)

To test the dashboard immediately with simulated multi-year temperature, humidity, and solar power data:

```bash
PYTHONPATH=. python scripts/seed_demo_data.py
PYTHONPATH=. python main.py
```

---

## Deployment with Docker

Run using `docker-compose`:

```bash
docker-compose up -d
```

---

## API Endpoints

- `GET /api/status`: System status, HA connection status, and DB statistics.
- `GET /api/sensors`: List all tracked sensors.
- `POST /api/sensors/{entity_id}/toggle`: Enable/disable tracking for an entity.
- `GET /api/stats`: Time series data (params: `entity_ids`, `interval`, `metric`, `start_time`, `end_time`).
- `GET /api/stats/summary`: Sensor statistics summary (24h/7d/30d/1y/all-time).
- `GET /api/stats/heatmap`: Calendar heatmap data for a given year.
- `GET /api/stats/compare`: Month-by-month comparison data across years.
- `GET /api/export`: CSV export endpoint.

---

## Testing

Run unit & integration tests:

```bash
PYTHONPATH=. pytest -v
```
