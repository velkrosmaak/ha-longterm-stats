import asyncio
import json
import logging
import datetime
from typing import Optional, Callable, Dict, Any, List, Tuple
import websockets
from ha_longterm_stats.config import config
from ha_longterm_stats.db import Database

logger = logging.getLogger("ha_longterm_stats.ha_client")

class HomeAssistantClient:
    def __init__(self, ha_url: str, token: str, db: Database):
        self.ha_url = ha_url
        self.token = token
        self.db = db
        self.is_connected = False
        self.last_connected_at: Optional[str] = None
        self.last_error: Optional[str] = None
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self._running = False
        self._msg_id = 1
        self._queue: asyncio.Queue[Tuple[str, str, Optional[float], str]] = asyncio.Queue()
        self._flush_task: Optional[asyncio.Task] = None

    def _get_ws_url(self) -> str:
        url = self.ha_url.rstrip("/")
        if url.startswith("http://"):
            url = "ws://" + url[7:]
        elif url.startswith("https://"):
            url = "wss://" + url[8:]
        elif not url.startswith("ws://") and not url.startswith("wss://"):
            url = "ws://" + url
        return f"{url}/api/websocket"

    def _next_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    def _parse_value(self, state_str: Any) -> tuple[Optional[float], str]:
        raw_val = str(state_str) if state_str is not None else ""
        if raw_val.lower() in ("unknown", "unavailable", "none", "", "null"):
            return None, raw_val
        try:
            val = float(raw_val)
            return val, raw_val
        except (ValueError, TypeError):
            return None, raw_val

    def _parse_state_object(
        self,
        state_obj: Dict[str, Any],
        now: datetime.datetime,
    ) -> Optional[Tuple[Tuple, Optional[Tuple]]]:
        """Pure CPU parse — zero DB calls.
        Returns (entity_meta_tuple, reading_tuple_or_None) or None if not a sensor.
        entity_meta: (entity_id, friendly_name, uom, device_class, state_class, now_str, now_str)
        reading:     (entity_id, ts_str, val_float, raw_val)
        """
        entity_id = state_obj.get("entity_id")
        if not entity_id or not entity_id.startswith("sensor."):
            return None

        attributes = state_obj.get("attributes", {})
        now_str = now.isoformat()
        entity_meta = (
            entity_id,
            attributes.get("friendly_name"),
            attributes.get("unit_of_measurement"),
            attributes.get("device_class"),
            attributes.get("state_class"),
            now_str,
            now_str,
        )

        last_updated = state_obj.get("last_updated") or state_obj.get("last_changed")
        dt = None
        if last_updated:
            try:
                dt = datetime.datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                ts_str = dt.isoformat()
            except Exception:
                ts_str = last_updated
        else:
            ts_str = now_str

        val_float, raw_val = self._parse_value(state_obj.get("state"))
        if val_float is not None:
            return (entity_meta, (entity_id, ts_str, val_float, raw_val))
        return (entity_meta, None)

    async def _process_state_object(self, state_obj: Dict[str, Any], queue_write: bool = True) -> None:
        """Handle a live state_changed event: parse, upsert entity in thread, queue reading."""
        now = datetime.datetime.now(datetime.timezone.utc)
        result = self._parse_state_object(state_obj, now)
        if result is None:
            return
        entity_meta, reading = result
        # Run the upsert in a thread so the WS message loop isn't blocked
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.db.upsert_entity,
            entity_meta[0], entity_meta[1], entity_meta[2], entity_meta[3], entity_meta[4],
        )
        if reading and queue_write:
            try:
                self._queue.put_nowait(reading)
            except asyncio.QueueFull:
                pass

    async def _batch_flush_worker(self):
        logger.info(f"Starting async batch flush worker (flush every {config.batch_flush_interval}s or {config.batch_max_size} items)...")
        loop = asyncio.get_event_loop()
        while self._running:
            try:
                batch: List[Tuple[str, str, Optional[float], str]] = []
                start_time = loop.time()
                while len(batch) < config.batch_max_size:
                    elapsed = loop.time() - start_time
                    remaining = config.batch_flush_interval - elapsed
                    if remaining <= 0 and batch:
                        break
                    try:
                        timeout = max(0.1, remaining) if batch else config.batch_flush_interval
                        item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                        batch.append(item)
                        self._queue.task_done()
                    except asyncio.TimeoutError:
                        break
                
                if batch:
                    logger.debug(f"Flushing batch of {len(batch)} sensor readings to database...")
                    await loop.run_in_executor(None, self.db.insert_readings_batch, batch)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in batch flush worker: {e}")
                await asyncio.sleep(1)

    async def start(self):
        self._running = True
        ws_url = self._get_ws_url()
        self._flush_task = asyncio.create_task(self._batch_flush_worker())

        backoff = 2
        while self._running:
            try:
                logger.info(f"Connecting to Home Assistant WebSocket at {ws_url}...")
                async with websockets.connect(ws_url, ping_interval=30, ping_timeout=10, max_size=None) as ws:
                    self.ws = ws
                    self.is_connected = True
                    self.last_connected_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    self.last_error = None
                    backoff = 2
                    logger.info("Connected to Home Assistant WebSocket!")

                    # Step 1: Wait for auth_required message
                    auth_req = await ws.recv()
                    auth_req_json = json.loads(auth_req)
                    if auth_req_json.get("type") != "auth_required":
                        logger.warning(f"Unexpected initial WS msg: {auth_req_json}")

                    # Step 2: Send auth
                    await ws.send(json.dumps({"type": "auth", "access_token": self.token}))
                    auth_res = await ws.recv()
                    auth_res_json = json.loads(auth_res)

                    if auth_res_json.get("type") != "auth_ok":
                        err_msg = auth_res_json.get("message", "Authentication failed")
                        logger.error(f"Home Assistant Auth Error: {err_msg}")
                        self.last_error = f"Auth failed: {err_msg}"
                        self.is_connected = False
                        await asyncio.sleep(10)
                        continue

                    logger.info("Home Assistant WebSocket authenticated successfully.")

                    # Step 3: Fetch initial snapshot of all states
                    get_states_id = self._next_id()
                    await ws.send(json.dumps({"id": get_states_id, "type": "get_states"}))

                    # Step 4: Subscribe to state_changed events
                    sub_events_id = self._next_id()
                    await ws.send(json.dumps({
                        "id": sub_events_id,
                        "type": "subscribe_events",
                        "event_type": "state_changed"
                    }))

                    # Step 5: Receive messages loop
                    async for msg in ws:
                        if not self._running:
                            break
                        data = json.loads(msg)
                        msg_type = data.get("type")

                        if msg_type == "result":
                            if data.get("id") == get_states_id and data.get("success"):
                                result_states = data.get("result", [])
                                logger.info(f"Received snapshot of {len(result_states)} states from Home Assistant.")

                                # Pure CPU parse — zero DB calls on the event loop
                                now = datetime.datetime.now(datetime.timezone.utc)
                                entity_metas = []
                                snapshot_readings = []
                                for i, st in enumerate(result_states):
                                    parsed = self._parse_state_object(st, now)
                                    if parsed is None:
                                        continue
                                    entity_meta, reading = parsed
                                    entity_metas.append(entity_meta)
                                    # Dormancy filter: skip readings older than dormant_days
                                    if reading:
                                        last_updated = st.get("last_updated") or st.get("last_changed")
                                        skip = False
                                        if last_updated:
                                            try:
                                                dt = datetime.datetime.fromisoformat(last_updated.replace("Z", "+00:00"))
                                                if (now - dt).days >= config.dormant_days:
                                                    skip = True
                                            except Exception:
                                                pass
                                        if not skip:
                                            snapshot_readings.append(reading)
                                    # Yield to event loop every 1000 items during parse
                                    if i % 1000 == 0 and i > 0:
                                        await asyncio.sleep(0)

                                # All DB work runs in a thread so the event loop stays responsive
                                loop = asyncio.get_event_loop()
                                logger.info(f"Batch upserting {len(entity_metas)} entities and inserting {len(snapshot_readings)} readings (background thread)...")
                                await loop.run_in_executor(None, self.db.upsert_entities_batch, entity_metas)
                                if snapshot_readings:
                                    await loop.run_in_executor(None, self.db.insert_readings_batch, snapshot_readings)
                                # Run initial rollups in background
                                await loop.run_in_executor(None, self.db.calculate_rollups)
                                logger.info("Snapshot ingestion complete. Now listening for live events.")

                        elif msg_type == "event":
                            event_data = data.get("event", {})
                            if event_data.get("event_type") == "state_changed":
                                new_state = event_data.get("data", {}).get("new_state")
                                if new_state:
                                    await self._process_state_object(new_state, queue_write=True)

            except asyncio.CancelledError:
                logger.info("Home Assistant Client loop cancelled.")
                break
            except Exception as e:
                self.is_connected = False
                self.last_error = str(e)
                logger.warning(f"Home Assistant WS connection error: {e}. Retrying in {backoff}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def stop(self):
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None
        if self.ws:
            await self.ws.close()
            self.ws = None
        self.is_connected = False
