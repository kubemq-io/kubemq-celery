"""Beat Solar Schedule — KubeMQ Celery Transport.

Demonstrates:
- Solar schedules based on sunrise, sunset, and other solar events
- Location-based scheduling with latitude/longitude
- Available solar events in Celery Beat
- Note: requires ephem or astral package

Usage:
    celery -A examples.scheduling.beat_solar worker --loglevel=info
    celery -A examples.scheduling.beat_solar beat --loglevel=info
    python examples/scheduling/beat_solar.py

Requirements:
    - Running KubeMQ broker on localhost:50000 (or set CELERY_BROKER_URL)
    - kubemq-celery installed
    - ephem or astral package (pip install ephem)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from celery import Celery

import kubemq_celery  # noqa: F401

NYC_LAT = 40.7128
NYC_LON = -74.0060

app = Celery("beat_solar")

_config: dict = {
    "broker_url": os.environ.get("CELERY_BROKER_URL", "kubemq://localhost:50000"),
    "result_backend": os.environ.get("CELERY_RESULT_BACKEND", "kubemq://localhost:50000"),
    "result_expires": 3600,
    "enable_utc": True,
}

_HAS_SOLAR = False
try:
    from celery.schedules import solar

    _test = solar("sunset", NYC_LAT, NYC_LON)
    _HAS_SOLAR = True
    del _test
    _config["beat_schedule"] = {
        "sunset-lights-on": {
            "task": "examples.scheduling.beat_solar.control_lights",
            "schedule": solar("sunset", NYC_LAT, NYC_LON),
            "kwargs": {"action": "on", "zone": "outdoor"},
        },
        "sunrise-lights-off": {
            "task": "examples.scheduling.beat_solar.control_lights",
            "schedule": solar("sunrise", NYC_LAT, NYC_LON),
            "kwargs": {"action": "off", "zone": "outdoor"},
        },
        "dawn-data-collection": {
            "task": "examples.scheduling.beat_solar.start_data_collection",
            "schedule": solar("dawn_astronomical", NYC_LAT, NYC_LON),
        },
        "dusk-data-stop": {
            "task": "examples.scheduling.beat_solar.stop_data_collection",
            "schedule": solar("dusk_astronomical", NYC_LAT, NYC_LON),
        },
    }
except Exception:
    pass

app.config_from_object(_config)


@app.task
def control_lights(action: str, zone: str) -> dict:
    """Control lights based on solar schedule."""
    return {
        "action": action,
        "zone": zone,
        "at": datetime.now(timezone.utc).isoformat(),
    }


@app.task
def start_data_collection() -> dict:
    """Start data collection at astronomical dawn."""
    return {"collection": "started", "at": datetime.now(timezone.utc).isoformat()}


@app.task
def stop_data_collection() -> dict:
    """Stop data collection at astronomical dusk."""
    return {"collection": "stopped", "at": datetime.now(timezone.utc).isoformat()}


if __name__ == "__main__":
    print("=== Beat Solar Schedule — KubeMQ Celery Transport ===\n")

    print(f"Location: New York City ({NYC_LAT}, {NYC_LON})\n")

    print("Available solar events:\n")
    solar_events = [
        ("dawn_astronomical", "When the sun is 18 deg below horizon (morning)"),
        ("dawn_nautical", "When the sun is 12 deg below horizon (morning)"),
        ("dawn_civil", "When the sun is 6 deg below horizon (morning)"),
        ("sunrise", "When the upper edge of the sun appears on the horizon"),
        ("solar_noon", "When the sun is at its highest point"),
        ("sunset", "When the sun disappears below horizon"),
        ("dusk_civil", "When the sun is 6 deg below horizon (evening)"),
        ("dusk_nautical", "When the sun is 12 deg below horizon (evening)"),
        ("dusk_astronomical", "When the sun is 18 deg below horizon (evening)"),
    ]
    for event, desc in solar_events:
        print(f"  solar('{event}', lat, lon)")
        print(f"    -> {desc}")
        print()

    if _HAS_SOLAR:
        print("Configured solar schedules:")
        for name, config in app.conf.beat_schedule.items():
            print(f"  {name}: {config['task']}")
            kw = config.get("kwargs", {})
            if kw:
                print(f"    kwargs: {kw}")
        print()
    else:
        print("NOTE: ephem package not installed — solar schedules not configured.")
        print("      Install with: pip install ephem\n")

    print("To use solar schedules:")
    print("  1. Install ephem: pip install ephem")
    print("  2. Start worker:  celery -A ... worker --loglevel=info")
    print("  3. Start beat:    celery -A ... beat --loglevel=info")
    print()
    print("NOTE: Solar schedules compute next event time dynamically.")
    print("      They account for daylight saving time and seasonal changes.")
    print("      Beat state is LOCAL — run exactly ONE Beat process.")

    print("\n=== Solar schedule demo complete ===")
