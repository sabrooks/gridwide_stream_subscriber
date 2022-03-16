from typing import Dict, Optional
from itertools import groupby
from flask import Flask, g, request, Response, session
import sqlite3

from meter_data_management.utils.events import CustomEventTopics, EventType
from settings import DATABASE

from .utils.meters import (
    Meter,
    init_meters,
    MeterStatus
)
from utils.events import EventTopic
from utils.intervals import MeterReadings


flask_app = Flask(__name__)


@flask_app.before_first_request
def app_build():
    """Initialize Meters in session"""
    meters: Dict[str, Meter] = init_meters()
    for meter_number, meter in meters.values():
        session[meter_number] = meter


@flask_app.before_request
def get_db() -> None:
    db = getattr(g, '_database', None)
    if db is None:
        g._database = sqlite3.connect(DATABASE)


@flask_app.route("/ingest", methods=["POST"])
def handle_events():
    """
    Interval Data Subscriber

    Parameters:
        POST:/ingest_interval - Interval Data Reads in XML

    Returns:
        Response
    """
    try:
        events = CustomEventTopics().parse_from_reponse(request.data)
        events.write_to_db()
        for event in events:
            if event.meter_number not in session:
                continue
            # Meter added to Remote Operation
            if event.event_type == EventType.STATE_REMOTE:
                session[event.meter_number] = Meter(
                    event.meter_number, status=MeterStatus.ENERGIZED)
            # Meter leaves remote operation
            elif event.event_type == EventType.STATE_INVENTORY:
                del session[event.meter_number]
            # De-Energize Event
            elif event.event_type == EventType.POWER_DOWN:
                meter = getattr(session, event.meter_number, Meter(
                    event.meter_number, status=MeterStatus.DEENERGIZED))
                meter.status = MeterStatus.DEENERGIZED
                session[meter.meter_number] = meter

        flask_app.logger.info(f'Processed {len(events)} events')
        return Response("Ok", status=200)
    except Exception as e:
        flask_app.logger.error("Failed to process events", e)
        return Response("Error", status=500)


@flask_app.route("/ingest_interval", methods=["POST"])
def handle_interval():
    """
    Interval Data Subscriber

    Parameters:
        POST:/ingest_interval - Interval Data Reads in XML

    Returns:
        Response
    """
    try:
        interval_reads = MeterReadings.parse_from_response(request.data)
        interval_reads.to_db()
        flask_app.logger.info(
            f'Processed {len(interval_reads)} interval_reads')
        return Response("Ok", status=200)
    except Exception as e:
        flask_app.logger.error("Failed to process interval reads", e)
        return Response("Error", status=500)


@flask_app.route("/meterstatus", methods=["GET"])
def get_meter_status():
    pass


@flask_app.appcontext_tearing_down
def close_connection(exception):
    db: Optional[sqlite3.Connection] = getattr(session, 'database', None)
    if db is not None:
        db.close()


if __name__ == "__main__":
    flask_app.run(debug=False)
