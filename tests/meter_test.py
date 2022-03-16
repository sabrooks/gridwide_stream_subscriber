from datetime import datetime, timedelta
from apps.service.gridwide_stream_subscriber.server.utils.meters import Meter
from shared.databases import get_postgres_engine
from .cis_copy import CisData
from apps.service.gridwide_stream_subscriber.server.utils.types.read_types import Event
from collections import deque

TEST_METER_NUMBER = "1ND092561788"
pg_engine = get_postgres_engine("oppg.penlightop.lan", "analysis", "analysis")
cis_resolver = CisData()
cis_meters = cis_resolver.get_meter_details()
CIS_TEST_METER = cis_meters.get(TEST_METER_NUMBER)

with pg_engine.begin() as pg_conn:
    raw_meter = pg_conn.execute(
        f"""select  meter_number
                        ,last_request_time
                        ,device_energy_status
                        ,device_network_connectivity
                        ,"data" -> 'voltage'
                    from gridwide_power_device_status_current
                    WHERE device_operational_status = 'RemoteOperation'
                    AND meter_number = '{TEST_METER_NUMBER}' ;
                """
    )
    address = CIS_TEST_METER.address
    meter_number, last_updated, energy_status, network_status, voltage_range = next(
        raw_meter)

TEST_METER = Meter(
    meter_number,
    CIS_TEST_METER.meter_model,
    CIS_TEST_METER.current_class,
    CIS_TEST_METER.account,
    CIS_TEST_METER.billing_route,
    CIS_TEST_METER.address,
    energy_status,
    network_status,
    voltage_range,
    last_updated,
    None,
)

assert TEST_METER.meter_number == TEST_METER_NUMBER
TEST_SYSTEM = {TEST_METER.meter_number: TEST_METER}

# Test Event
TEST_EVENT = Event(
    TEST_METER_NUMBER,
    CIS_TEST_METER.serv_loc,
    CIS_TEST_METER.account,
    CIS_TEST_METER.serv_loc,
    "Diagnostic 6 - Under voltage, Phase A",
    datetime(2021, 8, 20, 6, 9, 41),
    10,
    "GeMfg",
    "Warning",
)
# WARNING SENDS EMAILS AND WRITES TO PRDUCTION DB
assert TEST_METER.get_last_alert(
    TEST_EVENT.event_code) == datetime(1980, 1, 1, 0, 0)
assert TEST_METER.get_last_alert(
    TEST_EVENT.event_code) < datetime.now() - timedelta(days=1)
assert TEST_METER.events == deque([])
TEST_METER.handle_voltage_alert(TEST_EVENT, TEST_SYSTEM, "UNDER VOLTAGE")
assert TEST_METER.get_last_alert(
    TEST_EVENT.event_code) == TEST_EVENT.event_time
assert TEST_METER.events == deque([TEST_EVENT])
# assert TEST_METER.send_email_alert("UNDER VOLTAGE", TEST_EVENT) is None
assert TEST_METER.process_event(TEST_EVENT, TEST_SYSTEM) is None
