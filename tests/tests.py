import xml.etree.ElementTree as ET
from server.utils.gridwide_parser import CustomEventTopics
with open("tests/event_test.log", "r") as f:
    raw_events_xml = f.read()

events_xml = ET.fromstring(raw_events_xml)
custom_events_xml = events_xml.find(".//{urn:policynet/v5}customEventTopics")

events = CustomEventTopics(events_xml)
with open("apps\\ad_hoc\\amr_app\\AMR\\event_test.log", "r") as f:
    test_string = f.read()

test_event = ET.fromstring(test_string)
out = [parse_event_topic(topic) for topic in test_event.findall(
    ".//{urn:policynet/v5}customEventTopics")]
assert out[0][0] == Event(
    meter_number="1ND090757772",
    service_point_id="0852731",
    account=None,
    serv_loc=None,
    message="Power Restoration notification received",
    event_time=datetime(2020, 9, 30, 15, 35),
    event_code="4043",
    event_category="GridNet",
    event_severity="Warning",
)

for event in ET.iterparse(test_event):
    print(event)

with open("apps\\ad_hoc\\amr_app\\AMR\\power-1620781335.333874-b1043778-b2bd-11eb-8aa4-00155d015c08-body.log", "r") as f:
    interval_text = f.read()
out = MeterReadings.parse_from_response(interval_text)
