from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum, Enum
from xml import etree
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
from typing import List, Optional, Tuple
from flask import g
import sqlite3


class EventType(IntEnum):
    POWER_DOWN = 1
    POWER_UP = 2
    TIME_CHANGE = 3
    DEVICE_PROGRAMED = 11
    RECEIVED_KWH = 20
    SELF_READ = 21
    ENERGY_SERVICE_SWITCH_OPEN = 39
    ENERGY_SERVICE_SWITCH_CLOSE = 40
    POLICY_ENFORCEMENT_SUCCESS = 2022
    METER_PROGRAMMING_SUCESS = 2036
    FIELD_PROVISIONING_SUCCESS = 2334
    FIELD_PROVISIONING_FAILURE = 2335
    VOLTAGE_SAG = 1025
    STATE_INVENTORY = 4021
    STATE_DISCOVERED = 4022
    STATE_REMOTE = 4025
    LAST_GASP = 4042
    RESTORATION = 4043
    LINK_LORA = 4083
    LINK_LTE = 4085
    DEFAULT = 99999

    @classmethod
    def _missing_(cls, value: object) -> "EventType":
        return EventType.DEFAULT


class EventSeverity(Enum):
    WARNING = 'Warning'
    INFO = 'Info'


@dataclass
class DeviceSource:
    service_point_id: int
    utility_serial_number: str
    mac_address: str

    def __init__(self, element: Element) -> None:
        self.service_point_id = element.get("servicePointId")
        self.utility_serial_number = element.get("utilitySerial")
        self.mac_address = element.get("macAddress")


@dataclass
class EventSource:
    device_source: DeviceSource

    def __init__(self, element: Element) -> None:
        raw_device_source = element.find("{urn:policynet/v5}deviceSource")
        self.device_source = DeviceSource(raw_device_source)


@dataclass
class Event:
    event_desc: str
    event_type: EventType
    event_source: EventSource
    event_arguments: Optional[str]
    event_severity: EventSeverity
    event_time: datetime

    def __init__(self, element: Element) -> None:
        self.event_desc = element.get("eventDesc")
        raw_event_code = int(element.get("eventCode"))
        self.event_type = EventType(raw_event_code)
        self.event_source = EventSource(
            element.find(".//{urn:policynet/v5}eventSource"))
        self.event_arguments = element.get("eventArguments")
        self.event_severity = EventSeverity(element.get("eventSeverity"))
        self.event_time = element.get("eventTime")

    @property
    def meter_number(self) -> str:
        return self.event_source.device_source.utility_serial_number

    @property
    def service_point_id(self) -> int:
        return self.event_source.device_source.service_point_id

    @property
    def mac_address(self) -> str:
        return self.event_source.device_source.mac_address

    def db_row(self) -> Tuple:
        return self.meter_number, self.event_time, self.event_type.value, self.event_severity, self.event_desc, self.event_source, self.service_point_id, self.mac_address


class CustomEventTopics(List[Event]):

    @staticmethod
    def parse_from_reponse(raw: str) -> 'CustomEventTopics':
        xml = ET.fromstring(raw)
        return CustomEventTopics([Event(e)for e in xml.findall(".//{urn:policynet/v5}event")])

    def write_to_db(self) -> List[Tuple]:
        "Writes events to event table in sqlite db "
        db: sqlite3.Connection = getattr(g, '_database')
        cur = db.cursor()
        event_list = [event.db_row() for event in self]
        cur.executemany(
            "insert into events values (?,?,?,?,?,?,?,?)", event_list)
