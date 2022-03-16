from dataclasses import dataclass
from datetime import datetime
import sqlite3
from typing import List, Tuple
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import Element
from flask import g


@dataclass
class IntervalChannel:
    unit: str
    value: float
    channel_number: int

    @staticmethod
    def parse(et: Element) -> "IntervalChannel":
        attribs = et.attrib
        return IntervalChannel(attribs.get("u"), float(attribs.get("v")), int(attribs.get("n")))


@dataclass
class Status:
    daylight_savings_time: bool  # dst
    power_failure: bool  # pf
    clock_reset_forward: bool  # crf
    clock_reset_back: bool  # crb

    @staticmethod
    def parse(et: Element) -> "Status":
        attribs = et.attrib
        return Status(
            True if attribs.get("dst") == "true" else False,
            True if attribs.get("pf") == "true" else False,
            True if attribs.get("crf") == "true" else False,
            True if attribs.get("crb") == "true" else False,
        )


@dataclass
class Interval:
    lp_set_number: str
    read_time: datetime
    interval_timestamp: datetime
    interval_created_time: datetime
    channels: List[IntervalChannel]
    status: Status  # s

    @staticmethod
    def parse(et: Element) -> "Interval":
        attribs = et.attrib
        chs = [IntervalChannel.parse(c)
               for c in et.findall(".//{urn:policynet/v5}c")]
        status = Status.parse(et.find(".//{urn:policynet/v5}s"))
        return Interval(attribs.get("lpSetNumber"), attribs.get("rt"), attribs.get("t"), attribs.get("ct"), chs, status)


@dataclass
class IntervalData:
    intervals: List[Interval]
    read_time: datetime

    @staticmethod
    def parse(et: Element) -> "IntervalData":
        attribs = et.attrib
        intervals = [Interval.parse(i)
                     for i in et.findall(".//{urn:policynet/v5}I")]
        return IntervalData(intervals=intervals, read_time=attribs.get("readTimeLocal"))


@dataclass
class MeterRead:
    meter_read_data: List[IntervalData]
    wan_mac_address: str
    meter_number: str
    service_point_id: str
    meter_program_id: str

    @staticmethod
    def parse(et: Element) -> "MeterRead":
        attribs = et.attrib
        meter_read_data = [IntervalData.parse(
            x) for x in et.findall(".//{urn:policynet/v5}Is")]
        return MeterRead(
            meter_read_data,
            attribs.get("wanMacAddress"),
            attribs.get("utilitySerialNumber"),
            attribs.get("servicePointId"),
            attribs.get("meterProgramId"),
        )

    def max_load(self) -> float:
        out = 0
        for interval_data in self.meter_read_data:
            for intervals in interval_data.intervals:
                for channel in intervals.channels:
                    if channel.unit == 'kWh':
                        if channel.value > out:
                            out = channel.value

    def to_db(self) -> List[Tuple]:
        out: List[Tuple] = []
        for interval_data in self.meter_read_data:
            for interval in interval_data.intervals:
                for channel in interval.channels:
                    out.append(
                        (
                            self.meter_number,
                            self.service_point_id,
                            self.meter_program_id,
                            interval.read_time,
                            interval.interval_timestamp,
                            channel.channel_number,
                            channel.unit,
                            channel.value,
                            interval.status.daylight_savings_time,
                            interval.status.power_failure,
                            interval.status.clock_reset_back,
                            interval.status.clock_reset_forward,
                        )
                    )
        return out


class MeterReadings(List[MeterRead]):
    @staticmethod
    def parse_from_response(raw: str) -> 'MeterReadings':
        interval_xml = ET.fromstring(raw)
        return MeterReadings([MeterRead.parse(r) for r in interval_xml.findall(".//{urn:policynet/v5}R")])

    def to_db(self) -> None:
        reads = [meter_read.to_db() for meter_read in self]
        db: sqlite3.Connection = getattr(g, '_database')
        cur = db.cursor()
        cur.executemany(
            "insert into events values (?,?,?,?,?,?,?,?,?,?,?,?)", reads)
