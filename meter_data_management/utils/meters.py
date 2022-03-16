from dataclasses import dataclass
from typing import Dict, Optional
from enum import Enum


class MeterStatus(Enum):
    ENERGIZED = 'energized'
    DEENERGIZED = 'deenergized'
    UNKNOWN = 'unknown'


MeterNumber = str


@dataclass
class Meter:
    meter_number: MeterNumber
    account: Optional[str]
    serv_loc: Optional[str]
    billing_cycle: Optional[str]
    billing_route: Optional[str]
    address: Optional[str]
    meter_model: Optional[str]
    account_type: Optional[str]
    meter_type: Optional[str]
    current_class: Optional[str]
    status: MeterStatus
    peak_loading: float


class Meters([Dict[MeterNumber, Meter]]):

    def process_event(self, event: Event) -> None:
        """
        Update meter status based on event

        :param event: meter event
        """
        # Meter added to Remote Operation
        if event.event_type == EventType.STATE_REMOTE:
            self[event.meter_number] = Meter(
                event.meter_number, status=MeterStatus.ENERGIZED)
        # Meter leaves remote operation
        elif event.event_type == EventType.STATE_INVENTORY:
            del self[event.meter_number]
        # De-Energize Event
        elif event.event_type == EventType.POWER_DOWN:
            meter = self.get(event.meter_number, Meter(
                event.meter_number, status=MeterStatus.DEENERGIZED))
            meter.status = MeterStatus.DEENERGIZED
            self[meter.meter_number] = meter

    def process_interval(self, read: MeterRead) -> None:
        """
        Update meter peak loading based on meter interval reads

        :param read: Interval read from meter
        """
        meter: Meter = self[read.meter_number]
        max_load = read.max_load()
        if max_load > meter.peak_loading:
            meter.peak_loading = max_load


@dataclass
class MeterDetails:
    meter_number: str
    account: str
    serv_loc: str
    billing_cycle: str
    billing_route: str
    address: str
    meter_model: str
    account_type: str
    meter_type: str
    current_class: str

    @staticmethod
    def parse(
        meter_number: str, meter_serial, account, serv_loc, address, meter_model, billing_route, billing_cycle, account_type, meter_type: str,
    ) -> "MeterDetails":
        if meter_type is None:
            current_class = "UNKNOWN"
        else:
            split_meter_type = meter_type.split()
            current_class = split_meter_type[2] if len(
                split_meter_type) == 6 else "UNKNOWN"

        return MeterDetails(
            meter_number, account, serv_loc, int(
                billing_cycle), billing_route, address, meter_model, account_type, meter_type, current_class
        )


CisData = Dict[str, MeterDetails]


def get_cis_meter_data() -> CisData:
    engine = get_mssql_engine("opsql.penlightop.lan",
                              "ReplicaClones", "analysis")

    with engine.begin() as conn:
        result = conn.execute(
            """
                    SELECT
                        LTRIM(RTRIM(MeterNo)),
                        LTRIM(RTRIM(MeterSerial)),
                        LTRIM(RTRIM(AccountNo)) as AccountNo,
                        CONCAT(LTRIM(RTRIM(MemberNo)), LTRIM(RTRIM(AccountNo))) as ServLoc,
                        LTRIM(RTRIM(ServiceLocation)) as Address,
                        LTRIM(RTRIM(MeterModel)) as meter_model,
                        LTRIM(RTRIM(BillingRoute)) as BillingRoute,
                        BillingCycle,
                        LTRIM(RTRIM(AccountType)) as AccountType,
                        LTRIM(RTRIM(MeterType)) as MeterType
                    FROM PLC_CISMeterData
                    WHERE AccountStatus = 'Active'
                """
        )
        meter_details = [MeterDetails(*row) for row in result]
    return CisData({meter.meter_number: meter for meter in meter_details})
