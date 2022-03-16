from settings import DATABASE
import sqlite3

sql_create_meters_cis_table = """CREATE TABLE IF NOT EXISTS meters_cis (
    meter_number text PRIMARY KEY,
    account text,
    serv_loc text,
    billing_cycle text,
    billing_route text,
    address text,
    meter_model text,
    account_type text,
    meter_type text,
    current_class text,
    status text,
    peak_loading real
)
"""


sql_create_events_table = """CREATE TABLE IF NOT EXISTS  events(
    meter_number text,
    event_time text
    event_type text,
    event_severity text,
    event_desc text,
    event_source text,
    service_point_id text,
    mac_address text,
    PRIMARY KEY (meter_number, event_time, event_desc)
)
"""


sql_create_interval_read_table = """CREATE TABLE IF NOT EXISTS interval_read(
    meter_number text,
    service_point_id text
    meter_program text,
    read_time text,
    interval_end text,
    channel_number integer
    unit text,
    value real,
    daylight_savings_time text,
    power_failure text,
    clock_reset_back text,
    clock_reset_forward text,
    PRIMARY KEY (meter_number,interval_end, channel_number)
)
"""


def db_init() -> None:
    with sqlite3.connect(DATABASE) as conn:
        cur = conn.cursor()
        cur.execute(sql_create_meters_cis_table)
        cur.execute(sql_create_interval_read_table)
        cur.execute(sql_create_events_table)
        print(f"DB's initialized version {sqlite3.version}")


def cis_load() -> None:
    with sqlite3.connect(DATABASE) as conn:
        cur = conn.cursor()
        accounts = [("ABC", "123", "ABC123", "1", "MT", "Home St Anytown",
                     "FORM 4S", "RES", "i210+c", "200", "ENERGIZED", 10.2), ("DEF", "456", "DEF456", "2", "VN", "Away Ave Anytown",
                                                                             "FORM 2S", "RES", "i210+c", "200", "ENERGIZED", 5.3)]
        cur.executemany(
            "insert into meters_cis values (?,?,?,?,?,?,?,?,?,?,?,?)", accounts)


db_init()
cis_load()
