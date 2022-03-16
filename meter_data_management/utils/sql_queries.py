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


sql_create_events_table = """CREATE TABLE IF NOT EXISTS events(
    meter_number text PRIMARY KEY,
    event_time text
    event_type text,
    event_severity text,
    event_desc text,
    event_source text,
    service_point_id text,
    mac_address text,
)
"""


sql_create_interval_read_table = """CREATE TABLE IF NOT EXISTS interval_read(
    meter_number text PRIMARY KEY,
    service_point_id text
    meter_program text,
    read_time text,
    interval_end text
    channel_number integer
    unit text,
    value real,
    daylight_savings_time text,
    power_failure text,
    clock_reset_back text,
    clock_reset_forward text
)
"""
