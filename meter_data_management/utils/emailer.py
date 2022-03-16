from shared.image_email import send_email
from shared.cis_copy import CisData
from datetime import datetime, timedelta
from apps.service.gridwide_stream_subscriber.server.utils.meters import Meter
from apps.service.gridwide_stream_subscriber.server.utils.types.read_types import Event
from apps.service.gridwide_stream_subscriber.server.utils.plots.event_plot import event_plot


def send_alert_email(message: str, m: Meter, e: Event) -> Exception:
    cis_data = CisData()

    try:
        meter_detail = cis_data.get_account_details(meter_number=m.meter_number)
        address = meter_detail.get("address", "")
        billing_route = meter_detail.get("billing_route", "")
        meter_model = meter_detail.get("meter_model", "")

        subject = f"{message} - Meter: {m.meter_number}"
        report_html = f"<p><strong>Meter</strong>:  {m.meter_number}</p>"
        report_html += f"<p><strong>Alert</strong>:     {e.message}  @ {e.event_time} </p>"
        report_html += f"<p><strong>Account</strong>:   {e.account}</p>"
        report_html += f"<p><strong>Address</strong>:   {address}</p>"
        report_html += f"<p><strong>Feeder</strong>:    {billing_route}</p>"
        report_html += f"<p><strong>Meter Model</strong>:   {meter_model}</p>"
        img = None
        last_24_events = list(
            filter(lambda x: x.event_time > datetime.now() - timedelta(days=1), m.events)
        )
        if last_24_events:
            report_html += "<h2>Meter Event History (Last 24 Hours)</h2>"
            img = event_plot(last_24_events)
            report_html += "<br><img src=cid:meter_events.png><br>"

        send_email(
            from_name="Gridwide Notifications",
            recipient_addresses=[
                "amy@peninsulalight.org",
                "flow@praece.com",
                "steve@peninsulalight.org",
            ],
            subject=subject,
            html=report_html,
            img=img,
        )

        return None

    except Exception as e:
        return e
