# Meter Data Manager
Ingests streaming meter inteval and event data in xml format.  Data is stored in sqlite table.  A "digital twin" of the meter is maintained in memory, giving the app fast access to meter status requests.

## Parses XML to Python Dataclass
Rather than parsing xml to dictionaries, this app parses xml to dataclasss objects designed to mimic the XML structure.  The XML Structure is converted to a flat database (sqlite) row.

## Stores Reads and Events in Sqlite Table

## Maintains "Digital Twin" of Meters
1. Enables alert detection during parsing of the streaming data
2. Enables fast status requests (API)
