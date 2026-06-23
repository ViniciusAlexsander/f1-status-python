import strawberry


@strawberry.type
class Meeting:
    meeting_key: int
    meeting_name: str
    meeting_official_name: str
    location: str
    country_name: str
    country_flag: str
    circuit_short_name: str
    circuit_type: str
    circuit_image: str
    gmt_offset: str
    date_start: str
    date_end: str
    year: int
