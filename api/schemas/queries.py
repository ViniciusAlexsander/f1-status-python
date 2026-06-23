from datetime import datetime

import strawberry
from typing import List

from openf1_client import get_meetings

from .types import Meeting


@strawberry.type
class Query:
    @strawberry.field
    async def current_year_meetings(self) -> List[Meeting]:
        current_year = datetime.now().year
        current_year_meetings = await get_meetings(current_year)
        return [
            Meeting(
                meeting_key=m["meeting_key"],
                meeting_name=m["meeting_name"],
                meeting_official_name=m["meeting_official_name"],
                location=m["location"],
                country_name=m["country_name"],
                country_flag=m["country_flag"],
                circuit_short_name=m["circuit_short_name"],
                circuit_type=m["circuit_type"],
                circuit_image=m["circuit_image"],
                gmt_offset=m["gmt_offset"],
                date_start=m["date_start"],
                date_end=m["date_end"],
                year=m["year"],
            )
            for m in current_year_meetings
        ]

    @strawberry.field
    async def current_year_next_meeting(self) -> Meeting:
        current_year = datetime.now().year
        current_year_meetings = await get_meetings(current_year)
        next_meeting = current_year_meetings[0]
        return Meeting(
            meeting_key=next_meeting["meeting_key"],
            meeting_name=next_meeting["meeting_name"],
            meeting_official_name=next_meeting["meeting_official_name"],
            location=next_meeting["location"],
            country_name=next_meeting["country_name"],
            country_flag=next_meeting["country_flag"],
            circuit_short_name=next_meeting["circuit_short_name"],
            circuit_type=next_meeting["circuit_type"],
            circuit_image=next_meeting["circuit_image"],
            gmt_offset=next_meeting["gmt_offset"],
            date_start=next_meeting["date_start"],
            date_end=next_meeting["date_end"],
            year=next_meeting["year"],
        )
