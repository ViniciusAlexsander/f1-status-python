from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class Formula1Country(BaseModel):
    name: str
    twoCode: str | None
    threeCode: str


class Formula1Location(BaseModel):
    id: UUID
    name: str
    city: str
    country: Formula1Country


class Formula1ScheduleItem(BaseModel):
    id: UUID
    name: str
    type: str
    startTime: datetime
    endTime: datetime
    status: str


class Formula1Event(BaseModel):
    id: UUID
    name: str
    dateStart: date
    dateEnd: date
    status: str
    location: Formula1Location
    sportId: str
    schedule: list[Formula1ScheduleItem]


class PaginationMeta(BaseModel):
    page: int
    limit: int
    total: int
    totalPages: int


class Formula1EventsResponse(BaseModel):
    data: list[Formula1Event]
    meta: PaginationMeta