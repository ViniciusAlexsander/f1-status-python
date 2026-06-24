from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

class Country(BaseModel):
    name: str
    twoCode: str | None
    threeCode: str


class Location(BaseModel):
    id: UUID
    name: str
    city: str
    country: Country


class ScheduleItem(BaseModel):
    id: UUID
    name: str
    type: str
    startTime: datetime
    endTime: datetime
    status: str


class Race(BaseModel):
    id: UUID
    name: str
    dateStart: date
    dateEnd: date
    status: str
    location: Location
    sportId: str
    schedule: list[ScheduleItem]


class PaginationMeta(BaseModel):
    page: int
    limit: int
    total: int
    totalPages: int

class Formula1EventsResponse(BaseModel):
    data: list[Race]
    meta: PaginationMeta

class RaceListData(BaseModel):
    races: list[Race]
    nextRace: Race

class RaceListResponse(BaseModel):
    data: RaceListData
    meta: PaginationMeta