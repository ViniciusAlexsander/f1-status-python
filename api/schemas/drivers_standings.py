from uuid import UUID

from pydantic import BaseModel

class TeamData(BaseModel):
    id: UUID
    name: str
    shortName: str
    color: str

class DriverStandingData(BaseModel):
    id: UUID
    position: int
    points: float
    firstName: str
    lastName: str
    code: str
    number: int
    teams: list[TeamData]

class DriversStandingsResponse(BaseModel):
    drivers: list[DriverStandingData]
