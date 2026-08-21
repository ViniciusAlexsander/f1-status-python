from pydantic import BaseModel

class Driver(BaseModel):
    id: str
    firstName: str
    lastName: str
    code: str
    number: int | None

class Team(BaseModel):
    id: str
    name: str
    shortName: str
    color: str

class Chassis(BaseModel):
    id: int
    name: str

class EngineManufacturer(BaseModel):
    id: int
    name: str

class FastestLap(BaseModel):
    rank: str | None
    time: str | None
    lap: str | None

class Sectors(BaseModel):
    s1: str | None
    s2: str | None
    s3: str | None

class TireStrategy(BaseModel):
    compound: str
    laps: int
    isNew: bool

class SessionResultsResponse(BaseModel):
    id: str
    position: str
    lapTime: str | None
    displayTime: str | None
    laps: int | None
    driver: Driver
    carNumber: str | None
    team: Team
    chassis: Chassis
    engineManufacturer: EngineManufacturer
    fastestLap: FastestLap
    status: str  | None = None
    points: str  | None = None
    gap: str  | None = None
    interval: str | None
    pitStops: int | None
    bestLapTime: str | None
    bestLapNumber: int | None
    sectors: Sectors | None
    tireStrategy: list[TireStrategy] | None
    gridPosition: int | None
    q1Time: str | None
    q2Time: str | None
    q3Time: str | None