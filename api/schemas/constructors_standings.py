from uuid import UUID

from pydantic import BaseModel

class TeamStandingData(BaseModel):
    id: UUID
    position: int
    points: float
    name: str
    shortName: str
    color: str
