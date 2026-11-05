from uuid import UUID

from pydantic import BaseModel

from api.schemas.formula1_events import Country


class Formula1Location(BaseModel):
    id: UUID
    name: str
    latitude: str
    longitude: str
    city: str | None
    timezone: str
    country: Country
