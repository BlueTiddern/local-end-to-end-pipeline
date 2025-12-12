import datetime as dt
from ...models.base_bronze import BaseBronze
from typing import Optional
from sqlmodel import Field

class OHCLVBronze(BaseBronze, table=True):
    __tablename__ = "ohclv_bronze"

    id: Optional[int] = Field(default=None, primary_key=True)
    ticker: str
    date: dt.date
    open: float
    high: float
    low: float
    close: float
    volume: float
    insert_datetime : dt.datetime = dt.datetime.now()
