from datetime import datetime

from ...models.base_bronze import BaseBronze
from sqlmodel import Field
from typing import Optional
from sqlalchemy import PrimaryKeyConstraint
import datetime as dt

class ExchangeRateData(BaseBronze, table=True):

    __tablename__ = "exchange_rates_bronze"

    id: Optional[int] = Field(default=None, primary_key=True)
    date: datetime = Field(nullable=False)
    inr_rate : Optional[float] = Field(default=None, nullable=True)
    usd_amount : Optional[float] = Field(default=None, nullable=True)
    insert_datetime : dt.datetime = dt.datetime.now()
