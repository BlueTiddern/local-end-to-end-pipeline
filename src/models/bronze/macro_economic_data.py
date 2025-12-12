
from ...models.base_bronze import BaseBronze
from sqlmodel import Field
from typing import Optional
from sqlalchemy import PrimaryKeyConstraint
import datetime as dt

class MacroEconomicData(BaseBronze, table= True):

    __tablename__ = "macro_economic_data_bronze"

    id: Optional[int] = Field(default=None, primary_key=True)
    country_id : str = Field(nullable=False)
    year : int = Field(nullable=False)
    country_name : str
    nominal_gdp : Optional[float] = Field(default=None, nullable=True)
    real_gdp : Optional[float] = Field(default=None, nullable=True)
    inflation : Optional[float] = Field(default=None, nullable=True)
    unemployment : Optional[float] = Field(default=None, nullable=True)
    insert_datetime : dt.datetime = dt.datetime.now()
