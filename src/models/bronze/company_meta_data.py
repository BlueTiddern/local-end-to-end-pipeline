from ...models.base_bronze import BaseBronze
from typing import Optional
from sqlmodel import Field
from sqlalchemy import BigInteger,Column
import datetime as dt

class CompanyMetaDataBronze(BaseBronze, table=True):
    __tablename__ = 'company_meta_data_bronze'

    company_id: Optional[int] = Field(default=None,primary_key=True)
    company_name : str = Field(nullable=False)
    ticker : str
    price : float
    market_cap : int = Field(sa_column=Column(BigInteger))
    sector : str
    industry : str
    insert_datetime : dt.datetime = dt.datetime.now()
