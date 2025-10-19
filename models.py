from sqlalchemy import Column, Integer, String, Text, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Location(Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    citations = Column(Integer, nullable=False)
    total_citations = Column(Integer, default=0)
    group = Column(Integer, nullable=True)
    location_info = Column(Text, nullable=True, default=None)
    geolocation = Column(Text, nullable=True, default=None)

    __table_args__ = (
        UniqueConstraint("name", "type", name="_name_type_uc"),
    )
