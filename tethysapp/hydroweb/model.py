from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric


from geoalchemy2 import Geometry



Base = declarative_base()



class River(Base):
    """
    SQLAlchemy table definition for storing flood extent polygons.
    """
    
    __tablename__ = 'river'

    # Columns
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry('POINT'))
    name = Column(String)
    lat = Column(Numeric)
    lon = Column(Numeric)
    river_name = Column(String)
    basin = Column(String)
    status = Column(String)
    validation = Column(String)


    def __init__(self, wkt, name,lat,lon,river_name, basin,status,validation):
        """
        Constructor
        """
        # Add Spatial Reference ID
        self.geom = 'SRID=4326;{0}'.format(wkt)
        self.name = name
        self.lat = lat
        self.lon = lon
        self.river_name = river_name
        self.basin = basin
        self.status = status
        self.validation = validation


class Lake(Base):
    """
    SQLAlchemy table definition for storing flood extent polygons.
    """
    
    __tablename__ = 'lake'

    # Columns
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry('POINT'))
    name = Column(String)
    lat = Column(Numeric)
    lon = Column(Numeric)
    lake_name = Column(String)
    basin = Column(String)
    status = Column(String)
    validation = Column(String)


    def __init__(self, wkt, name,lat,lon,lake_name, basin,status,validation):
        """
        Constructor
        """
        # Add Spatial Reference ID
        self.geom = 'SRID=4326;{0}'.format(wkt)
        self.name = name
        self.lat = lat
        self.lon = lon
        self.lake_name = lake_name
        self.basin = basin
        self.status = status
        self.validation = validation

