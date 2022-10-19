from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric

import pandas as pd
import requests
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



class HistoricalData(Base):
    """
    SQLAlchemy interface for projects table
    """
    __tablename__ = 'historical_data'
    id = Column(Integer, primary_key=True)  # Record number.

    reach_id = Column(Integer)
    datetime = Column(String)
    stream_flow = Column(Numeric)
    type_data = Column(String)
    def __init__(self, reach_id,type_data, datetime,stream_flow):
        self.reach_id = reach_id
        self.type_data = type_data
        self.datetime= datetime
        self.stream_flow= stream_flow


def cache_historical_data(data_df,comid,type_data,session):
    historical_data_query = session.query(HistoricalData).filter(HistoricalData.reach_id == comid, HistoricalData.type_data == type_data)
    session.commit()
    # print("hey", response_content)
    if historical_data_query.first() is not None:
        historical_df = pd.read_sql(historical_data_query.statement, historical_data_query.session.bind, index_col='datetime')
        historical_df = historical_df.rename(columns={'stream_flow':'y','datetime':'x'})
        historical_df = historical_df.drop(columns=['reach_id', 'id','type_data'])
        return historical_df

    else:
        breakpoint()
        # data_df[data_df < 0] = 0
        data_df.index = pd.to_datetime(data_df.index)
        data_df.index = data_df.index.to_series().dt.strftime("%Y-%m-%d")
        new_data_df = data_df.assign(reach_id=comid)[['reach_id'] + data_df.columns.tolist()]
        new_data_df['type_data'] = type_data 

        # new_data_df = new_data_df.rename(columns={'streamflow_m^3/s': 'stream_flow'})
        new_data_df = new_data_df.reset_index()
        session.bulk_insert_mappings(HistoricalData, new_data_df.to_dict(orient="records"))
        session.commit()

        return data_df