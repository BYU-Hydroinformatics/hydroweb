from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, JSON

import pandas as pd
import json
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
    comid = Column(String)


    def __init__(self, wkt, name,lat,lon,river_name, basin,status,validation,comid):
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
        self.comid = comid


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


class HydrowebMetaData(Base):
    """
    SQLAlchemy Metadata Table for the Hydroweb product - reach-id
    """
    __tablename__ = 'hydroweb_meta_data'
    id = Column(Integer, primary_key=True)  # Record number.

    hydroweb_product = Column(String)
    geometry = Column(JSON)
    properties = Column(JSON)
    def __init__(self, hydroweb_product, geometry, properties):
        self.hydroweb_product = hydroweb_product
        self.geometry = geometry
        self.properties = properties

class HydrowebData(Base):
    """
    SQLAlchemy Hydroweb Water Level Data
    """
    __tablename__ = 'hydroweb_water_level_data'
    id = Column(Integer, primary_key=True)  # Record number.

    hydroweb_product = Column(String)
    datetime = Column(String)
    water_level = Column(Numeric)
    type_data = Column(String)
    def __init__(self, hydroweb_product,type_data, datetime, water_level):
        self.type_data = type_data
        self.hydroweb_product = hydroweb_product
        self.datetime= datetime
        self.water_level= water_level


class HistoricalCorrectedData(Base):
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

class ForecastData(Base):
    """
    SQLAlchemy interface for projects table
    """
    __tablename__ = 'forecast_data'
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
    historical_data_query = session.query(HistoricalCorrectedData).filter(HistoricalCorrectedData.reach_id == comid, HistoricalCorrectedData.type_data == type_data)
    session.commit()
    # print("hey", response_content)
    if historical_data_query.first() is not None:
        historical_df = pd.read_sql(historical_data_query.statement, historical_data_query.session.bind, index_col='datetime')
        historical_df = historical_df.rename(columns={'stream_flow':'y','datetime':'x'})
        historical_df = historical_df.drop(columns=['reach_id', 'id','type_data'])
        return historical_df

    else:
        # breakpoint()
        # data_df[data_df < 0] = 0
        data_df.index = pd.to_datetime(data_df.index)
        data_df.index = data_df.index.to_series().dt.strftime("%Y-%m-%d")
        new_data_df = data_df.assign(reach_id=comid)[['reach_id'] + data_df.columns.tolist()]
        new_data_df['type_data'] = type_data 

        # new_data_df = new_data_df.rename(columns={'streamflow_m^3/s': 'stream_flow'})
        new_data_df = new_data_df.reset_index()
        session.bulk_insert_mappings(HistoricalCorrectedData, new_data_df.to_dict(orient="records"))
        session.commit()

        return data_df


def cache_hydroweb_data(hydroweb_product,url,session):
    resp_obj = {}
    water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == hydroweb_product)
    session.commit()
    # print("hey", response_content)
    if water_level_data_query.first() is not None:
        if hydroweb_product.startswith('R'):
            df_val,df_min,df_max = retrive_hydroweb_river_data(water_level_data_query)
            # water_level_df = pd.read_sql(water_level_data_query.statement, water_level_data_query.session.bind, index_col='datetime')
            # water_level_df = water_level_df.reset_index()
            
            # water_level_df = water_level_df.rename(columns={'water_level':'y','datetime':'x'})
            # # Split dataframe into different types

            # df_val = water_level_df[water_level_df['type_data'] == "observed_mean"]
            # df_min = water_level_df[water_level_df['type_data'] == "observed_min"]
            # df_max = water_level_df[water_level_df['type_data'] == "observed_max"]

            # df_val = df_val.drop(columns=['id','type_data','hydroweb_product'])
            # df_min = df_min.drop(columns=['id','type_data','hydroweb_product'])
            # df_max = df_max.drop(columns=['id','type_data','hydroweb_product'])
            # breakpoint()
            df_min_extra = df_min.copy()
            df_min_extra = df_min_extra.rename(columns={'y': 'y0'})
            df_min_extra = df_min_extra.reset_index(drop=True)

            # df_min = df_min.rename(columns={'y': 'y0'})
            df_min = df_min.reset_index(drop=True)
            data_val = df_val.to_dict('records')
            data_min = df_min.to_dict('records')
            df_max= df_max.reset_index(drop=True)
            data_max = df_max.to_dict('records')

            df_max_min = pd.concat([df_min_extra,df_max], axis=1)
            df_max_min_val = df_max_min.to_dict('records')

            resp_obj['data'] = {
                'val': data_val,
                'min_max': df_max_min_val,
                'min': data_min,
                'max': data_max
            }

            ## Query metadata 
            metdata_object = session.query(HydrowebMetaData).filter(HydrowebMetaData.hydroweb_product == hydroweb_product).first()
            resp_obj['geometry'] = json.loads(metdata_object.geometry)
            resp_obj['properties'] = json.loads(metdata_object.properties)
            # breakpoint()
            return resp_obj

    else:
        # breakpoint()
        # data_df[data_df < 0] = 0
        response= requests.get(url)
        json_obj = json.loads(response.text)
        data_obj = json_obj['data']
        metadata_instance = HydrowebMetaData(hydroweb_product=hydroweb_product, geometry= json.dumps(json_obj['geometry']), properties= json.dumps(json_obj['properties']))
        session.add(metadata_instance)
        resp_obj['geometry'] = json_obj['geometry']
        resp_obj['properties'] = json_obj['properties']
        # print(data_obj)
        df = pd.DataFrame.from_dict(data_obj)

        if hydroweb_product.startswith('R'):

            data_df = df[['date', 'orthometric_height_of_water_surface_at_reference_position', 'associated_uncertainty']].copy()
            data_df ["up_uncertainty"] = data_df['orthometric_height_of_water_surface_at_reference_position'] + data_df['associated_uncertainty']
            data_df ["down_uncertainty"] = data_df['orthometric_height_of_water_surface_at_reference_position'] - data_df['associated_uncertainty']
            
            df_val = data_df[['date', 'orthometric_height_of_water_surface_at_reference_position']].copy()
            df_val = df_val.rename(columns={'date': 'x', 'orthometric_height_of_water_surface_at_reference_position': 'y'})

            df_min = data_df[['date', 'down_uncertainty']].copy()
            df_min = df_min.rename(columns={'date': 'x', 'down_uncertainty': 'y'})

            df_max = data_df[['date', 'up_uncertainty']].copy()
            df_max = df_max.rename(columns={'date': 'x', 'up_uncertainty': 'y'})

            
            data_val = df_val.to_dict('records')
            data_max = df_max.to_dict('records')
            data_min = df_min.to_dict('records')

            resp_obj['data'] = {
                'val': data_val,
                'min': data_min,
                'max': data_max
            }
            ## Persist data into the Model ##

            insert_hydroweb_data(session,df_val,"observed_mean",hydroweb_product)
            insert_hydroweb_data(session,df_min,"observed_min",hydroweb_product)
            insert_hydroweb_data(session,df_max,"observed_max",hydroweb_product)

            return resp_obj

    return resp_obj

def insert_hydroweb_data(session,data_df,type_data,hydroweb_product):
    new_data_df = data_df.rename(columns={'x': 'datetime', 'y': 'water_level'})
    new_data_df['type_data'] = type_data
    new_data_df['hydroweb_product'] = hydroweb_product 
    new_data_df = new_data_df.reset_index()
    session.bulk_insert_mappings(HydrowebData, new_data_df.to_dict(orient="records"))
    session.commit()

def retrive_hydroweb_river_data(water_level_data_query):

    water_level_df = pd.read_sql(water_level_data_query.statement, water_level_data_query.session.bind, index_col='datetime')
    water_level_df = water_level_df.reset_index()

    water_level_df = water_level_df.rename(columns={'water_level':'y','datetime':'x'})
    # Split dataframe into different types

    df_val = water_level_df[water_level_df['type_data'] == "observed_mean"]
    df_min = water_level_df[water_level_df['type_data'] == "observed_min"]
    df_max = water_level_df[water_level_df['type_data'] == "observed_max"]

    df_val = df_val.drop(columns=['id','type_data','hydroweb_product'])
    df_min = df_min.drop(columns=['id','type_data','hydroweb_product'])
    df_max = df_max.drop(columns=['id','type_data','hydroweb_product'])

    return [df_val,df_min,df_max]  
# def cache_forecast_data(data_df,comid,type_data,session):
#     forecast_data_query = session.query(ForecastData).filter(ForecastData.reach_id == comid, ForecastData.type_data == type_data)
#     session.commit()
#     # print("hey", response_content)
#     if forecast_data_query.first() is not None:
#         historical_df = pd.read_sql(forecast_data_query.statement, forecast_data_query.session.bind, index_col='datetime')
#         historical_df = historical_df.rename(columns={'stream_flow':'y','datetime':'x'})
#         historical_df = historical_df.drop(columns=['reach_id', 'id','type_data'])
#         return historical_df

#     else:
#         # breakpoint()
#         # data_df[data_df < 0] = 0
#         data_df.index = pd.to_datetime(data_df.index)
#         data_df.index = data_df.index.to_series().dt.strftime("%Y-%m-%d")
#         new_data_df = data_df.assign(reach_id=comid)[['reach_id'] + data_df.columns.tolist()]
#         new_data_df['type_data'] = type_data 

#         # new_data_df = new_data_df.rename(columns={'streamflow_m^3/s': 'stream_flow'})
#         new_data_df = new_data_df.reset_index()
#         session.bulk_insert_mappings(HistoricalCorrectedData, new_data_df.to_dict(orient="records"))
#         session.commit()

#         return data_df
