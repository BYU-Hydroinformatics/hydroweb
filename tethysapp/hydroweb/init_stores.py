# Put your persistent store initializer functions in here
import os
from sqlalchemy.orm import sessionmaker
from .model import Base, Lake, River

from requests import Request
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
from .app import Hydroweb as app

def init_flooded_addresses_db(engine,first_time):
    """
    Initialize the flooded addresses database.
    """
    # STEP 1: Create database tables
    Base.metadata.create_all(engine)
    # STEP 2: Add data to the database
    if first_time:
        print("initializing database")
        # Find path of parent directory relative to this file

        # Create a session object in preparation for interacting with the database
        SessionMaker = sessionmaker(bind=engine)
        session = SessionMaker()
        file_path = os.path.join(app.get_app_workspace().path, 'vstations.json')
        # url = 'https://geoserver.hydroshare.org/geoserver/HS-22714855232d44198d12aa4109ec8478/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=HS-22714855232d44198d12aa4109ec8478:GEOGLOWS_SilviaV3&outputFormat=application/json'
        # q = Request('GET', url).prepare().url
        # df = gpd.read_file(q, format='GML')
        df = gpd.read_file(file_path, format='GML')
        
        df.crs = 'EPSG:4326'
        df['geom'] = df['geometry'].apply(lambda x: WKTElement(x.wkt, srid=4326))
        df.drop('geometry', 1, inplace=True)
        
        # print(df)
        df_river = df[df['river'] != ""]
        df_lake = df[df['lake'] != ""]

        dest_river = gpd.GeoDataFrame(columns=['name','lat','lon','river_name', 'basin','status','validation', 'geom'], geometry='geom')
        dest_river['geom'] = df_river['geom']

        dest_river['name']= df_river['name']
        dest_river['lat']= df_river['lat']
        dest_river['lon']= df_river['lon']
        dest_river['river_name']= df_river['river']
        dest_river['basin']= df_river['basin']
        dest_river['status']= df_river['status']
        dest_river['validation']= df_river['validation']

        dest_lake = gpd.GeoDataFrame(columns=['name','lat','lon','lake_name', 'basin','status','validation', 'geom'], geometry='geom')
        dest_lake['geom'] = df_lake['geom']
        dest_lake['name']= df_lake['name']
        dest_lake['lat']= df_lake['lat']
        dest_lake['lon']= df_lake['lon']
        dest_lake['lake_name']= df_lake['lake']
        dest_lake['basin']= df_lake['basin']
        dest_lake['status']= df_lake['status']
        dest_lake['validation']= df_lake['validation']

        # print(dest_river)
        # print(dest_lake)

        dest_river.to_sql('river', engine, if_exists='append', index=False, 
                         dtype={'geom': Geometry('POINT', srid= 4326)})

        dest_lake.to_sql('lake', engine, if_exists='append', index=False, 
                         dtype={'geom': Geometry('POINT', srid= 4326)})

        session.commit()

        # Close the connection to prevent issues
        session.close()
