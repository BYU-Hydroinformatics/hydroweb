from django.shortcuts import render
from tethys_sdk.permissions import login_required
from django.http import JsonResponse
import requests
import json
import pandas as pd
from .app import Hydroweb as app
from rest_framework.decorators import api_view,authentication_classes, permission_classes
from django.test.client import Client
from .model import River, Lake
import geopandas as gpd
from sqlalchemy.orm import sessionmaker


Persistent_Store_Name = 'virtual_stations'

@login_required()
def home(request):
    client = Client(SERVER_NAME='localhost')
    resp = client.get('/getVirtualStationData/', data={'product': 'R_MAGDALENA-2_MAGDALENA_KM0839'}, follow=True)

    print(resp)
    context = {}

    return render(request, 'hydroweb/home.html', context)

@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def getVirtualStationData(request):
    print(request)
    resp_obj = {}
    product = request.data.get('product')
    user = app.get_custom_setting('Hydroweb Username')
    pwd = app.get_custom_setting('Hydroweb Password')
    url= f'https://hydroweb.theia-land.fr/hydroweb/authdownload?products={product}&format=json&user={user}&pwd={pwd}'
    print(url)
    response= requests.get(url)
    json_obj = json.loads(response.text)
    data_obj = json_obj['data']
    resp_obj['geometry'] = json_obj['geometry']
    resp_obj['properties'] = json_obj['properties']
    print(data_obj)
    df = pd.DataFrame.from_dict(data_obj)
    if product.startswith('R'):

        data_df = df[['date', 'orthometric_height_of_water_surface_at_reference_position', 'associated_uncertainty']].copy()
        data_df ["up_uncertainty"] = data_df['orthometric_height_of_water_surface_at_reference_position'] + data_df['associated_uncertainty']
        data_df ["down_uncertainty"] = data_df['orthometric_height_of_water_surface_at_reference_position'] - data_df['associated_uncertainty']
        data_dict = data_df.to_dict('records')

        resp_obj['data'] = data_dict
        # resp_obj['data'] ={
        #     'dates': data_df['date'].to_list(),
        #     'values': data_df['orthometric_height_of_water_surface_at_reference_position'].to_list(),
        #     'uncertainties': data_df['associated_uncertainty'].to_list()
        # }
    else:
        data_df = df[['datetime', 'water_surface_height_above_reference_datum', 'water_surface_height_uncertainty','area','volume']].copy()
        data_df ["up_uncertainty"] = data_df['water_surface_height_above_reference_datum'] + data_df['water_surface_height_uncertainty']
        data_df ["down_uncertainty"] = data_df['water_surface_height_above_reference_datum'] - data_df['water_surface_height_uncertainty']
        data_dict = data_df.to_dict('records')
        resp_obj['data'] = data_dict
    return JsonResponse(resp_obj)

@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def virtual_stations(request):
    geojson_stations = {}

    SessionMaker = app.get_persistent_store_database(Persistent_Store_Name, as_sessionmaker=True)
    session = SessionMaker()

    only_rivers_features= session.query(River.geom.ST_AsGeoJSON(), River.river_name, River.basin,River.status,River.validation,River.name).all()
    only_lakes_features= session.query(Lake.geom.ST_AsGeoJSON(), Lake.lake_name, Lake.basin,Lake.status,Lake.validation,Lake.name).all()
    session.commit()
    session.close()
    features = []

    for only_rivers_feature in only_rivers_features:
        river_extent_feature = {
            'type': 'Feature',
            'geometry': json.loads(only_rivers_feature[0]),
            'properties':{
                'river_name': only_rivers_feature[1],
                'basin':only_rivers_feature[2],
                'status':only_rivers_feature[3],
                'validation':only_rivers_feature[4],
                'comid': only_rivers_feature[5]

            }

        }
        features.append(river_extent_feature)

    for only_lakes_feature in only_lakes_features:
        lake_extent_feature = {
            'type': 'Feature',
            'geometry': json.loads(only_lakes_feature[0]),
            'properties':{
                'lake_name': only_lakes_feature[1],
                'basin':only_lakes_feature[2],
                'status':only_lakes_feature[3],
                'validation':only_lakes_feature[4],
                'comid': only_lakes_feature[5]

            }

        }
        features.append(lake_extent_feature)
    geojson_stations = {
        'type': 'FeatureCollection',
        'crs': {
            'type': 'name',
            'properties': {
                'name': 'EPSG:4326'
            }
        },
        'features': features
    }
    
    return JsonResponse(geojson_stations)