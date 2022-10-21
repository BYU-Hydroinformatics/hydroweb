from bdb import Breakpoint
from django.shortcuts import render
from tethys_sdk.permissions import login_required
from django.http import JsonResponse
from .app import Hydroweb as app
from rest_framework.decorators import api_view,authentication_classes, permission_classes
from django.test.client import Client
from .model import HistoricalCorrectedData, River, Lake
from tethys_sdk.routing import controller
from channels.layers import get_channel_layer
from tethysext.hydroviewer.controllers.utilities import Utilities
from tethysext.hydroviewer.model import ForecastRecords, HistoricalSimulation, ReturnPeriods
from asgiref.sync import sync_to_async
from .model import cache_historical_data, cache_hydroweb_data,retrive_hydroweb_river_data, HydrowebData
from datetime import dt

import requests
import json
import pandas as pd
import numpy as np
import io
import os
import geoglows
import asyncio
import httpx
import traceback
import math

Persistent_Store_Name = 'virtual_stations'
async_client = httpx.AsyncClient()
hydroviewer_utility_object = Utilities()

try: 
    SessionMaker = app.get_persistent_store_database("virtual_stations", as_sessionmaker=True)
    session = SessionMaker()
except Exception as e:
    print("assign database")


@controller(name='home',url='hydroweb')
@login_required()
def home(request):

    context = {}

    return render(request, 'hydroweb/home.html', context)



@controller(name='getVirtualStationData',url='hydroweb/getVirtualStationData')
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

    resp_obj = cache_hydroweb_data(product,url,session)

    # response= requests.get(url)
    # json_obj = json.loads(response.text)
    # data_obj = json_obj['data']
    # resp_obj['geometry'] = json_obj['geometry']
    # resp_obj['properties'] = json_obj['properties']
    # # print(data_obj)
    # df = pd.DataFrame.from_dict(data_obj)
    # if product.startswith('R'):
        
    #     data_df = df[['date', 'orthometric_height_of_water_surface_at_reference_position', 'associated_uncertainty']].copy()
    #     data_df ["up_uncertainty"] = data_df['orthometric_height_of_water_surface_at_reference_position'] + data_df['associated_uncertainty']
    #     data_df ["down_uncertainty"] = data_df['orthometric_height_of_water_surface_at_reference_position'] - data_df['associated_uncertainty']
        
    #     df_val = data_df[['date', 'orthometric_height_of_water_surface_at_reference_position']].copy()
    #     df_val = df_val.rename(columns={'date': 'x', 'orthometric_height_of_water_surface_at_reference_position': 'y'})

    #     df_min = data_df[['date', 'down_uncertainty']].copy()
    #     df_min = df_min.rename(columns={'date': 'x', 'down_uncertainty': 'y'})

    #     df_max = data_df[['date', 'up_uncertainty']].copy()
    #     df_max = df_max.rename(columns={'date': 'x', 'up_uncertainty': 'y'})


    #     data_val = df_val.to_dict('records')
    #     data_max = df_max.to_dict('records')
    #     data_min = df_min.to_dict('records')


    #     # data_dict = data_df.to_dict('records')

    #     # resp_obj['data'] = data_dict
    #     resp_obj['data'] = {
    #         'val': data_val,
    #         'min': data_min,
    #         'max': data_max
    #     }

    #     ## saving it to the workspaces for alter use ##
    #     if not os.path.exists(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json')):
    #         df_val.to_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json'))
    #         df_max.to_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_max.json'))
    #         df_min.to_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_min.json'))
    #     else:
    #         print("files are already saved")

    # else:
    #     data_df = df[['datetime', 'water_surface_height_above_reference_datum', 'water_surface_height_uncertainty','area','volume']].copy()
    #     data_df ["up_uncertainty"] = data_df['water_surface_height_above_reference_datum'] + data_df['water_surface_height_uncertainty']
    #     data_df ["down_uncertainty"] = data_df['water_surface_height_above_reference_datum'] - data_df['water_surface_height_uncertainty']
    #     data_dict = data_df.to_dict('records')
    #     resp_obj['data'] = data_dict
    return JsonResponse(resp_obj)


@controller(name='getVirtualStations',url='hydroweb/getVirtualStations')
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
                'comid': only_rivers_feature[5],
                'comid_geoglows':9007781

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

# we need to check how to enable all middleware to have asynch views
@controller(name='saveHistoricalSimulationData',url='hydroweb/saveHistoricalSimulationData') 
@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def saveHistoricalSimulationData(request):
    print("success")
    reach_id = request.data.get('reach_id')
    product = request.data.get('product')

    # reach_id = request.GET.get('reach_id')
    # reach_id = 9890
    print(reach_id)
    # loop = asyncio.get_event_loop()
    response = "executing"
    try:
        api_base_url = 'https://geoglows.ecmwf.int/api'        
        asyncio.run(make_api_calls(api_base_url,reach_id,product))

    except Exception as e:
        print('saveHistoricalSimulationData error')
        print(e)
        print(traceback.format_exc())
    finally:
        print('finally')

        # loop.close()

    ## here you need to do the controller asyncronico y ver si da o si no simplemente asi
    



    # return_format = request.data.get('return_format')
    # json_data = request.data.get('data')
    # era_res = requests.get(
    #      'https://geoglows.ecmwf.int/api/HistoricSimulation/?reach_id=' + reach_id + '&return_format=' + return_format,
    #     verify=False).content

    # simulated_df = pd.read_csv(io.StringIO(era_res.decode('utf-8')), index_col=0)
    # simulated_df[simulated_df < 0] = 0

    # simulated_df.to_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}'))

    # simulated_df[simulated_df < 0] = 0
    # simulated_df.index = pd.to_datetime(simulated_df.index)
    # simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
    # simulated_df.index = pd.to_datetime(simulated_df.index)
    return JsonResponse({'state':response })
    # return HttpResponse('Non-blocking HTTP request')

async def make_api_calls(api_base_url,reach_id,product):
    print("here")
    historical_simulation_query = session.query(HistoricalSimulation).filter(HistoricalSimulation.reach_id == reach_id)
    if historical_simulation_query.first() is None:

    # if not os.path.exists(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}.json')):
        task_get_geoglows_data = await asyncio.create_task(api_call(api_base_url,reach_id,product))
    
    else:
        task_get_geoglows_data = await asyncio.create_task(fake_print(api_base_url,reach_id,product))

    return task_get_geoglows_data

# A fast co-routine
async def fake_print(api_base_url,reach_id,product):
    print(f'request at {api_base_url},for {reach_id} already saved')
    await asyncio.sleep(1) # Mimic some network delay
    channel_layer = get_channel_layer()
   
    await channel_layer.group_send(
        "notifications_hydroweb",
        {
            "type": "simple_notifications",
            "reach_id": reach_id,
            "product": product,
            "mssg": "Complete",
            "command": "Data_Downloaded",

        },
    )
    return 0


async def api_call(api_base_url,reach_id,product):
    print("ghere async")
    mssge_string = "Complete"
    channel_layer = get_channel_layer()
    print(reach_id)
    print(f"{api_base_url}/HistoricSimulation/")

    try:
        response_await = await async_client.get(
                    url = f"{api_base_url}/HistoricSimulation/",
                    params = {
                        "reach_id": reach_id
                    },
                    timeout=None          
        )

        # response = response_await.json()
        # print(response_await)
        # print(response_await.text)

        print("saving data bro")
        simulated_df = hydroviewer_utility_object.cache_historical_simulation(app,api_base_url,reach_id,session,response_content=response_await.text)
        print(simulated_df.head)
        session.close()

        # simulated_df = pd.read_csv(io.StringIO(response_await.text), index_col=0)
        # simulated_df[simulated_df < 0] = 0

        # simulated_df.to_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}.json'))

        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Data_Downloaded",
                "mssg": mssge_string,
            },
        )
    except httpx.HTTPError as exc:

        print("api_call error")
        print(f"Error while requesting {exc.request.url!r}.")

        print(str(exc.__class__.__name__))
        mssge_string = "incomplete"
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Data_Downloaded Error",
                
            },
        )
    except Exception as e:
        print("api_call error 2")
        print(e)
    return mssge_string

# async def retrieve_data_from_file(data_id):
#     print("retrieve_data_from_file")
#     task_get_file_data = await asyncio.create_task(retrieve_data(data_id))
#     return task_get_file_data



# async def retrieve_data(data_id):
#     simulated_df = pd.read_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{data_id}.json'))
#     print("hola")
#     await print(simulated_df)
#     pass

def retrieve_data(data_id,product):
    json_obj = {}
    print("retriving data bro")

    simulated_df = hydroviewer_utility_object.cache_historical_simulation(active_app=app,cs_api_source=None,comid=data_id, session=session,response_content=None)
    session.close()

    # simulated_df = pd.read_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{data_id}.json'))
    # # Removing Negative Values
    # simulated_df[simulated_df < 0] = 0
    # simulated_df.index = pd.to_datetime(simulated_df.index)
    # simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
    # simulated_df.index = pd.to_datetime(simulated_df.index)
    simulated_df = simulated_df.reset_index()
    print(simulated_df)
    simulated_df = simulated_df.rename(columns={'datetime': 'x', 'streamflow_m^3/s': 'y'})
    # simulated_df['x']= simulated_df['x'].dt.strftime('%Y-%m-%d')

    simulated_json = simulated_df.to_json(orient='records')

    mssge_string = "Plot_Data"
    json_obj["data"] = simulated_json
    json_obj["mssg"] = "complete"
    json_obj['type'] = 'data_notifications'
    json_obj['product'] = product,
    json_obj['reach_id'] = data_id,
    json_obj['command'] = mssge_string

    return json_obj

# A fast co-routine
async def fake_print2(reach_id,product):
    print(f'request at corrected data,for {reach_id} already saved')
    await asyncio.sleep(1) # Mimic some network delay
    channel_layer = get_channel_layer()
   
    await channel_layer.group_send(
        "notifications_hydroweb",
        {
            "type": "simple_notifications",
            "reach_id": reach_id,
            "product": product,
            "mssg": "Complete",
            "command": "Bias_Data_Downloaded",

        },
    )
    return 0

async def make_bias_correction(reach_id,product):
    historical_corrected_simulation_query = session.query(HistoricalCorrectedData).filter(HistoricalCorrectedData.reach_id == reach_id)
    if historical_corrected_simulation_query.first() is None:
    # print(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json'))

    # if not os.path.exists(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json')):
        print("not here")
        task_get_bias_geoglows_data = await asyncio.create_task(bias_correction(product,reach_id))
    else:
        task_get_bias_geoglows_data = await asyncio.create_task(fake_print2(reach_id,product))

    return task_get_bias_geoglows_data

# we need to check how to enable all middleware to have asynch views
@controller(name='executeBiasCorrection',url='hydroweb/executeBiasCorrection') 
@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def executeBiasCorrection(request):
    print("success")
    reach_id = request.data.get('reach_id')
    product = request.data.get('product')

    # reach_id = request.GET.get('reach_id')
    # reach_id = 9890
    print(reach_id)
    # loop = asyncio.get_event_loop()
    response = "executing"
    try:

        asyncio.run(make_bias_correction(reach_id,product))

    except Exception as e:
        print('executeBiasCorrection error')
        print(e)
    finally:
        print('finally')
    return JsonResponse({'state':response })

async def bias_correction(product,reach_id):
    channel_layer = get_channel_layer()
    
    mssge_string = "Complete"
    try:
        #Hydroweb Observed Data
        water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == product)
        mean_wl,min_wl,max_wl = retrive_hydroweb_river_data(water_level_data_query)

        # mean_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json'))
        mean_wl.set_index('x', inplace=True)

        mean_wl.index = pd.to_datetime(mean_wl.index)

        # min_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_min.json'))
        min_wl.set_index('x', inplace=True)
        min_wl.index = pd.to_datetime(min_wl.index)
        
        # max_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_max.json'))
        max_wl.set_index('x', inplace=True)
        max_wl.index = pd.to_datetime(max_wl.index)

        #Mean Water Level
        min_value1 = mean_wl['y'].min()
        if min_value1 >= 0:
            min_value1 = 0

        mean_adjusted = mean_wl - min_value1

        #Min Water Level
        min_value2 = min_wl['y'].min()

        if min_value2 >= 0:
            min_value2 = 0

        min_adjusted = min_wl - min_value2

        #Max Water Level
        min_value3 = max_wl['y'].min()

        if min_value3 >= 0:
            min_value3 = 0

        max_adjusted = max_wl - min_value3

        #Geoglows Historical Simulation Data
        simulated_df = hydroviewer_utility_object.cache_historical_simulation(app,None,reach_id,session,response_content=None)
        simulated_df.index = pd.to_datetime(simulated_df.index)

        # print("from caache")
        # print(simulated_df)
        # simulated_df = pd.read_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}.json'))
        # print("from file")
        # print(simulated_df)
        #Bias Correction 

        #Mean Water Level
        corrected_mean_wl = geoglows.bias.correct_historical(simulated_df, mean_adjusted)
        corrected_mean_wl = corrected_mean_wl + min_value1

        #Min Water Level
        corrected_min_wl = geoglows.bias.correct_historical(simulated_df, min_adjusted)
        corrected_min_wl = corrected_min_wl + min_value2

        #Max Water Level
        corrected_max_wl = geoglows.bias.correct_historical(simulated_df, max_adjusted)
        corrected_max_wl = corrected_max_wl + min_value3

        #Save the bias corrected data locally
        corrected_mean_wl.index = corrected_mean_wl.index.to_series().dt.strftime("%Y-%m-%d")
        corrected_min_wl.index = corrected_min_wl.index.to_series().dt.strftime("%Y-%m-%d")
        corrected_max_wl.index = corrected_max_wl.index.to_series().dt.strftime("%Y-%m-%d")

        corrected_mean_wl = corrected_mean_wl.reset_index()
        corrected_min_wl = corrected_min_wl.reset_index()
        corrected_max_wl= corrected_max_wl.reset_index()
        # corrected_mean_wl = simulated_df.rename(columns={'Corrected Simulated Streamflow': 'x', 'streamflow_m^3/s': 'y'})
        # corrected_mean_wl = corrected_mean_wl.rename(columns={'index': 'x', 'Corrected Simulated Streamflow': 'y'})
        # corrected_min_wl = corrected_min_wl.rename(columns={'index': 'x', 'Corrected Simulated Streamflow': 'y'})
        # corrected_max_wl = corrected_max_wl.rename(columns={'index': 'x', 'Corrected Simulated Streamflow': 'y'})
        # print(corrected_mean_wl)

        # breakpoint()

        corrected_mean_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json'))
        corrected_min_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_min.json'))
        corrected_max_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_max.json'))
        
        
        corrected_mean_wl = corrected_mean_wl.rename(columns={'index': 'datetime', 'Corrected Simulated Streamflow': 'stream_flow'})
        # breakpoint()
        cache_historical_data(corrected_mean_wl,reach_id,"corrected_mean",session=session)
        # breakpoint()
        corrected_min_wl = corrected_min_wl.rename(columns={'index': 'datetime', 'Corrected Simulated Streamflow': 'stream_flow'})
        
        cache_historical_data(corrected_min_wl,reach_id,"corrected_min",session=session)
        corrected_max_wl = corrected_max_wl.rename(columns={'index': 'datetime', 'Corrected Simulated Streamflow': 'stream_flow'})
        cache_historical_data(corrected_max_wl,reach_id,"corrected_max",session=session)


        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Bias_Data_Downloaded",
                "mssg": mssge_string,
            },
        )
    except Exception as e:
        print("bias correction error 2")
        print(e)
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Bias_Data_Downloaded_Error",
                
            },
        )   
    # mssge_string = "Plot_Data"
    # json_obj["data"] = simulated_json
    # json_obj["mssg"] = "complete"
    # json_obj['type'] = 'data_notifications'
    # json_obj['product'] = product,
    # json_obj['reach_id'] = data_id,
    # json_obj['command'] = mssge_string
    
    # channel_layer = get_channel_layer()
    # await channel_layer.group_send (
    #     "notifications_hydroweb",
    #     json_obj,
    # )
    return mssge_string


def retrieve_data_bias_corrected(data_id,product):


    print(data_id,product )
    json_obj = {}
    # print(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_mean.json'))
    # if os.path.exists(os.path.join(app.get_app_workspace().path,f'corrected/{data_id}_mean.json')):

    # corrected_df_mean = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_mean.json'))
    # print(corrected_df_mean)
    # corrected_df_min = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_max.json'))
    # print(corrected_df_min)
    
    # corrected_df_max = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_min.json'))
    # print(corrected_df_max)
    # breakpoint()
    corrected_df_mean  = cache_historical_data(data_df=None,comid=data_id,type_data="corrected_mean",session=session)
    corrected_df_mean = corrected_df_mean.reset_index()

    corrected_df_mean = corrected_df_mean.rename(columns={'datetime': 'x', 'stream_flow': 'y'})
    
    corrected_df_min  = cache_historical_data(data_df=None,comid=data_id,type_data="corrected_min",session=session)
    corrected_df_min = corrected_df_min.reset_index()

    corrected_df_min = corrected_df_min.rename(columns={'datetime': 'x', 'stream_flow': 'y'})

    corrected_df_max  = cache_historical_data(data_df=None,comid=data_id,type_data="corrected_max",session=session)
    corrected_df_max = corrected_df_max.reset_index()

    corrected_df_max = corrected_df_max.rename(columns={'datetime': 'x', 'stream_flow': 'y'})
    # breakpoint()

    
    data_val = corrected_df_mean.to_dict('records')
    data_max = corrected_df_max.to_dict('records')
    data_min = corrected_df_min.to_dict('records')
    json_obj['data'] = {
        'val': data_val,
        'min': data_min,
        'max': data_max
    }
    # Removing Negative Values
    
    
    # corrected_df_mean[''] = pd.to_datetime(simulated_df.index)
    # simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
    # simulated_df.index = pd.to_datetime(simulated_df.index)
    # simulated_df = simulated_df.reset_index()
    # # print("hola")
    # print(simulated_df)
    # simulated_df = simulated_df.rename(columns={'index': 'x', 'streamflow_m^3/s': 'y'})
    # simulated_df['x']= simulated_df['x'].dt.strftime('%Y-%m-%d')

    # simulated_json = simulated_df.to_json(orient='records')

    mssge_string = "Plot_Bias_Corrected_Data"
    # json_obj["data"] = simulated_json
    json_obj["mssg"] = "complete"
    json_obj['type'] = 'data_notifications'
    json_obj['product'] = product,
    json_obj['reach_id'] = data_id,
    json_obj['command'] = mssge_string



    # # data_dict = data_df.to_dict('records')

    # # resp_obj['data'] = data_dict
    # resp_obj['data'] = {
    #     'val': data_val,
    #     'min': data_min,
    #     'max': data_max
    # }
    

    return json_obj

@controller(name='saveForecastData',url='hydroweb/saveForecastData')
@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def saveForecastData(request):
    print("success")
    reach_id = request.data.get('reach_id')
    product = request.data.get('product')

    response = "executing"
    try:
        api_base_url = 'https://geoglows.ecmwf.int/api'        
        asyncio.run(make_forecast_api_calls(api_base_url,reach_id,product))

    except Exception as e:
        print('saveForecastData error')
        print(e)
        print(traceback.format_exc())
    finally:
        print('finally')   
    
    return JsonResponse({'state':response })

async def make_forecast_api_calls(api_base_url,reach_id,product):
    list_async_task = []
    ## check if today is the last day
    # today = date.today()

    # forecast_records_query = session.query(ForecastRecords).filter(ForecastRecords.reach_id == reach_id)

    # if not os.path.exists(os.path.join(app.get_app_workspace().path,f'ensemble_forecast_data/{reach_id}.json')):
    task_get_forecast_ensembles_geoglows_data = asyncio.create_task(forecast_ensembles_api_call(api_base_url,reach_id,product))
    # else:
        # task_get_forecast_ensembles_geoglows_data =  await asyncio.create_task(fake_print_custom(api_base_url,reach_id,product,"Forecast_Ensemble_Data_Downloaded"))
    list_async_task.append(task_get_forecast_ensembles_geoglows_data)
    # if forecast_records_query.first() is None and forecast_records_query.first().datetime :
    # if not os.path.exists(os.path.join(app.get_app_workspace().path,f'forecast_data/{reach_id}.json')):
    task_get_forecast_geoglows_data =  asyncio.create_task(forecast_api_call(api_base_url,reach_id,product))
    # else:
        # task_get_forecast_geoglows_data = await asyncio.create_task(fake_print_custom(api_base_url,reach_id,product,"Forecast_Data_Downloaded"))
    
    list_async_task.append(task_get_forecast_geoglows_data)
    
    results = await asyncio.gather(*list_async_task)

    return results


async def fake_print_custom(api_base_url,reach_id,product,command):
    print(f'{command} request at {api_base_url},for {reach_id} already saved')
    await asyncio.sleep(1) # Mimic some network delay
    channel_layer = get_channel_layer()
   
    await channel_layer.group_send(
        "notifications_hydroweb",
        {
            "type": "simple_notifications",
            "reach_id": reach_id,
            "product": product,
            "mssg": "Complete",
            "command": command,

        },
    )
    # return  0


async def forecast_ensembles_api_call(api_base_url,reach_id,product):
    mssge_string = "Complete"
    channel_layer = get_channel_layer()
    print(reach_id)
    print(f"{api_base_url}/ForecastEnsembles/")
    try:
        response_await = await async_client.get(
                    url = f"{api_base_url}/ForecastEnsembles/",
                    params = {
                        "reach_id": reach_id
                    },
                    timeout=None          
        )

        # response = response_await.json()
        print(response_await)
        # print(response_await.text)
        forecast_ens = pd.read_csv(io.StringIO(response_await.text), index_col=0)
        forecast_ens.index = pd.to_datetime(forecast_ens.index)
        forecast_ens[forecast_ens < 0] = 0
        forecast_ens.index = forecast_ens.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        forecast_ens.index = pd.to_datetime(forecast_ens.index)
        
        forecast_ens.to_json(os.path.join(app.get_app_workspace().path,f'ensemble_forecast_data/{reach_id}.json'))

        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Ensemble_Forecast_Data_Downloaded",
                "mssg": mssge_string,
            },
        )
    except httpx.HTTPError as exc:

        print("api_call error")
        print(f"Error while requesting {exc.request.url!r}.")

        print(str(exc.__class__.__name__))
        mssge_string = "incomplete"
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Ensemble_Forecast_Data_Downloaded_Error",
                
            },
        )
    except Exception as e:
        print("api_call error 2")
        print(e)
    return  mssge_string


async def forecast_api_call(api_base_url,reach_id,product):
    mssge_string = "Complete"
    channel_layer = get_channel_layer()
    print(reach_id)
    print(f"{api_base_url}/ForecastRecords/")
    try:
        response_await = await async_client.get(
                    url = f"{api_base_url}/ForecastRecords/",
                    params = {
                        "reach_id": reach_id
                    },
                    timeout=None          
        )

        # response = response_await.json()
        print(response_await)
        # breakpoint()
        # print(response_await.text)
        forecast_records = pd.read_csv(io.StringIO(response_await.text), index_col=0)
    
        # forecast_records = hydroviewer_utility_object.cache_forecast_records(app,api_base_url,reach_id,session,response_content=response_await.text)


        forecast_records.index = pd.to_datetime(forecast_records.index)
        forecast_records[forecast_records < 0] = 0
        forecast_records.index = forecast_records.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
        forecast_records.index = pd.to_datetime(forecast_records.index)
        
        forecast_records.to_json(os.path.join(app.get_app_workspace().path,f'forecast_data/{reach_id}.json'))

        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Forecast_Data_Downloaded",
                "mssg": mssge_string,
            },
        )
    except httpx.HTTPError as exc:

        print("api_call error")
        print(f"Error while requesting {exc.request.url!r}.")

        print(str(exc.__class__.__name__))
        mssge_string = "incomplete"
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Forecast_Data_Downloaded_Error",
                
            },
        )
    except Exception as e:
        print("api_call error 2")
        print(e)
    
    return mssge_string


@controller(name='executeForecastBiasCorrection',url='hydroweb/executeForecastBiasCorrection') 
@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def execute_forecast_bias_corection(request):
    print("success")
    reach_id = request.data.get('reach_id')
    product = request.data.get('product')

    # reach_id = request.GET.get('reach_id')
    # reach_id = 9890
    print(reach_id)
    # loop = asyncio.get_event_loop()
    response = "executing"
    try:

        asyncio.run(make_forecast_bias_correction(reach_id,product))

    except Exception as e:
        print('executeForecastBiasCorrection error')
        print(e)
    finally:
        print('finally')
    return JsonResponse({'state':response })

async def make_forecast_bias_correction(reach_id,product):
    # if historical_corrected_simulation_query.first() is None:
    # print(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json'))

    # if not os.path.exists(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json')):
        # print("not here")
    task_get_bias_forecast_records_geoglows_data = await asyncio.create_task(forecast_records_bias_correction(product,reach_id))
    task_get_bias_forecast_ensembles_geoglows_data = await asyncio.create_task(forecast_ensembles_bias_correction(product,reach_id))

    # else:
    # task_get_bias_geoglows_data = await asyncio.create_task(fake_print2(reach_id,product))
    results = await asyncio.gather([task_get_bias_forecast_records_geoglows_data,task_get_bias_forecast_ensembles_geoglows_data])

    return results

async def return_periods_bias_correction(product,reach_id):
    channel_layer = get_channel_layer()
    
    mssge_string = "Complete"
    try:
        
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Return_Period_Bias_Data_Downloaded",
                "mssg": mssge_string,
            },
        )
    except Exception as e:
        print("return period bias correction error 2")
        print(e)
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Return_Period_Bias_Data_Downloaded_Error",
                
            },
        )   

    return mssge_string 
async def forecast_records_bias_correction(product,reach_id):
    channel_layer = get_channel_layer()
    simulated_df = hydroviewer_utility_object.cache_historical_simulation(active_app=app,cs_api_source=None,comid=reach_id, session=session,response_content=None)

    #Hydroweb Observed Data
    water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == product)
    mean_wl,min_wl,max_wl = retrive_hydroweb_river_data(water_level_data_query)
        # mean_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json'))
    mean_wl.set_index('x', inplace=True)

    mean_wl.index = pd.to_datetime(mean_wl.index)

    # min_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_min.json'))
    min_wl.set_index('x', inplace=True)
    min_wl.index = pd.to_datetime(min_wl.index)
    
    # max_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_max.json'))
    max_wl.set_index('x', inplace=True)
    max_wl.index = pd.to_datetime(max_wl.index)

    #Mean Water Level
    min_value1 = mean_wl['y'].min()
    if min_value1 >= 0:
        min_value1 = 0

    #Min Water Level
    min_value2 = min_wl['y'].min()

    if min_value2 >= 0:
        min_value2 = 0

    #Max Water Level
    min_value3 = max_wl['y'].min()

    if min_value3 >= 0:
        min_value3 = 0

    
    forecast_record = pd.read_json(os.path.join(app.get_app_workspace().path,f'forecast_data/{reach_id}.json'))
    forecast_record.index = pd.to_datetime(forecast_record.index)


    date_ini = forecast_record.index[0]
    month_ini = date_ini.month
    
    date_end = forecast_record.index[-1]
    month_end = date_end.month
    if month_end < month_ini:
        meses1 = np.arange(month_ini, 13, 1)
        meses2 = np.arange(1, month_end + 1, 1)
        meses = np.concatenate([meses1, meses2])
    else:
        meses = np.arange(month_ini, month_end + 1, 1)

    fixed_records1 = correct_bias_forecast_records(forecast_record,simulated_df,meses,mean_wl,min_value1)
    fixed_records2 = correct_bias_forecast_records(forecast_record,simulated_df,meses,min_wl,min_value2)
    fixed_records3 = correct_bias_forecast_records(forecast_record,simulated_df,meses,max_wl,min_value3)
    
    # reading the cache from the bias corection of the forecast ensembles
    if os.path.exists(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_fixed_stats.json')) and os.path.exists(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df1.json')) :
        fixed_stats = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_fixed_stats.json'))
   
        high_res_df1 = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df1.json'))
        
        high_res_df2 = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df2.json'))

        high_res_df3 = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df3.json'))



        x_vals = (fixed_stats.index[0], fixed_stats.index[len(fixed_stats.index) - 1], fixed_stats.index[len(fixed_stats.index) - 1], fixed_stats.index[0])
        max_visible = max(fixed_stats.max())

        record_plot1 = fixed_records1.copy()
        record_plot1 = record_plot1.loc[record_plot1.index >= pd.to_datetime(fixed_stats.index[0] - dt.timedelta(days=8))]
        record_plot1 = record_plot1.loc[record_plot1.index <= pd.to_datetime(fixed_stats.index[0])]
        


        data_plot = {}

        if len(record_plot1.index) > 0:
            records_plot_1_new_df = record_plot1.iloc[:, 0]
            records_plot_1_new_df = records_plot_1_new_df.reset_index()
            
            records_plot_1_new_df.rename(columns={"datetime":'x',"streamflow_m^3/s": 'y'}, inplace=True)
            records_plot_1_dict = records_plot_1_new_df.to_dict('records')
            data_plot['record_plot1'] = records_plot_1_dict

            x_vals = (record_plot1.index[0], fixed_stats.index[len(fixed_stats.index) - 1], fixed_stats.index[len(fixed_stats.index) - 1], record_plot1.index[0])
            max_visible = max(record_plot1.max().values[0], max_visible)

        record_plot2 = fixed_records2.copy()
        record_plot2 = record_plot2.loc[record_plot2.index >= pd.to_datetime(fixed_stats.index[0] - dt.timedelta(days=8))]
        record_plot2 = record_plot2.loc[record_plot2.index <= pd.to_datetime(fixed_stats.index[0])]

        record_plot3 = fixed_records3.copy()
        record_plot3 = record_plot3.loc[record_plot3.index >= pd.to_datetime(fixed_stats.index[0] - dt.timedelta(days=8))]
        record_plot3 = record_plot3.loc[record_plot3.index <= pd.to_datetime(fixed_stats.index[0])]


        max_min_record_WL = {
            'x': np.concatenate([record_plot3.index, record_plot2.index[::-1]]).tolist(),
            'y': np.concatenate([record_plot3.iloc[:, 0].values, record_plot2.iloc[:, 0].values[::-1]]).tolist()
        }

        max_min_record_WL_df = pd.DataFrame(max_min_record_WL)
        max_min_record_WL_df_dict = max_min_record_WL_df.to_dict('records')

        max_record_WL = {
            'x': record_plot3.index.values.tolist(),
            'y': record_plot3.iloc[:, 0].values.tolist()
        }
        max_record_WL_df = pd.DataFrame(max_record_WL)
        max_record_WL_df_dict = max_record_WL_df.to_dict('records')


        min_record_WL = {
            'x': record_plot2.index.values.tolist(),
            'y': record_plot2.iloc[:, 0].values.tolist()
        }
        min_record_WL_df = pd.DataFrame(min_record_WL)
        min_record_WL_df_dict = min_record_WL_df.to_dict('records')

        if len(record_plot2.index) > 0:
            data_plot['max_min_record_WL'] = max_min_record_WL_df_dict
            data_plot['max_record_WL'] = max_record_WL_df_dict
            data_plot['min_record_WL'] = min_record_WL_df_dict


        ### check for refactoring 
        max_min_high_res_WL = {
            'x': np.concatenate([high_res_df3.index, high_res_df2.index[::-1]]).tolist(),
            'y': np.concatenate([high_res_df3.iloc[:, 0].values, high_res_df2.iloc[:, 0].values[::-1]]).tolist()
        }

        max_min_high_res_WL_df = pd.DataFrame(max_min_high_res_WL)
        max_min_high_res_WL_df_dict = max_min_high_res_WL_df.to_dict('records')

        max_high_res_WL = {
            'x': high_res_df3.index.values.tolist(),
            'y': high_res_df3.iloc[:, 0].values.tolist()
        }
        max_high_res_WL_df = pd.DataFrame(max_high_res_WL)
        max_high_res_WL_df_dict = max_high_res_WL_df.to_dict('records')

        min_high_res_WL = {
            'x': high_res_df2.index.values.tolist(),
            'y': high_res_df2.iloc[:, 0].values.tolist()
        }
        min_high_res_WL_df = pd.DataFrame(min_high_res_WL)
        min_high_res_WL_df_dict = min_high_res_WL_df.to_dict('records')
        
        if len(high_res_df2.index) > 0:
            data_plot['max_min_high_res_WL'] = max_min_high_res_WL_df_dict
            data_plot['max_high_res_WL'] = max_high_res_WL_df_dict
            data_plot['min_high_res_WL'] = min_high_res_WL_df_dict

    
    else: 
        await asyncio.sleep(4) 

    mssge_string = "Complete"
    try:
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "data_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Plot_Forecast_Records_Bias_Data_Downloaded",
                "mssg": mssge_string,
                "data": data_plot
            },
        )        

    except Exception as e:
        print("forecast bias correction error 2")
        print(e)
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Forecast_Records_Bias_Data_Downloaded_Error",
                
            },
        )   

    return mssge_string

async def forecast_ensembles_bias_correction(product,reach_id):
    channel_layer = get_channel_layer()
    forecast_ens = pd.read_json(os.path.join(app.get_app_workspace().path,f'ensemble_forecast_data/{reach_id}.json'))
    # forecast_ens = forecast_ens.reset_index()
    forecast_ens.index = pd.to_datetime(forecast_ens.index)

    # mean_wl.set_index('x', inplace=True)

    # mean_wl.index = pd.to_datetime(mean_wl.index)

    simulated_df = hydroviewer_utility_object.cache_historical_simulation(active_app=app,cs_api_source=None,comid=reach_id, session=session,response_content=None)
    # water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == product)
    # mean_wl,min_wl,max_wl = retrive_hydroweb_river_data(water_level_data_query)
    
    #Hydroweb Observed Data
    water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == product)
    mean_wl,min_wl,max_wl = retrive_hydroweb_river_data(water_level_data_query)

    # mean_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json'))
    mean_wl.set_index('x', inplace=True)

    mean_wl.index = pd.to_datetime(mean_wl.index)

    # min_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_min.json'))
    min_wl.set_index('x', inplace=True)
    min_wl.index = pd.to_datetime(min_wl.index)
    
    # max_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_max.json'))
    max_wl.set_index('x', inplace=True)
    max_wl.index = pd.to_datetime(max_wl.index)

    #Mean Water Level
    min_value1 = mean_wl['y'].min()
    if min_value1 >= 0:
        min_value1 = 0

    mean_adjusted = mean_wl - min_value1
    #Min Water Level
    min_value2 = min_wl['y'].min()

    if min_value2 >= 0:
        min_value2 = 0

    min_adjusted = min_wl - min_value2


    #Max Water Level
    min_value3 = max_wl['y'].min()

    if min_value3 >= 0:
        min_value3 = 0

    max_adjusted = max_wl - min_value3



    session.close()

    ensemble1, high_res_df1 = correct_bias_ensemble(simulated_df,forecast_ens,mean_adjusted,min_value1)
    ensemble2, high_res_df2 = correct_bias_ensemble(simulated_df,forecast_ens,min_adjusted,min_value2)
    ensemble3, high_res_df3 = correct_bias_ensemble(simulated_df,forecast_ens,max_adjusted,min_value3)

    ensemble_list = [ensemble1,ensemble2,ensemble3]
    max_df,p75_df,p25_df,min_df,mean_df,high_res_df_mean = retrieve_corrected_forecast_ensemble(ensemble_list,high_res_df1)
    data_max = max_df.to_dict('records')
    data_p75 = p75_df.to_dict('records')
    data_p25 = p25_df.to_dict('records')
    data_min = min_df.to_dict('records')
    data_mean = mean_df.to_dict('records')
    data_highres = high_res_df_mean.to_dict('records')


    fixed_stats = {
        'mean':data_mean,
        'min': data_min,
        'max': data_max,
        'p25': data_p25,
        'p75': data_p75,
        'high_res':data_highres
    }
    # This is a file cache, will need to be replaced more elegant in the future ... #
    fixed_stats_df_cache = pd.concat([max_df, p75_df, mean_df, p25_df, min_df, high_res_df_mean], axis=1)
    fixed_stats_df_cache.to_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_fixed_stats.json'))
    high_res_df1.to_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df1.json'))
    high_res_df2.to_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df2.json'))
    high_res_df3.to_json(os.path.join(app.get_app_workspace().path,f'corrected_forecast_data/{reach_id}_high_res_df3.json'))


    # breakpoint()
    mssge_string = "Complete"
    try:

        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "data_notifications",
                "reach_id": reach_id,
                "product": product,
                "command": "Plot_Forecast_Ensemble_Bias_Data_Downloaded",
                "mssg": mssge_string,
                "data": fixed_stats
            },
        )
    except Exception as e:
        print("forecast ensembles bias correction error 2")
        print(e)
        await channel_layer.group_send(
            "notifications_hydroweb",
            {
                "type": "simple_notifications",
                "reach_id": reach_id,
                "product": product,
                "mssg": mssge_string,
                "command": "Plot_Forecast_Ensemble_Bias_Data_Downloaded_Error",
                
            },
        )   

    return mssge_string


def retrieve_corrected_forecast_ensemble(ensemble_list,high_res_df_mean):

    corrected_ensembles = pd.concat(ensemble_list, axis=1)
    print(corrected_ensembles)

    max_df = corrected_ensembles.quantile(1.0, axis=1).to_frame()
    max_df.index = pd.to_datetime(max_df.index)
    max_df.index = max_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    max_df= max_df.reset_index()
    max_df.rename(columns={"index":'x',1.0: 'y'}, inplace=True)



    p75_df = corrected_ensembles.quantile(0.75, axis=1).to_frame()
    p75_df.index = pd.to_datetime(p75_df.index)
    p75_df.index = p75_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    p75_df = p75_df.reset_index()
    p75_df.rename(columns={"index":'x',0.75: 'y'}, inplace=True)

    p25_df = corrected_ensembles.quantile(0.25, axis=1).to_frame()
    p25_df.index = pd.to_datetime(p25_df.index)
    p25_df.index = p25_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    p25_df = p25_df.reset_index()
    p25_df.rename(columns={"index":'x',0.25: 'y'}, inplace=True)

    min_df = corrected_ensembles.quantile(0, axis=1).to_frame()
    min_df.index = pd.to_datetime(min_df.index)
    min_df.index = min_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    min_df = min_df.reset_index()
    min_df.rename(columns={"index":'x',0.0: 'y'}, inplace=True)

    mean_df = corrected_ensembles.mean(axis=1).to_frame()
    mean_df.index = pd.to_datetime(mean_df.index)
    mean_df.index = mean_df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    mean_df = mean_df.reset_index()
    mean_df.rename(columns={"index":'x',0: 'y'}, inplace=True)

    high_res_df_mean.index = pd.to_datetime(high_res_df_mean.index)
    high_res_df_mean.index = high_res_df_mean.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
    high_res_df_mean = high_res_df_mean.reset_index()

    high_res_df_mean.rename(columns={"index":'x','ensemble_52_m^3/s': 'y'}, inplace=True)


    return [max_df,p75_df,p25_df,min_df,mean_df,high_res_df_mean]


# def format_df_ensembles(corrected_ensembles,y_column_to_rename,):
#     df = corrected_ensembles.quantile(1.0, axis=1).to_frame()
#     df.index = pd.to_datetime(df.index)
#     df.index = df.index.to_series().dt.strftime("%Y-%m-%d %H:%M:%S")
#     df= df.reset_index()
#     df.rename(columns={"datetime":'x',f'{y_column_to_rename}': 'y'}, inplace=True)
#     return df

def correct_bias_ensemble(simulated_df,forecast_ens,value_adjusted,min_value):
    simulated_df.index = pd.to_datetime(simulated_df.index)
    monthly_simulated = simulated_df[simulated_df.index.month == (forecast_ens.index[0]).month].dropna()
    # monthly_observed = mean_wl[mean_wl.index.month == (forecast_ens.index[0]).month].dropna()

    min_simulated = np.min(monthly_simulated.iloc[:, 0].to_list())
    max_simulated = np.max(monthly_simulated.iloc[:, 0].to_list())

    min_factor_df = forecast_ens.copy()
    max_factor_df = forecast_ens.copy()
    forecast_ens_df = forecast_ens.copy()

    for column in forecast_ens.columns:
        tmp = forecast_ens[column].dropna().to_frame()
        min_factor = tmp.copy()
        max_factor = tmp.copy()
        min_factor.loc[min_factor[column] >= min_simulated, column] = 1
        min_index_value = min_factor[min_factor[column] != 1].index.tolist()

        for element in min_index_value:
            min_factor[column].loc[min_factor.index == element] = tmp[column].loc[tmp.index == element] / min_simulated

        max_factor.loc[max_factor[column] <= max_simulated, column] = 1
        max_index_value = max_factor[max_factor[column] != 1].index.tolist()

        for element in max_index_value:
            max_factor[column].loc[max_factor.index == element] = tmp[column].loc[tmp.index == element] / max_simulated

        tmp.loc[tmp[column] <= min_simulated, column] = min_simulated
        tmp.loc[tmp[column] >= max_simulated, column] = max_simulated

        forecast_ens_df.update(pd.DataFrame(tmp[column].values, index=tmp.index, columns=[column]))
        min_factor_df.update(pd.DataFrame(min_factor[column].values, index=min_factor.index, columns=[column]))
        max_factor_df.update(pd.DataFrame(max_factor[column].values, index=max_factor.index, columns=[column]))

    corrected_ensembles = geoglows.bias.correct_forecast(forecast_ens_df, simulated_df, value_adjusted)
    corrected_ensembles = corrected_ensembles.multiply(min_factor_df, axis=0)
    corrected_ensembles = corrected_ensembles.multiply(max_factor_df, axis=0)
    corrected_ensembles = corrected_ensembles + min_value

    ensemble = corrected_ensembles.copy()
    high_res_df = ensemble['ensemble_52_m^3/s'].to_frame()
    ensemble.drop(columns=['ensemble_52_m^3/s'], inplace=True)
    ensemble.dropna(inplace=True)
    high_res_df.dropna(inplace=True)
    
    return [ensemble, high_res_df]


def correct_bias_forecast_records(forecast_record,simulated_df,meses,wl,min_value):
    #Mean WL

    fixed_records = pd.DataFrame()

    for mes in meses:
        values = forecast_record.loc[forecast_record.index.month == mes]

        monthly_simulated = simulated_df[simulated_df.index.month == mes].dropna()
        # monthly_observed = mean_wl[mean_wl.index.month == mes].dropna()

        min_simulated = np.min(monthly_simulated.iloc[:, 0].to_list())
        max_simulated = np.max(monthly_simulated.iloc[:, 0].to_list())

        min_factor_records_df = values.copy()
        max_factor_records_df = values.copy()
        fixed_records_df = values.copy()

        column_records = values.columns[0]
        tmp = forecast_record[column_records].dropna().to_frame()
        min_factor = tmp.copy()
        max_factor = tmp.copy()
        min_factor.loc[min_factor[column_records] >= min_simulated, column_records] = 1
        min_index_value = min_factor[min_factor[column_records] != 1].index.tolist()

        for element in min_index_value:
            min_factor[column_records].loc[min_factor.index == element] = tmp[column_records].loc[tmp.index == element] / min_simulated

        max_factor.loc[max_factor[column_records] <= max_simulated, column_records] = 1
        max_index_value = max_factor[max_factor[column_records] != 1].index.tolist()

        for element in max_index_value:
            max_factor[column_records].loc[max_factor.index == element] = tmp[column_records].loc[tmp.index == element] / max_simulated

        tmp.loc[tmp[column_records] <= min_simulated, column_records] = min_simulated
        tmp.loc[tmp[column_records] >= max_simulated, column_records] = max_simulated
        fixed_records_df.update(pd.DataFrame(tmp[column_records].values, index=tmp.index, columns=[column_records]))
        min_factor_records_df.update(pd.DataFrame(min_factor[column_records].values, index=min_factor.index, columns=[column_records]))
        max_factor_records_df.update(pd.DataFrame(max_factor[column_records].values, index=max_factor.index, columns=[column_records]))

        corrected_values = geoglows.bias.correct_forecast(fixed_records_df, simulated_df, wl)
        corrected_values = corrected_values.multiply(min_factor_records_df, axis=0)
        corrected_values = corrected_values.multiply(max_factor_records_df, axis=0)
        corrected_values = corrected_values + min_value
        fixed_records = fixed_records.append(corrected_values)

    fixed_records = fixed_records.sort_index(inplace=True)
    return fixed_records

def gumbel_1(std: float, xbar: float, rp: int or float) -> float:
  """
  Solves the Gumbel Type I probability distribution function (pdf) = exp(-exp(-b)) where b is the covariate. Provide
  the standard deviation and mean of the list of annual maximum flows. Compare scipy.stats.gumbel_r
  Args:
    std (float): the standard deviation of the series
    xbar (float): the mean of the series
    rp (int or float): the return period in years
  Returns:
    float, the flow corresponding to the return period specified
  """
  # xbar = statistics.mean(year_max_flow_list)
  # std = statistics.stdev(year_max_flow_list, xbar=xbar)
  return -math.log(-math.log(1 - (1 / rp))) * std * .7797 + xbar - (.45 * std)