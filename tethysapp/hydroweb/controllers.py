from django.shortcuts import render
from tethys_sdk.permissions import login_required
from django.http import JsonResponse
from .app import Hydroweb as app
from rest_framework.decorators import api_view,authentication_classes, permission_classes
from django.test.client import Client
from .model import River, Lake
from tethys_sdk.routing import controller
from channels.layers import get_channel_layer
from tethysext.hydroviewer.controllers.utilities import Utilities
from tethysext.hydroviewer.model import ForecastRecords, HistoricalSimulation, ReturnPeriods
from asgiref.sync import sync_to_async
from .model import cache_historical_data, cache_hydroweb_data

import requests
import json
import pandas as pd
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
    print(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json'))

    if not os.path.exists(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json')):
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
        mean_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json'))
        mean_wl.set_index('x', inplace=True)

        mean_wl.index = pd.to_datetime(mean_wl.index)

        min_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_min.json'))
        min_wl.set_index('x', inplace=True)
        min_wl.index = pd.to_datetime(min_wl.index)
        
        max_wl = pd.read_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_max.json'))
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
        print(corrected_mean_wl)


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

    corrected_df_mean = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_mean.json'))
    # print(corrected_df_mean)
    corrected_df_min = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_max.json'))
    # print(corrected_df_min)
    
    corrected_df_max = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_min.json'))
    # print(corrected_df_max)
    breakpoint()
    corrected_df_mean  = cache_historical_data(data_df=None,comid=data_id,type_data="corrected_mean",session=session)
    corrected_df_mean = corrected_df_mean.reset_index()

    corrected_df_mean = corrected_df_mean.rename(columns={'datetime': 'x', 'stream_flow': 'y'})
    
    corrected_df_min  = cache_historical_data(data_df=None,comid=data_id,type_data="corrected_min",session=session)
    corrected_df_min = corrected_df_min.reset_index()

    corrected_df_min = corrected_df_min.rename(columns={'datetime': 'x', 'stream_flow': 'y'})

    corrected_df_max  = cache_historical_data(data_df=None,comid=data_id,type_data="corrected_max",session=session)
    corrected_df_max = corrected_df_max.reset_index()

    corrected_df_max = corrected_df_max.rename(columns={'datetime': 'x', 'stream_flow': 'y'})
    breakpoint()

    
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
        print('saveHistoricalSimulationData error')
        print(e)
        print(traceback.format_exc())
    finally:
        print('finally')   
    
    return JsonResponse({'state':response })

async def make_forecast_api_calls(api_base_url,reach_id,product):
    list_async_task = []
    # forecast_records_query = session.query(ForecastRecords).filter(ForecastRecords.reach_id == comid)

    if not os.path.exists(os.path.join(app.get_app_workspace().path,f'ensemble_forecast_data/{reach_id}.json')):
        task_get_forecast_ensembles_geoglows_data = await asyncio.create_task(forecast_ensembles_api_call(api_base_url,reach_id,product))
    else:
        task_get_forecast_ensembles_geoglows_data =  await asyncio.create_task(fake_print_custom(api_base_url,reach_id,product,"Forecast_Ensemble_Data_Downloaded"))
    # list_async_task.append(task_get_forecast_ensembles_geoglows_data)
    
    if not os.path.exists(os.path.join(app.get_app_workspace().path,f'forecast_data/{reach_id}.json')):
        task_get_forecast_geoglows_data =  await asyncio.create_task(forecast_api_call(api_base_url,reach_id,product))
    else:
        task_get_forecast_geoglows_data = await asyncio.create_task(fake_print_custom(api_base_url,reach_id,product,"Forecast_Data_Downloaded"))
    
    # list_async_task.append(task_get_forecast_geoglows_data)
    
    results = await asyncio.gather([task_get_forecast_ensembles_geoglows_data,task_get_forecast_geoglows_data])

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
    # return await mssge_string


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
        # print(response_await.text)
        forecast_records = pd.read_csv(io.StringIO(response_await.text), index_col=0)
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
    # return await mssge_string


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