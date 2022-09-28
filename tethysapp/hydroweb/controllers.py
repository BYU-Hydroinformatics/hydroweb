from django.shortcuts import render
from tethys_sdk.permissions import login_required
from django.http import JsonResponse, HttpResponse
import requests
import json
import pandas as pd
from .app import Hydroweb as app
from rest_framework.decorators import api_view,authentication_classes, permission_classes
from django.test.client import Client
from .model import River, Lake
import geopandas as gpd
from sqlalchemy.orm import sessionmaker
import io
import os
import geoglows
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
import asyncio
import httpx
import traceback

Persistent_Store_Name = 'virtual_stations'
async_client = httpx.AsyncClient()

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
    # print(data_obj)
    df = pd.DataFrame.from_dict(data_obj)
    if product.startswith('R'):
        
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


        # data_dict = data_df.to_dict('records')

        # resp_obj['data'] = data_dict
        resp_obj['data'] = {
            'val': data_val,
            'min': data_min,
            'max': data_max
        }

        ## saving it to the workspaces for alter use ##
        if not os.path.exists(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json')):
            df_val.to_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_mean.json'))
            df_max.to_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_max.json'))
            df_min.to_json(os.path.join(app.get_app_workspace().path,f'observed_data/{product}_min.json'))
        else:
            print("files are already saved")

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


@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
# we need to check how to enable all middleware to have asynch views 
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
    if not os.path.exists(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}.json')):
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
        print(response_await)
        # print(response_await.text)
        simulated_df = pd.read_csv(io.StringIO(response_await.text), index_col=0)
        simulated_df[simulated_df < 0] = 0

        simulated_df.to_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}.json'))

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
    simulated_df = pd.read_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{data_id}.json'))
    # Removing Negative Values
    simulated_df[simulated_df < 0] = 0
    simulated_df.index = pd.to_datetime(simulated_df.index)
    simulated_df.index = simulated_df.index.to_series().dt.strftime("%Y-%m-%d")
    simulated_df.index = pd.to_datetime(simulated_df.index)
    simulated_df = simulated_df.reset_index()
    # print("hola")
    print(simulated_df)
    simulated_df = simulated_df.rename(columns={'index': 'x', 'streamflow_m^3/s': 'y'})
    simulated_df['x']= simulated_df['x'].dt.strftime('%Y-%m-%d')

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


@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
# we need to check how to enable all middleware to have asynch views 
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
        simulated_df = pd.read_json(os.path.join(app.get_app_workspace().path,f'simulated_data/{reach_id}.json'))
        print(simulated_df)
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
        corrected_mean_wl = corrected_mean_wl.rename(columns={'index': 'x', 'Corrected Simulated Streamflow': 'y'})
        corrected_min_wl = corrected_min_wl.rename(columns={'index': 'x', 'Corrected Simulated Streamflow': 'y'})
        corrected_max_wl = corrected_max_wl.rename(columns={'index': 'x', 'Corrected Simulated Streamflow': 'y'})
        print(corrected_mean_wl)

        # data_corrected_mean_wl = corrected_mean_wl.to_dict('records')
        # data_corrected_min_wl = corrected_min_wl.to_dict('records')
        # data_corrected_max_wl = corrected_max_wl.to_dict('records')

        # corrected_mean_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json'))
        # corrected_min_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_min.json'))
        # corrected_max_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_max.json'))
        corrected_mean_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_mean.json'))
        corrected_min_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_min.json'))
        corrected_max_wl.to_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{reach_id}_max.json'))
        
        channel_layer = get_channel_layer()
        
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
    print(corrected_df_mean)
    corrected_df_min = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_max.json'))
    print(corrected_df_min)
    
    corrected_df_max = pd.read_json(os.path.join(app.get_app_workspace().path,f'corrected_data/{data_id}_min.json'))
    print(corrected_df_max)

    
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