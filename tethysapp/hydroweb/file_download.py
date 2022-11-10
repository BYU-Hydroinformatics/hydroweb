from bdb import Breakpoint
import time
from django.shortcuts import render
from tethys_sdk.permissions import login_required
from django.http import JsonResponse, HttpResponse
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
import datetime as dt

import csv
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


@controller(name='download',url='hydroweb/download')
@api_view(['GET', 'POST'])
@authentication_classes([])
@permission_classes([])
def hydroweb_download(request):
    product = request.data.get('product')
    reach_id = request.data.get('reach_id')

    type_data = request.data.get('type_data')
    response = HttpResponse(content_type='text/csv')
    print(type_data)
    if type_data == "Water Level Mean Value":
        water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == product)

        mean_wl,min_wl,max_wl = retrive_hydroweb_river_data(water_level_data_query)
        
        response['Content-Disposition'] = f'attachment; filename={type_data}.csv'
        mean_wl.to_csv(response, index=False)

    if type_data == "Water Level Maximun-Minimun":
        water_level_data_query = session.query(HydrowebData).filter(HydrowebData.hydroweb_product == product)

        mean_wl,min_wl,max_wl = retrive_hydroweb_river_data(water_level_data_query)
        
        min_wl = min_wl.rename(columns={'x':'date', 'y':'min'})
        max_wl = max_wl.rename(columns={'x':'date', 'y':'max'})

        max_min_WL_df = pd.merge(min_wl, max_wl, on='date', how='outer')

        response['Content-Disposition'] = f'attachment; filename={type_data}.csv'
        max_min_WL_df.to_csv(response, index=False)

    if type_data == "Historical Simulation":
        simulated_df = hydroviewer_utility_object.cache_historical_simulation(app,None,reach_id,session,response_content=None)
        simulated_df = simulated_df.reset_index()
        breakpoint()
        response['Content-Disposition'] = f'attachment; filename={type_data}.csv'
        simulated_df.to_csv(response, index=False)
    return response
