from django.shortcuts import render
from tethys_sdk.permissions import login_required
from django.http import JsonResponse
import requests
import json
import pandas as pd
from .app import Hydroweb as app
from rest_framework.decorators import api_view,authentication_classes, permission_classes
from django.test.client import Client


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
    resp_obj = {}
    product = request.GET.get('product')
    user = app.get_custom_setting('Hydroweb Username')
    pwd = app.get_custom_setting('Hydroweb Password')
    url= f'https://hydroweb.theia-land.fr/hydroweb/authdownload?products={product}&format=json&user={user}&pwd={pwd}'

    response= requests.get(url)
    json_obj = json.loads(response.text)

    data_obj = json_obj['data']

    df = pd.DataFrame.from_dict(data_obj)
    data_df = df[['date', 'orthometric_height_of_water_surface_at_reference_position', 'associated_uncertainty']].copy()

    resp_obj['geometry'] = json_obj['geometry']
    resp_obj['properties'] = json_obj['properties']
    # print(data_df['date'].to_list)
    resp_obj['data'] ={
        'dates': data_df['date'].to_list(),
        'values': data_df['orthometric_height_of_water_surface_at_reference_position'].to_list(),
        'uncertainties': data_df['associated_uncertainty'].to_list()
    }
    return JsonResponse(resp_obj)
