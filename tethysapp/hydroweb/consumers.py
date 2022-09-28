import json
import asyncio
from channels.generic.websocket import AsyncWebsocketConsumer
from .controllers import retrieve_data, retrieve_data_bias_corrected
from asgiref.sync import async_to_sync


class DataConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        await self.accept()
        await self.channel_layer.group_add("notifications_hydroweb", self.channel_name)
        print(f"Added {self.channel_name} channel to hydroweb notifications")

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("notifications_hydroweb", self.channel_name)
        print(f"Removed {self.channel_name} channel from hydroweb notifications")

    async def receive(self, text_data):
        # Called with either text_data or bytes_data for each frame
        # You can call:
        print("receving function to consumer")
        text_data_json = json.loads(text_data)
        print(text_data_json)
        if "type" in text_data_json and text_data_json["type"] == "plot_hs_data":
            # asyncio.run(retrieve_data_from_file(text_data_json['reach_id']))
            json_obj = retrieve_data(text_data_json['reach_id'],text_data_json['product'])
            await self.channel_layer.group_send (
                "notifications_hydroweb",
                json_obj,
            )
        if "type" in text_data_json and text_data_json["type"] == "plot_bias_corrected_data":
            # asyncio.run(retrieve_data_from_file(text_data_json['reach_id']))
            json_obj = retrieve_data_bias_corrected(text_data_json['reach_id'],text_data_json['product'])
            await self.channel_layer.group_send (
                "notifications_hydroweb",
                json_obj,
            )    
            # print(mssge_string)
        # await self.send(text_data)


    async def data_notifications(self, event):
        # print(event)
        print("data_notifications from consumer")

        message = event["mssg"]
        reach_id = event["reach_id"]
        product = event["product"]
        command = event["command"]
        data = event['data']

        resp_obj = {
            "message": message,
            "reach_id": reach_id,
            "product": product,
            "command": command,
            "data": data,
        }
        await self.send(text_data=json.dumps(resp_obj))
        print(f"Got message {event} at {self.channel_name}")

    async def simple_notifications(self, event):
        print("simple notification from consumer")
        message = event["mssg"]
        reach_id = event["reach_id"]
        product = event["product"]
        command = event["command"]
        await self.send(text_data=json.dumps({"message": message,"reach_id":reach_id, "product": product,"command":command}))
        print(f"Got message {event} at {self.channel_name}")