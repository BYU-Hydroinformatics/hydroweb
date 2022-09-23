import json
from channels.generic.websocket import AsyncWebsocketConsumer




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
            print("yeah")

        # await self.send(text_data)
 

    async def data_notifications(self, event):
        print(event)
        print("data_notifications from consumer")

        count = event["count"]
        status = event["status"]
        file = event["file"]
        id_ = event["id"]
        total_count = event["total"]
        mssge = event["mssg"]

        resp_obj = {
            "count": count,
            "status": status,
            "file": file,
            "id": id_,
            "total": total_count,
            "mssg": mssge,
        }
        await self.send(text_data=json.dumps(resp_obj))
        print(f"Got message {event} at {self.channel_name}")

    async def simple_notifications(self, event):
        print("simple notification from consumer")
        message = event["mssg"]
        reach_id = event["reach_id"]
        product = event["product"]
        await self.send(text_data=json.dumps({"message": message,"reach_id":reach_id, "product": product}))
        print(f"Got message {event} at {self.channel_name}")