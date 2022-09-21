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
      
    async def data_notifications(self, event):
        print(event)
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
        message = event["message"]
        reach_id = event["reach_id"]
        await self.send(text_data=json.dumps({"message": message,"reach_id":reach_id}))
        print(f"Got message {event} at {self.channel_name}")