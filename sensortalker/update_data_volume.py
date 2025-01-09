import pymongo
from pymodbus.client import ModbusTcpClient
import math
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['astemo_energy_monitor']

collection = db['device_value']

data_cursor = collection.find({'slaveid':2})

for document in data_cursor:
   ip = document['ipaddress']
   p1 = document['sensordata']['volume_before']
   p2 = document['sensordata']['volume_after']
   id = document['_id']
   new_update = float(math.trunc((p1*100)+p2))
   print(id)
   collection.update_one({'_id' : id},{'$set':{'sensordata.volume':new_update}})
print("success")


