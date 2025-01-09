from pymongo import MongoClient

print("+" + "-" * (30 - 2) + "+")
text = "The program is starting"
text_line = 5 // 2
text_start = (30 - len(text)) // 2
for i in range(1, 5 - 1):
    if i == text_line:
        print("|" + " " * (text_start - 1) + text + " " * (30 - text_start - len(text) - 1) + "|")
    else:
        print("|" + " " * (30 - 2) + "|")

print("+" + "-" * (30 - 2) + "+")

client = MongoClient('mongodb://localhost:27017/')

db = client['astemo_energy_monitor']
# db = client['metadata']
collection = db['device_value']

data_cursor = collection.find({}).sort([('ipaddress', 1), ('date', 1)])

newdata_list = []
ip_data_count = {}

print("Sampling data . . .\n")
for document in data_cursor:
    current_ip = document['ipaddress']
    
    if current_ip not in ip_data_count:
        ip_data_count[current_ip] = 0

    ip_data_count[current_ip] += 1

    if ip_data_count[current_ip] == 19:
        newdata_list.append(document)
        ip_data_count[current_ip] = 0

print("Delete data . . .\n")
collection.delete_many({})
print("Add new data . . .\n")
collection.insert_many(newdata_list)
print(f'Number of new document: {len(newdata_list)}')
