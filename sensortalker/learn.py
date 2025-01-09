import pymongo
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException, ConnectionException
from datetime import datetime, timedelta
import struct
from threading import Thread

def main():
    print("Start program")
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client['metadata']
    # db = client['astemo_energy_monitor']
    schema_collection = db['device_schema']
    device_collection = db['device']
    value_device = db['device_value']
    for device_doc in device_collection.find({}):
        for device_data in device_doc['device_data']:
            get_id_device = device_doc['_id']
            thr = Thread(target=run_thread, args=(schema_collection, device_data, value_device, device_collection, get_id_device))
            thr.start()

def run_thread(schema_collection, device_data, value_device, device_collection, get_id_device):
    last_sent_time = datetime.now()
    last_check_time = datetime.now()
    is_first_checkvalues = True 
    is_first_checkstatus = True

    while True:
        slave_id = device_data['slaveid']
        current_time = datetime.now()

        try:
            doc = create_data_value(schema_collection, device_data, slave_id)
            if slave_id == 4:
                if is_first_checkvalues or current_time >= last_sent_time:
                    updated_data = update_document(doc)
                    value_device.insert_one(updated_data)
                    last_sent_time = next_hour()
                    is_first_checkvalues = False
            else:
                if is_first_checkvalues or current_time >= last_sent_time:
                    value_device.insert_one(doc)
                    last_sent_time = next_hour()
                    is_first_checkvalues = False
        except (IndexError, ModbusException, ConnectionException) as e:
            print(f'Error: {e}\n')
            
        try: 
            if is_first_checkstatus or current_time >= last_check_time:
                create_data_value(schema_collection, device_data, slave_id)
                device_collection.update_one({"_id": get_id_device, "device_data.slaveid": slave_id}, {"$set": {"device_data.$.status": 1}})
                last_check_time = next_second()
                is_first_checkstatus = False
        except (IndexError, ModbusException, ConnectionException) as e:
            device_collection.update_one({"_id": get_id_device, "device_data.slaveid": slave_id}, {"$set": {"device_data.$.status": 0}})
            print(f'Error: {e}\n')

def create_data_value(schema_collection, device_data, slave_id):
    current_time = datetime.now()
    ip_address = device_data['ip_address']
    schema_cursor = schema_collection.find({'slaveid': slave_id})
    schema_data = next(schema_cursor, None)
    offset, min = find_offset(schema_data)
    value_data, sensorname = read_value_form_register(ip_address, slave_id, min, offset, schema_data)

    sensordata_dict = dict(zip(sensorname, value_data))
    json_type = {
        "date": current_time,
        "ipaddress": ip_address,
        "sensordata": sensordata_dict,
        "slaveid": slave_id
    }
    return json_type

def find_offset(schema_data):
    min = 10000
    max = 0
    for _, value in schema_data['sensordata'].items():
        register_list = value['register']
        if len(register_list) > 1:
            first_register = register_list[0]
            last_register = register_list[1]
        else:
            first_register = register_list[0]
            last_register = register_list[0]

        if first_register > max:
            max = first_register
        if last_register > max:
            max = last_register
        if first_register < min:
            min = first_register
        if last_register < min:
            min = last_register
    offset = max - min
    if offset == 0:
        offset = 1
    return offset, min

def read_value_form_register(ipaddress, slave_id, min, offset, schema_data):
    client_modbus = ModbusTcpClient(
        ipaddress,
        port=8899,
        timeout=10,
        retries=3,
        retry_on_empty=False
    )
    result = client_modbus.read_holding_registers(min, offset, slave=slave_id)
    value_data, sensorname = get_value(schema_data, result,min)
    client_modbus.close()
    return value_data, sensorname

def get_value(schema_data, result,min):
    sensorname_key = {key: value['register'][0] for key, value in schema_data['sensordata'].items()}
    sensorname = list(dict(sorted(sensorname_key.items(), key=lambda item: item[1])).keys())
    array_data = []
    value_data = []
    if hasattr(result, 'registers'):
        array_data.extend(result.registers)
    if len(array_data) % 2 != 0:
        array_data.append(0)
    for _, values_doc in schema_data['sensordata'].items():
        register_list = values_doc['register']
        first_index = register_list[0]
        lsat_index = register_list[-1] if len(register_list) > 1 else first_index        
            
        register_0 = first_index - min
        register_1 = lsat_index - min

        raw_value = array_data[register_1] + (array_data[register_0] << 16)
        float_value = struct.unpack('f', struct.pack('I', raw_value))[0]
        formatted_float = float(format(float_value, ".4f"))
        value_data.append(formatted_float)
    return value_data, sensorname

def calculate_averages(doc):
    grouped_keys = {}
    for key, value in doc['sensordata'].items():
        if key.endswith(('_1', '_2', '_3')):
            base_key = key.rsplit('_', 1)[0]
            grouped_keys.setdefault(base_key, []).append(value)
    for base_key, values in grouped_keys.items():
        total = 0
        count = 0
        for value in values:
            if value > 0:
                total += value
                count += 1
        average = total / count if count > 0 else 0
        doc['sensordata'][base_key] = average
    return doc

def update_document(doc):
    updated_sensor_data = calculate_averages(doc)
    return updated_sensor_data

def next_hour():
    now = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return next_hour

def next_second():
    now = datetime.now()
    next_second = (now + timedelta(seconds=5)).replace(microsecond=0)
    return next_second

if __name__ == "__main__":
    main()
