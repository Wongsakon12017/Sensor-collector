import pymongo
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ConnectionException, ModbusIOException
from datetime import datetime, timedelta
import struct
from threading import Thread
import logging
import os

def handle_exception(error):
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    if isinstance(error, IndexError):
        logging.error(f'IndexError: {error}')
    elif isinstance(error, ModbusIOException):
        logging.error(f'ModbusIOException: {error}')
    elif isinstance(error, ConnectionException):
        logging.error(f'ConnectionException: {error}')
    else:
        logging.error(f'Unexpected error: {error}')

def draw_box_with_text(width, height, text):
    print("+" + "-" * (width - 2) + "+")

    text_line = height // 2
    text_start = (width - len(text)) // 2
    
    for i in range(1, height - 1):
        if i == text_line:
            print("|" + " " * (text_start - 1) + text + " " * (width - text_start - len(text) - 1) + "|")
        else:
            print("|" + " " * (width - 2) + "|")
    
    print("+" + "-" * (width - 2) + "+")

def main():
    draw_box_with_text(30, 5, "The program is starting")
    client = pymongo.MongoClient("mongodb://localhost:27017")
    # db = client['metadata']
    db = client['astemo_energy_monitor']
    schema_collection = db['device_schema']
    device_collection = db['device']
    value_device = db['device_value']

    for device_document_data in device_collection.find({}):
        for device_data in device_document_data['device_data']:
            get_id_device = device_document_data['_id']
            thr = Thread(target=run_thread, args=(schema_collection, device_data, value_device, device_collection, get_id_device))
            thr.start()


def run_thread(schema_collection, device_data, value_device, device_collection, get_id_device):
    last_check_time = datetime.now()
    is_first_checkstatus = True

    while True:
        slave_id = device_data['slaveid']
        ip_address = device_data['ip_address']
        current_time = datetime.now()
        if is_first_checkstatus or current_time >= last_check_time:
            # print(device_data['machine_name'])
            try:
                document_data = create_data_value(schema_collection, device_data, slave_id)
                if not document_data['sensordata']:
                    pass
                else:
                    if slave_id == 3:
                        updated_document_data = update_document_data(document_data)
                        value_device.insert_one(updated_document_data)
                        # print(updated_document_data)
                    elif slave_id == 2:
                        updated_document_data = convert_flowrate_data_value(document_data)
                        value_device.insert_one(updated_document_data)
                    else:
                        value_device.insert_one(document_data)

            except (IndexError, ModbusIOException, ConnectionException) as error:
                handle_exception(error)
            
            print("start process")
                
            try:
                document_data = create_data_value(schema_collection, device_data, slave_id)
                if not document_data['sensordata']: #ถ้า sensordata ใน document ไม่มีค่าให้เปลี่ยน status เป็น 0
                    device_collection.update_one({"_id": get_id_device, "device_data.slaveid": slave_id,"device_data.ip_address":ip_address}, {"$set": {"device_data.$.status": 0}}) 
                else:
                    if slave_id == 3: #power
                        value_data_text = update_document_data(document_data)
                        get_value = value_data_text.get('sensordata', {}).get('power')#power

                    elif slave_id == 2: #water
                        keys = ['flowrate', 'pressure', 'temperature']
                        value_data_text = convert_flowrate_data_value(document_data)
                        get_value = {key: value_data_text.get('sensordata', {}).get(key) for key in keys}

                    else: #pressure@water
                        get_value = document_data.get('sensordata', {}).get('pressure')
                        
                    create_text_file(device_data,slave_id,get_value)
                    device_collection.update_one({"_id": get_id_device, "device_data.slaveid": slave_id,"device_data.ip_address":ip_address}, {"$set": {"device_data.$.status": 1}})

            except (IndexError, ModbusIOException, ConnectionException) as error:
                device_collection.update_one({"_id": get_id_device, "device_data.slaveid": slave_id,"device_data.ip_address":ip_address}, {"$set": {"device_data.$.status": 0}})
                handle_exception(error)

            # P = device_collection.find{'groupid':10}
            # print(P['status'])
            last_check_time = next_second()
            is_first_checkstatus = False

def convert_flowrate_data_value(document_data):
    for key, value in document_data['sensordata'].items():
        if key == 'flowrate':
            cal_value = value / 3600
            document_data['sensordata']['flowrate'] = cal_value
        elif key == 'pressure':
            cal_value = value /1000
            document_data['sensordata']['pressure'] = cal_value
    # print(document_data)
    return document_data

def create_text_file(device_data, slave_id, get_value):
    machine_name = device_data['machine_name']
    
    data_directory = "data"
    
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    
    file_path = os.path.join(data_directory, machine_name+".txt")
    
    with open(file_path, "w") as create:
        if slave_id == 3: # power
            create.write(f'{str(get_value)} kW')
        elif slave_id == 2: # water
            create.write(f"{str(get_value['flowrate'])} cube,{str(get_value['temperature'])} C,{str(get_value['pressure'])} MPa")
        else:
            create.write(f"0 cube, 0 C, {str(get_value)} MPa")
    
def create_data_value(schema_collection, device_data, slave_id):
    try:

        current_time = datetime.now()
        ip_address = device_data['ip_address']
        schema_cursor = schema_collection.find({'slaveid': slave_id})
        schema_data = next(schema_cursor,None)
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
    except (TypeError,ValueError) as error:
        handle_exception(error)

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
    offset = max - min +1
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
    # print(result)
    value_data, sensorname = get_value_swap_byte(schema_data, result,min,slave_id)
    client_modbus.close()
    return value_data, sensorname

def get_value_swap_byte(schema_data, result,min,slave_id):
    sensorname_key = {key: value['register'][0] for key, value in schema_data['sensordata'].items()}
    sensorname = list(dict(sorted(sensorname_key.items(), key=lambda item: item[1])).keys())
    array_data = []
    value_data = []
    if hasattr(result, 'registers'):
        array_data.extend(result.registers)
    if len(array_data) % 2 != 0:
        array_data.append(0)

    if slave_id == 1:
        value_data = [x / 1000 for x in array_data]

    elif slave_id == 2:
        for read_name in sensorname:
            idx0 = schema_data['sensordata'][read_name]['register'][0]
            idx1 = schema_data['sensordata'][read_name]['register'][1]

            first_index = idx0
            last_index = idx1    
                
            register_0 = first_index - min
            register_1 = last_index - min

            raw_value = array_data[register_0] + (array_data[register_1] << 16)

            float_value = struct.unpack('f', struct.pack('I', raw_value))[0]
            # print(float_value)
            # formatted_float = float(format(float_value, ".4f"))
            value_data.append(float_value)
    else:
        for read_name in sensorname:
            idx0 = schema_data['sensordata'][read_name]['register'][0]
            idx1 = schema_data['sensordata'][read_name]['register'][1]
            first_index = idx0
            last_index = idx1    
            register_0 = first_index - min
            register_1 = last_index - min
            raw_value = array_data[register_1] + (array_data[register_0] << 16)
            float_value = struct.unpack('f', struct.pack('I', raw_value))[0]
            value_data.append(float_value)
    return value_data, sensorname

def calculate_averages(document_data):
    grouped_keys = {}
    for key, value in document_data['sensordata'].items():
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
        document_data['sensordata'][base_key] = average
    return document_data

def update_document_data(document_data):
    updated_sensor_data = calculate_averages(document_data)
    return updated_sensor_data

def next_second():
    now = datetime.now()
    next_second = (now + timedelta(seconds=20)).replace(microsecond=0)
    return next_second

if __name__ == "__main__":
    main()
