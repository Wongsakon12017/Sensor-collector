import pymongo
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ConnectionException, ModbusIOException
from datetime import datetime, timedelta
import struct
from threading import Thread
import logging
import os
import math

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

def main():
    last_check_time = datetime.now()
    is_first_checkstatus = True
    client = pymongo.MongoClient("mongodb://localhost:27017")
    while True:
        db = client['astemo_energy_monitor']
        schema_collection = db['device_schema']
        device_collection = db['device']
        value_device = db['device_value']
        thr_list = []
        current_time = datetime.now()
        if is_first_checkstatus or current_time >= last_check_time:
            for device_document_data in device_collection.find({}):
                for device_data in device_document_data['device_data']:
                    thr = Thread(target=run_thread, args=(schema_collection, device_data, value_device, device_collection))
                    thr_list.append(thr)
                    thr.start()
            for thr in thr_list:
                thr.join()
                
            last_check_time = next_second()
            is_first_checkstatus = False
        else:
            pass

def run_thread(schema_collection, device_data, value_device, device_collection):
    slave_id = device_data['slaveid']
    ip_address = device_data['ip_address']

    try:
        document_data = create_data_value(schema_collection, device_data, slave_id)
        if not document_data['sensordata']:
            device_collection.update_one(
                {
                    "device_data.slaveid":slave_id,"device_data.ip_address":ip_address
                    },
                    {
                        "$set": {
                            "device_data.$[x].status": 0,
                            "device_data.$[x].online": 0
                            }
                            },
                            array_filters=[
                                {
                                    "x.ip_address": ip_address,
                                    "x.slaveid": slave_id
                                    }])

            check_connection_device_failed(ip_address,slave_id)

        elif slave_id == 3:
            if document_data['sensordata'].get('current', float('inf')) <= 0: #New change <--------------------------
                device_collection.update_one(
                {
                    "device_data.slaveid":slave_id,"device_data.ip_address":ip_address
                    },
                    {
                        "$set": {
                            "device_data.$[x].status": 0,
                            "device_data.$[x].online": 1
                            }
                            },
                            array_filters=[
                                {
                                    "x.ip_address": ip_address,
                                    "x.slaveid": slave_id
                                    }])
                
                check_connection_device_failed(ip_address,slave_id)

            elif document_data['sensordata'].get('current', float('inf')) > 0:
                device_collection.update_one(
                {
                    "device_data.slaveid":slave_id,"device_data.ip_address":ip_address
                    },
                    {
                        "$set": {
                            "device_data.$[x].status": 1,
                            "device_data.$[x].online": 1
                            }
                            },
                            array_filters=[
                                {
                                    "x.ip_address": ip_address,
                                    "x.slaveid": slave_id
                                    }])
                
                updated_document_data = update_document_data(document_data)
                value_device.insert_one(updated_document_data)
                get_value = updated_document_data.get('sensordata', {}).get('power')#power
                check_connection_device_success(ip_address,slave_id)
                create_text_file(device_data,slave_id,get_value)
                
        else:   
            if slave_id == 2:
                updated_document_data = convert_flowrate_data_value(document_data)
                value_device.insert_one(updated_document_data)
                keys = ['flowrate', 'pressure', 'temperature']
                get_value = {key: updated_document_data.get('sensordata', {}).get(key) for key in keys}

                device_collection.update_one(
                {
                    "device_data.slaveid":slave_id,"device_data.ip_address":ip_address
                    },
                    {
                        "$set": {
                            "device_data.$[x].status": 1,
                            "device_data.$[x].online": 1
                            }
                            },
                            array_filters=[
                                {
                                    "x.ip_address": ip_address,
                                    "x.slaveid": slave_id
                                    }])
                
                check_connection_device_success(ip_address,slave_id)

            else:
                value_device.insert_one(document_data)
                get_value = document_data.get('sensordata', {}).get('pressure')

                device_collection.update_one(
                {
                    "device_data.slaveid":slave_id,"device_data.ip_address":ip_address
                    },
                    {
                        "$set": {
                            "device_data.$[x].status": 1,
                            "device_data.$[x].online": 1
                            }
                            },
                            array_filters=[
                                {
                                    "x.ip_address": ip_address,
                                    "x.slaveid": slave_id
                                    }])
                
                check_connection_device_success(ip_address,slave_id)
            
            create_text_file(device_data,slave_id,get_value)

    except (IndexError, ModbusIOException, ConnectionException) as error:
        device_collection.update_one(
                {
                    "device_data.slaveid":slave_id,"device_data.ip_address":ip_address
                    },
                    {
                        "$set": {
                            "device_data.$[x].status": 0,
                            "device_data.$[x].online": 0
                            }
                            },
                            array_filters=[
                                {
                                    "x.ip_address": ip_address,
                                    "x.slaveid": slave_id
                                    }])
        check_connection_device_failed(ip_address,slave_id)
        handle_exception(error)
    
def convert_flowrate_data_value(document_data):
    before_value = 0
    after_value = 0
    name = "volume"
    
    for key, value in document_data['sensordata'].items():
        # if key == 'flowrate':
        #     cal_value = value / 3600
        #     document_data['sensordata']['flowrate'] = cal_value
        
        if key == 'pressure':
            cal_value = value / 1000
            document_data['sensordata']['pressure'] = cal_value
        elif key == 'volume_before':
            before_value = value
        elif key == 'volume_after':
            after_value = value
    real_value = (before_value*100) + after_value
    # document_data['sensordata'][name] = real_value
    document_data['sensordata'][name] = float(math.trunc(real_value))
    return document_data

def check_connection_device_success(ip_address, slaveid):
    data_directory = "Success"
    name = f"{ip_address}_{slaveid}"
    
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    
    file_path = os.path.join(data_directory, name + ".txt")
    
    log_entry = f'Success ip: {ip_address} datetime: {datetime.now()}\n'
    
    with open(file_path, "a") as file:
        file.write(log_entry)

def check_connection_device_failed(ip_address, slaveid):
    data_directory = "Failed"
    name = f"{ip_address}_{slaveid}"
    
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    
    file_path = os.path.join(data_directory, name + ".txt")
    
    log_entry = f'Failed ip: {ip_address} datetime: {datetime.now()}\n'
    
    with open(file_path, "a") as file:
        file.write(log_entry)
    
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
        
        offset, min, offset2, min2 = find_offset(schema_data)
        print(f'schema data {schema_data}\n')
        value_data, sensorname = read_value_form_register(ip_address, slave_id, min, offset, schema_data,'case1')
        if offset2 > 0:
            second_data, sensorname2 = read_value_form_register(ip_address, slave_id, min2, offset2, schema_data,'case2')
            value_data.extend(second_data)
            sensorname.extend(sensorname2)
 
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
    min2 = 10000
    max2 = 0
    offset2 = 0
    for keys, value in schema_data['sensordata'].items():
        if keys == 'power' or keys == 'power_1' or keys == 'power_2' or keys == 'power_3':
            register_list = value['register']
            if len(register_list) > 1:
                first_register = register_list[0]
                last_register = register_list[1]
            else:
                first_register = register_list[0]
                last_register = register_list[0]

            if first_register > max2:
                max2 = first_register
            if last_register > max2:
                max2 = last_register
            if first_register < min2:
                min2 = first_register
            if last_register < min2:
                min2 = last_register
            
        else:
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
            
    #print('after done')
    #print('for oof1: ',max,min)
    offset = max - min +1
    if min2 != 10000 and max2 != 0:
        offset2 = max2 - min2 +1
    #print(f'offset = {offset} min = {min}\n')
    #print(f'offset2 = {offset2} min2 = {min2}\n')
    return offset, min, offset2, min2

def read_value_form_register(ipaddress, slave_id, min, offset, schema_data,case):
    client_modbus = ModbusTcpClient(
        ipaddress,
        port=8899,
        timeout=10,
        retries=3,
        retry_on_empty=False
    )
 
    result = client_modbus.read_holding_registers(min, offset, slave=slave_id)
    value_data, sensorname = get_value_swap_byte(schema_data, result,min,slave_id,case)
    client_modbus.close()
    return value_data, sensorname

def get_value_swap_byte(schema_data, result,min,slave_id,case):
    sensorname_key = {key: value['register'][0] for key, value in schema_data['sensordata'].items()}  
    sensorname = list(dict(sorted(sensorname_key.items(), key=lambda item: item[1])).keys())
    array_data = []
    value_data = []
    if hasattr(result, 'registers'):
        array_data.extend(result.registers)
    if len(array_data) % 2 != 0:
        array_data.append(0)
    # print(array_data)
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
        store_name = []               
        for read_name in sensorname:  
            if case == 'case2' and 'power' in read_name and not 'powerfactor' in read_name:  
                store_name.append(read_name) 
                idx0 = schema_data['sensordata'][read_name]['register'][0]
                idx1 = schema_data['sensordata'][read_name]['register'][1]

                first_index = idx0
                last_index = idx1    
                    
                register_0 = first_index - min
                register_1 = last_index - min              
                
                raw_value = array_data[register_1] + array_data[register_0]

                # uint32_value = struct.unpack('I', struct.pack('>I', raw_value))[0]
                value_data.append(raw_value)

            elif case == 'case1' and not 'power' in read_name:    
                # print(read_name)
                store_name.append(read_name) 
                idx0 = schema_data['sensordata'][read_name]['register'][0]
                idx1 = schema_data['sensordata'][read_name]['register'][1]

                first_index = idx0
                last_index = idx1    
                    
                register_0 = first_index - min
                register_1 = last_index - min

                raw_value = array_data[register_1] + (array_data[register_0] << 16)

                float_value = struct.unpack('f', struct.pack('I', raw_value))[0]
                # print(float_value)
                # formatted_float = float(format(float_value, ".4f"))
                value_data.append(float_value)
            elif case == 'case1' and 'powerfactor' in read_name:
                store_name.append(read_name) 
                idx0 = schema_data['sensordata'][read_name]['register'][0]
                idx1 = schema_data['sensordata'][read_name]['register'][1]

                first_index = idx0
                last_index = idx1    
                    
                register_0 = first_index - min
                register_1 = last_index - min

                raw_value = array_data[register_1] + (array_data[register_0] << 16)

                float_value = struct.unpack('f', struct.pack('I', raw_value))[0]
                # print(float_value)
                # formatted_float = float(format(float_value, ".4f"))
                value_data.append(float_value)
        sensorname = store_name
    return value_data, sensorname

def calculate_averages(document_data):
    # print(document_data)
    grouped_keys = {}
    for key, value in document_data['sensordata'].items():
        if key.endswith(('_1', '_2', '_3')):
            base_key = key.rsplit('_', 1)[0]
            grouped_keys.setdefault(base_key, []).append(value)
    for base_key, values in grouped_keys.items():
        total = 0
        count = 0

        if base_key == 'power' and not base_key == 'powerfactor':
            for value in values:
                if value > 0:
                    total += value
                    count += 1
            average = total if count > 0 else 0
        else:
            for value in values:
                if value > 0:
                    total += value
                    count += 1
            average = total / count if count > 0 else 0

        document_data['sensordata'][base_key] = average
    # print(document_data)
    return document_data

def update_document_data(document_data):
    updated_sensor_data = calculate_averages(document_data)
    return updated_sensor_data

def next_second():
    now = datetime.now()
    next_second = (now + timedelta(seconds=18)).replace(microsecond=0)
    return next_second

if __name__ == "__main__":
    main()
