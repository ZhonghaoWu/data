# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 21:27:22 2020

@author: basli
"""
import dateutil.parser
import pytz
import time
from datetime import datetime

import pandas as pd
from influxdb import InfluxDBClient

client = InfluxDBClient(host='129.28.179.54',port=8086) 
client.switch_database('t')

file_path = r'C:\cl.csv'

csvReader = pd.read_csv(file_path)

print(csvReader.shape)
print(csvReader.columns)

for row_index, row in csvReader.iterrows():
    tags = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.mktime(dateutil.parser.parse(row[0]).astimezone(pytz.timezone('Asia/Shanghai')).timetuple())))
    #a = time.strftime("%Y-%m-%d %H:%M:%S", tags)
    #tags = time.mktime(row[0].timetuple())
    fieldValue1=row[1]
    fieldValue2=row[2]
    fieldValue3=row[3]
    fieldValue4=row[4]
    fieldValue5=row[5]
    fieldValue6=row[6]
    
    json_body=[
        {
            "measurement": "CL",
            #"tags": {
            #    "Date":tags
            #},
    
            "fields": {
                "Date":tags,
                "High":fieldValue1,
                "Low":fieldValue2,
                "Open":fieldValue3,
                "Close":fieldValue4,
                "Volume":fieldValue5,
                "Adj_close":fieldValue6
                        }
        }]
    print(json_body)
    client.write_points(json_body)