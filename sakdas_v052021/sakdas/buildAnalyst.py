import json
import uuid
import re
import pandas as pd
from datetime import datetime
from time import gmtime, strftime
import numpy as np
import tracemalloc

from DataQualityMetric import *

def buildAnalyst(df):

    tracemalloc.start()

    with open('dataQualityConfig.json') as f:
        data = json.load(f)

   

    for a, v in data.items():
        code = "{} = {}".format(a, v)
        a = v
        print(code)

    profile = {}
    sakdasVersion   = {"profile_engine" : "2.4.8"}
    profilingId     = {"profile_id" : str(uuid.uuid4())}
    dataName        = {"data_name" : "Test Data"}
    profilingDateTime = {"profiling_datetime" : datetime.now().strftime("%Y-%m-%dT%H:%M:%S{}".format(strftime("%z", gmtime())))} 
    profile.update(sakdasVersion)
    profile.update(profilingId)
    profile.update(dataName)
    profile.update(profilingDateTime)

    for metric, func in data.items():
        func = globals()[func](df)
        new = {metric:func}
        profile.update(new)

    profiles = json.dumps(profile, indent=4)

    print(profiles)


    f = open("new.json", "wt")
    f.write(profiles)
    f.close()


    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
    tracemalloc.stop()
    
    return profile


#  df = pd.DataFrame([
#         [1236, 'Customer A', '123 Street', '0',12.0],
#         [1234, 'Customer A', '0', '333 Street',10],
#         [1234, 'Customer A', '0', '333 Street',10],
#         [1237, 'Customer A', None ,'333 Street',100],
#         [1233, 'Customer B', '444 Street', '333 Street',20.41],
#         [1235, 'Customer B', '444 Street', '666 Street',1.01]
#         ], columns = ['ID', 'Customer', 'Billing Address', 'Shipping Address','Number'])

    #print(df)

#buildAnalyst(df)