from Sakdas import Sakda as sd
import pandas as pd
import unittest
import os
import pathlib

df = pd.read_csv("/Users/sakdaloetpipatwanich/Desktop/SAKDAS/sakdas_dm/BankChurners_mini.csv")

auditing_config = {'audit':{
    'audit_missing_value': True,
    'define_custom_missing_value': ['.','999'],
    'audit_data_pattern':[
                {'column_name':'CLIENTNUM', 'regex_pattern': '(Female|Male)'},
                {'column_name':'Customer_Age', 'regex_pattern': '(^[0-9]{3}-[0-9]{2}-[0-9]{4})'}
            ],
    'audit_outlier': True,
    'audit_data_range' : [{'column_name':'Customer_Age', 'min': 0, 'max': 100}]
    }
}
sample_supermarket_sales = sd(
    df,'sample_supermarket_sales',
    '/Users/sakdaloetpipatwanich/Documents/Sakdas_Result', auditing_config = auditing_config)
    

#auditing_config = auditing_config)
#print(sample_supermarket_sales.profile_json)

f = open("ori.json", "a")
f.write(sample_supermarket_sales.profile_json)
f.close()
print(os.path.abspath(pd.__file__))
print(pathlib.Path().absolute())
