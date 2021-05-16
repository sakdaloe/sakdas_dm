#from sakdas.Commander import Commander
import json
#for Internal Test

import sys
from sys import platform
sys.path.append('/Users/sakdaloetpipatwanich/Desktop/SAKDAS/sakdas_dm/sakdas_v052021/sakdas')
from Commander import Commander

class Sakda:
    def __init__(self, 
        df, 
        df_name,
        path_to_output,
        *args,
        **kwargs):
        self.df                 = df
        self.df_name            = df_name
        self.path_to_output     = path_to_output
        #self.data_engine        = kwargs.get('data_engine', None)
        self.auditing_config    = kwargs.get('auditing_config', None)
        self.report_config      = kwargs.get('report_config', None)

        def checkPlatform():
            userPlatform = None
            if platform == "linux" or platform == "linux2" or platform == "darwin":
                userPlatform = "Linux"
            elif platform == "win32":
                userPlatform = "Window"
            print(userPlatform)
            return userPlatform

        def validateJSON(jsonData):
            try:
                json.loads(jsonData)
            except ValueError as err:
                return False
            return True
        def audited_dataset_to_csv(audited_dataset_with_tag):
            df = audited_dataset_with_tag
            del df['audit_result']
            df.rename(columns={'audit_OOR': 'is_out_of_range', 'audit_pattern': 'is_incorrect_pattern','audit_OL': 'is_outliner','audit_MISS': 'is_missing_data'}, inplace=True)
            df['is_out_of_range']     = [
                            'N' if x == 'pass'
                            else 'out_of_range' 
                            for x in df['is_out_of_range']
                            ]
            df['is_incorrect_pattern']     = [
                            'N' if x == 'pass'
                            else 'incorrect_pattern' 
                            for x in  df['is_incorrect_pattern']
                            ]
            df['is_outliner']     = [
                            'N' if x == 'pass'
                            else 'outliner' 
                            for x in df['is_outliner']
                            ]
            df['is_missing_data']     = [
                            'N' if x == 'pass'
                            else 'missing_data' 
                            for x in df['is_missing_data']
                            ]

            #print(df.columns)
            
            path_to_output = "{}/{}/{}/csv/".format(self.path_to_output, self.dataset_name, self.profile['profile_id'])
            fileName = "{}_with_tag.csv".format(self.dataset_name)
            df.to_csv('{}{}'.format(path_to_output, fileName),index=False)     

        if (self.auditing_config != None):
            if (validateJSON(json.dumps(self.auditing_config))):
                print("auditing_config is Valid")
            else:
                print("auditing_config is not Valid")
                sys.exit(9)
                
        #----------------------------------Start Sakda-----------------------
        self.userPlatform = checkPlatform()
        result          = Commander(self).action()
        data_profile    = result['data_profile']
        data_auditing   = result['data_auditing']
        data_viz        = result['data_viz']

        self.dataset_name       = self.df_name
        self.profile            = data_profile.profile
        self.duplicated_record  = data_profile.df_duplicated_data
        self.schema             = data_profile.schema
        self.schema_json        = data_profile.schema_json
        self.blank_column       = data_profile.blank_column 
        self.columns            = data_profile.columns
        self.profile_json       = data_profile.profile_json
        self.column_profile     = data_profile.column_profile
        self.column_profile_json = data_profile.column_profile_json
        if self.userPlatform == "Window":
            path_to_output = "{}\{}".format(path_to_output, self.dataset_name)
            print('====> {}\{}\csv\\'.format(path_to_output, data_profile.profile['profile_id']))
            print('====> {}\{}\img\\'.format(path_to_output, data_profile.profile['profile_id']))
            print('====> {}\{}\\report\\'.format(self.path_to_output, data_profile.profile['profile_id']))
        else:
            path_to_output = "{}/{}".format(self.path_to_output, self.dataset_name)
            print('====> {}/{}/csv/'.format(path_to_output, data_profile.profile['profile_id']))
            print('====> {}/{}/img/'.format(path_to_output, data_profile.profile['profile_id']))
            print('====> {}/{}/report/'.format(path_to_output, data_profile.profile['profile_id']))
            #------------------------------------------------------------- 
        if (self.auditing_config != None):     
            self.audited_result             = data_auditing.audit_result           
            self.audited_result_json        = data_auditing.audit_result_json 
            self.audited_dataset_with_tag   = data_auditing.audited_dataset_with_tag  
            self.audited_dataset_with_tag_to_csv   =   audited_dataset_to_csv(data_auditing.audited_dataset_with_tag)                
            self.audited_dataset            = data_auditing.audited_dataset 


        


