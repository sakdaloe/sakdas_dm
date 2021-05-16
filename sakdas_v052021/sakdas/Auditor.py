
from datetime import datetime
import pandas as pd
import json
import re
class Auditor:
    def __init__(self, data_profile, audit_config, path_to_output, *args, **kwargs):
        if audit_config != None:
            
            self.profile                                            = data_profile
            self.audit_config                                       = audit_config
            self.audit_result                                       = {'audit_result':{}}
            self.audit_result['audit_result']['file_name']          = self.profile.df_name.split("/")[-1]
            self.audit_result['audit_result']['audit_datetime']     = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            self.audited_dataset_with_tag                           = None  
            self.audited_dataset                                    = data_profile.df_cleaned                                   
            self.path_to_output                                     = path_to_output
            self.audited_column                                     = []
        else:
            self.audit_config  = None
    def task_submit(self):
        #========================================================================================================
        
        def auditDatarange():
            if (self.audited_dataset_with_tag is None): 
                self.audited_dataset_with_tag = self.profile.df_cleaned
            self.audited_dataset_with_tag['audit_OOR'] = 'pass'
            try:
                if (self.audit_config['audit']['audit_data_range']):
                    print('1/4 Audit data range')

                    auditResult             = []
                    countNotPass            = None
                    countPass               = None
                    df                      = self.audited_dataset_with_tag
                    auditDatarangeConfig    = self.audit_config['audit']['audit_data_range']
                    totalRecord             = self.audited_dataset_with_tag.shape[0]
                    
                    for audit in auditDatarangeConfig:
                        auditColumnName     = '{}_out_of_range'.format(audit['column_name'])
                        self.audited_column.append(auditColumnName)
                        if (audit['min'] == None):
                            df.loc[df[audit['column_name']] > audit['max'], auditColumnName] = "Y"
                            df.loc[df[audit['column_name']] > audit['max'], 'audit_OOR'] = "Out of Range"
                            #df.loc[df[audit['column_name']] <= audit['max'], 'audit_OOR'] = "pass"


                        elif (audit['max'] == None):
                            df.loc[df[audit['column_name']] < audit['min'], auditColumnName] = "Y"
                            df.loc[df[audit['column_name']] < audit['min'], 'audit_OOR'] = "Out of Range"
                            #df.loc[df[audit['column_name']] >= audit['min'], 'audit_OOR'] = "pass"

                        else:
                            df.loc[(df[audit['column_name']] < audit['min']) | (df[audit['column_name']] > audit['max']), auditColumnName] = "Y"
                            df.loc[(df[audit['column_name']] < audit['min']) | (df[audit['column_name']] > audit['max']), 'audit_OOR'] = "Out of Range"
                            #df.loc[(df[audit['column_name']] >= float(audit['min'])) | (df[audit['column_name']] <= float(audit['max'])), 'audit_OOR'] = "pass"
                        
                        
                        countPass           = df[df[auditColumnName] != 'Y'].shape[0]
                        countNotPass        = totalRecord - countPass
                        auditResult.append({
                            'column_name'   : audit['column_name'], 
                            'range'         : {'min': audit['min'], 'max': audit['max']},
                            'pass'          : countPass, 
                            'not_pass'      : countNotPass, 
                            'pass_ratio'    : round((countPass/totalRecord),2)})
                        
                        del df[auditColumnName]

                    self.audit_result['audit_result']['audit_data_range'] = auditResult
                    table = []
                    table.append("<thead><tr><th class=\"column_name\">Column Name</th><th>Range</th><th class=\"pass\">Pass</th><th class=\"pass_pct\">Pass(%)</th></tr></thead>")

                    for result in self.audit_result['audit_result']['audit_data_range']:
                        row = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}%</td></tr>".format(
                                result['column_name'],
                                result['range'],
                                result['pass'],
                                float(result['pass_ratio']) * 100
                        )
                        table.append(row)
                    
                    table = "\n".join(table)
                   
                    return table
                    del countNotPass, countPass, auditResult, df, auditColumnName, auditDatarangeConfig, totalRecord
                else:
                    print('1/4 Skiped audit data range')
                    return None
            except KeyError:
                print('Key Error: in audit_data_range')
                pass
            
        def auditDataPattern():
            if (self.audited_dataset_with_tag is None): 
                self.audited_dataset_with_tag = self.profile.df_cleaned
            self.audited_dataset_with_tag['audit_pattern'] = 'pass'
            try:
                if (self.audit_config['audit']['audit_data_pattern']):
                    print('2/4 Audit data pattern')
                    auditDataPatternConfig  = self.audit_config['audit']['audit_data_pattern']
                    auditResult             = []
                    df                      = self.audited_dataset_with_tag
                    rr = ['pass'] * df.shape[0]

                    for audit in auditDataPatternConfig:
                        totalRecord             = self.profile.df_cleaned.shape[0]
                        auditColumnName     = '{}_pattern_not_valid'.format(audit['column_name'])
                        self.audited_column.append(auditColumnName)

                        df[auditColumnName] = ['' 
                        if re.search(audit['regex_pattern'], str(x))
                        else 'Y' 
                        for x in df[audit['column_name']]
                        ]

                        """ df['audit_pattern'] = ['pass' 
                        if re.search(audit['regex_pattern'], str(x))
                        else 'pattern not valid' 
                        for x in df[audit['column_name']]
                        ] """

                        for index, value in df[audit['column_name']].items():        
                            if re.search(audit['regex_pattern'], str(value)):
                                pass
                            else:
                                if rr[index] != 'Incorrect Pattern':
                                    rr[index] = 'Incorrect Pattern'

                       
                        if self.profile.profile["column_profile"][audit['column_name']]['missing_value'] > 0:
                            totalRecord = totalRecord - self.profile.profile["column_profile"][audit['column_name']]['missing_value']

                        countPass       = df[df[auditColumnName] != 'Y'].shape[0]
                        countNotPass    = totalRecord - countPass
                        auditResult.append({
                            'regex_pattern' : audit['regex_pattern'],
                            'column_name'   : audit['column_name'], 
                            'pass'          : countPass, 
                            'not_pass'      : countNotPass, 
                            'pass_ratio'    : round((countPass/totalRecord), 4)})
                        del df[auditColumnName]
                    self.audit_result['audit_result']['audit_data_pattern'] = auditResult
                    df['audit_pattern'] = rr
                    
                    table = []
                    table.append("<thead><tr><th class=\"column_name\">Column Name</th><th>Regex Pattern</th><th class =\"pass\">Pass</th><th class=\"pass_pct\">Pass(%)</th></tr></thead>")
                    for result in self.audit_result['audit_result']['audit_data_pattern']:
                       
                        row = "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}%</td></tr>".format(
                                result['column_name'],
                                result['regex_pattern'],
                                result['pass'],
                                float(result['pass_ratio']) * 100
                        )
                        table.append(row)
                        
                    table = "\n".join(table)
                    return table

                    del countNotPass, countPass, auditResult, df, auditColumnName, auditDataPatternConfig, totalRecord
                else:
                    print('2/4 Skiped audit data pattern')
                    return None

            except KeyError:
                print('Key Error: in audit_data_pattern')
                pass

        def auditOutlier():
            if (self.audited_dataset_with_tag is None): 
                self.audited_dataset_with_tag = self.profile.df_cleaned
            self.audited_dataset_with_tag['audit_OL'] = 'pass'
            try:
                if (self.audit_config['audit']['audit_outlier']):
                    print('3/4 Audit outlier')

                    auditResult             = []
                    df                      = self.audited_dataset_with_tag
                    totalRecord             = self.profile.profile['number_of_record_after_dedup']
                    rr = ['pass'] * df.shape[0]
 
                    for column in self.profile.profile["column_list"]:
                        columnName = column['columnName']
                        dataType    = column['dataType']
                        if (dataType == 'Numerical'):
                            auditColumnName = '{}_outlier'.format(columnName)
                            
                            minimum = self.profile.profile["column_profile"][columnName]['minimum']
                            maximum = self.profile.profile["column_profile"][columnName]['maximum']

                            df[auditColumnName]     = [
                            'nagative' if x < minimum
                            else 'positive' if x > maximum
                            else '' 
                            for x in df[columnName]
                            ]
                            df['audit_OL']     = [
                            'Outlier' if x < minimum
                            else 'Outlier' if x > maximum
                            else 'pass' 
                            for x in df[columnName]
                            ]
                            
                            for index, value in df[columnName].items():
                                value = round(value,2)
                                if value < minimum or value > maximum:
                                    if rr[index] != 'Outlier':
                                        rr[index] = 'Outlier'

                            countPass               = df[~df[auditColumnName].isin(['nagative','positive'])].shape[0]
                            countNotPassNegative    = df[df[auditColumnName] == 'nagative'].shape[0]
                            countNotPassPositive    = df[df[auditColumnName] == 'positive'].shape[0]
                            pass_ratio = round((countPass/totalRecord),2)
                            del df[auditColumnName]
                            if self.profile.profile["column_profile"][columnName]['distinct_value'] ==  1 :
                                countPass               = self.audited_dataset_with_tag.shape[0] - int(self.profile.profile["column_profile"][columnName]['missing_value'])
                                countNotPassNegative    = 0 
                                countNotPassPositive    = 0
                                pass_ratio              = 1.0

                            auditResult.append({
                                'column_name'       : columnName, 
                                'pass'              : countPass, 
                                'negative'          : countNotPassNegative, 
                                'positive'          : countNotPassPositive, 
                                'pass_ratio'        : pass_ratio })
                    
                    df['audit_OL'] = rr
                    self.audit_result['audit_result']['audit_outlier'] = auditResult
                    table = []
                    table.append("<thead><tr><th class=\"column_name\">Column Name</th><th>Range</th><th class=\"pass\">Pass</th><th class=\"pass_pct\">Pass(%)</th></tr></thead>")
                    for result in self.audit_result['audit_result']['audit_outlier']:
                        _minimum = float(self.profile.profile["column_profile"][result['column_name']]['minimum'])
                        _maximum = float(self.profile.profile["column_profile"][result['column_name']]['maximum'])
                        row = "<tr><td>{}</td><td>{} - {}</td><td>{}</td><td>{}%</td></tr>".format(
                                result['column_name'],
                                _minimum,
                                _maximum,
                                result['pass'],
                                float(result['pass_ratio']) * 100
                        )
                        table.append(row)
                    
                    table = "\n".join(table)
                    pd.set_option('display.max_rows', self.audited_dataset_with_tag.shape[0]+1)
                    
                    return table

                    del auditResult, auditColumnName, df, totalRecord, countPass, countNotPassNegative, countNotPassPositive
                else:
                    print('3/4 Skiped audit outlier')
                    return None
                
            except Exception as e:
                print(e)
                print('Key Error: in audit_outlier')
                pass

        def auditMissingValue():
            if (self.audited_dataset_with_tag is None): 
                self.audited_dataset_with_tag = self.profile.df_cleaned
            self.audited_dataset_with_tag['audit_MISS'] = 'pass'
            #try:
            if (self.audit_config['audit']['audit_missing_value'] == True):
                print('4/4 Audit missing data')
                auditResult             = []
                customMissingValue      = None
                
                if (1 == 2):
                    customMissingValue = self.audit_config['audit']['define_custom_missing_value']
                    df = self.profile.df_cleaned
                    for column in self.profile.profile["column_list"]:
                        columnName = column['columnName']
                        countAddMissingValue    = df[df[columnName].isin(customMissingValue)].shape[0]
                        countSTDMissingValus    = self.profile.profile["column_profile"][columnName]['missing_value']
                        totalMissing            = float(countSTDMissingValus)+float(countAddMissingValue)
                        self.profile.profile["column_profile"][columnName]['missing_value']         = str(int(totalMissing))
                        self.profile.profile["column_profile"][columnName]['ratio_missing_value']   = str(round(float(totalMissing) / float(df.shape[0]),6))
                        df.loc[df[columnName].isnull() | df[columnName].isin(customMissingValue), 'audit_MISS'] = "Missing"
                        df.loc[df['audit_MISS'] != "Missing", 'audit_MISS'] = "pass"
                elif (1 == 1):
                    
                    df = self.profile.df_cleaned
                    for column in self.profile.profile["column_list"]:
                        columnName = column['columnName']
                        
                        #countSTDMissingValus    = self.profile.profile["column_profile"][columnName]['missing_value']
                        #totalMissing            = float(countSTDMissingValus)
                        #print(columnName)
                        #print(self.profile.profile["column_profile"][columnName])
                        #print(self.profile.profile["column_profile"][columnName]['missing_value'])
                        #print(self.profile.profile["column_profile"][columnName]['ratio_missing_value'])
                        #print(countSTDMissingValus)
                        #print(totalMissing)
                        #print(totalMissing)
                        #self.profile.profile["column_profile"][columnName]['missing_value']         = str(int(totalMissing))
                        #self.profile.profile["column_profile"][columnName]['ratio_missing_value']   = str(round(float(totalMissing) / float(df.shape[0]),6))
                        #print(df['audit_MISS'])
                        df.loc[df[columnName].isnull(), 'audit_MISS'] = "Missing"
                        df.loc[df['audit_MISS'] != "Missing", 'audit_MISS'] = "pass"

                for column in self.profile.profile["column_list"]:
                    columnName = column['columnName']
                    auditResult.append({
                        'column_name'           : columnName,
                        'missing_value'         : self.profile.profile["column_profile"][column['columnName']]['missing_value'] ,
                        'ratio_missing_value'   : self.profile.profile["column_profile"][column['columnName']]['ratio_missing_value']})
                self.audit_result['audit_result']['audit_missing_value'] = {
                    'custom_missing_value': 'NA',
                    'result':  auditResult}

                table = []
                table.append("<thead><tr><th class=\"column_name\">Column Name</th><th>Missing Data</th><th>Pass(%)</th></tr></thead>")
                for result in self.audit_result['audit_result']['audit_missing_value']['result']:

                    row = "\t\t\t\t\t<tr><td>{}</td><td>{}</td><td>{}%</td></tr>".format(
                            result['column_name'],
                            result['missing_value'],
                            (1 - float(result['ratio_missing_value'])) * 100
                    )
                    table.append(row)
                
                table = "\n".join(table)
                return table
            else:
                print('4/4 Skiped audit missing data')
                return None
                

            #except KeyError:
                #print('Key Error: in audit_missing_value')
        def summaryAudtingResult():
            #print(self.audited_dataset_with_tag)
            #print(self.audited_dataset_with_tag['audit_OOR'])
            #print(self.audited_dataset_with_tag['audit_pattern'])
            #print(self.audited_dataset_with_tag['audit_OL'])
            #print(self.audited_dataset_with_tag['audit_MISS'])
            #print(self.audited_dataset_with_tag['audit_OOR'].map(str) +','+ self.audited_dataset_with_tag['audit_pattern'].map(str) +','+ self.audited_dataset_with_tag['audit_OL'].map(str)+','+ self.audited_dataset_with_tag['audit_MISS'].map(str))
            self.audited_dataset_with_tag['audit_result']   = self.audited_dataset_with_tag['audit_OOR'].map(str) +','+ self.audited_dataset_with_tag['audit_pattern'].map(str) +','+ self.audited_dataset_with_tag['audit_OL'].map(str)+','+ self.audited_dataset_with_tag['audit_MISS'].map(str)
            self.audited_dataset['audit_result']            = self.audited_dataset_with_tag['audit_OOR'].map(str) +','+ self.audited_dataset_with_tag['audit_pattern'].map(str) +','+ self.audited_dataset_with_tag['audit_OL'].map(str)+','+ self.audited_dataset_with_tag['audit_MISS'].map(str)

            self.audited_dataset_with_tag.reset_index(drop=True, inplace=True)
            df4pie = self.audited_dataset_with_tag.groupby('audit_result').size().reset_index(name='count').sort_values(by='count', ascending=False)
            _lable = []
            idx = 0
            cntCount = 0
            for lable in df4pie['audit_result']:
                #print(lable)
                lable = df4pie['audit_result'][idx]
                if (lable =='pass,pass,pass,pass'):
                    lable = 'Pass'
                    cntCount = cntCount + 1
                elif (lable !='pass,pass,pass,pass'):
                    lable = lable.split(',')
                    ll = []
                    for r in lable:
                        if (r != 'pass'):
                            ll.append(r)
                    lable = ll
                    lable = ','.join(lable)
                _lable.append(lable)
                idx = idx + 1
            self.auditCntPass = self.audited_dataset[self.audited_dataset['audit_result'] == 'pass,pass,pass,pass'].shape[0]
        #==================================================================================================================
        

        if self.audit_config != None:
            print('====> Start Auditing')
            begin_time = datetime.now()
            pd.set_option('mode.chained_assignment', None)
            self.tableOOR = auditDatarange()
            self.tablePattern = auditDataPattern()
            self.tableOutlier = auditOutlier()
            self.tableMissing = auditMissingValue()
            summaryAudtingResult()
            #getPiPlot()
            #getReport()
            self.audit_result['audit_result']['overall_pass'] = self.auditCntPass
            self.audit_result['audit_result']['overall_pass(%)'] = round(self.auditCntPass/self.profile.df_cleaned.shape[0],2)
            self.audit_result_json = json.dumps(self.audit_result, indent=4)
            #self.audited_dataset_with_tag.to_csv("{}/sakdas/{}/csv/audited_with_tag_{}.csv".format(self.path_to_output, self.profile.profile['profile_id'], self.profile.df_name.split("/")[-1].replace(".csv",'')))
            self.audited_dataset = self.audited_dataset[self.audited_dataset['audit_result'] == 'pass,pass,pass,pass']
            #print(self.df)
            self.audited_dataset = self.audited_dataset.drop(columns='audit_result')
            #self.audited_dataset.to_csv("{}/sakdas/{}/csv/audited_{}.csv".format(self.path_to_output, self.profile.profile['profile_id'], self.profile.df_name.split("/")[-1].replace(".csv",'')))
            #print(self.audit_result_json)
            #print('Done !!!')
            end_time = datetime.now()
            time_eslap = (end_time - begin_time).total_seconds()
            print("time elapsed: {} sec".format(time_eslap))

        else:
            print('Skip Auditing')
            return None
        
        return self
