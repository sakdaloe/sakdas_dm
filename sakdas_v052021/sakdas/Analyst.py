from multiprocessing import Process
from datetime import datetime
import pandas as pd
import numpy as np
import json
import uuid
import re
from buildAnalyst import buildAnalyst

packageVersion = "sakdas version 2.10.88"
class Analyst:
    def __init__(self,df,df_name, *args, **kwargs):
        self.df                         = df
        self.df_name                    = df_name
        self.df_duplicated_data         = None      #Done
        self.schema                     = None      #Done
        self.blank_column               = []        #Done
        self.columns                    = []        #Done
        self.schema_json                = None      #Done
        self.profile                    = None      #Done
        self.profile_json               = None      #Done
        self.column_profile             = None      #Done
        self.column_profile_json        = None      #Done
    def task_submit(self):
      
        def step_1_drop_duplicated_data():
            self.df_cleaned                = self.df.drop_duplicates()
            self.df_audited_dataset        = self.df.drop_duplicates()
            self.df_duplicated_data        = self.df[self.df.duplicated()]
            
        def getTop5(columnName):
            dataValueResult = []
            df              = self.df_cleaned.groupby(columnName).size().reset_index(name = "Group_Count").sort_values(by='Group_Count', ascending=False)
            df              = df.nlargest(5, 'Group_Count')
            dfSize          = self.df_cleaned  .shape[0]
            value           = ''
            countValue      = 0
            valueRatio      = 0.0

            for data in df.values.tolist():
                value       = data[0]
                countValue  = data[1]
                valueRatio  = round((countValue/dfSize), 4)
                dataValueResult.append({
                    'data_value'            : str(value),
                    'count_data_value'      : str(countValue),
                    'value_ratio'           : str(valueRatio)
                    })
            self.top5Value = json.dumps(dataValueResult, indent=4)

        def getDataLen(dataValues):
            dataLenList = []
            for value in dataValues:
                dataLenList.append(len(str(value)))

            lenDf = pd.DataFrame(dataLenList)
            varStd = round(lenDf.loc[:,0].std(),2)
            varMin = round(lenDf.loc[:,0].min(),2)
            varMax = round(lenDf.loc[:,0].max(),2)
            varAvg = round(lenDf.loc[:,0].mean(),2)
            self.dataLen =  varStd, varMin, varMax, varAvg

        def getDataPattern(dataValues):
            dataPattern         = []
            dataPattern_result  = []
            for value in dataValues:
                valuePattern    = []
            
                if (str(value) != 'nan'):
                    for char in str(value):
                        
                        if re.match('[A-Z]|[a-z]|[0-9]|[ก-ฮ]', char):
                            valuePattern.append('X')
                        else:
                            valuePattern.append(char)
                    valuePattern = ''.join(valuePattern)
                    dataPattern.append(valuePattern)
                

            dataPatternDfSize   = pd.DataFrame({'pattern':dataPattern}).shape[0]
            dataPatternDf       = pd.DataFrame({'pattern':dataPattern}).groupby('pattern').size().reset_index(name = "Group_Count").sort_values(by='Group_Count', ascending=False)
            df                  = dataPatternDf.nlargest(5, 'Group_Count')
    
            pattern         = ''
            countPattern    = 0
            patternRatio    = 0.0
  
            for data in df.values.tolist():
                pattern         = data[0]
                countPattern    = data[1]
                patternRatio    = round((countPattern/dataPatternDfSize), 4)
                dataPattern_result.append({
                    'pattern'           : str(pattern),
                    'count_pattern'     : str(countPattern),
                    'pattern_ratio'     : str(patternRatio)
                    })

            self.dataPattern =  json.dumps(dataPattern_result, indent=4)

        def getCountOutliner(dataValues, minimum, maximum):
            negativeOutlierDataPoint = 0
            positiveOutlierDataPoint = 0

            for value in dataValues:
    
                if (float(value) > float(maximum)):
                    negativeOutlierDataPoint = negativeOutlierDataPoint + 1
                elif (float(value) < float(minimum)):
                    positiveOutlierDataPoint = positiveOutlierDataPoint + 1
            
            return negativeOutlierDataPoint, positiveOutlierDataPoint
            
        def step_2_analyze_df():
            self.schema         = []
            self.schema.append("===Schema===")
            self.schema_json    = []
            self.column_profile = []
            df                  = self.df_cleaned
            totalRecord         = df.shape[0]

            for (columnName, columnData) in df.iteritems():
                self.schema.append("{}: {}".format(columnName, columnData.dtypes))
                isPrimaryKey            = False
                uniqueValue             = columnData.nunique()
                uniqueValueRatio        = round((columnData.nunique()/totalRecord),4)
                self.schema_json.append({
                    'columnName'    : columnName, 
                    'dataType'      : str(columnData.dtypes)
                })

                if (uniqueValue == totalRecord):
                    isPrimaryKey = True

                totalMissing                = columnData.isnull().sum()
                totalMissingRatio         = (columnData.isnull().sum()/totalRecord)

                if (totalMissingRatio == 1.0):
                    self.blank_column.append(columnName)
                else:
                    self.columns.append(columnName)
                    p1 = Process(target=getDataPattern(columnData.values))
                    p1.start()
                    p2 = Process(target=getDataLen(columnData.values))
                    p2.start()
                    p3 = Process(target=getTop5(columnName))
                    p3.start()

                    p1.join()
                    p2.join()
                    p3.join()
                    
                    
                    if (str(columnData.dtypes) != 'object' and totalMissingRatio != 1.0):
                        valueMin                = round(columnData.min(),2)
                        valueMax                = round(columnData.max(),2)
                        mean                    = round(columnData.mean(),2)
                        mode                    = round(columnData.mode()[0],2)
                        variance                = round(columnData.var(),2)
                        std                     = round(columnData.std(),2)
                        median                  = round(columnData.quantile(.5),2)
                        firstQuartile           = round(columnData.quantile(.25),2)
                        third_quartile          = round(columnData.quantile(.75),2)
                        iqr                     = round(third_quartile - firstQuartile,2)
                        minimum                 = round(third_quartile - 1.5 * iqr,2)
                        maximum                 = round(firstQuartile + 1.5 * iqr,2)
                        negativeOutlierDataPoint, positiveOutlierDataPoint = getCountOutliner(columnData.values, minimum, maximum)
                        
                        if variance ==  0 :
                            negativeOutlierDataPoint = 0
                            positiveOutlierDataPoint = 0

                        columnProfileJson = (
                        "\"{}\":".format(columnName) +
                        "{" +
                        "\"dataType\":\"{}\"".format(columnData.dtypes) + "," +
                        "\"is_primary_key\":\"{}\"".format(isPrimaryKey) + "," +
                        "\"distinct_value\":\"{}\"".format(uniqueValue) + "," +
                        "\"ratio_distinct_value\":\"{}\"".format(uniqueValueRatio) + "," +
                        "\"missing_value\":\"{}\"".format(totalMissing) + "," +
                        "\"ratio_missing_value\":\"{}\"".format(totalMissingRatio) + "," +
                        "\"data_lenght_min\":\"{}\"".format(self.dataLen[1]) + "," +
                        "\"data_lenght_max\":\"{}\"".format(self.dataLen[2]) + "," +
                        "\"min_value\":\"{}\"".format(valueMin) + "," +
                        "\"max_value\":\"{}\"".format(valueMax) + "," +
                        "\"mean\":\"{}\"".format(mean) + "," +
                        "\"median\":\"{}\"".format(median) + "," +
                        "\"mode\":\"{}\"".format(mode) + "," +
                        "\"variance\":\"{}\"".format(variance) + "," +
                        "\"std\":\"{}\"".format(std) + "," +
                        "\"first_quartile\":\"{}\"".format(firstQuartile) + "," +
                        "\"third_quartile\":\"{}\"".format(third_quartile) + "," +
                        "\"iqr\":\"{}\"".format(iqr) + "," +
                        "\"minimum\":\"{}\"".format(minimum) + "," +
                        "\"maximum\":\"{}\"".format(maximum) + "," +
                        "\"negative_outliner_datapoint\":\"{}\"".format(negativeOutlierDataPoint) + "," +
                        "\"positive_outliner_datapoint\":\"{}\"".format(positiveOutlierDataPoint) + "," +
                        "\"top_5_data_value\":{}".format(self.top5Value) + "," +
                        "\"top_5_data_pattern\":{}".format(self.dataPattern) +
                        "}"
                        )
                        #print("m",mode)
                        #print("s",std)
                        #print("v",variance)
                    else:
                        columnProfileJson = (
                        "\"{}\":".format(columnName) +
                        "{" +
                        "\"dataType\":\"{}\"".format(columnData.dtypes) + "," +
                        "\"is_primary_key\":\"{}\"".format(isPrimaryKey) + "," +
                        "\"distinct_value\":\"{}\"".format(uniqueValue) + "," +
                        "\"ratio_distinct_value\":\"{}\"".format(uniqueValueRatio) + "," +
                        "\"missing_value\":\"{}\"".format(totalMissing) + "," +
                        "\"ratio_missing_value\":\"{}\"".format(totalMissingRatio) + "," +
                        "\"data_lenght_min\":\"{}\"".format(self.dataLen[1]) + "," +
                        "\"data_lenght_max\":\"{}\"".format(self.dataLen[2]) + "," +
                        "\"top_5_data_value\":{}".format(self.top5Value) + "," +
                        "\"top_5_data_pattern\":{}".format(self.dataPattern) +
                        "}"
                        )
                    
                 
                    
                    self.column_profile.append(columnProfileJson)
               
            self.schema        = "\n".join(self.schema)
            self.column_profile      = ",".join(self.column_profile)
            self.column_profile      = "{{{}}}".format(self.column_profile)
            self.column_profile      = json.loads(self.column_profile)
            self.column_profile_json = json.dumps(self.column_profile, indent=4)
            
         

            self.column_profile_json = json.dumps(self.column_profile, indent=4)
           
            del totalMissing
            del totalMissingRatio
            del df
            del totalRecord

        def getBlankColumn():
            return len(self.blank_column)

        def getMissingData():
            result = []
            missingData = 0
            totalCell = self.df_cleaned  .shape[0] * self.df_cleaned.shape[1]
            for columnName in self.columns:
                missingData = missingData + int(self.column_profile[columnName]['missing_value'])
            result.append("null")
            missingDataFromBlankColumn = len(self.blank_column) * self.df_cleaned  .shape[0]
            missingData = missingData + missingDataFromBlankColumn
            result.append(str(missingData))
            result.append(str(missingData / totalCell))
            return result

        def getCompletedRecord():
            result = []
            completedRecord = self.df_cleaned  .dropna().shape[0]
            completedRecordRatio = self.df_cleaned  .dropna().shape[0] / self.df_cleaned  .shape[0]
            result.append(str(completedRecord))
            result.append(str(completedRecordRatio))
            return result
        
        def getPK():
            result = []
            for column in self.columns:
                isPrimaryKey = self.column_profile[column]['is_primary_key']
                if (isPrimaryKey == "True"):
                    result.append(column)
            if (len(result) == 0):
                result.append("Not Avaiable")
            return result

        def step_3_getProfile():
            
            strUUID = str(uuid.uuid4())
            #os.makedirs("{}/sakdas/{}/img".format(self.outputPath, strUUID))
            #os.makedirs("{}/sakdas/{}/report".format(self.outputPath, strUUID))
            #os.makedirs("{}/sakdas/{}/csv".format(self.outputPath, strUUID))
            missing_condition = getMissingData()[0]

            self.profile = {
            'profile_engine'                    : packageVersion,
            'profile_id'                        : strUUID,
            'file_name'                         : self.df_name.split("/")[-1],
            'profile_datetime'                  : datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            'total_record'                      : self.df.shape[0],
            'total_record_after_deduplication'  : self.df_cleaned  .shape[0],
            'duplicated_data'                   : str(int(self.df.shape[0]) - int(self.df_cleaned  .shape[0])),
            'primary_key'                       : ",".join(getPK()),
            'total_column'                      : self.df.shape[1],
            'completed_record'                  : getCompletedRecord()[0],
            'completed_record_ratio'            : getCompletedRecord()[1],
            'blank_column'                      : getBlankColumn(),
            'missing_condition'                 : getMissingData()[0],
            'missing_data'                      : getMissingData()[1],
            'missing_data_ratio'                : getMissingData()[2],
            'schema'                            : self.schema,
            'column_profile'                    : self.column_profile,
            }

            self.profile_json = json.dumps(self.profile, indent=4)
           
        

        begin_time = datetime.now()
        

        
        print('====> Start Analysing')
        print("1/3 Load Dataframe")
        step_1_drop_duplicated_data()

        print("2/3 Collect Dataset Schema")
        #step_2_analyze_df()

        print("3/3 Generate Dataset Profile")
        #step_3_getProfile()
        self.profile = buildAnalyst(self.df)
        self.profile_json = json.dumps(self.profile, indent=4)

        end_time = datetime.now()
        time_eslap = (end_time - begin_time).total_seconds()
        print("time elapsed: {} sec".format(time_eslap))
        
        return self