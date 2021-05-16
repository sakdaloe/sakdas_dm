import re
import json
import pandas as pd

def func_getColumnListWithType(df):
    result = []
    for (columnName, columnData) in df.iteritems():
        dataType = str(columnData.dtypes)
        if (dataType == "object"):
            dataType = 'Categorical'
        else:
            dataType = 'Numerical'
        
        if (columnName.lower().find("id")) != -1 :
            dataType = 'Categorical'
       
        result.append({"columnName" : columnName, "dataType" : dataType})
    return result 

def func_cntNumberOfRecord(df):
    result = df.shape[0]
    return result

def func_cntNumberOfRecordAfterDedup(df):
    result = df.drop_duplicates().shape[0]
    return result

def func_cntNumberOfDupRecord(df):
    result = df[df.duplicated()].shape[0]
    return result

def func_cntPrimaryKey(df):
    df = df.drop_duplicates()
    cntPK = 0
    totalRecord = df.shape[0]
    for (columnName, columnData) in df.iteritems():
        uniqueValue = columnData.nunique()
        if (uniqueValue == totalRecord) :
            cntPK =  cntPK + 1
    return cntPK

def func_cntNumberOfColumn(df):
    result = df.drop_duplicates().shape[1]
    return result

def func_cntNumberOfCompleteRecord(df):
    df = df.drop_duplicates()
    result = df.dropna().shape[0]
    del df
    return result

def func_cntRatioOfCompleteRecord(df):
    df = df.drop_duplicates()
    result = df.dropna().shape[0]/df.shape[0]
    del df
    return round(result, 2)

def func_cntRatioOfCompleteColumn(df):
    df = df.drop_duplicates()
    totalRecord = df.shape[0]
    totalColumn = df.shape[1]
    result = 0
    for (columnName, columnData) in df.iteritems():
        totalMissingRatio = columnData.isnull().sum()/totalRecord

        if (totalMissingRatio == 0.0):
            result = result + 1
    result = result / totalColumn 
    del df
    return round(result, 2)

def func_cntNumberOfBlankColumn(df):
    df = df.drop_duplicates()
    totalRecord = df.shape[0]
    result = 0
    for (columnName, columnData) in df.iteritems():
        totalMissingRatio = columnData.isnull().sum()/totalRecord

        if (totalMissingRatio == 1.0):
            result = result + 1
    del df
    return result

def func_cntNumberOfCompleteColumn(df):
    df = df.drop_duplicates()
    totalRecord = df.shape[0]
    result = 0
    for (columnName, columnData) in df.iteritems():
        totalMissingRatio = columnData.isnull().sum()/totalRecord

        if (totalMissingRatio == 0.0):
            result = result + 1
    del df
    return result

def func_cntNumberOfMissingData(df):
    df = df.drop_duplicates()
    result = 0
    for (columnName, columnData) in df.iteritems():
        result = result + columnData.isnull().sum()
    print(result)
    del df
    return int(result)

def func_cntRatioOfMissingData(df):
    df = df.drop_duplicates()
    totalCell = df.shape[0] * df.shape[1]
    result = 0
    for (columnName, columnData) in df.iteritems():
        result = result + columnData.isnull().sum()

    result = result / totalCell
    
    del df
    return round(result, 2)

def func_getPrimaryKeyColumn(df):
    df              = df.drop_duplicates()
    totalRecord     = df.shape[0]
    result          = []

    for (columnName, columnData) in df.iteritems():
        uniqueValueRatio        = round((columnData.nunique()/totalRecord),2)
        if (uniqueValueRatio == 1.0):
            result.append(columnName)
    return ','.join(result)

#----------------------------------func_getColumnProfile-----------------------#
def subFunc_cntNumberOfMissingDataByColumn(dataValuesInColumn):
    result = dataValuesInColumn.isnull().sum()
    return int(result)
def subFunc_cntRatioOfMissingDataByColumn(dataValuesInColumn, df):
    result = dataValuesInColumn.isnull().sum()/df.drop_duplicates().shape[0]
    return round(result, 2)
def subFunc_cntNumberOfDistinctDataByColumn(dataValuesInColumn):
    result = dataValuesInColumn.nunique()
    return int(result)
def subFunc_cntRatioOfDistinctDataByColumn(dataValuesInColumn, df):
    result = dataValuesInColumn.nunique()/df.drop_duplicates().shape[0]
    return round(result, 2)
def subFunc_checkIsPrimaryKeyByColumn(dataValuesInColumn, df):
    result = dataValuesInColumn.nunique()/df.drop_duplicates().shape[0]
    if (result == 1.0): isPrimaryKey = True 
    else: isPrimaryKey = False
    return isPrimaryKey
def subFunc_getDataType(dataType, columnName):
    if (dataType == "object"): dataType = 'Categorical' 
    else: dataType = 'Numerical'
    if (columnName.lower().find("id")) != -1 : dataType = 'Categorical'
    if (columnName.lower().find("code")) != -1 : dataType = 'Categorical'
    return dataType
def subFunc_extractPattern(value):
    valuePattern    = []
    if (str(value) != 'nan'):
        for char in str(value):
            if re.match('[A-Z]|[a-z]|[ก-ฮ]', char): valuePattern.append('X')
            elif re.match('[0-9]', char): valuePattern.append('N')
            else: valuePattern.append(char)
        pattern = ''.join(valuePattern)
    return pattern
def subFunc_checkExistingPattern(dataPattern, pattern):
    isExistingPattern = False
    idx = 0
    for index, p in enumerate(dataPattern):
        if pattern == p["pattern"]:
            isExistingPattern = True
            idx = index
    return isExistingPattern, idx
def subFunc_updateDataPattern(isExistingPattern, pattern,  existingIdx, dataPattern):
    if isExistingPattern == False:
        dataPattern.append({"pattern":pattern, "cnt": 1})
    else:
        existingObj = dataPattern[existingIdx]
        newObj      = {"pattern":existingObj["pattern"], "cnt": existingObj["cnt"]+1}
        dataPattern[existingIdx] = newObj
    return dataPattern
def subFunc_calPatternRatio(dataPattern, df):
    for p in dataPattern:
        ratio = round(p["cnt"]/ df.shape[0],2)
        p.update({"ratio": ratio})
    return dataPattern
def subFunc_getDataLen(dataValuesInColumn):
    dataLenList = []
    for value in dataValuesInColumn: #Each Cell
        dataLenList.append(len(str(value)))
        #if len(str(value)) not in dataLenList: dataLenList.append(len(str(value)))

    lenDf   = pd.DataFrame(dataLenList)
    lenStd  = round(lenDf.loc[:,0].std(),2)
    lenMin  = round(lenDf.loc[:,0].min(),2)
    lenMax  = round(lenDf.loc[:,0].max(),2)
    lenMean = round(lenDf.loc[:,0].mean(),2)
    lenMode = round(lenDf.loc[:,0].mode(),2)
    lenMedian = round(lenDf.loc[:,0].median(),2)
    lenVar  = round(lenDf.loc[:,0].var(),2)
    del lenDf
    return lenStd, lenMin, lenMax, lenMean, lenMode, lenMedian, lenVar
def subFunc_sumProfile(profile):
    result = {
            'profile_engine'                    : profile.packageVersion,
            'profile_id'                        : profile.id,
            'file_name'                         : profile.fileName,
            'profile_datetime'                  : profile.dateTime,
            'total_record'                      : profile.numberOfRecord,
            'total_record_after_deduplication'  : profile.numberOfRecordAfterDedup,
            'duplicated_data'                   : profile.numberOfDupRecord,
            'primary_key'                       : profile.numberOfPrimaryKey,
            'total_column'                      : profile.numberOfColumn,
            'completed_record'                  : profile.numberOfCompleteRecord,
            'completed_record_ratio'            : profile.ratioOfCompleteRecord,
            'blank_column'                      : profile.numberOfBlankColumn,
            'missing_condition'                 : profile.missingCondition,
            'missing_data'                      : profile.numberOfMissingData,
            'missing_data_ratio'                : profile.ratioOfMissingData,
            'schema'                            : profile.a,
            'column_profile'                    : profile.b,
            }
def subFunc_getCountOutliner(dataValuesInColumn, minimum, maximum):
    negativeOutlierDataPoint = 0
    positiveOutlierDataPoint = 0
    variance                = round(dataValuesInColumn.var(),2)
    if (variance !=  0):
        for value in dataValuesInColumn.values:

            if (float(value) > float(maximum)):
                negativeOutlierDataPoint = negativeOutlierDataPoint + 1
            elif (float(value) < float(minimum)):
                positiveOutlierDataPoint = positiveOutlierDataPoint + 1
    
    return negativeOutlierDataPoint, positiveOutlierDataPoint
def subFunc_getTop5DataValue(df, columnName):
    df              = df.drop_duplicates()
    dataValueResult = []
    dfGroup         = df.groupby(columnName).size().reset_index(name = "Group_Count").sort_values(by='Group_Count', ascending=False)
    dfGroup         = dfGroup.nlargest(5, 'Group_Count')
    dfSize          = df.shape[0]
    value           = ''
    countValue      = 0
    valueRatio      = 0.0

    for data in dfGroup.values.tolist():
        value       = data[0]
        countValue  = data[1]
        valueRatio  = round((countValue/dfSize), 4)
        dataValueResult.append({
            'data_value'            : value,
            'count_data_value'      : int(countValue),
            'value_ratio'           : round(valueRatio, 2)
            })
    #top5Value = json.dumps(dataValueResult, indent=4)
    #print(top5Value)
    del df, dfGroup 
    return dataValueResult
def subFunc_getTop5DataPattern(dataPattern):
    pattern = []
    
    for p in dataPattern:
        pattern.append([p["pattern"], p["ratio"]])

    df = pd.DataFrame(pattern, columns=['pattern','ratio'])
    df1 = df.sort_values('ratio',ascending = False).head(5)

    result = df1.to_dict('records')
    print(result)
    del df, df1
    return result
#----------------------------------func_getColumnProfile-----------------------#
def func_getColumnProfile(df):
    result = []
    df = df.drop_duplicates()
    allColumnProfile = {}
    for (columnName, columnData) in df.iteritems(): #Each Column
        columnProfile = {}
        #=========Get Data Pattern==========================================================#
        dataPattern         = []

        for value in columnData.values: #Each Cell
            
            pattern = subFunc_extractPattern(value)
            newPattern = True

            if (len(dataPattern) == 0): #Initial
                dataPattern.append({"pattern":pattern, "cnt": 1})
            else:
                isExistingPattern, existingIdx = subFunc_checkExistingPattern(dataPattern, pattern)
                dataPattern = subFunc_updateDataPattern(isExistingPattern, pattern, existingIdx, dataPattern)          
        #---------------
        dataPattern = subFunc_calPatternRatio(dataPattern, df)
        #=========Get Data Len==========================================================#
        lenStd, lenMin, lenMax, lenMean, lenMode, lenMedian, lenVar = subFunc_getDataLen(columnData.values)
        
        #==========Calculate Statistical Information=========================================================#
        dataType = subFunc_getDataType(str(columnData.dtypes), columnName)
        print(dataType)
        if (dataType == 'Numerical'):
            firstQuartile           = round(columnData.quantile(.25),2)
            third_quartile          = round(columnData.quantile(.75),2)
            iqr                     = round(third_quartile - firstQuartile,2)
            minimum                 = round(third_quartile - 1.5 * iqr,2)
            maximum                 = round(firstQuartile + 1.5 * iqr,2)

            negativeOutlierDataPoint, positiveOutlierDataPoint = subFunc_getCountOutliner(columnData, minimum, maximum)

            columnProfile.update({"dataType"            : dataType})
            columnProfile.update({"is_primary_key"      : subFunc_checkIsPrimaryKeyByColumn(columnData, df)})
            columnProfile.update({"distinct_value"      : int(subFunc_cntNumberOfDistinctDataByColumn(columnData))})
            columnProfile.update({"ratio_distinct_value": float(round(subFunc_cntRatioOfDistinctDataByColumn(columnData, df), 2))})
            columnProfile.update({"missing_value"       : int(subFunc_cntNumberOfMissingDataByColumn(columnData))})
            columnProfile.update({"ratio_missing_value" : float(round(subFunc_cntRatioOfMissingDataByColumn(columnData, df), 2))})
            columnProfile.update({"data_lenght_min"     : int(lenMin)})
            columnProfile.update({"data_lenght_max"     : int(lenMax)})
            columnProfile.update({"data_lenght_mean"    : float(round(lenMean,2))})
            columnProfile.update({"data_lenght_mode"    : int(lenMode)})
            columnProfile.update({"data_lenght_median"  : float(round(lenMedian,2))})
            columnProfile.update({"data_lenght_std"     : float(round(lenStd,2))})
            columnProfile.update({"data_lenght_var"     : float(round(lenVar,2))})
            columnProfile.update({"min_value"           : float(round(columnData.min(),2))})
            columnProfile.update({"max_value"           : float(round(columnData.max(),2))})
            columnProfile.update({"mean"                : float(round(columnData.mean(),2))})
            columnProfile.update({"median"              : float(round(columnData.quantile(.5),2))})
            columnProfile.update({"mode"                : float(round(columnData.mode()[0],2))})
            columnProfile.update({"variance"            : float(round(columnData.var(),2))})
            columnProfile.update({"std"                 : float(round(columnData.std(),2))})
            columnProfile.update({"first_quartile"      : float(round(columnData.quantile(.25),2))})
            columnProfile.update({"third_quartile"      : float(round(columnData.quantile(.75),2))})
            columnProfile.update({"iqr"                 : float(round(third_quartile - firstQuartile,2))})
            columnProfile.update({"minimum"             : float(round(third_quartile - 1.5 * iqr,2))})
            columnProfile.update({"maximum"             : float(round(firstQuartile + 1.5 * iqr,2))})
            columnProfile.update({"negative_outliner_datapoint": int(negativeOutlierDataPoint)})
            columnProfile.update({"positive_outliner_datapoint": int(positiveOutlierDataPoint)})
            columnProfile.update({"top_5_data_value"    : subFunc_getTop5DataValue(df, columnName)})
            columnProfile.update({"top_5_data_pattern"  : subFunc_getTop5DataPattern(dataPattern)})
            columnProfile.update({"data_pattern"        : dataPattern})

        else: 
            columnProfile.update({"dataType"            : dataType})
            columnProfile.update({"is_primary_key"      : subFunc_checkIsPrimaryKeyByColumn(columnData, df)})
            columnProfile.update({"distinct_value"      : int(subFunc_cntNumberOfDistinctDataByColumn(columnData))})
            columnProfile.update({"ratio_distinct_value": float(round(subFunc_cntRatioOfDistinctDataByColumn(columnData, df), 2))})
            columnProfile.update({"missing_value"       : int(subFunc_cntNumberOfMissingDataByColumn(columnData))})
            columnProfile.update({"ratio_missing_value" : float(round(subFunc_cntRatioOfMissingDataByColumn(columnData, df), 2))})
            columnProfile.update({"data_lenght_min"     : int(lenMin)})
            columnProfile.update({"data_lenght_max"     : int(lenMax)})
            columnProfile.update({"data_lenght_mean"    : float(round(lenMean, 2))})
            columnProfile.update({"data_lenght_mode"    : int(lenMode)})
            columnProfile.update({"data_lenght_median"  : float(round(lenMedian, 2))})
            columnProfile.update({"data_lenght_std"     : float(round(lenStd, 2))})
            columnProfile.update({"data_lenght_var"     : float(round(lenVar, 2))})
            columnProfile.update({"top_5_data_value"    : subFunc_getTop5DataValue(df, columnName)})
            columnProfile.update({"top_5_data_pattern"  : subFunc_getTop5DataPattern(dataPattern)})
            columnProfile.update({"data_pattern"        : dataPattern})

        #print(allDataPattern)   
        #columnProfile = json.dumps(columnProfile, indent=4)
        allColumnProfile.update({columnName:columnProfile})
        #print(columnProfile)
        subFunc_getTop5DataPattern(dataPattern)
        print("----------End Column----------------")
    #print(allColumnProfile)
    
    return allColumnProfile