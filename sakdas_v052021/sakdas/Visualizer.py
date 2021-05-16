import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import pathlib
import sys
import os
from sys import platform



plt.rcParams.update({'font.size': 11})
plotColorCode   = "#67849b"

class Visualizer:
    def __init__(self, data_profiling_result, data_auditing_result, path_to_output, reporting_config, audit_config, *args, **kwargs):
        self.data_profiling_result      = data_profiling_result
        self.data_auditing_result       = data_auditing_result
        self.path_to_output             = path_to_output
        self.reporting_config           = reporting_config
        self.audit_config               = audit_config
        self.ini                        = None
        self.pathToInternalMat       = None
        self.pathToSaveImg              = None
        self.pathToMkDir                = None
        self.pathToLoadReportTemplate   = None
        self.pathToLoadTemplate   = None
        self.pathToSaveReport           = None
        self.fileNameToSave             = None
        
   
    def task_submit(self):
        
        def get_profile_report(self):
            def checkPlatform():
                userPlatform = None
                if platform == "linux" or platform == "linux2" or platform == "darwin":
                    userPlatform = "Linux"
                elif platform == "win32":
                    userPlatform = "Window"
                return userPlatform

            def manageFilePath(self):
                userPlatform               = checkPlatform()
                profile                         = self.data_profiling_result.profile
                if (userPlatform == 'Window'):
                    path_to_output                  = "{}/{}".format(self.path_to_output, self.fileNameToSave)
                    self.pathToSaveImg              = "{}/{}/img/".format(path_to_output, profile["profile_id"])
                    self.pathToInternalMat          = os.path.abspath(pd.__file__).replace('pandas/__init__.py','sakdas')
                    self.pathToMkDir                = "{}/{}/".format(path_to_output, profile['profile_id'])
                    self.pathToSaveReport           = "{}/{}/report/".format(path_to_output, profile['profile_id'])
                    self.pathToLoadTemplate         = "{}/html_template/".format(self.pathToInternalMat)
                else:
                    self.fileNameToSave             = self.data_profiling_result.df_name.split("/")[-1].replace(".csv",'')
                    path_to_output                  = "{}/{}".format(self.path_to_output, self.fileNameToSave)
                    self.pathToSaveImg              = "{}/{}/img/".format(path_to_output, profile["profile_id"])
                    self.pathToInternalMat          = os.path.abspath(pd.__file__).replace('pandas/__init__.py','sakdas')
                    self.pathToMkDir                = "{}/{}/".format(path_to_output, profile['profile_id'])
                    self.pathToSaveReport           = "{}/{}/report/".format(path_to_output, profile['profile_id'])
                    self.pathToLoadTemplate         = "{}/html_template/".format(self.pathToInternalMat)
                    
            def getTop5UniquePlot(profile):
                for column in profile["column_list"]:
                    columnName = column["columnName"]
                    pattern         = []
                    patternRatio    = []
                    #print(columnName)
                    top5DataPattern = profile["column_profile"][columnName]['top_5_data_value']
                    ratioMissingValue = profile["column_profile"][columnName]['ratio_missing_value']
                    if (ratioMissingValue != 1.0):
                        #print(top5DataPattern)
                        for _pattern in top5DataPattern:
                            if (len(str(_pattern['data_value'])) > 10):
                                #print(len(_pattern['data_value']))
                                pattern.append('{}+N'.format(str(_pattern['data_value'])[:8]))
                            else:
                                pattern.append(str(_pattern['data_value']))
                            patternTag  = _pattern['value_ratio']* 100
                            patternRatio.append(round(patternTag, 2))
                        #print(pattern)
                        #print(patternRatio)
                        df          = pd.DataFrame({'pattern':pattern, 'pertag':patternRatio})
                        ax          = df.sort_values('pertag', ascending=True).plot.barh(color=plotColorCode,x='pattern', y='pertag', rot=0,figsize=(11,5))
                        xmin, xmax  = 0, 100
                        ax.set_xlim(xmin, xmax)
                        for p in ax.patches:
                            #print(p)
                            plt.text(5 + p.get_width(), 
                            p.get_y() + 0.5 * p.get_height(),
                            '{} %'.format(float(p.get_width())),
                            ha='center', va='center')
                        #plt.show()
                        
                        
                        fig = plt
                        fileName = "top5_unique_{}.png".format(columnName)
                        fig.savefig("{}{}".format(self.pathToSaveImg, fileName))
                        plt.close()
            def getTop5PatternPlot(profile): 
                
                fig = plt.figure(figsize=(11, 5))
                for column in profile["column_list"]:
                    columnName = column["columnName"]
                    pattern         = []
                    patternRatio    = []

                    top5DataPattern = profile["column_profile"][columnName]['top_5_data_pattern']
                    ratioMissingValue = profile["column_profile"][columnName]['ratio_missing_value']
                    if (ratioMissingValue != 1.0):
                        for _pattern in top5DataPattern:
                            if (len(str(_pattern['pattern'])) > 10):
                                #print(len(_pattern['data_value']))
                                pattern.append('{}+N'.format(_pattern['pattern'][:8]))
                            else:
                                pattern.append(str(_pattern['pattern']))
                            patternTag  = _pattern['ratio'] * 100
                            patternRatio.append(round(patternTag, 2))
                        #print(pattern)
                        #print(patternRatio)
                        df          = pd.DataFrame({'pattern':pattern, 'pertag':patternRatio})
                        ax          = df.sort_values('pertag', ascending=True).plot.barh(color=plotColorCode ,x='pattern', y='pertag', rot=0,figsize=(11,5))
                        xmin, xmax  = 0, 100
                        ax.set_xlim(xmin, xmax)
                        for p in ax.patches:
                            #print(p)
                            plt.text(5 + p.get_width(), 
                            p.get_y() + 0.5 * p.get_height(),
                            '{} %'.format(float(p.get_width())),
                            ha='center', va='center')
                        #plt.show()
                        fig = plt
                        fileName = "top5_pattern_{}.png".format(columnName)
                        fig.savefig("{}{}".format(self.pathToSaveImg, fileName))
                        plt.close()
            def getHistoPlot(profile):
                dataType = None
                
                for column in profile["column_list"]:
                    columnName = column["columnName"]

                    dataType    = profile["column_profile"][columnName]['dataType'] 
                    ratioMissingValue = profile["column_profile"][columnName]['ratio_missing_value']
                    if (dataType == 'Numerical' and ratioMissingValue != 1.0):
                        fig = plt.figure(figsize=(11, 5))
                        #print(column['columnName'])
                        plt.hist(self.data_profiling_result.df_cleaned[columnName], color=plotColorCode, edgecolor = 'black',
                            bins = int(180/5))

                        
                        fileName = "hist_{}.png".format(columnName)
                        fig.savefig("{}{}".format(self.pathToSaveImg, fileName))
                        plt.close()
            def getBoxPlot(profile):
                
                for column in profile["column_list"]:
                    columnName = column["columnName"]
                    if (profile["column_profile"][columnName]['dataType'] == 'Numerical'):
                        fig = plt.figure(figsize=(11, 5))
                        self.data_profiling_result.df_cleaned.boxplot(column = columnName, vert=False, return_type='dict')
                        #plt.title('Box Plot')
                        #annotate_boxplot(bpdict)
                        fileName = "boxplot_{}.png".format(columnName)
                        fig.savefig("{}{}".format(self.pathToSaveImg, fileName))
                        plt.close()

            def getHeaderReport():
                f = open("{}template_report_header.html".format(self.pathToLoadTemplate), "r")
                reportHeaderTemplate    = f.read()
                reportHeader            = reportHeaderTemplate
                infoToReplace           = [
                    {'selector':'@profilingDate'        , 'replacement_value':self.data_profiling_result.profile['profiling_datetime']},
                    {'selector':'@sakdasVersion'        , 'replacement_value':self.data_profiling_result.profile['profile_engine']},
                    {'selector':'@profilingId'          , 'replacement_value':self.data_profiling_result.profile['profile_id']},
                    {'selector':'@fileName'             , 'replacement_value':self.data_profiling_result.profile['data_name']},
                    {'selector':'@pk'                   , 'replacement_value':self.data_profiling_result.profile['primary_key_column']},
                    {'selector':'@missingCondition'     , 'replacement_value': 'default'},
                    {'selector':'@totalRecord'          , 'replacement_value':str(self.data_profiling_result.profile['number_of_record'])},
                    {'selector':'@totalColumn'          , 'replacement_value':str(self.data_profiling_result.profile['number_of_column'])},
                    {'selector':'@blankColumn'          , 'replacement_value':str(self.data_profiling_result.profile['number_of_blank_column'])},
                    {'selector':'@perMissingData'       , 'replacement_value':str(round(float(self.data_profiling_result.profile['ratio_of_missing_data'])*100,2))+'%'},
                    {'selector':'@ARD'                  , 'replacement_value':str(self.data_profiling_result.profile['number_of_record_after_dedup'])},
                    {'selector':'@dupData'              , 'replacement_value':str(self.data_profiling_result.profile['number_of_dupRecord'])},
                    {'selector':'@completedData'        , 'replacement_value':str(self.data_profiling_result.profile['number_of_complete_record'])},
                    {'selector':'@perCompletedData'     , 'replacement_value':str(round(float(self.data_profiling_result.profile['ratio_of_complete_record'])*100,2))+'%'}
                    ]
                #print(infoToReplace)
                for replace in infoToReplace:
                    #print(replace['replacement_value'])
                    reportHeader = reportHeader.replace(replace['selector'], replace['replacement_value'])

                return reportHeader

            def getColumnProfileReport():
                f = open("{}template_column_profile.html".format(self.pathToLoadTemplate), "r")
                columnProfile = []
                columnProfileTemplate = f.read()
                f.close()
                columnPlot = None
                columnInfoToReplace = []
                profile =  self.data_profiling_result.profile
                for column in profile["column_list"]:
                    #print(profile["column_profile"] )
                    columnName = column["columnName"] 
                    columnProfileRe = columnProfileTemplate
                    dataType        = column["dataType"]
                    ratioMissingValue = profile["column_profile"][columnName]['ratio_missing_value']

                    if (dataType == 'Categorical' and ratioMissingValue != 1.0):
                        f = open("{}template_column_profile_obj.html".format(self.pathToLoadTemplate), "r")
                        columnPlot = f.read()
                        f.close()
                        columnInfoToReplace = [
                            {'selector':'@pathTop5Unique'   , 'replacement_value':"{}top5_unique_{}.png".format(self.pathToSaveImg, columnName)},
                            {'selector':'@pathTop5Pattern'  , 'replacement_value':"{}top5_pattern_{}.png".format(self.pathToSaveImg, columnName)}
                        ]
                    elif(ratioMissingValue != 1.0):
                        f = open("{}template_column_profile_int.html".format(self.pathToLoadTemplate), "r")
                        columnPlot = f.read()
                        f.close()
                        columnInfoToReplace = [
                            {'selector':'@pathTop5Unique'   , 'replacement_value':"{}top5_unique_{}.png".format(self.pathToSaveImg, columnName)},
                            {'selector':'@pathTop5Pattern'  , 'replacement_value':"{}top5_pattern_{}.png".format(self.pathToSaveImg, columnName)},
                            {'selector':'@pathHist'         , 'replacement_value':"{}hist_{}.png".format(self.pathToSaveImg, columnName)},
                            {'selector':'@pathBoxplot'      , 'replacement_value':"{}boxplot_{}.png".format(self.pathToSaveImg, columnName)},
                            {'selector':'@min_value'        , 'replacement_value':str(profile["column_profile"][columnName]['min_value'])},
                            {'selector':'@max_value'        , 'replacement_value':str(profile["column_profile"][columnName]['max_value'])},
                            {'selector':'@mean'             , 'replacement_value':str(profile["column_profile"][columnName]['mean'])},
                            {'selector':'@mode'             , 'replacement_value':str(profile["column_profile"][columnName]['mode'])},
                            {'selector':'@median'           , 'replacement_value':str(profile["column_profile"][columnName]['median'])},
                            {'selector':'@variance'         , 'replacement_value':str(profile["column_profile"][columnName]['variance'])},
                            {'selector':'@std'              , 'replacement_value':str(profile["column_profile"][columnName]['std'])},
                            {'selector':'@distinct_value'   , 'replacement_value':str(profile["column_profile"][columnName]['distinct_value'])},
                            {'selector':'@first_quartile'   , 'replacement_value':str(profile["column_profile"][columnName]['first_quartile'])},
                            {'selector':'@third_quartile'   , 'replacement_value':str(profile["column_profile"][columnName]['third_quartile'])},
                            {'selector':'@iqr'              , 'replacement_value':str(profile["column_profile"][columnName]['iqr'])},
                            {'selector':'@minimum'          , 'replacement_value':str(profile["column_profile"][columnName]['minimum'])},
                            {'selector':'@maximum'          , 'replacement_value':str(profile["column_profile"][columnName]['maximum'])},
                            {'selector':'@median'           , 'replacement_value':str(profile["column_profile"][columnName]['median'])},
                            {'selector':'@negative_outliner_datapoint'  , 'replacement_value':str(profile["column_profile"][columnName]['negative_outliner_datapoint'])},
                            {'selector':'@positive_outliner_datapoint'  , 'replacement_value':str(profile["column_profile"][columnName]['positive_outliner_datapoint'])}
                        ]
                    
                    for replace in columnInfoToReplace:
                        #print(replace)
                        columnPlot = columnPlot.replace(replace['selector'], replace['replacement_value'])
                        #print(columnProfileRe)
                    

                    #print('===================')
                    #print(column['columnName'])
                    #print(columnProfileTemplate)
                    #print(self.data_profiling_result.column_profile[column['columnName']]['dataType'])
                    dataLen = None
                    if (int(profile["column_profile"][columnName]['data_lenght_min']) == int(profile["column_profile"][columnName]['data_lenght_max'])):
                        dataLen = "{}".format(profile["column_profile"][columnName]['data_lenght_min'])
                    else :
                        dataLen = "{} - {}".format(profile["column_profile"][columnName]['data_lenght_min'], profile["column_profile"][columnName]['data_lenght_max'])
                    


                    columnInfoToReplace = [
                        {'selector':'@columnName'   , 'replacement_value':columnName},
                        {'selector':'@dataType'     , 'replacement_value':profile["column_profile"][columnName]['dataType']},
                        {'selector':'@primaryKey'   , 'replacement_value':str(profile["column_profile"][columnName]['is_primary_key'])},
                        {'selector':'@uniqueData'   , 'replacement_value':str(profile["column_profile"][columnName]['ratio_distinct_value']*100)+'%'},
                        {'selector':'@missingData'  , 'replacement_value':str(profile["column_profile"][columnName]['missing_value'])},
                        {'selector':'@dataLen'      , 'replacement_value':dataLen},
                        {'selector':'@columnPlot'   , 'replacement_value':columnPlot}
                        ]
                    #print(column['columnName'])
                    for replace in columnInfoToReplace:
                        #print(replace)
                        columnProfileRe = columnProfileRe.replace(replace['selector'], replace['replacement_value'])
                        #print(columnProfileRe)
                    

                    columnProfile.append(columnProfileRe)

                for blankColumn in self.data_profiling_result.blank_column:
                    columnProfileRe = columnProfileTemplate
                    
                    columnInfoToReplace = [
                        {'selector':'@columnName'   , 'replacement_value':blankColumn},
                        {'selector':'@dataType'     , 'replacement_value':'N/A'},
                        {'selector':'@primaryKey'   , 'replacement_value':'N/A'},
                        {'selector':'@uniqueData'   , 'replacement_value':'N/A'},
                        {'selector':'@missingData'  , 'replacement_value':str(self.data_profiling_result.df_cleaned.shape[0])},
                        {'selector':'@dataLen'      , 'replacement_value':'N/A'},
                        {'selector':'@columnPlot'   , 'replacement_value':'N/A'}
                        ]
                    #print(column['columnName'])
                    for replace in columnInfoToReplace:
                        #print(replace)
                        columnProfileRe = columnProfileRe.replace(replace['selector'], replace['replacement_value'])
                        #print(columnProfileRe)
                    

                    columnProfile.append(columnProfileRe)

                #print(columnProfile)


                    #top5DataPattern = self.data_profiling_result.column_profile[column['columnName']]['top_5_data_pattern']
                #print('\n'.join(columnProfile))
                #print('\n'.join(columnProfile))
                f = open("{}columnProfile.html".format(self.pathToSaveReport),"w+")
                f.write('\n'.join(columnProfile))
                f.close()
                columnProfileReport = '\n'.join(columnProfile)
                return columnProfileReport
            #def getAuditReport(): 
            
            def getFullReport(profile):
                

                f=open("{}template_full_report.html".format(self.pathToLoadTemplate), "r")
                reportTemplate = f.read()
                f.close()
                
                report = reportTemplate.replace('@reportHeader', getHeaderReport())
                report = report.replace('@columnProfile', getColumnProfileReport())

                
                reportFile = "{}profile_{}.html".format(self.pathToSaveReport, self.fileNameToSave )
                f = open(reportFile,"w+")
                f.write(report)
                print("Profiling report:")
                print("========> {}".format(reportFile))
            #------------------------------------------------
            manageFilePath(self)

            profile =  self.data_profiling_result.profile
            os.makedirs("{}img".format(self.pathToMkDir))
            os.makedirs("{}report".format(self.pathToMkDir))
            os.makedirs("{}csv".format(self.pathToMkDir))

            
            getBoxPlot(profile)
            getHistoPlot(profile)
            
            getTop5UniquePlot(profile)
            getTop5PatternPlot(profile)
            getFullReport(profile)
        if (self.reporting_config == False):
            print('Skip Visualizing')
        else:
            print('====> Start Visualizing: Dataset Profile')
            print(pathlib.Path().absolute())
            begin_time = datetime.now()
            #----------------------------
            get_profile_report(self)
            #----------------------------
            end_time = datetime.now()
            time_eslap = (end_time - begin_time).total_seconds()
            print("time elapsed: {} sec".format(time_eslap))
            #----


        if self.data_auditing_result != None:
            print("Gen Auditing Report")
            def get_auditing_report(self):
                #print("Gen Auditing Report")
                def getPiPlot():

                    self.data_auditing_result.audited_dataset_with_tag.reset_index(drop=True, inplace=True)
                    #print(self.data_auditing_result.audited_dataset_with_tag['audit_result'])
                    #print('AAA')
                    df4pie = self.data_auditing_result.audited_dataset_with_tag.groupby('audit_result').size().reset_index(name='count').sort_values(by='count', ascending=False)
                    #print(df4pie)
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
                                    #print(r)
                                    ll.append(r)
                            lable = ll
                            lable = ','.join(lable)
                        _lable.append(lable)
                        idx = idx + 1
                    #print(cntCount)
                    #print(_lable)

                    #================
                    _value = []
                    _smallValue = []
                    _lableNew = []
                    _smallLable = []
                    idx = 0
                    for v in df4pie['count']:
                        lable = _lable[idx]
                        v = df4pie['count'][idx]
                        onePCT = float(self.data_auditing_result.audited_dataset_with_tag.shape[0]) / 100
                        #print(onePCT)
                        if v > onePCT:
                            _value.append(v)
                            _lableNew.append(lable)
                        else:
                            _smallValue.append(v)
                            _smallLable.append(lable)
                        idx = idx +1
                    if (len(_smallLable) > 0):
                        _lableNew.append(_smallLable)
                        _value.append(sum(_smallValue))
                    #print(_lableNew)
                    #print(_value)
                    
                    my_colors = ['lightblue','lightsteelblue','silver']
                    plt.pie(_value, labels=_lableNew, autopct='%1.2f%%', startangle=0, colors=my_colors)
                    plt.axis('equal') 
                    plt.tight_layout()
                    fig = plt
                    fileName = "piplot.png"
                    fig.savefig("{}{}".format(self.pathToSaveImg, fileName))
                def getReport():
                    #print('Generate audit report')
                    blockOOR        = ''
                    blockPattern    = ''
                    blockOutlier    = ''
                    blockMissing    = ''
                    if (self.data_auditing_result.tableOOR != None):
                        blockOOR = """
                                        <div class="col-md-12 table-b">
                                            <h4>
                                                Out of Range
                                            </h4>
                                            <div>
                                                <table class="table-striped table-hover table-condensed">
                                                    {}
                                                </table>
                                            </div>
                                        </div>""".format(self.data_auditing_result.tableOOR)
                    if (self.data_auditing_result.tablePattern != None):
                        blockPattern = """
                                        <div class="col-md-12 table-b">
                                            <h4>
                                                Data Pattern
                                            </h4>
                                            <div>
                                                <table class="table-striped table-hover table-condensed">
                                                    {}
                                                </table>
                                            </div>
                                        </div>""".format(self.data_auditing_result.tablePattern)
                    if (self.data_auditing_result.tableOutlier != None):
                        blockOutlier = """
                                        <div class="col-md-12 table-b">
                                            <h4>
                                                Outlier
                                            </h4>
                                            <div>
                                                <table class="table-striped table-hover table-condensed">
                                                    {}
                                                </table>
                                            </div>
                                        </div>""".format(self.data_auditing_result.tableOutlier)
                    if (self.data_auditing_result.tableMissing != None):
                        #if (self.audit_config['audit']['define_custom_missing_value'] == False):
                        missingCondition = 'null'
                        #else:
                            #missingCondition = ', '.join(self.audit_config['audit']['define_custom_missing_value'])

                        
                        blockMissing = """
                                        <div class="col-md-12 table-b">
                                            <h4>
                                                Missing Data
                                            </h4>
                                            <h6>
                                                Null Condition: {}
                                            </h6>
                                            <div>
                                                <table class="table-striped table-hover table-condensed">
                                                    {}
                                                </table>
                                            </div>
                                        </div>""".format(missingCondition,self.data_auditing_result.tableMissing)
                    fileName = "profile_{}.html".format(self.fileNameToSave)
                    f=open("{}{}".format(self.pathToSaveReport, fileName) , "r")
                    reportTemplate = f.read()
                    f.close()

                    fileName = "template_auditing_report.html"
                    f=open("{}{}".format(self.pathToLoadTemplate, fileName), "r")
                    reportAuditingReport = f.read()
                    f.close()
                    reportAuditingReport = reportAuditingReport.replace('@auditedRecord',str(self.data_profiling_result.df_cleaned.shape[0]))
                    reportAuditingReport = reportAuditingReport.replace('@pass', str(self.data_auditing_result.auditCntPass))
                    reportAuditingReport = reportAuditingReport.replace('@ppct', str(round(float(self.data_auditing_result.auditCntPass) / float(self.data_profiling_result.df_cleaned.shape[0]),2) * 100))
                    
                    reportAuditingReport = reportAuditingReport.replace('@piplot', "{}piplot.png".format(self.pathToSaveImg))
                    reportAuditingReport = reportAuditingReport.replace('@audit_OOR', blockOOR)
                    reportAuditingReport = reportAuditingReport.replace('@audit_pattern', blockPattern)
                    reportAuditingReport = reportAuditingReport.replace('@audit_outlier', blockOutlier)
                    reportAuditingReport = reportAuditingReport.replace('@audit_missing', blockMissing)
                    report = reportTemplate.replace("<div>____</div>", reportAuditingReport)

                    fileName = "report_{}.html".format(self.fileNameToSave)
                    reportFile = "{}{}".format(self.pathToSaveReport, fileName)
                    f=open(reportFile, "w+")
                    f.write(report)
                    print("Profiling and auditing report:")
                    print("========> {}".format(reportFile))
                #try:
                
                getPiPlot()
                getReport()
                
                #except FileNotFoundError:
                    #self.get_profile_report()
                    #getPiPlot()
                    #getReport()
                #    pass
                return None

            if (self.reporting_config == False):
                print('Skip Visualizing')
            else:
                print('====> Start Visualizing: Auditing Report')
                begin_time = datetime.now()
                #-------------------
                get_auditing_report(self)
                #-------------------
                end_time = datetime.now()
                time_eslap = (end_time - begin_time).total_seconds()
                print("time elapsed: {} sec".format(time_eslap))
