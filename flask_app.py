# -*- coding: utf-8 -*-
"""
Created on Sat Apr 10 15:07:15 2021

@author: ramya
"""

import pandas as pd
import flask 
from flask import request, jsonify
import requests
import json

# =============================================================================
#district data url
districtDataUrl = pd.read_csv('https://covid19-dashboard.ages.at/data/CovidFaelle_Timeline_GKZ.csv',sep=";")
districtDataUrl.info(verbose=False)
districtDataUrl.info()
districtDataUrl.dtypes
print('count of nan values')
print(districtDataUrl.isna().sum())
print(districtDataUrl.isnull().sum(axis = 0))

importantColumns = districtDataUrl[['Time','Bezirk','AnzEinwohner','AnzahlFaelle','AnzahlFaelleSum','AnzahlFaelle7Tage']]

#convert to datetime format of time column for grouping by week,month,year dayfirst=true for correct conversion format(yyyy-mm-dd)
importantColumns['Time']=pd.to_datetime(districtDataUrl['Time'],dayfirst=True)

# =============================================================================

#R VALUE url

rValueUrl = pd.read_csv('https://www.ages.at/fileadmin/AGES2015/Wissen-Aktuell/COVID19/R_eff.csv',sep=";",decimal=',')
rValueUrl.info(verbose=False)
rValueUrl.info()
rValueUrl.dtypes

importantColumnsREFF = rValueUrl[['Datum','R_eff']]
importantColumnsREFF['Datum']=pd.to_datetime(rValueUrl['Datum'])

# =============================================================================
#Vaccination data url
vaccinationDataUrl = pd.read_csv('https://info.gesundheitsministerium.gv.at/data/timeline-bundeslaendermeldungen.csv',sep=';')
vaccinationDataUrl.info(verbose=False)
vaccinationDataUrl.info()
vaccinationDataUrl.dtypes

print('count of nan values')
print(vaccinationDataUrl.isna().sum())
print(vaccinationDataUrl.isnull().sum(axis = 0))
#delete the rows where column value is NaN
#vaccinationDataUrl.dropna(axis=0)
importantColumnsVacc=vaccinationDataUrl[["Datum","Name","Bevölkerung","GemeldeteImpfungenLaender"]]
#print(importantColumnsVacc.describe())
importantColumnsVacc['Datum']=pd.to_datetime(importantColumnsVacc['Datum'],utc=True)
importantColumnsVacc['Datum']=importantColumnsVacc['Datum'].dt.tz_convert('CET')
#print(importantColumnsVacc['Datum'])
#print(importantColumnsVacc.describe())

# =============================================================================
#read json file for warn level
response = requests.get("https://corona-ampel.gv.at/sites/corona-ampel.gv.at/files/assets/Warnstufen_Corona_Ampel_aktuell.json",timeout=5) 
#response.close()
entiredata=json.loads(response.text)

finallist=[]
# read loacl csv file for coordinates
df = pd.read_csv (r'AustrianCitiesWithCoordinates.csv')   

# =============================================================================
def getMarkerColor(i):
    switcher={
                '1':'green',
                '2':'yellow',
                '3':'orange',
                '4':'red',
             }
    return switcher.get(i,"Invalid number")
def getWeight(j):
    switcher={
                '1':1,
                '2':1,
                '3':1,
                '4':1,
             }
    return switcher.get(j,"Invalid number")
# =============================================================================
#API
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<p>District data: pass parameters district name, year  and interval  to get json view of data</p>"

# A route to return all the json data.
@app.route('/api/positivecasesbydistrict/', methods=['GET'])
def api_DistrictPositiveCases_Filter():
    districtname=''
    year=''
    interval=''
    #get query parameters
    query_parameters = request.args
    # assign param to filter data
    districtname=query_parameters.get('districtname')
    year=query_parameters.get('year')
    interval=query_parameters.get('interval')
    
    
    
    if 'districtname' in query_parameters:
        districtnametofilter=districtname
        filteredDistrict=importantColumns[importantColumns['Bezirk'].apply(lambda val:districtnametofilter in val)]
   
    else:
        return 'Error:No district name provided. Please choose a district name.'
    if 'year' in query_parameters:
        yeartofilter=year
    else:
        return 'Error:No year provided. Please choose a year.'
   
    if 'interval' in query_parameters:
      dataintervaltofilter=interval
        
    if(dataintervaltofilter=='Monthly'):
           districtDataByMonth=filteredDistrict.assign(DistrictName=filteredDistrict['Bezirk'],Interval=filteredDistrict['Time'].dt.strftime('%m').sort_index(),Year=filteredDistrict['Time'].dt.strftime('%Y').sort_index()).groupby(['DistrictName','Interval','Year'])['AnzahlFaelle'].sum()
           districtDataByMonth_FilterYear=districtDataByMonth.filter(like=yeartofilter)
           convertedJson = districtDataByMonth_FilterYear.to_json(orient="table")
             
    elif(dataintervaltofilter=='Weekly'):
           districtDataByWeek=filteredDistrict.assign(DistrictName=filteredDistrict['Bezirk'],Interval=filteredDistrict['Time'].dt.strftime('%W').sort_index(),Year=filteredDistrict['Time'].dt.strftime('%Y').sort_index()).groupby(['DistrictName','Interval','Year'])['AnzahlFaelle'].sum()
           districtDataByWeek_FilterYear=districtDataByWeek.filter(like=yeartofilter)
           convertedJson = districtDataByWeek_FilterYear.to_json(orient="table")
          
    elif(dataintervaltofilter=='Yearly'):
           
           districtDataByYear=filteredDistrict.assign(DistrictName=filteredDistrict['Bezirk'],YearlyInterval='1',Year=filteredDistrict['Time'].dt.strftime('%Y').sort_index()).groupby(['DistrictName','Year','YearlyInterval'])['AnzahlFaelle'].sum()
           districtDataByYear_FilterYear=districtDataByYear.filter(like=yeartofilter)
           convertedJson = districtDataByYear_FilterYear.to_json(orient="table")
           
    else:
        return 'Error:No interval provided. Please choose a data interval.'
   
    
    
    #convertedJson = districtDataByMonth.to_json(orient="table")
    #de-serialize into python obj
    parsedJson = json.loads(convertedJson)
    #serialize into json
    json.dumps(parsedJson) 
    #json op to mime-type application/json
    return jsonify(parsedJson)

# =============================================================================


@app.route('/api/dropdownvalues/', methods=['GET'])
def get_all_district_names():
 
 districtnames=districtDataUrl['Bezirk'].unique()
 statenames=vaccinationDataUrl['Name'].unique()
 Dates=['2021-09-09','2021-09-02','2021-08-26','2021-08-19','2021-08-12','2021-08-05','2021-07-29','2021-07-22','2021-07-15','2021-07-08','2021-07-01','2021-06-24','2021-06-17','2021-06-10','2021-06-02','2021-05-27','2021-05-20','2021-05-12','2021-05-06','2021-04-29','2021-04-22','2021-04-15','2021-04-08','2021-03-31','2021-03-25','2021-03-18','2021-03-11','2021-03-04','2021-02-25','2021-02-18','2021-02-11','2021-02-04','2021-01-28','2021-01-21','2021-01-14','2021-01-07','2020-12-30','2020-12-22','2020-12-17','2020-12-10','2020-12-03','2020-11-26','2020-11-19','2020-11-12','2020-11-05','2020-10-29','2020-10-22','2020-10-15','2020-10-09','2020-10-01','2020-09-24','2020-09-18','2020-09-15','2020-09-11','2020-09-04']
 
 cn={}
 sn={}
 
 cn['districtName']=districtnames
 sn['stateName']=statenames

 ddvalues=pd.DataFrame(cn,columns=['districtName'])
 ddvalues.insert(0,'id',range(1,1+len(ddvalues)))
 dd1values=pd.DataFrame(sn,columns=['stateName'])
 dd1values.insert(0,'id',range(1,1+len(dd1values)))
 dd2values=pd.DataFrame(Dates,columns=['Date'])
 dd2values.insert(0,'id',range(1,1+len(dd2values)))

 dd=ddvalues.to_json(orient='records') 
 dd1=dd1values.to_json(orient='records') 
 dd2=dd2values.to_json(orient='records') 
 


 des=json.loads(dd)
 des1=json.loads(dd1)
 des2=json.loads(dd2)
 json.dumps(des) 
 json.dumps(des1) 
 json.dumps(des2) 
 return jsonify(Districts=des,States=des1,WarnLevelDates=des2)

# =============================================================================

@app.route('/REff', methods=['GET'])
def REffhome():
    return "<p>R_Effective data: R effective value for austria grouped by week month and year</p>"

# A route to return all the json data.
@app.route('/api/R_eff_Austria/', methods=['GET'])
def api_REffectiveValue_Filter():
    
    year=''
    interval=''
    #get query parameters
    query_parameters = request.args
    # assign param to filter data
    
    year=query_parameters.get('year')
    interval=query_parameters.get('interval')
    
    if 'year' in query_parameters:
        yeartofilter=year
    else:
        return 'Error:No year provided. Please choose a year.'
    if 'interval' in query_parameters:
        dataintervaltofilter=interval
        
        if(dataintervaltofilter=='Monthly'):
             REffDataByMonth=importantColumnsREFF.assign(Interval=importantColumnsREFF['Datum'].dt.strftime('%m').sort_index(),Year=importantColumnsREFF['Datum'].dt.strftime('%Y').sort_index()).groupby(['Interval','Year'])['R_eff'].sum()
             REffDataByMonth_FilterYear=REffDataByMonth.filter(like=yeartofilter)
             convertedJsonREff = REffDataByMonth_FilterYear.to_json(orient="table")
             
        elif(dataintervaltofilter=='Weekly'):
           REffDataByWeek=importantColumnsREFF.assign(Interval=importantColumnsREFF['Datum'].dt.strftime('%W').sort_index(),Year=importantColumnsREFF['Datum'].dt.strftime('%Y').sort_index()).groupby(['Interval','Year'])['R_eff'].sum()
           REffDataByWeek_FilterYear=REffDataByWeek.filter(like=yeartofilter)
           convertedJsonREff = REffDataByWeek_FilterYear.to_json(orient="table")
          
        elif(dataintervaltofilter=='Yearly'):
           REffDataByYear=importantColumnsREFF.assign(YearlyInterval='1',Year=importantColumnsREFF['Datum'].dt.strftime('%Y').sort_index()).groupby(['Year','YearlyInterval'])['R_eff'].sum()
           REffDataByYear_FilterYear=REffDataByYear.filter(like=yeartofilter)
           convertedJsonREff = REffDataByYear_FilterYear.to_json(orient="table")
           
    else:
        return 'Error:No interval provided. Please choose a data interval.'
    
    parsedJsonREff = json.loads(convertedJsonREff)
    json.dumps(parsedJsonREff) 
    return jsonify(parsedJsonREff)


# =============================================================================

@app.route('/Vaccination', methods=['GET'])
def Vaccination():
    return "<p>Vaccination data: Vaccination data for countries grouped by week month and year</p>"

# A route to return all the json data.
@app.route('/api/Vaccination/', methods=['GET'])
def api_Vaccination_Filter():
    statename=''
    year=''
    interval=''
    #get query parameters
    query_parameters = request.args
    # assign param to filter data
    
    statename=query_parameters.get('statename')
    year=query_parameters.get('year')
    interval=query_parameters.get('interval')
    
    if 'statename' in query_parameters:
        countrynametofilter=statename
        filteredCountry=importantColumnsVacc[importantColumnsVacc['Name'].apply(lambda val:countrynametofilter in val)]
   
    else:
        return 'Error:No country name provided. Please choose a country name.'
    if 'year' in query_parameters:
        yeartofilter=year
    else:
        return 'Error:No year provided. Please choose a year.'
   
    if 'interval' in query_parameters:
        dataintervaltofilter=interval
        
        if(dataintervaltofilter=='Monthly'):
             VaccDataByMonth=filteredCountry.assign(Country_Region=filteredCountry['Name'],Interval=filteredCountry['Datum'].dt.strftime('%m').sort_index(),Year=filteredCountry['Datum'].dt.strftime('%Y').sort_index()).groupby(['Country_Region','Bevölkerung','Interval','Year'])['GemeldeteImpfungenLaender'].sum()
             
             # ['GemeldeteImpfungenLaender']/VaccDataByMonth['Bevölkerung'])*100
             #VaccDataByMonth.assign(percentagePopulationVaccinated)
             VaccDataByMonth_FilterYear=VaccDataByMonth.filter(like=yeartofilter)
             convertedJsonVacc = VaccDataByMonth_FilterYear.to_json(orient="table")
             
        elif(dataintervaltofilter=='Weekly'):
           VaccDataByWeek=filteredCountry.assign(Country_Region=filteredCountry['Name'],Interval=filteredCountry['Datum'].dt.strftime('%W').sort_index(),Year=filteredCountry['Datum'].dt.strftime('%Y').sort_index()).groupby(['Country_Region','Bevölkerung','Interval','Year'])['GemeldeteImpfungenLaender'].sum()
           VaccDataByWeek_FilterYear=VaccDataByWeek.filter(like=yeartofilter)
           convertedJsonVacc = VaccDataByWeek_FilterYear.to_json(orient="table")
          
        elif(dataintervaltofilter=='Yearly'):
           VaccDataByYear=filteredCountry.assign(Country_Region=filteredCountry['Name'],Interval=filteredCountry['Datum'].dt.strftime('%Y').sort_index()).groupby(['Country_Region','Bevölkerung','Interval'])['GemeldeteImpfungenLaender'].sum()
           VaccDataByYear_FilterYear=VaccDataByYear.filter(like=yeartofilter)
           convertedJsonVacc = VaccDataByYear_FilterYear.to_json(orient="table")
           
    else:
        return 'Error:No interval provided. Please choose a data interval.'
    
    parsedJsonVacc = json.loads(convertedJsonVacc)
    json.dumps(parsedJsonVacc) 
    return jsonify(parsedJsonVacc)

# =============================================================================

@app.route('/api/warnLevelRegion/', methods=['GET'])

def api_warningLevelRegion():
  
  date=''
  query_parameters = request.args
  date=query_parameters.get('date')
  if 'date' in query_parameters:
   datetofilter=date
  else:
   return 'Error:No date provided. Please choose a date.'
  citiesWithCoordinatesByDate=[]
 
  for warnLevelObjects in entiredata:
    warnLevelObjects['Stand']=warnLevelObjects['Stand'][0:10]
    if warnLevelObjects['Stand']== datetofilter:
     for region in warnLevelObjects['Warnstufen']:
      if region['Name'] is not None: 
       for entry in df.iterrows(): 
            if entry[1]['cityName'] == region['Name']:
              citiesDict ={}
              
              citiesDict['cityName'] =region['Name']
              citiesDict['Latitude'] =entry[1]['Latitude']
              citiesDict['Longitude'] =entry[1]['Longitude']
              citiesDict['Warnstufe'] =region['Warnstufe']
              citiesDict['MarkerColor']=getMarkerColor(citiesDict['Warnstufe'])
              citiesDict['weight']=getWeight(citiesDict['Warnstufe'])
              citiesWithCoordinatesByDate.append(citiesDict)
  print(citiesWithCoordinatesByDate)      
  responseWarnLevel = jsonify(citiesWithCoordinatesByDate)
  return responseWarnLevel
                

app.run(port=8080)