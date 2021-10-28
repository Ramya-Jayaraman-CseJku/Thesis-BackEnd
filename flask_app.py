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

# setttingcopywithwarning remove
pd.options.mode.chained_assignment = None  # default='warn'

# =============================================================================
# district data url
districtDataUrl = pd.read_csv(
    'https://covid19-dashboard.ages.at/data/CovidFaelle_Timeline_GKZ.csv', sep=";")
districtDataUrl.info(verbose=False)
districtDataUrl.info()
districtDataUrl.dtypes
print('count of nan values')
print(districtDataUrl.isna().sum())
print(districtDataUrl.isnull().sum(axis=0))

importantColumns = districtDataUrl[[
    'Time', 'Bezirk', 'AnzahlFaelle']]
# check if the rows contain value zero
print(importantColumns == 0)
# non zero value rows
importantColumns = importantColumns[~(importantColumns == 0).any(axis=1)]
importantColumns.info(verbose=False)
importantColumns.info()
importantColumns.dtypes
# convert to datetime format of time column for grouping by week,month,year dayfirst=true for correct conversion format(yyyy-mm-dd)
importantColumns['Time'] = pd.to_datetime(
    districtDataUrl['Time'], dayfirst=True)

# =============================================================================

# R VALUE url

rValueUrl = pd.read_csv(
    'https://www.ages.at/fileadmin/AGES2015/Wissen-Aktuell/COVID19/R_eff.csv', sep=";", decimal=',')
rValueUrl.info(verbose=False)
rValueUrl.info()
rValueUrl.dtypes

importantColumnsREFF = rValueUrl[['Datum', 'R_eff']]
importantColumnsREFF['Datum'] = pd.to_datetime(rValueUrl['Datum'])

# =============================================================================
# Vaccination data url
vaccinationDataUrl = pd.read_csv(
    'https://info.gesundheitsministerium.gv.at/data/timeline-eimpfpass.csv', sep=';')
pd.options.mode.chained_assignment = None  # default='warn'

# delete the rows where column value is NaN
# vaccinationDataUrl.dropna(axis=0)
importantColumnsVacc = vaccinationDataUrl[[
    "Datum", "Name", "Bevölkerung", "Vollimmunisierte"]]
print('count of nan and null values')
print('nan')
print(importantColumnsVacc.isna().sum())
print('null')
print(importantColumnsVacc.isnull().sum(axis=0))
importantColumnsVacc.dropna(axis=0)
importantColumnsVacc.info(verbose=False)
importantColumnsVacc.info()
importantColumnsVacc.dtypes

# check if the rows contain value zero
print('place name no assignment')
print((importantColumnsVacc == 'KeineZuordnung').sum())
importantColumnsVacc = importantColumnsVacc[~(
    importantColumnsVacc == 'KeineZuordnung').any(axis=1)]
importantColumnsVacc.info(verbose=False)
importantColumnsVacc.info()
importantColumnsVacc.dtypes
print('row values 0')
print(importantColumnsVacc == 0)
print((importantColumnsVacc == 0).sum())

# non zero value rows
importantColumnsVacc = importantColumnsVacc[~(
    importantColumnsVacc == 0).any(axis=1)]
importantColumnsVacc.info(verbose=False)
importantColumnsVacc.info()
importantColumnsVacc.dtypes

importantColumnsVacc['Datum'] = pd.to_datetime(
    importantColumnsVacc['Datum'], utc=True)
importantColumnsVacc['Datum'] = importantColumnsVacc['Datum'].dt.tz_convert(
    'CET')
# print(importantColumnsVacc['Datum'])
# print(importantColumnsVacc.describe())

# =============================================================================
# read json file for warn level
response = requests.get(
    "https://corona-ampel.gv.at/sites/corona-ampel.gv.at/files/assets/Warnstufen_Corona_Ampel_aktuell.json", timeout=5)
# response.close()
entiredata = json.loads(response.text)

finallist = []
# read loacl csv file for coordinates
df = pd.read_csv(r'AustrianCitiesWithCoordinates.csv')

# =============================================================================


def getMarkerColor(i):
    switcher = {
        '1': 'green',
        '2': 'yellow',
        '3': 'orange',
        '4': 'red',
    }
    return switcher.get(i, "Invalid number")


# =============================================================================
# API
app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    sample = "Welcome to the home page of flask API try following routes to see covid related information <br/>1./api/positivecasesbydistrict/<br/>2./api/Vaccination/</br>3./api/R_eff_Austria/</p>"
    return sample

# A route to return all the json data.


@app.route('/api/positivecasesbydistrict/', methods=['GET'])
def api_DistrictPositiveCases_Filter():
    districtname = ''

    interval = ''
    # get query parameters
    query_parameters = request.args
    # assign param to filter data
    districtname = query_parameters.get('districtname')

    interval = query_parameters.get('interval')

    if 'districtname' in query_parameters:
        districtnametofilter = districtname
        filteredDistrict = importantColumns[importantColumns['Bezirk'].apply(
            lambda val:districtnametofilter in val)]

    else:
        return 'Error:No district name provided. Please choose a district name.'

    if 'interval' in query_parameters:
        dataintervaltofilter = interval
    else:
        return 'Error:No interval provided. Please choose a interval .'

    if(dataintervaltofilter == 'Monthly'):

        districtDataByMonth = filteredDistrict.assign(DistrictName=filteredDistrict['Bezirk'], Interval=filteredDistrict['Time'].dt.strftime('%b %Y'), Year=filteredDistrict['Time'].dt.strftime(
            '%Y').sort_index()).groupby(['DistrictName', 'Interval', 'Year'], sort=False)['AnzahlFaelle'].sum()
        convertedJson = districtDataByMonth.to_json(orient="table")

    elif(dataintervaltofilter == 'Weekly'):
        districtDataByWeek = filteredDistrict.assign(DistrictName=filteredDistrict['Bezirk'], Interval='week '+filteredDistrict['Time'].dt.strftime(
            '%W %Y'), Year=filteredDistrict['Time'].dt.strftime('%Y').sort_index()).groupby(['DistrictName', 'Interval', 'Year'], sort=False)['AnzahlFaelle'].sum()
        convertedJson = districtDataByWeek.to_json(orient="table")

    elif(dataintervaltofilter == 'Yearly'):

        districtDataByYear = filteredDistrict.assign(DistrictName=filteredDistrict['Bezirk'], Interval=filteredDistrict['Time'].dt.strftime(
            '%Y').sort_index()).groupby(['DistrictName', 'Interval'])['AnzahlFaelle'].sum()
        convertedJson = districtDataByYear.to_json(orient="table")

    else:
        return 'Error: Interval type provided is mismatched  . Please choose one of the data interval Weekly,Monthly or Yearly.'

    #convertedJson = districtDataByMonth.to_json(orient="table")
    # de-serialize into python obj
    parsedJson = json.loads(convertedJson)
    # serialize into json
    json.dumps(parsedJson)
    # json op to mime-type application/json
    return jsonify(parsedJson)

# =============================================================================


@app.route('/REff', methods=['GET'])
def REffhome():
    return "<p>R_Effective data: R effective value for austria grouped by week month and year</p>"

# A route to return all the json data.


@app.route('/api/R_eff_Austria/', methods=['GET'])
def api_REffectiveValue_Filter():

    interval = ''
    # get query parameters
    query_parameters = request.args
    # assign param to filter data

    interval = query_parameters.get('interval')

    if 'interval' in query_parameters:
        dataintervaltofilter = interval
    else:
        return 'Error:No interval provided. Please choose a interval .'
    if(dataintervaltofilter == 'Daily'):
        REffDataEveryday = importantColumnsREFF.assign(Interval=importantColumnsREFF['Datum'].dt.strftime(
            '%d %b %Y'), Year=importantColumnsREFF['Datum'].dt.strftime('%Y').sort_index())
        convertedJsonREff = REffDataEveryday.to_json(orient="table")

    elif(dataintervaltofilter == 'Monthly'):
        REffDataByMonth = importantColumnsREFF.assign(Interval=importantColumnsREFF['Datum'].dt.strftime(
            '%b %Y'), Year=importantColumnsREFF['Datum'].dt.strftime('%Y').sort_index()).groupby(['Interval', 'Year'], sort=False)['R_eff'].sum()
        convertedJsonREff = REffDataByMonth.to_json(orient="table")

    elif(dataintervaltofilter == 'Weekly'):
        REffDataByWeek = importantColumnsREFF.assign(Interval='week '+importantColumnsREFF['Datum'].dt.strftime(
            '%W %Y'), Year=importantColumnsREFF['Datum'].dt.strftime('%Y').sort_index()).groupby(['Interval', 'Year'], sort=False)['R_eff'].sum()
        convertedJsonREff = REffDataByWeek.to_json(orient="table")

    else:
        return 'Error:Interval type provided is mismatched  . Please choose one of the data interval Weekly,Monthly.'

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
    statename = ''

    interval = ''
    # get query parameters
    query_parameters = request.args
    # assign param to filter data

    statename = query_parameters.get('statename')

    interval = query_parameters.get('interval')

    if 'statename' in query_parameters:
        countrynametofilter = statename
        filteredCountry = importantColumnsVacc[importantColumnsVacc['Name'].apply(
            lambda val:countrynametofilter in val)]

    else:
        return 'Error:No country name provided. Please choose a country name.'

    if 'interval' in query_parameters:
        dataintervaltofilter = interval
    else:
        return 'Error:No interval provided. Please choose a interval .'

    if(dataintervaltofilter == 'Monthly'):
        VaccDataByMonth = filteredCountry.assign(Country_Region=filteredCountry['Name'], Interval=filteredCountry['Datum'].dt.strftime(
            '%b %Y'), Year=filteredCountry['Datum'].dt.strftime('%Y').sort_index()).groupby(['Country_Region', 'Bevölkerung', 'Interval', 'Year'], sort=False)['Vollimmunisierte'].last()
        # ['GemeldeteImpfungenLaender']/VaccDataByMonth['Bevölkerung'])*100
        # VaccDataByMonth.assign(percentagePopulationVaccinated)

        convertedJsonVacc = VaccDataByMonth.to_json(
            orient="table")

    elif(dataintervaltofilter == 'Weekly'):
        VaccDataByWeek = filteredCountry.assign(Country_Region=filteredCountry['Name'], Interval='week '+filteredCountry['Datum'].dt.strftime(
            '%W %Y'), Year=filteredCountry['Datum'].dt.strftime('%Y').sort_index()).groupby(['Country_Region', 'Bevölkerung', 'Interval', 'Year'], sort=False)['Vollimmunisierte'].last()

        convertedJsonVacc = VaccDataByWeek.to_json(
            orient="table")

    elif(dataintervaltofilter == 'Yearly'):
        VaccDataByYear = filteredCountry.assign(Country_Region=filteredCountry['Name'], Interval=filteredCountry['Datum'].dt.strftime(
            '%Y').sort_index()).groupby(['Country_Region', 'Bevölkerung', 'Interval'])['Vollimmunisierte'].last()

        convertedJsonVacc = VaccDataByYear.to_json(
            orient="table")

    else:
        return 'Error:Interval type provided is mismatched  . Please choose one of the data interval Weekly,Monthly,Yearly.'

    parsedJsonVacc = json.loads(convertedJsonVacc)
    json.dumps(parsedJsonVacc)
    return jsonify(parsedJsonVacc)

# =============================================================================


@app.route('/api/warnLevelRegion/', methods=['GET'])
def api_warningLevelRegion():

    date = ''
    query_parameters = request.args
    date = query_parameters.get('date')
    if 'date' in query_parameters:
        datetofilter = date
    else:
        return 'Error:No date provided. Please choose a date.'
    citiesWithCoordinatesByDate = []

    for warnLevelObjects in entiredata:
        warnLevelObjects['Stand'] = warnLevelObjects['Stand'][0:10]
        if warnLevelObjects['Stand'] == datetofilter:
            for region in warnLevelObjects['Warnstufen']:
                if region['Name'] is not None:
                    for entry in df.iterrows():
                        if entry[1]['cityName'] == region['Name']:
                            citiesDict = {}

                            citiesDict['cityName'] = region['Name']
                            citiesDict['Latitude'] = entry[1]['Latitude']
                            citiesDict['Longitude'] = entry[1]['Longitude']
                            citiesDict['Warnstufe'] = region['Warnstufe']
                            citiesDict['MarkerColor'] = getMarkerColor(
                                citiesDict['Warnstufe'])
                            citiesWithCoordinatesByDate.append(citiesDict)
    print(citiesWithCoordinatesByDate)
    responseWarnLevel = jsonify(citiesWithCoordinatesByDate)
    return responseWarnLevel


if __name__ == '__main__':
    app.run(port=8081, debug=True)
