import json
import geocoder
import requests
import csv

response = requests.get("https://corona-ampel.gv.at/sites/corona-ampel.gv.at/files/assets/Warnstufen_Corona_Ampel_aktuell.json") 
entiredata=json.loads(response.text)
# remove null values 
rlist=[]
datetofilter='2020-09-04'

for warnLevelObjects in entiredata:
 warnLevelObjects['Stand']=warnLevelObjects['Stand'][0:10]

filteredData = [d for d in entiredata if d['Stand'] == datetofilter]
#print(filteredData)

csv_columns = ['cityName','Latitude','Longitude']

csv_file = "AustrianCitiesWithCoordinates.csv"

locationLongLatDict ={}
try:
    with open(csv_file, 'w',encoding='utf-8') as csvfile:
        for warnLevelData in entiredata:
          for region in warnLevelData['Warnstufen']:
           if region['Name'] is not None:
               if region['Name'] not in locationLongLatDict.keys():
                print(region['Name'])
                geocoding=geocoder.google(region['Name']+',Austria',key='AIzaSyCHYcm_6OAyehFdx3niFgKWgpSrwPtR8NA')
                for far in geocoding:
                    locationLongLatDict[region['Name']]={'cityName':region['Name'],'Latitude':far.latlng[0],'Longitude':far.latlng[1]}
        
        
        #print(locationLongLatDict)
            

        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in locationLongLatDict.items():
            writer.writerow(data[1])

         
except IOError:
    print("I/O error")
        
