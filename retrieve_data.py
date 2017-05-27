import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import os
import zipfile
import csv

class retrieve_data(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = []
    writes = ['anuragp1_jl101995.subway_stations', \
              'anuragp1_jl101995.pedestriancounts', \
              'anuragp1_jl101995.weather,' \
              'anuragp1_jl101995.turnstile' \
              'anuragp1_jl101995.citbike'
              'anuragp1_jl101995.geocoded_turnstile']

    @staticmethod
    def execute(Trial = False):
        '''Retrieve some datasets'''

        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')
        
        # Retrieve NYC Subway Station data (Updated Source)
        # url = "http://datamechanics.io/data/anuragp1_jl101995/subway_stations.json"
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # print(json.dumps(r, sort_keys=True, indent=2))
        # repo.dropPermanent("subway_stations")
        # repo.createPermanent("subway_stations")
        # repo['anuragp1_jl101995.subway_stations'].insert_many(r)

        # Retrieve Bi-Annual Pedestrian Counts data  
        # url = "https://data.cityofnewyork.us/resource/cqsj-cfgu.json"
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)
        # print(json.dumps(r, sort_keys=True, indent=2))
        # repo.dropPermanent("pedestriancounts")
        # repo.createPermanent("pedestriancounts")
        # repo['anuragp1_jl101995.pedestriancounts'].insert_many(r)

        # Load NYC Weather Data (source: http://www.ncdc.noaa.gov/cdo-web/)
        # repo.dropPermanent("weather")
        # repo.createPermanent("weather")

        # url = "http://datamechanics.io/data/anuragp1_jl101995/weather.csv"
        # urllib.request.urlretrieve(url, 'weather.csv')
        # weather_df = pd.DataFrame.from_csv('weather.csv')
        # repo['anuragp1_jl101995.weather'].insert_many(weather_df.to_dict('records'))
        # os.remove('weather.csv')

        # Retrieve Subway Turnstile Data from 2015 to mid-2016
        # repo.dropPermanent("turnstile")
        # repo.createPermanent("turnstile")

        # base_url = 'http://web.mta.info/developers/data/nyct/turnstile/'
        # all_extensions = ["turnstile_161001", "turnstile_160924", "turnstile_160917" , "turnstile_160910", "turnstile_160903", "turnstile_160827", \
        #  "turnstile_160820", "turnstile_160813", "turnstile_160806", "turnstile_160730", "turnstile_160723", "turnstile_160716", "turnstile_160709", \
        #  "turnstile_160702", "turnstile_160625", "turnstile_160618", "turnstile_160611", "turnstile_160604", "turnstile_160528", "turnstile_160521", \
        #  "turnstile_160514", "turnstile_160507", "turnstile_160430", "turnstile_160423", "turnstile_160416", "turnstile_160409", "turnstile_160402", \
        #  "turnstile_160326", "turnstile_160319", "turnstile_160312", "turnstile_160305", "turnstile_160227", "turnstile_160220", "turnstile_160213", \
        #  "turnstile_160206", "turnstile_160130", "turnstile_160123", "turnstile_160116", "turnstile_160109", "turnstile_160102", "turnstile_151226", \
        #  "turnstile_151219", "turnstile_151212", "turnstile_151205", "turnstile_151128", "turnstile_151121", "turnstile_151114", "turnstile_151107", \
        #  "turnstile_151031", "turnstile_151024", "turnstile_151017", "turnstile_151010", "turnstile_151003", "turnstile_150926", "turnstile_150919", \
        #  "turnstile_150912", "turnstile_150905", "turnstile_150829", "turnstile_150822", "turnstile_150815", "turnstile_150808", "turnstile_150801", \
        #  "turnstile_150725", "turnstile_150718", "turnstile_150711", "turnstile_150704", "turnstile_150627", "turnstile_150620", "turnstile_150613", \
        #  "turnstile_150606", "turnstile_150530", "turnstile_150523", "turnstile_150516", "turnstile_150509", "turnstile_150502", "turnstile_150425", \
        #  "turnstile_150418", "turnstile_150411", "turnstile_150404", "turnstile_150328", "turnstile_150321", "turnstile_150314", "turnstile_150307", \
        #  "turnstile_150228", "turnstile_150221", "turnstile_150214", "turnstile_150207", "turnstile_150131", "turnstile_150124", "turnstile_150117", \
        #   "turnstile_150110", "turnstile_150103"]

        # for x in range(0, len(all_extensions)):
        #     extension = all_extensions[x]
        #     urllib.request.urlretrieve(base_url+extension+".txt", extension + ".csv")

        #     csvfile = open(extension + ".csv", 'r')

        #     turnstile_df = pd.DataFrame.from_csv(csvfile) 
        #     repo['anuragp1_jl101995.turnstile'].insert_many(turnstile_df.to_dict('records'))
            
        #     os.remove(extension + ".csv")

        # Retrieve CitiBike Trip Histories
        # def listUrls(file):
        #     with open(file) as f:
        #         return [url.split('\n')[0] for url in f]

        # repo.dropPermanent('citibike')
        # repo.createPermanent('citibike')

        # # iterate through list of citibike trip data .zip urls
        # for url in listUrls('citizip_urls.txt'):    

        #     urllib.request.urlretrieve(url, 'tripdata.zip')

        #     with zipfile.ZipFile('tripdata.zip', 'r') as zip_ref:
        #         for fn in zip_ref.namelist():
        #             zip_ref.extractall()
        #             csv = pd.read_csv(fn)
        #             tripdata_df = pd.DataFrame(csv)
        #             repo['anuragp1_jl101995.citibike'].insert_many(tripdata_df.to_dict('records'))
        #             os.remove(fn)

        #     os.remove('tripdata.zip')
      
        # Retrieve Geocoded Turnstile Data
        repo.dropPermanent("geocoded_turnstile")
        repo.createPermanent("geocoded_turnstile")

        url = "http://datamechanics.io/data/anuragp1_jl101995/turnstiles_geocoded.csv"
        urllib.request.urlretrieve(url, 'turnstiles_geocoded.csv')
        geoturnstile_df = pd.read_csv('turnstiles_geocoded.csv', header=None, names=['UNIT1', 'UNIT2', 'STATION', 'LINENAME', 'DIVISION', 'LAT', 'LONG'])
        #geoturnstile_df = pd.DataFrame.from_csv('turnstiles_geocoded.csv', index_col=['UNIT1', 'UNIT2', 'STATION', 'LINENAME', 'DIVISION', 'LAT', 'LONG'])
        repo['anuragp1_jl101995.geocoded_turnstile'].insert_many(geoturnstile_df.to_dict('records'))
        os.remove('turnstiles_geocoded.csv')

        # end database connection
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cny', 'https://data.cityofnewyork.us/resource/') # NYC Open Data 
        doc.add_namespace('mch', 'http://datamechanics.io/data/anuragp1_jl101995/')  # Data Mechanics S3 bucket
        doc.add_namespace('mta', 'http://web.mta.info/developers/') # MTA Data (turnstile source) 
        doc.add_namespace('cbd', 'https://www.citibikenyc.com/system-data/') # CitiBike System Data

        this_script = doc.agent('alg:anuragp1_jl101995#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        stations_resource = doc.entity('cny:kk4q-3rt2', {'prov:label':'NYC Subway Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        pedestrian_resource = doc.entity('cny:cqsj-cfgu', {'prov:label':'Bi-Annual Pedestrian Counts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        weather_resource = doc.entity('mch:weather', {'prov:label':'NYC Weather Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        turnstile_resource = doc.entity('mta:turnstile', {'prov:label':'Turnstile Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        citibike_resource = doc.entity('cbd:tripdata', {'prov:label':'CitiBike Trip Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        geoturnstile_resource = doc.entity('mch:geoturnstile', {'prov:label':'Geocoded Turnstile Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})

        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_pedestrian = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_turnstile = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_citibike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_geoturnstile = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
   
        doc.wasAssociatedWith(get_stations, this_script)
        doc.wasAssociatedWith(get_pedestrian, this_script)
        doc.wasAssociatedWith(get_weather, this_script)
        doc.wasAssociatedWith(get_turnstile, this_script)
        doc.wasAssociatedWith(get_citibike, this_script)
        doc.wasAssociatedWith(get_geoturnstile, this_script)

        doc.usage(get_stations, stations_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
        doc.usage(get_pedestrian, pedestrian_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
        doc.usage(get_weather, weather_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
        doc.usage(get_turnstile, turnstile_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
        doc.usage(get_citibike, citibike_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
        doc.usage(get_geoturnstile, geoturnstile_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
    
        stations = doc.entity('dat:anuragp1_jl101995#subway_stations', {prov.model.PROV_LABEL:'NYC Subway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, stations_resource, get_stations, get_stations, get_stations)

        pedestrian = doc.entity('dat:anuragp1_jl101995#pedestriancounts', {prov.model.PROV_LABEL:'NYC Bi-Annual Pedestrian Counts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pedestrian, this_script)
        doc.wasGeneratedBy(pedestrian, get_pedestrian, endTime)
        doc.wasDerivedFrom(pedestrian, pedestrian_resource, get_pedestrian, get_pedestrian, get_pedestrian)

        weather = doc.entity('dat:anuragp1_jl101995#weather', {prov.model.PROV_LABEL:'NYC Weather Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(weather, this_script)
        doc.wasGeneratedBy(weather, get_weather, endTime)
        doc.wasDerivedFrom(weather, weather_resource, get_weather, get_weather, get_weather)
 
        turnstile = doc.entity('dat:anuragp1_jl101995#turnstile', {prov.model.PROV_LABEL:'Turnstile Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile, this_script)
        doc.wasGeneratedBy(turnstile, get_turnstile, endTime)
        doc.wasDerivedFrom(turnstile, turnstile_resource, get_turnstile, get_turnstile, get_turnstile)

        citibike = doc.entity('dat:anuragp1_jl101995#citibike', {prov.model.PROV_LABEL:'CitiBike Trip Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(citibike, this_script)
        doc.wasGeneratedBy(citibike, get_citibike, endTime)
        doc.wasDerivedFrom(citibike, citibike_resource, get_citibike, get_citibike, get_citibike)
        
        geoturnstile = doc.entity('dat:anuragp1_jl101995#geocoded_turnstile', {prov.model.PROV_LABEL:'Geocoded Turnstile Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(geoturnstile, this_script)
        doc.wasGeneratedBy(geoturnstile, get_geoturnstile, endTime)
        doc.wasDerivedFrom(geoturnstile, geoturnstile_resource, get_geoturnstile, get_geoturnstile, get_geoturnstile)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        
        return doc


retrieve_data.execute()
doc = retrieve_data.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof