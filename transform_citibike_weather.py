import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code


class transform_citibike_weather(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.citibike,' 'anuragp1_jl101995.weather']
    writes = ['anuragp1_jl101995.citibike_weather']

    @staticmethod
    def execute(Trial=False):
        '''Retrieve some datasets'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        # When Trial is True, perform function on random sample of size SIZE)
        SIZE = 100

        def get_collection(coll_name):
            # Collection for station sample
            sample_coll_name = coll_name.split(
                'repo.anuragp1_jl101995.')[1] + '_sample'
            repo.dropPermanent(sample_coll_name)
            repo.createPermanent(sample_coll_name)

            if Trial == True:
                sample = eval(coll_name).aggregate(
                    [{'$sample': {'size': SIZE}}])
                for s in sample:
                    eval('repo.anuragp1_jl101995.%s' %
                         (sample_coll_name)).insert_one(s)
                return eval('repo.anuragp1_jl101995.%s' % (sample_coll_name))
            else:
                return eval(coll_name)

        print('Loading citibike_data from Mongo')
        citibike_data = repo.anuragp1_jl101995.citibike.find()

        print('Cleaning citibike_data')
        cb_data = []
        for entry in citibike_data:
            date = entry['starttime']
            date = date[:date.index(' ')]
            if '-' in date:
                date = date.split('-')
            elif '/' in date:
                date = date.split('/') 
            # 5/31/2015 12:12:12
            # date = ['5', '31, '2015']
            # [2015 - 4 - 10]
            
            if len(date[0]) < 2:
                date[0] = '0' + date[0]
            if len(date[1]) < 2:
                date[1] = '0' + date[1]
            if len(date[2]) < 2:
                date[2] = '0' + date[2]
                
            if date[0] in ['2014', '2015', '2016']:
                new_date = date[0] + date[1] + date[2]
                cb_data.append([new_date, entry['start station name']])
                
            elif date[2] in ['2014', '2015', '2016']:
                new_date = date[2] + date[0] + date[1]
                cb_data.append([new_date, entry['start station name']])

        print('Creating pandas dataframe from cleaned citibike_data')
        cb_df = pd.DataFrame(cb_data, columns = ['date', 'start_station_name'])


        print('Getting citibike totals for each day')
        cb_df['Count'] = cb_df['start_station_name']
        counted_cb_df = pd.DataFrame(cb_df.groupby(['date'], as_index=False)['Count'].count())

        weatherdata = repo.anuragp1_jl101995.weather.find()
        date_weather = []

        # Collection for associating daily weather with each turnstile entry
        repo.dropPermanent('citibike_weather')
        repo.createPermanent('citibike_weather')

        print('Comparing weatherdata and counted_cb_df') ######

        for w in weatherdata:

            for index, row in counted_cb_df.iterrows():

                if int(float(w['DATE'])) == int(row[0]):

                    datestring = str(w['DATE'])[4:6] + '/' + str(w['DATE'])[6:8] + '/' + str(w['DATE'])[0:4]
                    avgtemp = statistics.mean([w['TMAX'], w['TMIN']])
                    insert_weather = {'Date': datestring, 'AvgTemp': avgtemp, 'Precip': w['PRCP'], 'Citibike_Usage' : int(row[1])}
                    repo.anuragp1_jl101995.citibike_weather.insert_one(insert_weather)

                    break

        print('Finished combining weatherdata and citibike_data')
        # end database connection
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
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
        doc.add_namespace('mch', 'http://datamechanics.io/data/anuragp1_jl101995/')  # Data Mechanics S3 bucket (weather file source)
        doc.add_namespace('cbd', 'https://www.citibikenyc.com/system-data/') # CitiBike System Data 

        this_script = doc.agent('alg:anuragp1_jl101995#transform_citibike_weather', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        citibike_weather_resource = doc.entity('dat:citibike_weather', {'prov:label':'CitiBike and Weather Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_citibike_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_citibike_weather, this_script)
        doc.usage(get_citibike_weather, citibike_weather_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        citibike_weather = doc.entity('dat:anuragp1_jl101995#citibike_weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(citibike_weather, this_script)
        doc.wasGeneratedBy(citibike_weather, get_citibike_weather, endTime)
        doc.wasDerivedFrom(citibike_weather, turnstile_citibike_resource, get_citibike_weather, get_citibike_weather, get_citibike_weather)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


transform_citibike_weather.execute(Trial=False)
doc = transform_citibike_weather.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
