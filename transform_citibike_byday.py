import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code

class transform_citibike_byday(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.citibike']
    writes = ['anuragp1_jl101995.citibike_startstation_byday']

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

            return eval(coll_name)

        print('Loading citibike_data from Mongo')
        citibike_data = repo.anuragp1_jl101995.citibike.find()

        print('Cleaning citibike_data')
        #
        # Citibike date format changed halfway through the records 
        #
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
        counted_cb_df = pd.DataFrame(cb_df.groupby(['date','start_station_name'], as_index=False)['Count'].count())
        counted_cb_df

        print('Inserting into Mongo')
        repo.dropPermanent('citibike_startstation_byday')
        repo.createPermanent('citibike_startstation_byday')

        for index, row in counted_cb_df.iterrows():
            #datestring = str(row[0])[4:6] + '/' + str(row[0])[6:8] + '/' + str(row[0])[0:4]
            insert_this = {'Date': row[0], 'Start_Station' : row[1], 'Count': int(row[2])}
            repo.anuragp1_jl101995.citibike_startstation_byday.insert_one(insert_this)


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
        doc.add_namespace('cny', 'https://data.cityofnewyork.us/resource/') # NYC Open Data
        doc.add_namespace('mta', 'http://web.mta.info/developers/') # MTA Data (turnstile source)

        this_script = doc.agent('alg:anuragp1_jl101995#transform_citibike_byday', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transformation for CitiBike startstation by day Data
        citibike_byday_resource = doc.entity('dat:citibike_byday', {'prov:label':'CitiBike by Day Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_citibike_byday = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        doc.wasAssociatedWith(get_citibike_byday, this_script)
        doc.usage(get_citibike_byday, citibike_byday_resource, startTime, None, {prov.model.PROV_TYPE:'ont:DataSet'} )
        citibike_byday = doc.entity('dat:anuragp1_jl101995#citibike_byday', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(citibike_byday, this_script)
        doc.wasGeneratedBy(citibike_byday, get_citibike_byday, endTime)
        doc.wasDerivedFrom(citibike_byday, citibike_byday_resource, get_citibike_byday, get_citibike_byday, get_citibike_byday)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc

transform_citibike_byday.execute(Trial=False)
doc = transform_citibike_byday.provenance()