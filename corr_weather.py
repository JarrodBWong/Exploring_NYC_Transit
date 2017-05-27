import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
import scipy.stats
import matplotlib.pyplot as plt


class corr_weather(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.turnstile_weather', 'anuragp1_jl101995.citibike_weather']
    writes = []

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
        	sample_coll_name = coll_name.split('repo.anuragp1_jl101995.')[1] + '_sample'
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

        turnstile_weather_data = get_collection('repo.anuragp1_jl101995.turnstile_weather').find()

        x_precip_subway, x_temp_subway, y_subway_usage = [], [], []
        for d in turnstile_weather_data:
        	x_precip_subway.append(d['Precip'])
        	x_temp_subway.append(d['AvgTemp'])
        	y_subway_usage.append(d['Entries'])

        # get corr(precipitation, subway ridership)
        precip_subway_result = scipy.stats.pearsonr(x_precip_subway, y_subway_usage)

        # # get corr(temperature, suwbay ridership)
        temp_subway_result = scipy.stats.pearsonr(x_temp_subway, y_subway_usage)

        citibike_weather_data = get_collection('repo.anuragp1_jl101995.citibike_weather').find()

        x_precip_citi, x_temp_citi, y_citibike_usage = [], [], []
        for d in citibike_weather_data:
        	x_precip_citi.append(d['Precip'])
        	x_temp_citi.append(d['AvgTemp'])
            y_citibike_usage.append(d['Citibike_Usage'])

        # # get corr(precipitation, citibike ridership
        precip_citi_result = scipy.stats.pearsonr(x_precip_citi, y_citibike_usage)

        # # get corr(temperature, citibike ridership)
        temp_citi_result = scipy.stats.pearsonr(x_temp_citi, y_citibike_usage)

        print('Correlation between precipitation and subway usage is ' + str(precip_subway_result[0]) + ' with a p-value of ' + str(precip_subway_result[1]))
        print('Correlation between temperature and subway usage is ' + str(temp_subway_result[0]) + ' with a p-value of ' + str(temp_subway_result[1]))
        print('Correlation between precipitation and citibike usage is ' + str(precip_citi_result[0]) + ' with a p-value of ' + str(precip_citi_result[1]))
        print('Correlation between temperature and citibike usage is ' + str(temp_citi_result[0]) + ' with a p-value of ' + str(temp_citi_result[1]))

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
 
        this_script = doc.agent('alg:anuragp1_jl101995#corr_weather', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


corr_weather.execute(Trial=False)
doc = corr_weather.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof



