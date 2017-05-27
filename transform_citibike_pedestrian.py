import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code

'''
Matches subway_station names to their regions pedestrian counts 
-> Need to match this to the turnstile counts to see subway usage vs pedestrian counts 
Matches citibike usage to the region's pedestrian counts 
'''
class transform_citibike_pedestrian(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.pedestriancounts,' 'anuragp1_jl101995.subway_regions' 'citibike_by_region']
    writes = ['daily_pedestrian' 'subway_pedestriancount' 'citibike_pedestriancount']

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

       
        pc_data = repo.anuragp1_jl101995.pedestriancounts.find()
        yearly_counts = []

        repo.dropPermanent('daily_pedestrian')
        repo.createPermanent('daily_pedestrian')

        for pc in pc_data:
            total = 0
            try:
                total += int(float(pc['sept14_pm']))
            except KeyError:
                pass
            try:
                total += int(float(pc['sept14_am']))
            except KeyError:
                pass
            try:
                total += int(float(pc['may14_pm']))
            except KeyError:
                pass
            try:
                total += int(float(pc['may14_am']))
            except KeyError:
                pass
            
            yearly_counts.append([int(total/2), pc['the_geom'],pc['street']])
            
            insert_daily = {'the_geom': pc['the_geom'], 'street': pc['street'], 'daily_avg' : int(total/2) }
            repo.anuragp1_jl101995.daily_pedestrian.insert_one(insert_daily)
                


        sub_reg_data = repo.anuragp1_jl101995.subway_regions.find()

        repo.dropPermanent('subway_pedestriancount')
        repo.createPermanent('subway_pedestriancount')


        for sr in sub_reg_data:
            index = int(float(sr['Closest_Region']))
            station_daily = 0
            try:
                station_daily = yearly_counts[index-1][0]
            except IndexError:
                print(index)
                
            insert_subway_pc = {'Station_Name' : sr['Station_Name'],
                                'Line':sr['Line'], 'Pedestrian_Avg': station_daily }
            repo.anuragp1_jl101995.subway_pedestriancount.insert_one(insert_subway_pc)


        citi_region_data = repo.anuragp1_jl101995.citibike_by_region.find()

        cr_count = []
        for cr in citi_region_data:
            cr_count.append([cr['Closest_Region'], cr['datetime']])

 
        citibike_pc_df = pd.DataFrame(cr_count, columns = ['Region', 'datetime'])
        temp_df = pd.DataFrame(citibike_pc_df.groupby(by = ['Region']).size())

        citibike_region = []
        for index, row in temp_df.iterrows():
            this_index = int(float(index))
            
            citibike_region.append([yearly_counts[this_index][0], yearly_counts[this_index][2],
                                   row[0]])
            
        w_region_name_df = pd.DataFrame(citibike_region, columns = ['pedestrian_avg', 'street_of_region', 'citibike_total'])
        print('Finished')

        repo.dropPermanent('citibike_pedestriancount')
        repo.createPermanent('citibike_pedestriancount')

        for index, row in w_region_name_df.iterrows():
            insert_citibike_pc = {'ped_avg': row[0], 'street_of_region': row[1],
                                 'citibike_total': row[2]}

            repo.anuragp1_jl101995.citibike_pedestriancount.insert_one(insert_citibike_pc)
            
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
        doc.add_namespace('cny', 'https://data.cityofnewyork.us/resource/') # NYC Open Data

        this_script = doc.agent('alg:anuragp1_jl101995#transform_citibike_pedestrian', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform getting daily pedestrian
        daily_pedestrian_resource = doc.entity('dat:daily_pedestrian',{'prov:label':'Daily Pedestrian Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_daily_pedestrian = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_daily_pedestrian, this_script)
        doc.usage(get_daily_pedestrian, daily_pedestrian_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        daily_pedestrian = doc.entity('dat:anuragp1_jl101995#daily_pedestrian', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(daily_pedestrian, this_script)
        doc.wasGeneratedBy(daily_pedestrian, get_daily_pedestrian, endTime)
        doc.wasDerivedFrom(pedestrian_weather, pedestrian_weather_resource, get_pedestrian_weather, get_pedestrian_weather, get_pedestrian_weather)

        # Transform getting daily pedestrian count with daily citibike usage
        pedestrian_citibike_resource = doc.entity('dat:citibike_pedestriancount',{'prov:label':'Pedestrian Count and CitiBike Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_pedestrian_citibike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_pedestrian_citibike, this_script)
        doc.usage(get_pedestrian_citibike, pedestrian_citibike_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        pedestrian_citibike = doc.entity('dat:anuragp1_jl101995#pedestrian_citibike', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pedestrian_citibike, this_script)
        doc.wasGeneratedBy(pedestrian_citibike, get_pedestrian_citibike, endTime)
        doc.wasDerivedFrom(pedestrian_citibike, pedestrian_citibike_resource, get_pedestrian_citibike, get_pedestrian_citibike, get_pedestrian_citibike)

        # Transform getting daily pedestrian count with daily subway usage
        pedestrian_subway_resource = doc.entity('dat:subway_pedestriancount',{'prov:label':'Pedestrian Count and Subway Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_pedestrian_subway = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_pedestrian_subway, this_script)
        doc.usage(get_pedestrian_subway, pedestrian_subway_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        pedestrian_subway = doc.entity('dat:anuragp1_jl101995#pedestrian_subway', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pedestrian_subway, this_script)
        doc.wasGeneratedBy(pedestrian_subway, get_pedestrian_subway, endTime)
        doc.wasDerivedFrom(pedestrian_subway, pedestrian_subway_resource, get_pedestrian_subway, get_pedestrian_subway, get_pedestrian_subway)


        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


transform_citibike_pedestrian.execute(Trial=False)
doc = transform_citibike_pedestrian.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
