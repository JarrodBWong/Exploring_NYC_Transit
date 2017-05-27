import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code


class transform_turnstile_weather(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.subway_stations,' 'anuragp1_jl101995.turnstile']
    writes = ['anuragp1_jl101995.turnstile_weather', 'anuragp1_jl101995.turnstile_total_byday']

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

        turnstile_data = repo.anuragp1_jl101995.turnstile.find()

        print('Starting to iterate over turnstile_data.') #######

        ts_data = []
        for entry in turnstile_data:
            ts_data.append([entry['STATION'], entry['DATE'], entry['ENTRIES']])


        print('Finished iterating over turnstile_data. About to create dataframe.') ######

        df = pd.DataFrame(ts_data, columns = ['STATION', 'DATE', 'ENTRIES'])

        df_max = df
        df_max = df_max.groupby(by=['STATION', 'DATE'], as_index=False)['ENTRIES'].max()
        
        temp_df1 = df_max
        temp_arr1 = []
        for index, row in temp_df1.iterrows():
            row_1 = row[1]
            row_1 = row_1.replace('/', '')
            row_1 = row_1[-4:] + row_1[:-4]
            temp_arr1.append([row[0],int(row_1),row[2]])
            
        temp_df1 = pd.DataFrame(temp_arr1, columns = ['STATION', 'DATE', 'ENTRIES'])
        temp_df1 = temp_df1.sort(['STATION','DATE'], ascending=[False,True])

        temp_df = temp_df1
        last_station = ''
        last_entry = 0
        temp_arr = []
        last_nonzero = ['place', 0]

        for index, row in temp_df.iterrows():
            
            if row[0] == last_station:
                temp =row[2] 
                row_2 = abs(row[2] - last_entry) 
                
                if row_2 == 0:
                    if last_nonzero[0] == row[0]:
                        row_2 = abs(row[2] - last_nonzero[1])
                else:
                    last_nonzero = [row[0], row[2]] 
     
                # 
                # Incase the data is messy we insert 200,000 as it is the cap we determined 
                #
                if row[0] == 'WINTHROP ST' and row_2 > 5000:
                    temp_arr.append([row[0], row[1], 3000])
                elif row[0] == 'JAMAICA CENTER' and row_2 > 10000:
                    temp_arr.append([row[0], row[1], 7000])
                elif row[0] == 'NEWARK C' and row_2 > 1000:
                    temp_arr.append([row[0], row[1], 500])
                elif row[0] ==  'HOWARD BCH JFK' and row_2 > 1000:
                    temp_arr.append([row[0], row[1], 750])
                elif row[0] ==  '157 ST' and row_2 > 4500:
                    temp_arr.append([row[0], row[1], 3500])
                elif row[0] ==  '163 ST-AMSTERDM' and row_2 >  3000:
                    temp_arr.append([row[0], row[1], 2200])
                elif row[0] ==  'TWENTY THIRD ST' and row_2 >  5000:
                    temp_arr.append([row[0], row[1], 3800])



                if row_2 < 10000 and row_2 != 0:            
                    temp_arr.append([row[0], row[1], row_2])

                else: #row_2 > 20000 and row_2 != 0:            
                    temp_arr.append([row[0], row[1], 2000])

                last_entry = temp  
            
            else:
                temp = row[2]
                row_2 = 0
                last_entry = temp  

                temp_arr.append([row[0], row[1], 0])   
                
            last_station = row[0]
            
        temp_df = pd.DataFrame(temp_arr, columns = ['STATION', 'DATE', 'ENTRIES'])


        # Save in database incase we want the daily usage of each turnstile in the future
        print('Storing turnstile totals by day')
        repo.dropPermanent('turnstile_total_byday')
        repo.createPermanent('turnstile_total_byday')

        for entry in temp_arr:
            insert_ts_byday = {'STATION': entry[0], 'DATE': entry[1], 'ENTRIES' : entry[2]}
            repo.anuragp1_jl101995.turnstile_total_byday.insert_one(insert_ts_byday)

        temp_df = pd.DataFrame(temp_df.groupby(by=['DATE'])['ENTRIES'].sum())

        # print('Loading weatherdata')
        # weatherdata = repo.anuragp1_jl101995.weather.find()
        # date_weather = []

        # # Collection for associating daily weather with each turnstile entry
        # repo.dropPermanent('turnstile_weather')
        # repo.createPermanent('turnstile_weather')

        # print('Combining weatherdata and daily turnstile totals') ######

        # for w in weatherdata:
        #     for index, row in temp_df.iterrows():
        #     #print('w is ' + str(int(w['DATE'])) + ' and row[1] is ' + str(row[1]))
        #         if int(w['DATE']) == index:
        #         #print('MATCH: w is ' + str(w) + ' and row[1] is ' + str(row[1]))
        #             datestring = str(w['DATE'])[4:6] + '/' + str(w['DATE'])[6:8] + '/' + str(w['DATE'])[0:4]
        #             avgtemp = statistics.mean([w['TMAX'], w['TMIN']])
        #             insert_weather = {'Date': datestring, 'AvgTemp': avgtemp, 'Precip': w['PRCP'], 'Entries' : int(row[0])}
        #             repo.anuragp1_jl101995.turnstile_weather.insert_one(insert_weather)

        #             break
        # print('Finished combining weather and turnstiles')


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
        doc.add_namespace('mta', 'http://web.mta.info/developers/') # MTA Data (turnstile source)

        this_script = doc.agent('alg:anuragp1_jl101995#transform_turnstile_weather', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform associating weather with turnstile
        turnstile_weather_resource = doc.entity('dat:turnstile_weather',{'prov:label':'Turnstile Weather Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_turnstile_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_turnstile_weather, this_script)
        doc.usage(get_turnstile_weather, turnstile_weather_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        turnstile_weather = doc.entity('dat:anuragp1_jl101995#turnstile_weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile_weather, this_script)
        doc.wasGeneratedBy(turnstile_weather, get_turnstile_weather, endTime)
        doc.wasDerivedFrom(turnstile_weather, turnstile_weather_resource, get_turnstile_weather, get_turnstile_weather, get_turnstile_weather)

        # Transform getting daily weather with daily entries total 
        turnstile_daily_resource = doc.entity('dat:',{'prov:label':'Turnstile Total by Day', prov.model.PROV_TYPE:'ont:DataSet'})
        get_turnstile_daily = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_turnstile_daily, this_script)
        doc.usage(get_turnstile_daily, turnstile_daily_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        turnstile_daily = doc.entity('dat:anuragp1_jl101995#turnstile_daily', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile_daily, this_script)
        doc.wasGeneratedBy(turnstile_daily, get_turnstile_daily, endTime)
        doc.wasDerivedFrom(turnstile_daily, turnstile_daily_resource, get_turnstile_daily, get_turnstile_daily, get_turnstile_daily)        

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


transform_turnstile_weather.execute(Trial=False)
doc = transform_turnstile_weather.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
