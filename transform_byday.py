import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt
import pylab
import seaborn as sns

class transform_byday(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.citibike_startstation_byday', 'anuragp1_jl101995.turnstile_total_byday']
    writes = ['anuragp1_jl101995.timeseries_csv']

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

        def scaleCiti(OldValue):
            OldMax = 400
            OldMin = 0
            NewMax = 2000
            NewMin = 0
            OldRange = (OldMax - OldMin)  
            NewRange = (NewMax - NewMin)  
            NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
            return NewValue

        print('Loading citibike_startstation_byday from Mongo')
        citibike_data = repo.anuragp1_jl101995.citibike_startstation_byday.find()
        # "Count" : 60, "Start_Station" : "1 Ave & E 15 St", "Date" : "01/01/2014"


        print('Loading turnstile_total_byday from Mongo')
        turnstile_data = repo.anuragp1_jl101995.turnstile_total_byday.find()
        # "ENTRIES" : 2, "DATE" : 20150423, "STATION" : "ZEREGA AVE"


        data = []
        citibike_entries, turnstile_entries = [], []

        for entry in citibike_data:
            if int(entry['Date']) <= 20150531 and int(entry['Date']) >= 20141228:
                stationCode = entry['Start_Station'].replace(' ', '').replace('&', '').replace('-', '') # join and remove weird characters
                data.append((str(entry['Date']), entry['Start_Station'], stationCode, int(scaleCiti(entry['Count'])), 'C'))
                citibike_entries.append(int(entry['Count']))
                
        for entry in turnstile_data:
            if int(entry['DATE']) <= 20150531 and int(entry['DATE']) >= 20141228:
                stationCode = entry['STATION'].replace(' ', '').replace('&', '').replace('-', '') # join and remove weird characters
                data.append((str(entry['DATE']), entry['STATION'], stationCode, int(entry['ENTRIES']), 'S'))
                turnstile_entries.append(int(entry['ENTRIES']))
                
        df = pd.DataFrame(data, columns=['Date', 'Station', 'StationCode', 'Count', 'Type'])


        print('Average subway turnstile entries is ' + str(np.mean(turnstile_entries)))
        print('Average citibike station entries is ' + str(np.mean(citibike_entries)))

        stations = list(set(df['Station']))

        station_groups = df.groupby('Station')
        rows = []

        print('iterating dataframe to set formatting')
        for g, data in station_groups:
            station_row = {}
            
            #print(list(data['Count']))
            station_row['Station'] = g
            
            station_row['StationCode'] = g.replace(' ', '').replace('&', '').replace('-', '') # join and remove weird characters

            temp = data['Date']
            for i in range(len(temp)):
                #print('appending ' + str(data['Date'].iloc[i]), str(data['Count'].iloc[i]))
            
                station_row[data['Date'].iloc[i]] = data['Count'].iloc[i]
                
            rows.append(station_row)

        dates = [k for k in list(rows[0].keys()) if k not in ['Station', 'StationCode']]
        dates = np.array(dates)
        dates = list(np.sort(dates))

        cols = ['Station', 'StationCode'] + dates
        timeseries_df = pd.DataFrame(rows, columns=cols)

        cats = pd.DataFrame(df, columns=['StationCode', 'Type'])

        print('Removing any null values from timeseries dataframe')
        timeseries_df_cleaned = timeseries_df.dropna(how='any', thresh=len(timeseries_df.columns)/2, subset=dates)
        timeseries_df_cleaned[dates] = timeseries_df_cleaned[dates].astype(float)
        timeseries_df_cleaned[dates] = timeseries_df_cleaned[dates].filter(regex='^20').interpolate(axis=1)
        timeseries_df_cleaned[dates] = timeseries_df_cleaned[dates].apply(pd.to_numeric)

        # export to csv
        print('Creating csv files for d3 visualization')
        # timeseries_df_cleaned.to_csv('station_usage.csv', index=False)
        # cats.to_csv('station_types.csv', index=False)
        timeseries_df_cleaned.to_csv('visualizations/usage_vis/station_usage_cleaned.csv', index=False)
        cats.to_csv('visualizations/usage_vis/station_types_cleaned.csv', index=False)

        print('Merging dataframes to create final dataframe')
        final_df = pd.merge(timeseries_df_cleaned, cats, how='left', on=['StationCode'])
        final_df = final_df.drop('Station', 1)
        final_df = final_df.drop_duplicates()
        final_df = final_df.groupby(['Type'], as_index = False).sum()
        final_df = final_df.set_index('Type')
        final_df = final_df.T

        print('Adding final dataframe to Mongo')
        repo.dropPermanent('timeseries_csv')
        repo.createPermanent('timeseries_csv')
        timeseries_records = json.loads(final_df.to_json()).values()
        repo.anuragp1_jl101995.timeseries_csv.insert(timeseries_records)

        print('Running linear regression comparing subway and citibike usage')
        x = final_df['C']
        y = final_df['S']
        slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(x, y)
        r_squared = r_value**2
        print('~~~~~~~~~~~~~~~~~~ Linear Regression Values ~~~~~~~~~~~~~~~~~~')
        print('\n\n\n')
f
        print('slope is ' + str(slope))
        print('intercept is ' + str(intercept))
        print('r-squared is ' + str(r_squared))

        result = scipy.stats.pearsonr(x, y)
        print('~~~~~~~~~~~~~~~~~~ Correlation Coefficient and p-value  ~~~~~~~~~~~~~~~~~~')
        print('\n')

        print('Correlation between subway and citibike usage is ' + str(result[0]) + ' with a p-value of ' + str(result[1]))
        print('\n\n\n')

        sns.set_style("ticks")

        sns.lmplot(x='C', y='S', 
                   data=final_df, 
                   fit_reg=False,
                   scatter_kws={"marker": "D", 'color':'#673AB7','alpha':.5,'s':100},
                   size=6, aspect=2)
                    
        plt.title('CitiBike & Subway Usage by Day')  
        plt.xlabel('CitiBike Entries')
        plt.ylabel('Subway Entries')
        plt.savefig('visualizations/usage_vis/scatter_subwayciti.png')

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

        this_script = doc.agent('alg:anuragp1_jl101995#transform_byday', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform creating timeseries_csv collection
        timeseries_csv_resource = doc.entity('dat:timeseries_csv',{'prov:label':'Time Series CSV Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_timeseries_csv = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_timeseries_csv, this_script)
        doc.usage(get_timeseries_csv, timeseries_csv_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'} )
        timeseries_csv = doc.entity('dat:anuragp1_jl101995#timeseries_csv', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(timeseries_csv, this_script)
        doc.wasGeneratedBy(timeseries_csv, get_timeseries_csv, endTime)
        doc.wasDerivedFrom(timeseries_csv, timeseries_csv_resource, get_timeseries_csv, get_timeseries_csv, get_timeseries_csv)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


transform_byday.execute(Trial=False)
doc = transform_byday.provenance()

