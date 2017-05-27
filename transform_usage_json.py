import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code

class transform_usage_json(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.pedestriancounts','anuragp1_jl101995.daily_pedestrian', 'anuragp1_jl101995.citibike', 
             'anuragp1_jl101995.turnstile_total_byday', 'anuragp1_jl101995.subway_pedestriancount', 'anuragp1_jl101995.subway_stations']
    writes = ['anuragp1_jl101995.citi_coord_json','anuragp1_jl101995.ped_coord_json', 'anuragp1_jl101995.subway_coord_json']

    @staticmethod
    def execute(Trial = False):
        '''Retrieve some datasets'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        print('Loading pedestriancounts from Mongo')
        ped_data = repo.anuragp1_jl101995.pedestriancounts.find()

        data =[]
        for entry in ped_data:
            data.append((entry['the_geom']['coordinates'][0],entry['the_geom']['coordinates'][1] , entry['street']))

        ped_df = pd.DataFrame(data, columns = ['lat', 'lng', 'street'])
        ped_df = ped_df.drop_duplicates('street')

        print('Loading daily_pedestrian from Mongo')
        daily_ped_data = repo.anuragp1_jl101995.daily_pedestrian.find()
        data =[]
        for entry in daily_ped_data:
            data.append((entry['daily_avg'] , entry['street']))
        daily_ped_df = pd.DataFrame(data, columns = ['daily_avg', 'street'])
        daily_ped_df = daily_ped_df.drop_duplicates('street')

        print('Combining pedestrian dataframes ')
        final_ped_df = pd.merge(ped_df, daily_ped_df, how='left', on=['street'])
        final_ped_df = final_ped_df.set_index('street')
        final_ped_df = final_ped_df.T

        print('Creating initial json for pedestrian coordinates and traffic')
        final_ped_df.to_json('visualizations/map_vis/ped_coord_init.json')

        repo.dropPermanent('ped_coord_json')
        repo.createPermanent('ped_coord_json')
        ped_records = json.loads(final_ped_df.to_json()).values()
        repo.anuragp1_jl101995.ped_coord_json.insert(ped_records)

        print('Loading citibike from Mongo')
        citi_data = repo.anuragp1_jl101995.citibike.find()
        data =[]
        for entry in citi_data:
            data.append((entry['start station longitude'], entry['start station latitude'] , entry['start station name']))
            
        citi_df = pd.DataFrame(data, columns = ['lng', 'lat', 'street'])
        count_citi_df = pd.DataFrame(data, columns = ['count', 'lat', 'street'])

        print('Creating citibike dataframe for counts')
        count_citi_df = pd.DataFrame(data, columns = ['count', 'lat', 'street'])
        temp_count_citi_df = count_citi_df
        count_citi_df = temp_count_citi_df.groupby('street', as_index = False ).count()
        temp_df = count_citi_df
        count_citi_df.columns = ['street', 'count', 'lat']

        print('Creating citibike dataframe for geolocation')
        temp_df = temp_df.rename(index=str, columns ={'street':'street', 'lat':'count', 'lng':'lng'})
        temp_df = temp_df.drop('lng', 1)
        citi_df = citi_df.drop_duplicates('street')

        print('Combining citibike dataframes ')
        final_citi_df = pd.merge(citi_df, temp_df, how='left', on=['street'])
        final_citi_df = final_citi_df.drop_duplicates('street')
        final_citi_df = final_citi_df.set_index('street')

        temp_final_citi_df = final_citi_df
        temp_final_citi_df.columns = ['lng','lat','count','drop']
        temp_final_citi_df.drop('drop', 1)

        final_citi_df = temp_final_citi_df
        final_citi_df= final_citi_df.T

        print('Creating initial json for citibike coordinates and usage')
        final_citi_df.to_json('visualizations/map_vis/citi_coord_init.json')

        repo.dropPermanent('citi_coord_json')
        repo.createPermanent('citi_coord_json')
        citi_records = json.loads(final_citi_df.to_json()).values()
        repo.anuragp1_jl101995.citi_coord_json.insert(citi_records)

        print('Matching subways to turnstiles based on matched data')
        manual_matches = {'103rd St - Corona Plaza': '103 ST-CORONA', '116th St - Columbia University': '116 ST-COLUMBIA', '137th St - City College': '137 ST CITY COL',  '138th St - Grand Concourse': '138/GRAND CONC', '149th St - Grand Concourse': '149/GRAND CONC', '15th St - Prospect Park': '15 ST-PROSPECT',  '161st St - Yankee Stadium': '161/YANKEE STAD', '163rd St - Amsterdam Av': '163 ST-AMSTERDM', '182nd-183rd Sts': '182-183 STS',  '21st St - Queensbridge': '21 ST-QNSBRIDGE', '34th St - Penn Station': '34 ST-PENN STA', '3rd Ave - 138th St': '3 AV 138 ST',  '3rd Ave - 149th St': '3 AV-149 ST', '40th St': '40 ST LOWERY ST', '42nd St - Bryant Pk': '42 ST-BRYANT PK', '42nd St - Port Authority Bus Term': '42 ST-PORT AUTH',  '47th-50th Sts - Rockefeller Ctr': '47-50 STS ROCK', '4th Ave': '4 AV-9 ST', '52nd St': '52 ST', '59th St - Columbus Circle': '59 ST COLUMBUS',  '5th Ave - 53rd St': '5 AV/53 ST', '5th Ave - 59th St': '5 AV/59 ST', '5th Ave - Bryant Pk': '5 AVE', '63rd Dr - Rego Park': '63 DR-REGO PARK',  '66th St - Lincoln Ctr': '66 ST-LINCOLN', '68th St - Hunter College': '68ST-HUNTER CO', '72nd St': '72 ST', '74th St - Broadway': '74 ST-BROADWAY',  '75th St - Eldert Ln': '75 ST-ELDERTS', '81st St': '81 ST-MUSEUM', '82nd St - Jackson Hts': '82 ST-JACKSON H', '8th St - NYU': '8 ST-NYU',  '90th St - Elmhurst Av': '90 ST-ELMHURST', '9th St': '4 AV-9 ST', 'Aqueduct - North Conduit Av': 'AQUEDUCT N.COND', 'Astoria - Ditmars Blvd': 'ASTORIA DITMARS',  'Atlantic Av - Pacific St': 'ATL AV-BARCLAY', 'Ave H': 'AVENUE H', 'Ave I': 'AVENUE I', 'Ave J': 'AVENUE J', 'Ave M': 'AVENUE M', 'Ave N': 'AVENUE N',  'Ave P': 'AVENUE P', 'Ave U': 'AVENUE U', 'Ave X': 'AVENUE X', 'Bay Pky': 'BAY PKWY', 'Bay Ridge - 95th St': 'BAY RIDGE-95 ST', 'Bedford - Nostrand Aves': 'BEDFORD-NOSTRAN',  'Bedford Park Blvd': 'BEDFORD PK BLVD', 'Bleecker St (Downtown)': 'BLEECKER ST', 'Briarwood - Van Wyck Blvd': 'BRIARWOOD', 'Broadway - Lafayette St': "B'WAY-LAFAYETTE",  'Broadway - Nassau St': 'NASSAU ST', 'Broadway Junction': 'BROADWAY JCT', 'Brooklyn Bridge - City Hall': 'BROOKLYN BRIDGE', 'Brooklyn College - Flatbush Ave': 'FLATBUSH AV-B.C',  'Bushwick - Aberdeen': 'BUSHWICK AV', 'Canarsie - Rockaway Pkwy': 'CANARSIE-ROCKAW', 'Cathedral Pkwy (110th St)': 'CATHEDRAL PKWY', 'Central Park North (110th St)': 'CENTRAL PK N110',  'Christopher St - Sheridan Sq': 'CHRISTOPHER ST', 'Clinton - Washington Aves': 'CLINTON-WASH AV', 'Coney Island - Stillwell Av': 'CONEY IS-STILLW',  'Cortlandt St (NB only)': 'CORTLANDT ST', 'Cortlandt St (Temporarily Closed)': 'CORTLANDT ST', 'Crown Hts - Utica Ave': 'CROWN HTS-UTICA', 'Delancey St': 'DELANCEY/ESSEX',  'E 105th St': 'EAST 105 ST', "E 143rd St - St Mary's St": "E 143/ST MARY'S", 'Eastchester - Dyre Ave': 'EASTCHSTER/DYRE', 'Eastern Pkwy - Bklyn Museum': 'EASTN PKWY-MUSM',  'Essex St': 'DELANCEY/ESSEX', 'Far Rockaway - Mott Ave': 'FAR ROCKAWAY', 'Flushing - Main St': 'FLUSHING-MAIN', 'Forest Hills - 71st Av': 'FOREST HILLS 71',  'Ft Hamilton Pkwy': 'FT HAMILTON PKY', 'Grand Army Plaza': 'GRAND ARMY PLAZ', 'Grand Ave - Newtown': 'GRAND-NEWTOWN', 'Grand Central - 42nd St': 'GRD CNTRL-42 ST',  'Harlem - 148 St': 'HARLEM 148 ST', 'Herald Sq - 34th St': '34 ST-HERALD SQ', 'Howard Beach - JFK Airport': 'HOWARD BCH JFK', 'Hoyt - Schermerhorn Sts': 'HOYT-SCHER',  'Hunters Point Ave': 'HUNTERS PT AV', 'Inwood - 207th St': 'INWOOD-207 ST', 'Jackson Hts - Roosevelt Av': '82 ST-JACKSON H', 'Jamaica - 179th St': 'JAMAICA 179 ST',  'Jamaica - Van Wyck': 'JAMAICA VAN WK', 'Jamaica Ctr - Parsons / Archer': 'JAMAICA CENTER', 'Jay St - Borough Hall': 'JAY ST-METROTEC', 'Kew Gardens - Union Tpke': 'KEW GARDENS',  'Kingston - Throop Aves': 'KINGSTON-THROOP', 'Knickerbocker Ave': 'KNICKERBOCKER', 'Lexington Ave - 53rd St': 'LEXINGTON AV/53', 'Lexington Ave - 63rd St': 'LEXINGTON AV/63',  'Long Island City - Court Sq': 'COURT SQ', 'Lower East Side - 2nd Ave': '2 AV', 'Marble Hill - 225th St': 'MARBLE HILL-225', 'Mets - Willets Point': 'METS-WILLETS PT',  'Myrtle-Willoughby Aves': 'MYRTLE-WILLOUGH', 'Nereid Ave (238 St)': 'NEREID AV', 'Norwood - 205th St': 'NORWOOD 205 ST', 'Ozone Park - Lefferts Blvd': 'OZONE PK LEFFRT',  'Park Pl': 'PARK PLACE', 'Queens Plz': 'QUEENS PLAZA', 'Rockaway Park - Beach 116 St': 'ROCKAWAY PARK B', 'Roosevelt Island - Main St': 'ROOSEVELT ISLND',  'Smith - 9th Sts': 'SMITH-9 ST', 'Sutphin Blvd - Archer Av': 'SUTPHIN-ARCHER', 'Sutter Ave - Rutland Road': 'SUTTER AV-RUTLD', 'Times Sq - 42nd St': 'TIMES SQ-42 ST',  'Union Sq - 14th St': '14 ST-UNION SQ', 'Van Cortlandt Park - 242nd St': 'V.CORTLANDT PK', 'Vernon Blvd - Jackson Ave': 'VERNON-JACKSON', 'W 4th St - Washington Sq (Upper)': 'W 4 ST-WASH SQ',  'W 8th St - NY Aquarium': 'W 8 ST-AQUARIUM', 'Wakefield - 241st St': 'WAKEFIELD/241', 'West Farms Sq - E Tremont Av': 'WEST FARMS SQ',  'Westchester Sq - E Tremont Ave': 'WESTCHESTER SQ', 'Whitehall St': 'WHITEHALL S-FRY', 'Woodside - 61st St': '61 ST WOODSIDE', 'World Trade Center': 'WORLD TRADE CTR', 'Wyckoff Ave': 'MYRTLE-WYCKOFF'}
        matches = [('103rd St', '103 ST'), ('104th St', '104 ST'), ('110th St', '110 ST'), ('111th St', '111 ST'), ('116th St', '116 ST'), ('121st St', '121 ST'), ('125th St', '125 ST'), ('135th St', '135 ST'), ('145th St', '145 ST'), ('14th St', '14 ST'), ('155th St', '155 ST'), ('157th St', '157 ST'), ('167th St', '167 ST'), ('168th St', '168 ST'), ('169th St', '169 ST'), ('170th St', '170 ST'), ('174th St', '174 ST'), ('174th-175th Sts', '174-175 STS'), ('175th St', '175 ST'), ('176th St', '176 ST'), ('181st St', '181 ST'), ('183rd St', '183 ST'), ('18th Ave', '18 AV'), ('18th St', '18 ST'), ('190th St', '190 ST'), ('191st St', '191 ST'), ('1st Ave', '1 AV'), ('207th St', '207 ST'), ('20th Ave', '20 AV'), ('215th St', '215 ST'), ('219th St', '219 ST'), ('21st St', '21 ST'), ('225th St', '225 ST'), ('231st St', '231 ST'), ('233rd St', '233 ST'), ('238th St', '238 ST'), ('23rd St', '23 ST'), ('25th Ave', '25 AV'), ('25th St', '25 ST'), ('28th St', '28 ST'), ('30th Ave', '30 AV'), ('33rd St', '33 ST'), ('36th Ave', '36 AV'), ('36th St', '36 ST'), ('39th Ave', '39 AV'), ('3rd Ave', '3 AV'), ('45th St', '45 ST'), ('46th St', '46 ST'), ('49th St', '49 ST'), ('50th St', '50 ST'), ('51st St', '51 ST'), ('53rd St', '53 ST'), ('55th St', '55 ST'), ('57th St', '57 ST'), ('59th St', '59 ST'), ('65th St', '65 ST'), ('67th Ave', '67 AV'), ('69th St', '69 ST'), ('6th Ave', '6 AV'), ('71st St', '71 ST'), ('75th Ave', '75 AV'), ('77th St', '77 ST'), ('79th St', '79 ST'), ('7th Ave', '7 AV'), ('80th St', '80 ST'), ('86th St', '86 ST'), ('88th St', '88 ST'), ('8th Ave', '8 AV'), ('96th St', '96 ST'), ('9th Ave', '9 AV'), ('Alabama Ave', 'ALABAMA AV'), ('Allerton Ave', 'ALLERTON AV'), ('Astor Pl', 'ASTOR PL'), ('Astoria Blvd', 'ASTORIA BLVD'), ('Atlantic Ave', 'ATLANTIC AV'), ('Bay 50th St', 'BAY 50 ST'), ('Bay Ridge Ave', 'BAY RIDGE AV'), ('Baychester Ave', 'BAYCHESTER AV'), ('Beach 105th St', 'BEACH 105 ST'), ('Beach 25th St', 'BEACH 25 ST'), ('Beach 36th St', 'BEACH 36 ST'), ('Beach 44th St', 'BEACH 44 ST'), ('Beach 60th St', 'BEACH 60 ST'), ('Beach 67th St', 'BEACH 67 ST'), ('Beach 90th St', 'BEACH 90 ST'), ('Beach 98th St', 'BEACH 98 ST'), ('Bedford Ave', 'BEDFORD AV'), ('Bergen St', 'BERGEN ST'), ('Beverly Rd', 'BEVERLY RD'), ('Borough Hall', 'BOROUGH HALL'), ('Botanic Garden', 'BOTANIC GARDEN'), ('Bowery', 'BOWERY'), ('Bowling Green', 'BOWLING GREEN'), ('Brighton Beach', 'BRIGHTON BEACH'), ('Broad Channel', 'BROAD CHANNEL'), ('Broad St', 'BROAD ST'), ('Broadway', 'BROADWAY'), ('Bronx Park East', 'BRONX PARK EAST'), ('Brook Ave', 'BROOK AV'), ('Buhre Ave', 'BUHRE AV'), ('Burke Ave', 'BURKE AV'), ('Burnside Ave', 'BURNSIDE AV'), ('Canal St', 'CANAL ST'), ('Carroll St', 'CARROLL ST'), ('Castle Hill Ave', 'CASTLE HILL AV'), ('Central Ave', 'CENTRAL AV'), ('Chambers St', 'CHAMBERS ST'), ('Chauncey St', 'CHAUNCEY ST'), ('Church Ave', 'CHURCH AV'), ('City Hall', 'CITY HALL'), ('Clark St', 'CLARK ST'), ('Classon Ave', 'CLASSON AV'), ('Cleveland St', 'CLEVELAND ST'), ('Cortelyou Rd', 'CORTELYOU RD'), ('Crescent St', 'CRESCENT ST'), ('Cypress Ave', 'CYPRESS AV'), ('Cypress Hills', 'CYPRESS HILLS'), ('DeKalb Ave', 'DEKALB AV'), ('Ditmas Ave', 'DITMAS AV'), ('Dyckman St', 'DYCKMAN ST'), ('E 149th St', 'E 149 ST'), ('E 180th St', 'E 180 ST'), ('East Broadway', 'EAST BROADWAY'), ('Elder Ave', 'ELDER AV'), ('Elmhurst Ave', 'ELMHURST AV'), ('Euclid Ave', 'EUCLID AV'), ('Flushing Ave', 'FLUSHING AV'), ('Fordham Rd', 'FORDHAM RD'), ('Forest Ave', 'FOREST AVE'), ('Franklin Ave', 'FRANKLIN AV'), ('Franklin St', 'FRANKLIN ST'), ('Freeman St', 'FREEMAN ST'), ('Fresh Pond Rd', 'FRESH POND RD'), ('Fulton St', 'FULTON ST'), ('Gates Ave', 'GATES AV'), ('Graham Ave', 'GRAHAM AV'), ('Grand St', 'GRAND ST'), ('Grant Ave', 'GRANT AV'), ('Greenpoint Ave', 'GREENPOINT AV'), ('Gun Hill Rd', 'GUN HILL RD'), ('Halsey St', 'HALSEY ST'), ('Hewes St', 'HEWES ST'), ('High St', 'HIGH ST'), ('Houston St', 'HOUSTON ST'), ('Hoyt St', 'HOYT ST'), ('Hunts Point Ave', 'HUNTS POINT AV'), ('Intervale Ave', 'INTERVALE AV'), ('Jackson Ave', 'JACKSON AV'), ('Jefferson St', 'JEFFERSON ST'), ('Junction Blvd', 'JUNCTION BLVD'), ('Junius St', 'JUNIUS ST'), ('Kings Hwy', 'KINGS HWY'), ('Kingsbridge Rd', 'KINGSBRIDGE RD'), ('Kingston Ave', 'KINGSTON AV'), ('Kosciuszko St', 'KOSCIUSZKO ST'), ('Lafayette Ave', 'LAFAYETTE AV'), ('Liberty Ave', 'LIBERTY AV'), ('Livonia Ave', 'LIVONIA AV'), ('Longwood Ave', 'LONGWOOD AV'), ('Lorimer St', 'LORIMER ST'), ('Marcy Ave', 'MARCY AV'), ('Metropolitan Ave', 'METROPOLITAN AV'), ('Middletown Rd', 'MIDDLETOWN RD'), ('Montrose Ave', 'MONTROSE AV'), ('Morgan Ave', 'MORGAN AV'), ('Morris Park', 'MORRIS PARK'), ('Mosholu Pkwy', 'MOSHOLU PKWY'), ('Mt Eden Ave', 'MT EDEN AV'), ('Myrtle Ave', 'MYRTLE AV'), ('Neck Rd', 'NECK RD'), ('Neptune Ave', 'NEPTUNE AV'), ('Nevins St', 'NEVINS ST'), ('New Lots Ave', 'NEW LOTS AV'), ('New Utrecht Ave', 'NEW UTRECHT AV'), ('Newkirk Ave', 'NEWKIRK AV'), ('Northern Blvd', 'NORTHERN BLVD'), ('Norwood Ave', 'NORWOOD AV'), ('Nostrand Ave', 'NOSTRAND AV'), ('Ocean Pkwy', 'OCEAN PKWY'), ('Parkchester', 'PARKCHESTER'), ('Parkside Ave', 'PARKSIDE AV'), ('Parsons Blvd', 'PARSONS BLVD'), ('Pelham Bay Park', 'PELHAM BAY PARK'), ('Pelham Pkwy', 'PELHAM PKWY'), ('Pennsylvania Ave', 'PENNSYLVANIA AV'), ('President St', 'PRESIDENT ST'), ('Prince St', 'PRINCE ST'), ('Prospect Ave', 'PROSPECT AV'), ('Prospect Park', 'PROSPECT PARK'), ('Queensboro Plz', 'QUEENSBORO PLZ'), ('Ralph Ave', 'RALPH AV'), ('Rector St', 'RECTOR ST'), ('Rockaway Ave', 'ROCKAWAY AV'), ('Rockaway Blvd', 'ROCKAWAY BLVD'), ('Saratoga Ave', 'SARATOGA AV'), ('Seneca Ave', 'SENECA AVE'), ('Sheepshead Bay', 'SHEEPSHEAD BAY'), ('Shepherd Ave', 'SHEPHERD AV'), ('Simpson St', 'SIMPSON ST'), ('South Ferry', 'SOUTH FERRY'), ('Spring St', 'SPRING ST'), ('St Lawrence Ave', 'ST LAWRENCE AV'), ('Steinway St', 'STEINWAY ST'), ('Sterling St', 'STERLING ST'), ('Sutphin Blvd', 'SUTPHIN BLVD'), ('Sutter Ave', 'SUTTER AV'), ('Tremont Ave', 'TREMONT AV'), ('Union St', 'UNION ST'), ('Utica Ave', 'UTICA AV'), ('Van Siclen Ave', 'VAN SICLEN AV'), ('Wall St', 'WALL ST'), ('Whitlock Ave', 'WHITLOCK AV'), ('Wilson Ave', 'WILSON AV'), ('Winthrop St', 'WINTHROP ST'), ('Woodhaven Blvd', 'WOODHAVEN BLVD'), ('Woodlawn', 'WOODLAWN'), ('York St', 'YORK ST'), ('Zerega Ave', 'ZEREGA AV')]
        this_arr = []
        for key, value in manual_matches.items():
            temp = (key, value)
            this_arr.append(temp)

        all_matches = []
        for entry in this_arr:
            all_matches.append(entry)
        for entry in matches:
            all_matches.append(entry)
        df_1 = pd.DataFrame(all_matches, columns=['subway','turnstile'])

        print('Loading turnstile_total_byday from Mongo')
        turnstile_byday_data = repo.anuragp1_jl101995.turnstile_total_byday.find()

        ts_data = []
        for entry in turnstile_byday_data:
            ts_data.append([entry['ENTRIES'], entry['STATION']])
        df_2 = pd.DataFrame(ts_data, columns=['entries', 'turnstile'])
        df_2_grouped = pd.DataFrame(df_2.groupby(['turnstile'], as_index=False)['entries'].sum())

        print('Loading subway_pedestriancount from Mongo')
        subway_pc_data = repo.anuragp1_jl101995.subway_pedestriancount.find()

        print('Manipulating dataframes to come up with total traffic for each turnstile location')
        s_pc_data = []
        for entry in subway_pc_data:
            s_pc_data.append([entry['Station_Name'], entry['Pedestrian_Avg']])
            df_3 = pd.DataFrame(s_pc_data, columns =['subway', 'ped_avg'])

        df_1_3_arr = []
        for index1, row1 in df_1.iterrows():
            for index3, row3 in df_3.iterrows():
                if row1['subway'] == row3['subway']:
                    df_1_3_arr.append([row1['subway'], row1['turnstile'], row3['ped_avg']])

        df_arr_all= []
        for entry in df_1_3_arr:
            for index,row in df_2_grouped.iterrows():
                if entry[1] == row['turnstile']:
                    df_arr_all.append([entry[0], entry[1], entry[2], row['entries']])
        df_all = pd.DataFrame(df_arr_all, columns = ['subway', 'turnstile', 'ped_avg', 'entries'])
        df_all = df_all.drop_duplicates('subway')
        df_all = df_all.drop(['turnstile','ped_avg'],1)


        print('Loading subway_stations from Mongo')
        subway_data = repo.anuragp1_jl101995.subway_stations.find()
        data =[]
        for entry in subway_data:
            data.append((entry['the_geom']['coordinates'][0],entry['the_geom']['coordinates'][1] , entry['name']))
           
        print('Creating subway dataframe') 
        subway_df = pd.DataFrame(data, columns = ['lng', 'lat', 'subway'])
        subway_df = subway_df.drop_duplicates('subway')

        print('Combining subway dataframe and turnstile dataframe to match coordinates with total counts')
        final_subway_df = pd.merge(subway_df, df_all, how='left', on=['subway'])
        final_subway_df = final_subway_df.fillna(value=50)

        final_subway_df = final_subway_df.set_index('subway')
        final_subway_df= final_subway_df.T

        print('Creating initial json for subway coordinates and usage')
        final_subway_df.to_json('visualizations/map_vis/subway_coord_init.json')

        repo.dropPermanent('subway_coord_json')
        repo.createPermanent('subway_coord_json')
        subway_records = json.loads(final_subway_df.to_json()).values()
        repo.anuragp1_jl101995.subway_coord_json.insert(subway_records)


        #### NOTE: The above code performs the original transformations and creates the necesary JSON files,
        #### but the following code loads in the cleaned JSON files that are needed for D3 map visualization
        url = "http://datamechanics.io/data/anuragp1_jl101995/citi_coord.json"
        urllib.request.urlretrieve(url, 'visualizations/map_vis/citi_coord.json')
        url = "http://datamechanics.io/data/anuragp1_jl101995/ped_coord_v2.json"
        urllib.request.urlretrieve(url, 'visualizations/map_vis/ped_coord.json')
        url = "http://datamechanics.io/data/anuragp1_jl101995/subway_coord.json"
        urllib.request.urlretrieve(url, 'visualizations/map_vis/subway_coord.json')

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

        this_script = doc.agent('alg:anuragp1_jl101995#transform_usage_json', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform creating citi_coord_json
        citi_coord_json_resource = doc.entity('dat:citi_coord_json',{'prov:label':'CitiBike Station Coordinates and Usage JSON', prov.model.PROV_TYPE:'ont:DataSet'})
        get_citi_coord_json = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_citi_coord_json, this_script)
        doc.usage(get_citi_coord_json, citi_coord_json_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'}) 
        citi_coord_json = doc.entity('dat:anuragp1_jl101995#citi_coord_json', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})   
        doc.wasAttributedTo(citi_coord_json, this_script)
        doc.wasGeneratedBy(citi_coord_json, get_citi_coord_json, endTime)
        doc.wasDerivedFrom(citi_coord_json, citi_coord_json_resource, get_citi_coord_json, get_citi_coord_json, get_citi_coord_json)

        # Transform creating ped_coord_json
        ped_coord_json_resource = doc.entity('dat:ped_coord_json',{'prov:label':'Pedestrian Region Coordinates and Pedestrian Count JSON', prov.model.PROV_TYPE:'ont:DataSet'})
        get_ped_coord_json = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_ped_coord_json, this_script)
        doc.usage(get_ped_coord_json, ped_coord_json_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'}) 
        ped_coord_json = doc.entity('dat:anuragp1_jl101995#ped_coord_json', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})   
        doc.wasAttributedTo(ped_coord_json, this_script)
        doc.wasGeneratedBy(ped_coord_json, get_ped_coord_json, endTime)
        doc.wasDerivedFrom(ped_coord_json, ped_coord_json_resource, get_ped_coord_json, get_ped_coord_json, get_ped_coord_json)

        # Transform creating subway_coord_json
        subway_coord_json_resource = doc.entity('dat:subway_coord_json',{'prov:label':'Subway Station Coordinates and Usage JSON', prov.model.PROV_TYPE:'ont:DataSet'}) 
        get_subway_coord_json = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_subway_coord_json, this_script)
        doc.usage(get_subway_coord_json, subway_coord_json_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'}) 
        subway_coord_json = doc.entity('dat:anuragp1_jl101995#subway_coord_json', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})   
        doc.wasAttributedTo(subway_coord_json, this_script)
        doc.wasGeneratedBy(subway_coord_json, get_subway_coord_json, endTime)
        doc.wasDerivedFrom(subway_coord_json, subway_coord_json_resource, get_subway_coord_json, get_subway_coord_json, get_subway_coord_json)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc

transform_usage_json.execute()
doc = transform_usage_json.provenance()
