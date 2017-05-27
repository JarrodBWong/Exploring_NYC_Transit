import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
import scipy.stats
from bson.code import Code

class regression(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp_jl101995.subway_pedestriancount', 'anuragp']
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


        # # Collection combining desired turnstile and station data
        # repo.dropPermanent('turnstile_station')
        # repo.createPermanent('turnstile_station')

        # map_function_subway = Code('''function() {
        #                         emit(this.name, this.name);
        #                      }
        #                     ''')

        # map_function_turnstile = Code('''function() {
        #                         emit(this.STATION, this.STATION);
        #                      }
        #                     ''')

        # reduce_function_st = Code('''function(key, values) {
        #                             var names = [];
        #                             for (var i = 0; i < values.length; i++) {
        #                                     if(!(names.indexOf(values[i]) >= 0)) {
        #                                     names.push(values[i]);
        #                                 }
        #                             }
        #                             return {names};
        #                        }''')

        # # use map reduce to get names for subway_stations
        # station_result = get_collection('repo.anuragp1_jl101995.subway_stations').map_reduce(map_function_subway, reduce_function_st, 'anuragp1_jl101995.turnstile_station')
        # station_names = []
        # for doc in station_result.find():
        #     station_names.append(doc['_id'])

        # turnstile_result = get_collection('repo.anuragp1_jl101995.turnstile').map_reduce(map_function_turnstile, reduce_function_st, 'anuragp1_jl101995.turnstile_station')
        # turnstile_names = []
        # for doc in turnstile_result.find():
        #     turnstile_names.append(doc['_id'])

        # # get matches
        # matched_pairs = {}
        # subway_matches, turnstile_matches = set(), set()  # all matched names

        # for station in station_names:
        #     this_station = station.lower()
        #     this_station = this_station.replace('rd', '')
        #     this_station = this_station.replace('th', '')
        #     this_station = this_station.replace('st', '')
        #     this_station = this_station.replace('e', '')

        #     for stile in turnstile_names:
        #         this_stile = stile.lower()
        #         this_stile = this_stile.replace('rd', '')
        #         this_stile = this_stile.replace('th', '')
        #         this_stile = this_stile.replace('st', '')
        #         this_stile = this_stile.replace('e', '')

        #         if this_stile == this_station:
        #             matched_pairs[station] = stile
        #             subway_matches.add(station)
        #             turnstile_matches.add(stile)
        #             break


        # Insert into DB
        # repo.dropPermanent('turnstile_station')
        # repo.createPermanent('turnstile_station')

        # print(matched_pairs)
        # unmatched_subway = set(station_names) - subway_matches
        # unmatched_turnstile = set(turnstile_names) - turnstile_matches
        # print('\n')
        # print(unmatched_subway)
        # print(len(unmatched_subway))
        # print('\n')
        # print(unmatched_turnstile)
        # print(len(unmatched_turnstile))

        manual_matches = \
        {'103rd St - Corona Plaza': '103 ST-CORONA', '116th St - Columbia University': '116 ST-COLUMBIA', '137th St - City College': '137 ST CITY COL', \
         '138th St - Grand Concourse': '138/GRAND CONC', '149th St - Grand Concourse': '149/GRAND CONC', '15th St - Prospect Park': '15 ST-PROSPECT', \
         '161st St - Yankee Stadium': '161/YANKEE STAD', '163rd St - Amsterdam Av': '163 ST-AMSTERDM', '182nd-183rd Sts': '182-183 STS', \
         '21st St - Queensbridge': '21 ST-QNSBRIDGE', '34th St - Penn Station': '34 ST-PENN STA', '3rd Ave - 138th St': '3 AV 138 ST', \
         '3rd Ave - 149th St': '3 AV-149 ST', '40th St': '40 ST LOWERY ST', '42nd St - Bryant Pk': '42 ST-BRYANT PK', '42nd St - Port Authority Bus Term': '42 ST-PORT AUTH', \
         '47th-50th Sts - Rockefeller Ctr': '47-50 STS ROCK', '4th Ave': '4 AV-9 ST', '52nd St': '52 ST', '59th St - Columbus Circle': '59 ST COLUMBUS', \
         '5th Ave - 53rd St': '5 AV/53 ST', '5th Ave - 59th St': '5 AV/59 ST', '5th Ave - Bryant Pk': '5 AVE', '63rd Dr - Rego Park': '63 DR-REGO PARK', \
         '66th St - Lincoln Ctr': '66 ST-LINCOLN', '68th St - Hunter College': '68ST-HUNTER CO', '72nd St': '72 ST', '74th St - Broadway': '74 ST-BROADWAY', \
         '75th St - Eldert Ln': '75 ST-ELDERTS', '81st St': '81 ST-MUSEUM', '82nd St - Jackson Hts': '82 ST-JACKSON H', '8th St - NYU': '8 ST-NYU', \
         '90th St - Elmhurst Av': '90 ST-ELMHURST', '9th St': '4 AV-9 ST', 'Aqueduct - North Conduit Av': 'AQUEDUCT N.COND', 'Astoria - Ditmars Blvd': 'ASTORIA DITMARS', \
         'Atlantic Av - Pacific St': 'ATL AV-BARCLAY', 'Ave H': 'AVENUE H', 'Ave I': 'AVENUE I', 'Ave J': 'AVENUE J', 'Ave M': 'AVENUE M', 'Ave N': 'AVENUE N', \
         'Ave P': 'AVENUE P', 'Ave U': 'AVENUE U', 'Ave X': 'AVENUE X', 'Bay Pky': 'BAY PKWY', 'Bay Ridge - 95th St': 'BAY RIDGE-95 ST', 'Bedford - Nostrand Aves': 'BEDFORD-NOSTRAN', \
         'Bedford Park Blvd': 'BEDFORD PK BLVD', 'Bleecker St (Downtown)': 'BLEECKER ST', 'Briarwood - Van Wyck Blvd': 'BRIARWOOD', 'Broadway - Lafayette St': "B'WAY-LAFAYETTE", \
         'Broadway - Nassau St': 'NASSAU ST', 'Broadway Junction': 'BROADWAY JCT', 'Brooklyn Bridge - City Hall': 'BROOKLYN BRIDGE', 'Brooklyn College - Flatbush Ave': 'FLATBUSH AV-B.C', \
         'Bushwick - Aberdeen': 'BUSHWICK AV', 'Canarsie - Rockaway Pkwy': 'CANARSIE-ROCKAW', 'Cathedral Pkwy (110th St)': 'CATHEDRAL PKWY', 'Central Park North (110th St)': 'CENTRAL PK N110', \
         'Christopher St - Sheridan Sq': 'CHRISTOPHER ST', 'Clinton - Washington Aves': 'CLINTON-WASH AV', 'Coney Island - Stillwell Av': 'CONEY IS-STILLW', \
         'Cortlandt St (NB only)': 'CORTLANDT ST', 'Cortlandt St (Temporarily Closed)': 'CORTLANDT ST', 'Crown Hts - Utica Ave': 'CROWN HTS-UTICA', 'Delancey St': 'DELANCEY/ESSEX', \
         'E 105th St': 'EAST 105 ST', "E 143rd St - St Mary's St": "E 143/ST MARY'S", 'Eastchester - Dyre Ave': 'EASTCHSTER/DYRE', 'Eastern Pkwy - Bklyn Museum': 'EASTN PKWY-MUSM', \
         'Essex St': 'DELANCEY/ESSEX', 'Far Rockaway - Mott Ave': 'FAR ROCKAWAY', 'Flushing - Main St': 'FLUSHING-MAIN', 'Forest Hills - 71st Av': 'FOREST HILLS 71', \
         'Ft Hamilton Pkwy': 'FT HAMILTON PKY', 'Grand Army Plaza': 'GRAND ARMY PLAZ', 'Grand Ave - Newtown': 'GRAND-NEWTOWN', 'Grand Central - 42nd St': 'GRD CNTRL-42 ST', \
         'Harlem - 148 St': 'HARLEM 148 ST', 'Herald Sq - 34th St': '34 ST-HERALD SQ', 'Howard Beach - JFK Airport': 'HOWARD BCH JFK', 'Hoyt - Schermerhorn Sts': 'HOYT-SCHER', \
         'Hunters Point Ave': 'HUNTERS PT AV', 'Inwood - 207th St': 'INWOOD-207 ST', 'Jackson Hts - Roosevelt Av': '82 ST-JACKSON H', 'Jamaica - 179th St': 'JAMAICA 179 ST', \
         'Jamaica - Van Wyck': 'JAMAICA VAN WK', 'Jamaica Ctr - Parsons / Archer': 'JAMAICA CENTER', 'Jay St - Borough Hall': 'JAY ST-METROTEC', 'Kew Gardens - Union Tpke': 'KEW GARDENS', \
         'Kingston - Throop Aves': 'KINGSTON-THROOP', 'Knickerbocker Ave': 'KNICKERBOCKER', 'Lexington Ave - 53rd St': 'LEXINGTON AV/53', 'Lexington Ave - 63rd St': 'LEXINGTON AV/63', \
         'Long Island City - Court Sq': 'COURT SQ', 'Lower East Side - 2nd Ave': '2 AV', 'Marble Hill - 225th St': 'MARBLE HILL-225', 'Mets - Willets Point': 'METS-WILLETS PT', \
         'Myrtle-Willoughby Aves': 'MYRTLE-WILLOUGH', 'Nereid Ave (238 St)': 'NEREID AV', 'Norwood - 205th St': 'NORWOOD 205 ST', 'Ozone Park - Lefferts Blvd': 'OZONE PK LEFFRT', \
         'Park Pl': 'PARK PLACE', 'Queens Plz': 'QUEENS PLAZA', 'Rockaway Park - Beach 116 St': 'ROCKAWAY PARK B', 'Roosevelt Island - Main St': 'ROOSEVELT ISLND', \
         'Smith - 9th Sts': 'SMITH-9 ST', 'Sutphin Blvd - Archer Av': 'SUTPHIN-ARCHER', 'Sutter Ave - Rutland Road': 'SUTTER AV-RUTLD', 'Times Sq - 42nd St': 'TIMES SQ-42 ST', \
         'Union Sq - 14th St': '14 ST-UNION SQ', 'Van Cortlandt Park - 242nd St': 'V.CORTLANDT PK', 'Vernon Blvd - Jackson Ave': 'VERNON-JACKSON', 'W 4th St - Washington Sq (Upper)': 'W 4 ST-WASH SQ', \
         'W 8th St - NY Aquarium': 'W 8 ST-AQUARIUM', 'Wakefield - 241st St': 'WAKEFIELD/241', 'West Farms Sq - E Tremont Av': 'WEST FARMS SQ', \
         'Westchester Sq - E Tremont Ave': 'WESTCHESTER SQ', 'Whitehall St': 'WHITEHALL S-FRY', 'Woodside - 61st St': '61 ST WOODSIDE', 'World Trade Center': 'WORLD TRADE CTR', 'Wyckoff Ave': 'MYRTLE-WYCKOFF'}

        # skips = 0
        # for name in unmatched_subway:
        #     try:
        #         matched_pairs[name] = manual_matches[name]
        #     except Exception as e:
        #         skips += 1
        #         print('Didnt find a match for name ' + str(name) + ' and so far ' + str(skips) + ' skips')
        #         continue

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

        turnstile_byday_data = get_collection('repo.anuragp1_jl101995.turnstile_total_byday').find()

        ts_data = []
        for entry in turnstile_byday_data:
        	ts_data.append([entry['ENTRIES'], entry['STATION']])
        df_2 = pd.DataFrame(ts_data, columns=['entries', 'turnstile'])
        df_2_grouped = pd.DataFrame(df_2.groupby(['turnstile'], as_index=False)['entries'].sum())

        subway_pc_data = get_collection('repo.anuragp1_jl101995.subway_pedestriancount').find()
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

       	x_pedestrian_count = list(df_all['ped_avg'])
       	y_subway_usage = list(df_all['entries'])

       	slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(x_pedestrian_count, y_subway_usage)
       	r_squared = r_value**2

       	print('slope is ' + str(slope))
       	print('intercept is ' + str(intercept))
       	print('r-squared is ' + str(r_squared))

       	print('Our regression equation is : Subway_Usage = 517937 + 18.9*(Region_Pedestrian_Count)')

        # repo.dropPermanent('regression_data')
        # repo.createPermanent('regression_data')

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


regression.execute(Trial=False)
doc = regression.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
