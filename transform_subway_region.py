import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transform_subway_region(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.subway_stations', 'anuragp1_jl101995.pedestriancounts']
    writes = ['anuragp1_jl101995.subway_regions']

    @staticmethod
    def execute(Trial = False):
        '''Retrieve some data sets'''

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        repo.anuragp1_jl101995.subway_stations.ensure_index( [( 'the_geom' ,dml.pymongo.GEOSPHERE )] )
        repo.anuragp1_jl101995.pedestriancounts.ensure_index( [('the_geom' ,dml.pymongo.GEOSPHERE )] )

        repo.dropPermanent("subway_regions")
        repo.createPermanent("subway_regions")

        for this_loc in repo.anuragp1_jl101995.subway_stations.find():
            closest_stations = repo.command(
                'geoNear','anuragp1_jl101995.pedestriancounts',
                near={
                'type' : 'Point', 
                'coordinates' : this_loc['the_geom']['coordinates']},
                spherical=True,
                maxDistance = 8000
                )['results']

            subway_regions = { 'Line' : this_loc['line'], 'Station_Name' : this_loc['name'], 'Closest_Region' :closest_stations[0]['obj']['loc']}
            repo['anuragp1_jl101995.subway_regions'].insert_one(subway_regions)

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

        this_script = doc.agent('alg:anuragp1_jl101995#transform_subway_region', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform associating each subway station with its pedestrian count region
        subway_regions_resource = doc.entity('dat:subway_regions',{'prov:label':'Subway Station Region Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_subwayregions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_subwayregions, this_script)
        doc.usage(get_subwayregions, subway_regions_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        subway_regions = doc.entity('dat:anuragp1_jl101995#subway_regions', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(subway_regions, this_script)
        doc.wasGeneratedBy(subway_regions, get_subwayregions, endTime)
        doc.wasDerivedFrom(subway_regions, subway_regions_resource, get_subwayregions, get_subwayregions, get_subwayregions) 

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
            
        return doc


transform_subway_region.execute()
doc = transform_subway_region.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof