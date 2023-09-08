import json
from logging import root
import sys
import os
import pandas as pd
import numpy as np
from argparse import ArgumentParser

def np_encoder(object):
    if isinstance(object, np.generic):
        return object.item()

parser = ArgumentParser(description="Creates json files from assembly metadata.")

parser.add_argument("-i", dest="input", default="metadata_ena_661K.txt", type=str, help="Name of the input file (default: metadata_ena_661.txt)")
parser.add_argument("-o", dest="output", default="assembly_meta_json.txt", type=str, help="Name of the output file (default: assembly_meta_json.txt)")
args=parser.parse_args()
meta_json = {}

meta_data=pd.read_csv(args.input,sep='\t', usecols=["sample_id","study_accession","run_accession","project_name","isolation_source","instrument_platform","host","first_public","country","collection_date","center_name","accession","bio_material","broker_name","collected_by","culture_collection","depth","environment_biome","environment_feature","environment_material","environmental_package","environmental_sample","host_sex","host_status","host_tax_id","instrument_model","isolate","lat","location","lon","sample_alias","secondary_sample_accession","secondary_study_accession","serotype","serovar","strain","study_alias","study_title","sub_strain","submission_accession"], na_filter=False)

for line in range(0,meta_data.shape[0]):
    meta_dict= {str(meta_data.sample_id[line]): { "study_accession": str(meta_data.study_accession[line]), "run_accession": str(meta_data.run_accession[line]), "project_name": str(meta_data.project_name[line]), "isolation_source": str(meta_data.isolation_source[line]), "instrument_platform": str(meta_data.instrument_platform[line]), "host": str(meta_data.host[line]), "first_public": str(meta_data.first_public[line]), "country": str(meta_data.country[line]), "collection_date": str(meta_data.collection_date[line]), "center_name": str(meta_data.center_name[line]), "accession": str(meta_data.accession[line]), "bio_material": str(meta_data.bio_material[line]), "broker_name": str(meta_data.broker_name[line]), "collected_by": str(meta_data.collected_by[line]), "culture_collection": str(meta_data.culture_collection[line]), "depth": str(meta_data.depth[line]), "environment_biome": str(meta_data.environment_biome[line]), "environment_feature": str(meta_data.environment_feature[line]), "environment_material": str(meta_data.environment_material[line]), "environmental_package": str(meta_data.environmental_package[line]), "environmental_sample": str(meta_data.environmental_sample[line]), "host_sex": str(meta_data.host_sex[line]), "host_status": str(meta_data.host_status[line]),"host_tax_id": str(meta_data.host_tax_id[line]),"instrument_model": str(meta_data.instrument_model[line]), "isolate": str(meta_data.isolate[line]), "lat": str(meta_data.lat[line]), "location": str(meta_data.location[line]), "lon": str(meta_data.lon[line]), "sample_alias": str(meta_data.sample_alias[line]), "secondary_sample_accession": str(meta_data.secondary_sample_accession[line]), "secondary_study_accession": str(meta_data.secondary_study_accession[line]), "serotype": str(meta_data.serotype[line]), "serovar": str(meta_data.serovar[line]), "strain": str(meta_data.strain[line]), "study_alias": str(meta_data.study_alias[line]), "study_title": str(meta_data.study_title[line]), "sub_strain": str(meta_data.sub_strain[line]), "submission_accession": str(meta_data.submission_accession[line])}}
    #meta_json.update(meta_dict)
    meta_json = json.dumps(meta_dict, default=np_encoder)  

#jsonFile = "assembly_json/661k.meta.json" 
#os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
#with open(jsonFile, "w") as file:
#        json.dump(meta_json, file)
        
# Creates one file for each ID   
    jsonFile = "./assembly_json/" + meta_data.sample_id[line] + ".meta.json"
    os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
    with open(jsonFile, "w") as file:
        file.write(meta_json)