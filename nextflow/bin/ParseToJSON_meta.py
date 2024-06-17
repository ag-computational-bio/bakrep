import json
from logging import root
import sys
import os
import pandas as pd
import numpy as np
from argparse import ArgumentParser
from parse import *

def np_encoder(object):
    if isinstance(object, np.generic):
        return object.item()

parser = ArgumentParser(description="Creates json files from assembly metadata.")

parser.add_argument("-i", dest="input", default="metadata_ena_661K.txt", type=str, help="Name of the input file (default: metadata_ena_661K.txt)")
parser.add_argument("-o", dest="output", default="assembly_meta_json.txt", type=str, help="Name of the output file (default: assembly_meta_json.txt)")
args=parser.parse_args()
meta_json = {}

meta_data=pd.read_csv(args.input,sep='\t', usecols=["sample_id","study_accession","run_accession","project_name","isolation_source","instrument_platform","host","first_public","country","collection_date","center_name","accession","bio_material","broker_name","collected_by","culture_collection","depth","environment_biome","environment_feature","environment_material","environmental_package","environmental_sample","host_sex","host_status","host_tax_id","instrument_model","isolate","lat","location","lon","sample_alias","secondary_sample_accession","secondary_study_accession","serotype","serovar","strain","study_alias","study_title","sub_strain","submission_accession"], na_filter=False)

for line in range(0,meta_data.shape[0]):
            
    if meta_data.isolation_source[line] == "":
        meta_data.loc['isolation_source',line] = None 
        
    if meta_data.host[line] == "":
        meta_data.host[line] = None 

    if meta_data.first_public[line] == "":
        meta_data.first_public[line] = None 
        
    if meta_data.country[line] == "":
        meta_data.country[line] = None 
        
    if meta_data.center_name[line] == "":
        meta_data.center_name[line] = None 
        
    if meta_data.bio_material[line] == "":
        meta_data.bio_material[line] = None 
        
    if meta_data.broker_name[line] == "":
        meta_data.broker_name[line] = None 
        
    if meta_data.collected_by[line] == "":
        meta_data.collected_by[line] = None 
        
    if meta_data.culture_collection[line] == "":
        meta_data.culture_collection[line] = None 
        
    if meta_data.depth[line] == "":
        meta_data.depth[line] = None 
        
    if meta_data.environment_biome[line] == "n.a." or meta_data.environment_biome[line] == "":
        meta_data.environment_biome[line] = None 
        
    if meta_data.environment_feature[line] == "":
        meta_data.environment_feature[line] = None 
        
    if meta_data.environment_material[line] == "" or meta_data.environment_material[line] == "n.a." :
        meta_data.environment_material[line] = None
        
    if meta_data.environmental_package[line] == "":
        meta_data.environmental_package[line] = None
      
    if meta_data.environmental_sample[line] == "false":
        meta_data.loc['environmental_sample',line] = False
        
    if meta_data.isolation_source[line] == "":
        meta_data.isolation_source[line] = None 
        
    if meta_data.host_sex[line] == "":
        meta_data.host_sex[line] = None 
        
    if meta_data.host_status[line] == "":
        meta_data.host_status[line] = None 
        
    if meta_data.host_tax_id[line] == "NA" or meta_data.host_tax_id[line] == "":
        meta_data.host_tax_id[line] = None 
        
    if meta_data.isolate[line] == "":
        meta_data.isolate[line] = None 
        
    if meta_data.location[line] == "":
        meta_data.location[line] = None
        
    if meta_data.lon[line] == "NA":
        meta_data.lon[line] = None 
        
    if meta_data.lat[line] == "NA":
        meta_data.lat[line] = None 
        
    if meta_data.serotype[line] == "":
        meta_data.serotype[line] = None
        
    if meta_data.serovar[line] == "":
        meta_data.serovar[line] = None
        
    if meta_data.strain[line] == "":
        meta_data.strain[line] = None
        
    if meta_data.sub_strain[line] == "":
        meta_data.sub_strain[line] = None
    
    
for line in range(0,meta_data.shape[0]):
    meta_dict = {"id":(meta_data.sample_id[line]), "study_accession": (meta_data.study_accession[line]), "run_accession": (meta_data.run_accession[line]), "project_name": (meta_data.project_name[line]), "isolation_source": (meta_data.isolation_source[line]), "instrument_platform": (meta_data.instrument_platform[line]), "host": (meta_data.host[line]), "first_public": (meta_data.first_public[line]), "country": (meta_data.country[line]), "collection_date": (meta_data.collection_date[line]), "center_name": (meta_data.center_name[line]), "accession": (meta_data.accession[line]), "bio_material": (meta_data.bio_material[line]), "broker_name": (meta_data.broker_name[line]), "collected_by": (meta_data.collected_by[line]), "culture_collection": (meta_data.culture_collection[line]), "depth": (meta_data.depth[line]), "environment_biome": (meta_data.environment_biome[line]), "environment_feature": (meta_data.environment_feature[line]), "environment_material": (meta_data.environment_material[line]), "environmental_package": (meta_data.environmental_package[line]), "environmental_sample": (meta_data.environmental_sample[line]), "host_sex": (meta_data.host_sex[line]), "host_status": (meta_data.host_status[line]),"host_tax_id": (meta_data.host_tax_id[line]),"instrument_model": (meta_data.instrument_model[line]), "isolate": (meta_data.isolate[line]), "location": ({"type": "Point", "coordinates": [meta_data.lat[line], meta_data.lon[line]]}), "sample_alias": (meta_data.sample_alias[line]), "secondary_sample_accession": (meta_data.secondary_sample_accession[line]), "secondary_study_accession": (meta_data.secondary_study_accession[line]), "serotype": (meta_data.serotype[line]), "serovar": (meta_data.serovar[line]), "strain": (meta_data.strain[line]), "study_alias": (meta_data.study_alias[line]), "study_title": (meta_data.study_title[line]), "sub_strain": (meta_data.sub_strain[line]), "submission_accession": (meta_data.submission_accession[line])}
    
    meta_json = json.dumps(meta_dict, default=np_encoder)  
            
# Creates one file for each ID   
    jsonFile = "./assembly_json/" + meta_data.sample_id[line] + "/" + meta_data.sample_id[line] + ".meta.json"
    os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
    with open(jsonFile, "w") as file:
        file.write(meta_json)
