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

parser = ArgumentParser(description="Creates json and tsv file from assembly metadata.")

parser.add_argument("-i", dest="input", default="metadata_ena_661K.txt", type=str, help="Name of the input file (default: metadata_ena_661.txt)")
parser.add_argument("-o", dest="output", default="assembly_meta_json.txt", type=str, help="Name of the output file (default: assembly_meta_json.txt)")
args=parser.parse_args()
meta_json = {}

meta_data=pd.read_csv(args.input,sep='\t', usecols=["sample_id","study_accession","run_accession","experiment_accession","first_public","project_name","study_title","country","collection_date","environment_biome","location"], na_filter=False)

for line in range(0,meta_data.shape[0]):
    meta_dict= {str(meta_data.sample_id[line]): { "study_accession": str(meta_data.study_accession[line]), "run_accession": str(meta_data.run_accession[line]), "experiment_accession": str(meta_data.experiment_accession[line]), "info": {"study_title": str(meta_data.study_title[line]), "project_name": str(meta_data.project_name[line]), "first_public": str(meta_data.first_public[line])}, "isolation": {"collection_date": str(meta_data.collection_date[line]), "country": str(meta_data.country[line]), "location": str(meta_data.location[line]), "environment_biome": str(meta_data.environment_biome[line])} }}

    meta_json.update(meta_dict)

jsonFile = "assembly_json/661k.meta.json" 
os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
with open(jsonFile, "w") as file:
        json.dump(meta_json, file)
        
# Creates one file for each ID   
#    jsonFile = "./assembly_json/" + meta_data.sample_id[line] + ".checkm.json"
#    os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
#    with open(jsonFile, "w") as file:
#        file.write(meta_json)