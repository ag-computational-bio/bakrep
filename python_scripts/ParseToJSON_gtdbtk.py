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

parser = ArgumentParser(description="Creates JSON file from GTDBtk results.")

parser.add_argument("-i", dest="input", default="661k.bac120.summary.tsv", type=str, help="Name of the input file (default: 661k.bac120.summary.tsv).")
args=parser.parse_args()

gtdb_data=pd.read_csv(args.input,sep='\t', usecols=["user_genome","classification","fastani_reference","classification_method","warnings"])

for line in range(0,gtdb_data.shape[0]):
    gtdb_dict={"classification": gtdb_data.classification[line], "fastani_reference": gtdb_data.fastani_reference[line], 
                          "classification_method": gtdb_data.classification_method[line],"warnings": json.dumps(gtdb_data.warnings[line])}
    
    # .dumps hier nötig, damit der Text in den "warnings" auch wirklich als Text interpretiert werden kann!

    gtdb_json=json.dumps(gtdb_dict, default=np_encoder)
    
    jsonFile = "./taxonomy/" + gtdb_data.user_genome[line].split('.')[0] + ".gtdb.json"
    os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
    with open(jsonFile, "w") as file:
        file.write(gtdb_json)