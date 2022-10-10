#!/usr/bin/python3

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

parser = ArgumentParser(description="Creates JSON file from checkm2 results.")

parser.add_argument("-i", dest="input", default="checkm2.tsv", type=str, help="Name of the input file (default: checkm.tsv)")
parser.add_argument("-o", dest="output", default="checkm2.json", type=str, help="Name of the output file (default: checkm2.json).")
args=parser.parse_args()

checkm2_data=pd.read_csv(args.input,sep='\t', names=["Name","Completeness","Contamination","Completeness_Model_Used","Translation_Table_Used","Additional_Notes"], skiprows=1)

for line in range(0,checkm2_data.shape[0]):
    checkm2_dict={"quality": {"completeness": checkm2_data.Completeness[line], "contamination": checkm2_data.Contamination[line]}, 
                "calculation": {"model": checkm2_data.Completeness_Model_Used[line], "translation_table": checkm2_data.Translation_Table_Used[line], "notes": checkm2_data.Additional_Notes[line]}} 

    checkm2_json=json.dumps(checkm2_dict, default=np_encoder)

    #jsonFile = checkm_data.BinID[line][3:7] + "/" + checkm_data.BinID[line].split('.')[0] + ".checkm.json"
    jsonFile = args.output
    #os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
    with open(jsonFile, "w") as file:
        file.write(checkm2_json)