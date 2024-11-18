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

parser = ArgumentParser(description="Creates JSON file from GTDBtk results.")

parser.add_argument("-i", dest="input", default="661k.bac120.summary.tsv", type=str, help="Name of the input file (default: 661k.bac120.summary.tsv).")
parser.add_argument("-o", dest="output", default="gtdbtk.json", type=str, help="Name of the output file (default: gtdbtk.json).")
args=parser.parse_args()

SpecList = ['d','p','c','o','f','g','s']
SpecList2 = ["domain","phylum","class","order","family","genus","species"]
gtdb_data=pd.read_csv(args.input,sep='\t', usecols=["user_genome","classification","closest_genome_reference","classification_method","warnings"])

for line in range(0,gtdb_data.shape[0]):
    classList = gtdb_data.classification[line].split(";")
    classDict = {}
    for item in range(0,len(classList)):
        if classList[item].split("__")[0] == SpecList[item]:
            classDict.update([(SpecList2[item], classList[item].split("__")[1])])
    gtdb_dict={"classification": classDict, "closest_genome_reference": gtdb_data.closest_genome_reference[line], 
                          "classification_method": gtdb_data.classification_method[line],"warnings": json.dumps(gtdb_data.warnings[line])}

    gtdb_json=json.dumps(gtdb_dict, default=np_encoder)
    
    jsonFile = args.output
    with open(jsonFile, "w") as file:
        file.write(gtdb_json)