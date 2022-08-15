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

parser = ArgumentParser(description="Creates JSON file from checkm results.")

parser.add_argument("-i", dest="input", default="checkm.tsv", type=str, help="Name of the input file (default: checkm.tsv)")
parser.add_argument("-o", dest="output", default="/vol/bakrep/linda_test/no_backup/nextflow/", type=str, help="Name of the output file.")
args=parser.parse_args()

checkm_data=pd.read_csv(args.input,sep='\t', names=["BinID","MarkerLA","NumGen","NumMark","NumMarkSets","Zero","One","Two","Three","Four","FivePlus","Compl","Conta","StrainHet"], skiprows=1)

for line in range(0,checkm_data.shape[0]):
    checkm_dict={"marker": {"marker_lineage": checkm_data.MarkerLA[line], "#genomes": checkm_data.NumGen[line], 
                "#markers": checkm_data.NumMark[line], "#marker_sets": checkm_data.NumMarkSets[line]}, "gene_copies": {"0": checkm_data.Zero[line], "1": checkm_data.One[line], 
                "2": checkm_data.Two[line], "3": checkm_data.Three[line], "4": checkm_data.Four[line], "5+": checkm_data.FivePlus[line]}, 
                "quality": {"completeness": checkm_data.Compl[line], "contamination": checkm_data.Conta[line], "strain_heterogeneity": checkm_data.StrainHet[line]}}

    checkm_json=json.dumps(checkm_dict, default=np_encoder)

    jsonFile = args.output + checkm_data.BinID[line].split('.')[0] + ".checkm.json"
    os.makedirs(os.path.dirname(jsonFile), exist_ok=True)
    with open(jsonFile, "w") as file:
        file.write(checkm_json)