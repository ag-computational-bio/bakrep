import shutil
import os
import sys
import pandas as pd
import numpy as numpy
from argparse import ArgumentParser

parser = ArgumentParser(description="Distributes files to appropriate subfolders.")
parser.add_argument("-i", dest="input", default="metadata_661k_filtered.tsv", type=str, help="Name of the input file (default: metadata_661k_filtered.tsv)")
parser.add_argument("-o", dest="output", default="assemblies/", type=str, help="Path to output directory (default: asseblies/")
args=parser.parse_args()

# Takes sampleID from metadata file
meta_id=pd.read_csv(args.input,sep='\t', usecols=["sample_id"])
filenames = []
dicnames = {'Dict' : ["test"], 'Amount' : [0]}
dic_df = pd.DataFrame(dicnames)
files = []

# A list is created in which all IDs are collected. For each ID then only the characters 4-6 are collected in another list.
# If no folder with this character combination exists yet, one is created. The files, which fit to the scheme are copied then into the suitable folder.
for line in range(0,meta_id.shape[0]):
    helpls = meta_id.sample_id[line]
    helpls_part = helpls[3:7]
    if not helpls_part in next(os.walk(args.output))[1]:
        os.mkdir(args.output + helpls_part)
    shutil.copy("assemblies/" + helpls + ".fna.gz", args.output + str(helpls_part) + "/")
    