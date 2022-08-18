import shutil
import os
import sys
import pandas as pd
import numpy as numpy
from argparse import ArgumentParser

parser = ArgumentParser(description="Distributes files to appropriate subfolders.")
parser.add_argument("-i", dest="input", default="metadata_661k_filtered.tsv", type=str, help="Name of the input file (default: metadata_661k_filtered.tsv)")
parser.add_argument("-o", dest="output", default="/vol/bakrep/assemblies1/", type=str, help="Path to output directory (default: /vol/bakrep/asseblies1/)")
args=parser.parse_args()

# Liest gefilterte Metadata-Datei ein, und nimmt sich daraus die Sample-ID
meta_id=pd.read_csv(args.input,sep='\t', usecols=["sample_id"])
filenames = []
dicnames = {'Dict' : ["test"], 'Amount' : [0]}
dic_df = pd.DataFrame(dicnames)
files = []

# Es wird eine Liste erstellt in der alle IDs gesammelt werden. Vor jeder ID werden anschließend nur die Zeichen 4-6 in einer weiteren Liste gesammelt.
# Wenn noch kein Ordner mit dieser Zeichenkombination existiert, wird einer erstellt. Die Dateien, die zum Schema passen werden dann in den passenden Ordner kopiert.
for line in range(0,meta_id.shape[0]):
    helpls = meta_id.sample_id[line]
    helpls_part = helpls[3:7]
    if not helpls_part in next(os.walk(args.output))[1]:
        os.mkdir(args.output + helpls_part)
    shutil.copy("/vol/bakrep/data/assemblies/" + helpls + ".fna.gz", args.output + str(helpls_part) + "/")
    