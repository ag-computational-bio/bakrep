import sys
import pandas as pd
import numpy as numpy
from argparse import ArgumentParser

parser = ArgumentParser(description="Filtert metadata_ena_661K.txt nach passenden Spalten.")
parser.add_argument("-i", dest="input", default="metadata_ena_661K.txt", type=str, help="name of the input file (default: metadata_ena_661K.txt)")
parser.add_argument("-o", dest="output", default="metadata_filtered.tsv", type=str, help="name of the output file (default: metadata_filtered.tsv)")
args=parser.parse_args()


#reading the csv file; 'usecols' makes sure that only these specific columns are read in (name must be correct!);
#'na_filter' makes sure that empty rows are not filled with NA, but are really empty
# column 'scientific_name' is renamed to 'genus' and column for 'species' is inserted.
data_meta=pd.read_csv(args.input,sep='\t', usecols=["sample_id","scientific_name","strain","sub_strain","study_accession","run_accession","experiment_accession","first_public","project_name","study_title","country","collection_date","environment_biome","location"], na_filter=False)
data_meta.insert(11, "species", "unknown")
data_meta.rename(columns={"scientific_name": "genus"}, inplace=True)

#Goes through all columns and sets 'genus' and/or 'species' to 'unknown' if nothing is entered.
#If there is an entry in 'genus', it will be split at whitespace and the second part will be entered as 'species'.
#If only one string is entered, it will be set as 'genus' and 'species' will be set to 'unknown'.
for line in range(0,data_meta.shape[0]):
    if data_meta.genus[line] == "":
        data_meta.genus[line] = "unknown"
        data_meta.species[line] = "unknown"

    elif len(data_meta.genus[line].split(' ')) == 1:
        data_meta.species[line] = "unknown"
    
    else:
        data_meta.species[line] = data_meta.genus[line].split(' ')[1] 
        data_meta.genus[line] = data_meta.genus[line].split(' ')[0]
             
#If there is no entry in 'strain', it is checked if there is an entry in 'sub_strain', otherwise 'strain' is set to 'unknown'. 
    if data_meta.strain[line] == "":
        if data_meta.sub_strain[line] != "":
            data_meta.strain[line] = data_meta.sub_strain[line]
        else:
            data_meta.strain[line] = "unknown"

    if data_meta.strain[line].split(" ")[0] == data_meta.genus[line]:
        if len(data_meta.strain[line]) == 1:
            data_meta.strain[line]= "unknown"

        else:
            data_meta.strain[line] = data_meta.strain[line].lstrip(data_meta.genus[line] + " ")
            if data_meta.strain[line].split(" ")[0] == data_meta.species[line]:
                if len(data_meta.strain[line]) == 1:
                    data_meta.strain[line]= "unknown"
                else:
                    data_meta.strain[line] = data_meta.strain[line].lstrip(data_meta.species[line] + " ")
                    #print(data_meta.strain[line])

    
    if(len(data_meta.strain[line]) >= 20):
        print(data_meta.strain[line])  
 
data_meta.drop(columns="sub_strain", inplace=True) 
data_meta = data_meta.reindex(columns=['sample_id','genus','species','strain','study_accession','run_accession','experiment_accession','project_name','study_title','first_public','collection_date','country','environment_biome','location'])
data_meta.to_csv(args.output,sep='\t',index=0)
