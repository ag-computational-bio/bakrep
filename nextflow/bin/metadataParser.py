import sys
import pandas as pd
import numpy as numpy
from argparse import ArgumentParser

parser = ArgumentParser(description="Filtert metadata_ena_661K.txt nach passenden Spalten.")
parser.add_argument("-i", dest="input", default="metadata_ena_661K.txt", type=str, help="Name der Inputdatei (Default: metadata_ena_661K.txt)")
parser.add_argument("-o", dest="output", default="metadata_filtered.tsv", type=str, help="Name der Outputdatei (Default: metadata_filtered.tsv)")
args=parser.parse_args()


#Einlesen des csv-Datei; 'usecols' sorgt dafür, dass nur diese spezifischen Spalten eingelesen werden (Name muss stimmen!);
#'na_filter' sorgt dafür, dass leere Zeilen nicht mit NA gefüllt werden, sondern wirklich leer sind
# Spalte 'scientific_name' wird in 'genus' umbenannt und Spalte für 'species' wird eingefügt.
data_meta=pd.read_csv(args.input,sep='\t', usecols=["sample_id","scientific_name","strain","sub_strain","study_accession","run_accession","experiment_accession","first_public","project_name","study_title","country","collection_date","environment_biome","location"], na_filter=False)
data_meta.insert(11, "species", "unknown")
data_meta.rename(columns={"scientific_name": "genus"}, inplace=True)

#Geht durch alle Spalten und setzt 'genus' und/oder 'species' auf 'unknown', falls nichts eingetragen sein sollte.
#Ist ein Eintrag in 'genus' vorhanden, wird dieser am Leerzeichen gesplitet und der zweite Teil wird als 'species' eingetragen.
#Ist nur ein string eingetragen, wird dieser als 'genus' gesetzt und 'species' auf 'unknown'.
for line in range(0,data_meta.shape[0]):
    if data_meta.genus[line] == "":
        data_meta.genus[line] = "unknown"
        data_meta.species[line] = "unknown"

    elif len(data_meta.genus[line].split(' ')) == 1:
        data_meta.species[line] = "unknown"
    
    else:
        data_meta.species[line] = data_meta.genus[line].split(' ')[1] 
        data_meta.genus[line] = data_meta.genus[line].split(' ')[0]
             
#Wenn in 'strain' kein Eintrag vorhanden ist, wird geschaut ob in 'sub_strain' ein Eintrag ist, ansonsten wird 'strain' auf 'unknown' gesetzt. 
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
 
#Die Spalte 'sub_strain' wird zum Schluss nicht mehr benötigt und deshalb gelöscht.
data_meta.drop(columns="sub_strain", inplace=True) 
data_meta = data_meta.reindex(columns=['sample_id','genus','species','strain','study_accession','run_accession','experiment_accession','project_name','study_title','first_public','collection_date','country','environment_biome','location'])
data_meta.to_csv(args.output,sep='\t',index=0)
