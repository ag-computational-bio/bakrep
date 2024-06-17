#!/usr/bin/env python3

import json
import os
import pandas as pd
from argparse import ArgumentParser

relevant_fields = [
    "sample_id",
    "study_accession",
    "run_accession",
    "project_name",
    "isolation_source",
    "instrument_platform",
    "host",
    "first_public",
    "country",
    "collection_date",
    "center_name",
    "accession",
    "bio_material",
    "broker_name",
    "collected_by",
    "culture_collection",
    "depth",
    "environment_biome",
    "environment_feature",
    "environment_material",
    "environmental_package",
    "environmental_sample",
    "host_sex",
    "host_status",
    "host_tax_id",
    "instrument_model",
    "isolate",
    "lat",
    "location",
    "lon",
    "sample_alias",
    "secondary_sample_accession",
    "secondary_study_accession",
    "serotype",
    "serovar",
    "strain",
    "study_alias",
    "study_title",
    "sub_strain",
    "submission_accession"]


def cleanup_empty_field(text: str):
    t = text.strip()
    x = t.lower()
    if x == "" or x == "na" or x == "n.a.":
        return None
    return t


def parse_boolean(value):
    # As the value will most probably be a numpy._bool value,
    # we convert it to a standard python bool value here
    # This will simplify json conversion later, as we won't
    # require any additional encoders
    if value == True:
        return True
    if value == False:
        return False
    # Fallback if it is a string
    t = cleanup_empty_field(value)
    if t == "TRUE":
        return True
    if t == "FALSE":
        return False
    return None


def parse_number(text: str):
    t = cleanup_empty_field(text)
    try:
        return float(t)
    except:
        return None


def parse_location(data, line: int):
    location = None
    lon = parse_number(data.lon[line])
    lat = parse_number(data.lat[line])
    if lon != None and lat != None:
        location = {"type": "Point", "coordinates": [lon, lat]}
    return location


def read_row_to_dict(data, line):
    metadata = {
        "id": cleanup_empty_field(data.sample_id[line]),
        "study_accession": cleanup_empty_field(data.study_accession[line]),
        "run_accession": cleanup_empty_field(data.run_accession[line]),
        "project_name": cleanup_empty_field(data.project_name[line]),
        "isolation_source": cleanup_empty_field(data.isolation_source[line]),
        "instrument_platform": cleanup_empty_field(data.instrument_platform[line]),
        "host": cleanup_empty_field(data.host[line]),
        "first_public": cleanup_empty_field(data.first_public[line]),
        "country": cleanup_empty_field(data.country[line]),
        "collection_date": cleanup_empty_field(data.collection_date[line]),
        "center_name": cleanup_empty_field(data.center_name[line]),
        "accession": cleanup_empty_field(data.accession[line]),
        "bio_material": cleanup_empty_field(data.bio_material[line]),
        "broker_name": cleanup_empty_field(data.broker_name[line]),
        "collected_by": cleanup_empty_field(data.collected_by[line]),
        "culture_collection": cleanup_empty_field(data.culture_collection[line]),
        "depth": cleanup_empty_field(data.depth[line]),
        "environment_biome": cleanup_empty_field(data.environment_biome[line]),
        "environment_feature": cleanup_empty_field(data.environment_feature[line]),
        "environment_material": cleanup_empty_field(data.environment_material[line]),
        "environmental_package": cleanup_empty_field(data.environmental_package[line]),
        "environmental_sample": parse_boolean(data.environmental_sample[line]),
        "host_sex": cleanup_empty_field(data.host_sex[line]),
        "host_status": cleanup_empty_field(data.host_status[line]),
        "host_tax_id": cleanup_empty_field(data.host_tax_id[line]),
        "instrument_model": cleanup_empty_field(data.instrument_model[line]),
        "isolate": cleanup_empty_field(data.isolate[line]),
        "location": parse_location(data, line),
        "sample_alias": cleanup_empty_field(data.sample_alias[line]),
        "secondary_sample_accession": cleanup_empty_field(data.secondary_sample_accession[line]),
        "secondary_study_accession": cleanup_empty_field(data.secondary_study_accession[line]),
        "serotype": cleanup_empty_field(data.serotype[line]),
        "serovar": cleanup_empty_field(data.serovar[line]),
        "strain": cleanup_empty_field(data.strain[line]),
        "study_alias": cleanup_empty_field(data.study_alias[line]),
        "study_title": cleanup_empty_field(data.study_title[line]),
        "sub_strain": cleanup_empty_field(data.sub_strain[line]),
        "submission_accession": cleanup_empty_field(data.submission_accession[line])
    }

    return metadata


def main():
    parser = ArgumentParser(
        description="Creates json files from assembly metadata.")
    parser.add_argument("-i", dest="input", default="metadata_ena_661K.head.txt",
                        type=str, help="Name of the input file (default: metadata_ena_661K.head.txt)")
    parser.add_argument("-o", dest="output", default=".",
                        type=str, help="Output directory")
    args = parser.parse_args()

    meta_data = pd.read_csv(args.input, sep='\t',
                            usecols=relevant_fields, na_filter=False)
    for line in range(0, meta_data.shape[0]):
        metadata = read_row_to_dict(meta_data, line)
        json_string = json.dumps(metadata)
        id = metadata["id"]

        output_path = f"{args.output}/{id[3:7]}/{metadata['id']}.metadata.json"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as file:
            file.write(json_string)


if __name__ == "__main__":
    main()
