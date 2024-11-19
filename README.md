[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-brightgreen.svg)](https://github.com/ag-computational-bio/bakrep/blob/main/LICENSE)

:microbe: BakRep: A searchable large-scale web repository for bacterial genomes, characterizations and metadata
=======

BakRep is a searchable large-scale web repository for bacterial genomes enriched with standardized genome characterizations and related metadata. Researchers can query this repository via a flexible search engine thus combining genomic information and metadata. We gratefully acknowledge the initial assembly of all genomes, which was conducted by _Blackwell et al._ in 2021.

The web repository containing all results is available here: https://bakrep.computational.bio/

# :mag: Background 

Public databases are brimming with bacterial WGS data posing a genetic treasure for many different applications. However, most analyses focus on particular tiny subgroups that are hard to establish from raw sequencing data or metadata, only. To this end, BakRep (Denglish blend of Bakterien & Repository) provides access to both comprehensive standardized genome characterizations and metadata via a flexible search engine. BakRep comprises assembly QC metrics, robust taxonomic classifications, MLST typing, genome annotations and original metadata for a precious collection of bacterial genomes assembled by _Blackwell et al._.

# :hammer_and_wrench:  Installation  

The workflow is written in [Nextflow](https://www.nextflow.io/docs/latest/index.html) and all tools are installed via [Conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html). Python3 is also required to create the result json files.

Clone the repository:

```bash
git clone https://github.com/ag-computational-bio/bakrep
```

## Running

Run the nextflow-script:

```bash
nextflow run /nextflow/661k.nf 
--samples <metadata> 
--setupdir <setupdir>
-with-conda
```

# 🗃️ Parameters 

All optional parameters:

```
--samples             Metadata file which contains sample-id, genus, species and strain. 
--setupdir            Path to folder which contains all data.
--db                  Path to database folder which contains GTDBtk, CheckM2 and Bakta database (default: $setupdir/database)
--gtdb                Path to GTDBtk database (no need, when set --db)
--checkm2db           Path to CheckM2 database (no need, when set --db)
--baktadb             Path to Bakta database (no need, when set --db)
--data                Path to input assembly files (default: $setuptdir/assemblies)
--results             Path to output folder (default: $setupdir/results)
```

# :page_facing_up: Citation

>Fenske L, Jelonek L, Goesmann A, Schwengers O. BakRep - a searchable large-scale web repository for bacterial genomes, characterizations and metadata. Microb Genom. 2024 Oct;10(10):001305. doi: 10.1099/mgen.0.001305. PMID: 39475723; PMCID: PMC11524574.

>Blackwell GA, Hunt M, Malone KM, Lima L, Horesh G, Alako BTF, Thomson NR, Iqbal Z. Exploring bacterial diversity via a curated and searchable snapshot of >archived DNA sequences. PLoS Biol. 2021 Nov 9;19(11):e3001421. doi: 10.1371/journal.pbio.3001421. PMID: 34752446; PMCID: PMC8577725.

## Tools

- [GTDBtk](https://github.com/Ecogenomics/GTDBTk)
- [Checkm2](https://github.com/chklovski/CheckM2)
- [assembly-scan](https://github.com/rpetit3/assembly-scan)
- [mlst](https://github.com/tseemann/mlst)
- [Bakta](https://github.com/oschwengers/bakta)
