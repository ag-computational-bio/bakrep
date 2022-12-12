:microbe: bakrep
=======

# :hammer_and_wrench:  Installation  

The workflow is written in [Nextflow](https://www.nextflow.io/docs/latest/index.html) and all tools used can be installed via [Conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).

Clone the repository:

```bash
git clone https://github.com/ag-computational-bio/bakrep
```
## Running

Run the nextflow-script:

```bash
nextflow run /nextflow/661k.nf 
-c /nextflow/nextflow.config
--samples <metadata> 
--setupdir <setupdir>
```

# 🗃️ Parameters 

All optional parameters:

```
--samples                  Metadata file which contains sample-id, genus, species and strain. 
--setupdir                 Path to folder which contains all data.
--db                       Path to database folder which contains GTDBtk, Checkm2 and Bakta database (default: $setupdir/database)
--gtdb                     Path to GTDBtk database (no need, when set --db)
--checkm2db                Path to Checkm2 folder (no need, when set --db)
--baktadb                  Path to Bakta database (no need, when set --db)
--data                     Path to input assembly files (default: $setuptdir/assemblies)
--results                  Path to output folder (default: $setupdir/results)
```

# :page_facing_up: Citation

>Blackwell GA, Hunt M, Malone KM, Lima L, Horesh G, Alako BTF, Thomson NR, Iqbal Z. Exploring bacterial diversity via a curated and searchable snapshot of >archived DNA sequences. PLoS Biol. 2021 Nov 9;19(11):e3001421. doi: 10.1371/journal.pbio.3001421. PMID: 34752446; PMCID: PMC8577725.

## Tools

- [GTDBtk](https://github.com/Ecogenomics/GTDBTk)
- [Checkm2](https://github.com/chklovski/CheckM2)
- [assembly-scan](https://github.com/rpetit3/assembly-scan)
- [mlst](https://github.com/tseemann/mlst)
- [Bakta](https://github.com/oschwengers/bakta#usage)


