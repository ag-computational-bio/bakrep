nextflow.enable.dsl=2

/*
	Runs GTDBtk for taxonomic classification.
*/

process taxonomy {
    
	tag "${sample}" 
	//conda "gtdbtk=2.1.1"
	conda "/homes/lfenske/miniconda3/envs/gtdbtk_test/"
	errorStrategy { task.exitStatus in 104..143 ? 'retry' : 'ignore' } //Tries to ignore the error if GTDBtk cannot find marker genes.
	maxRetries 3
	cpus 1
	memory { 150.GB * task.attempt }
    
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
	output:
		path("${sample}.gtdbtk.json.gz")
		publishDir "${params.results}/taxonomy/${batch}/", pattern: "${sample}.gtdbtk.json.gz", mode: 'copy'
        
	script:
		"""
		export GTDBTK_DATA_PATH="${params.gtdb}"
		mkdir ./tmp_gtdbtk
		cp ${assemblyPath} ./tmp_gtdbtk
		gtdbtk classify_wf --genome_dir ./tmp_gtdbtk --out_dir "./" --prefix "${sample}" --extension gz --cpus ${task.cpus}
		ParseToJSON_gtdbtk.py -i "${sample}.bac120.summary.tsv" -o "${sample}.gtdbtk.json"
		gzip "${sample}.gtdbtk.json"
		"""
}

/*
	Runs CheckM for assessing the quality of the genomes.
*/

process qualityCheck {

	tag "${sample}"    
	conda "checkm-genome=1.2.1"
	errorStrategy 'retry'
	maxRetries 3
	cpus 1
	memory { 100.GB * task.attempt }
    
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)

	output:
		path("${sample}.checkm.json.gz") 
		publishDir "${params.results}/quality_control/${batch}/", pattern: "${sample}.checkm.json.gz", mode: 'copy'

	script:
		"""
		mkdir ./tmp_checkm
		cp ${assemblyPath} ./tmp_checkm
		checkm lineage_wf ./tmp_checkm checkm/ -x .gz --tab_table --file "${sample}_results.tsv"
		ParseToJSON_checkm.py -i "${sample}_results.tsv" -o "${sample}.checkm.json"
		gzip "${sample}.checkm.json"
		"""    
}

/*
	Runs CheckM2 for assessing the quality of the genomes. 
	CheckM2 is the new version of CheckM which uses a machine learning approach for quality assessement.
*/

process qualityCheck2 {

	tag "${sample}"
	conda "/homes/lfenske/miniconda3/envs/checkm2/"
	errorStrategy 'retry'
	maxRetries 3
	cpus 1
	memory  { 20.GB * task.attempt }
	
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
		
	output:
		path("${sample}.checkm2.json.gz")
		publishDir "${params.results}/quality_control2/${batch}/", pattern: "${sample}.checkm2.json.gz", mode: 'copy'
		
	script:
		"""
		mkdir ./tmp_checkm2
		cp ${assemblyPath} ./tmp_checkm2
		/vol/bakrep/database/checkm2/bin/checkm2 predict --input ./tmp_checkm2 --output-directory ./checkm2 -x .gz
		ParseToJSON_checkm2.py -i "./checkm2/quality_report.tsv" -o "${sample}.checkm2.json"
		gzip "${sample}.checkm2.json"
		"""
}

/*
Runs mlst for multilocus-sequenz-typing.
*/

process mlst {
	
	tag "${sample}"
	conda "mlst=2.23.0"
	errorStrategy 'retry'
	maxRetries 3
	cpus 1
	memory { 8.GB * task.attempt }
	
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
	
	output: 
		path("${sample}.mlst.json.gz")
		publishDir "${params.results}/mlst/${batch}/", pattern: "${sample}.mlst.json.gz", mode: 'copy'
			 
	script:
		"""
		mkdir ./tmp_mlst
		cp ${assemblyPath} ./tmp_mlst
		mlst --json "${sample}.mlst.json" --label "${sample}" ./tmp_mlst/*
		gzip "${sample}.mlst.json"
		"""
}

/*
	Runs Bakta for annotation.
*/

process annotation {

	tag "${sample}"
	conda "bakta=1.5.1"
	errorStrategy 'retry'
	maxRetries 3
	cpus 4
	memory { 16.GB * task.attempt }
   
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath) 

	output:
		tuple path("${sample}.bakta.json.gz"), path("${sample}.bakta.gff3.gz"), path("${sample}.bakta.gbff.gz"), path("${sample}.bakta.ffn.gz"), path("${sample}.bakta.faa.gz")
		publishDir "${params.results}/annotation/${batch}", pattern: "${sample}.bakta.{json,gff3,gbff,ffn,faa}.gz", mode: 'copy'
		//publishDir "${params.results}/annotation/${batch}", pattern: "${sample}.{json,gff,gbff,ffn,faa}.gz", saveAs: { "${sample}.bakta.${file(it).extension }" }, mode: 'copy'

	script:
		"""
		bakta --db "${params.baktadb}" --prefix "${sample}.bakta" --genus "${genus}" --species "${species}" --strain "${strain}" --keep-contig-headers --threads ${task.cpus} "${assemblyPath}" 
		gzip "${sample}.bakta.json" "${sample}.bakta.gff3" "${sample}.bakta.gbff" "${sample}.bakta.ffn" "${sample}.bakta.faa"
		"""
}


workflow {
/*
	Defines some default parameters. These can be adjusted via the command line when runnig the pipeline.
	The default executor is 'local'. To use the slurm cluster add the parameter '-profile slurm'.
*/
	params.gtdb = "/vol/bakrep/database/gtdb/release207v2"
	params.baktadb = "/vol/software/share/bakta/db"
	params.data = "/vol/bakrep/assemblies"
	results = Path.of(params.results).toAbsolutePath()
	
/*
	Stop the pipeline in the case the specified output_dir is not empty.
*/
	if (results != null && results.list().size() != 0) {
    println "!!! WARNING: Directory $results is not empty. Rsync will overwrite existing files. !!!"
    //System.exit(-1)    
	}
	
	log.info """\
	         B A K R E P
	===================================
	samples   :   ${params.samples}
	data      :   ${params.data}
	baktadb   :   ${params.baktadb}
	gtdb      :   ${params.gtdb}
	results   :   ${params.results}
	"""
	.stripIndent()
	
/*
	Coverts the user input of the the data folder to an absolute path and checks if the folder really contains data.
*/

	def dataPath = null
	if(params.data != null) {
		dataPath = Path.of(params.data).toAbsolutePath()
			if (dataPath.list().size() == 0) {
				println "WARNING: Directory $dataPath is empty."
				System.exit(-1)
				}
	} else {
		println('Data directory is null! Please provide the Data directory path via --data')
		System.exit(-1)
}

/*
	Collects specific parameters from the metadata file.
	Defines the full file path from the user input and the folder name, which is a substring of the sample id.
*/

	samples = channel.fromPath( params.samples )
	.splitCsv( sep: '\t', skip: 1  )
	.map( {
		def sample = it[0]
		def genus = it[1]
		def species = it[2]
		def strain = it[3]
		def batch = sample.substring(3,7)
		def assemblyPath = dataPath.resolve(batch).resolve("${sample}.fna.gz").toAbsolutePath()
		return [sample, genus, species, strain, batch, assemblyPath]
	} )
	//.view()
         
	taxonomy(samples)
	qualityCheck(samples)
	qualityCheck2(samples)
	mlst(samples)
	annotation(samples)     
}

