nextflow.enable.dsl=2

*/
	Runs GTDBtk taxonomic classification.
/*

process taxonomy {
    
	tag "${sample}" 
	//conda "gtdbtk=2.1.1"
	conda "/homes/lfenske/miniconda3/envs/gtdbtk_test/"
	errorStrategy { task.exitStatus in 104..143 ? 'retry' : 'ignore' } //Tries to ignore the error if GTDBtk cannot find marker genes.
	maxRetries 1
	cpus 4
	memory { 100.GB * task.attempt }
    
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
	output:
		path("gtdbtk.json")
		publishDir "./taxonomy/${batch}/",pattern: "gtdbtk.json", saveAs: { "${sample}.gtdbtk.json" }, mode: 'copy'
        
	script:
		"""
		export GTDBTK_DATA_PATH=$gtdb
		mkdir ./tmp_gtdbtk
		cp ${assemblyPath} ./tmp_gtdbtk
		gtdbtk classify_wf --genome_dir ./tmp_gtdbtk --out_dir "./" --prefix "${sample}" --extension gz --cpus $task.cpus
		ParseToJSON_gtdbtk.py -i "${sample}.bac120.summary.tsv"
		"""
}

*/
Runs checkm for assessing the quality of the genomes.
/*

process qualityCheck {

	tag "${sample}"    
	conda "checkm-genome=1.2.1"
	errorStrategy 'retry'
	maxRetries 3
	cpus 4
	memory { 80.GB * task.attempt }
    
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)

	output:
		path("checkm.json") 
		publishDir "./quality_control/${batch}/", saveAs: { filename -> "${sample}.checkm.json" }, mode: 'copy'

	script:
		"""
		mkdir ./tmp_checkm
		cp ${assemblyPath} ./tmp_checkm
		checkm lineage_wf ./tmp_checkm checkm/ -x .gz --tab_table --file "${sample}_results.tsv"
		ParseToJSON_checkm.py -i "${sample}_results.tsv"
		"""    
}

*/
	Runs checkm2 for assessing the quality of the genomes.
	Checkm2 is the new version of checkm which uses a machine learning approach for quality assessement.
/*

process qualityCheck2 {

	tag "${sample}"
	conda "/homes/lfenske/miniconda3/envs/checkm2/"
	errorStrategy 'retry'
	maxRetries 3
	cpus 4
	memory  { 60.GB * task.attempt }
	
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
		
	output:
		path("checkm2.json")
		publishDir "./quality_control2/${batch}/", saveAs: { filename -> "${sample}.checkm2.json" }, mode: 'copy'
		
	script:
		"""
		mkdir ./tmp_checkm2
		cp ${assemblyPath} ./tmp_checkm2
		/vol/bakrep/database/checkm2/bin/checkm2 predict --input ./tmp_checkm2 --output-directory ./checkm2 -x .gz
		ParseToJSON_checkm2.py -i "./checkm2/quality_report.tsv"
		"""
}

*/
	Runs Bakta for annotation.
/*

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
		tuple path("${sample}.json"), path("${sample}.gff3"), path("${sample}.gbff"), path("${sample}.ffn"), path("${sample}.faa")
		publishDir "./annotation/${batch}", pattern: "${sample}.{json,gff,gbff,ffn,faa}", saveAs: { "${sample}.bakta.${file(it).extension }" }, mode: 'copy'

	script:
		"""
		bakta --db "${params.baktadb}" --prefix "${sample}" --genus "${genus}" --species "${species}" --strain "${strain}" --keep-contig-headers --threads ${task.cpus} "${assemblyPath}" 
		"""
}

workflow {
*/
	Defines some default parametes, can adjust via the command line when runnig the pipeline.
/*
	params.gtdb = "/vol/bakrep/database/gtdb/release207v2/"
	params.baktadb = "/vol/software/share/baktadb"
	params.data = "/vol/bakrep/assemblies"
	results = Path.of(params.results).toAbsolutePath()
	
/* 
   Stop the pipeline in the case the specified output_dir is not empty.
*/
	if (results != null && results.list().size() != 0) {
    println "Warning: Directory $results is not empty."	
    System.exit(-1)    
}
	
	log.info """\
	         B A K R E P
	===================================
	samples   :   ${params.samples}
	data      :   ${params.data}
	baktadb   :   ${params.baktadb}
	gtdb      :   ${params.gtdb}
	"""
	.stripIndent()
	
*/
	Coverts the user iput of the the data folder to an absolute path and checks if the folder really contains data.
/*

	def dataPath = null
	if(params.data != null) {
		dataPath = Path.of(params.data).toAbsolutePath()
			if (dataPath.list().size() == 0) {
				println "Warning: Directory $dataPath is empty."
				System.exit(-1)
				}
	} else {
		println('Data directory is null! Please provide the Data directory path via --data')
		System.exit(-1)
}

*/
	Collects specific parameters from the metadata file.
	Defines the full file path from the user input and the folder name, which is a substring of the sample id.
/*

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
	annotation(samples)
     
}
