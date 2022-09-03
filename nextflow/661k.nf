nextflow.enable.dsl=2

process taxonomy {
    
	tag "${batch}" 
	conda "gtdbtk=2.1.1"
	errorStrategy { task.exitStatus in 104..143 ? 'retry' : 'ignore' } 
	//Tries to ignore the error if GTDBtk cannot find marker genes.
	//errorStrategy 'retry'
	maxRetries 3
	cpus 1
	memory { 100.GB * task.attempt }
    
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
	output:
		tuple path(".gtdbtk.json"), path("gtdbtk.log")
		publishDir "./taxonomy/${batch}/", saveAs: { filename -> "${sample}.gtdbtk.json" }, mode: 'copy'
		publishDir "./taxonomy/${batch}/", pattern: "gtdbtk.log", saveAs: { "${sample}.gtdbtk.log"}, mode: 'copy'
        
	script:
		"""
		export GTDBTK_DATA_PATH=/vol/bakrep/database/gtdb/release207v2/
		mkdir ./tmp_gtdbtk
		cp ${assemblyPath} ./tmp_gtdbtk
		gtdbtk classify_wf --genome_dir ./tmp_gtdbtk --out_dir "./" --prefix "${sample}" --extension gz --cpus $task.cpus
		ParseToJSON_gtdbtk.py -i "${sample}.bac120.summary.tsv"
		"""
}

process qualityCheck {

	tag "${batch}"    
	conda "checkm-genome=1.2.1"
	errorStrategy 'retry'
	maxRetries 3
	cpus 1
	memory { 80.GB * task.attempt }
    
	input:
		tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)

	output:
		file(".checkm.json") 
		publishDir "./quality_control/${batch}/", saveAs: { filename -> "${sample}.checkm.json" }, mode: 'copy'

	script:
		"""
		mkdir ./tmp_checkm
		cp ${assemblyPath} ./tmp_checkm
		checkm lineage_wf ./tmp_checkm checkm/ -x .gz --tab_table --file "${sample}_results.tsv"
		ParseToJSON_checkm.py -i "${sample}_results.tsv"
		"""    
}

process annotation {

	tag "${batch}"
	conda "bakta=1.5.0"
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

	def dataPath = null
	if(params.data != null) {
		dataPath = Path.of(params.data).toAbsolutePath()
		print("Data: ${dataPath}")
	} else {
		println('Data directory is null! Please provide the Data directory path via --data')
		System.exit(-1)
}
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
	annotation(samples)
   
// Parameters to pass: --samples; --data; --baktadb;   
}