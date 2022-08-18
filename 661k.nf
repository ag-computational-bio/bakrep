nextflow.enable.dsl=2

process taxonomy {
    
    tag "${batch}" 
    conda "/homes/lfenske/miniconda3/envs/gtdbtk-2.1.1" 
    errorStrategy { task.exitStatus in 104..143 ? 'retry' : 'ignore' } 
    maxRetries 4
    cpus 4
    memory { 55.GB * task.attempt }
    
    input:
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
    output:
        path("${sample}.bac120.summary.tsv") 
        //publishDir pattern: "${sample}.bac120.summary.tsv", path: "gtdbtk/${batch}", mode: 'copy'  
        
    script:
    """
    mkdir ./temp
    cp ${assemblyPath} ./temp
    gtdbtk classify_wf --genome_dir ./temp --out_dir "./" --prefix "${sample}" --extension gz --cpus $task.cpus
    """
}

process taxonomyToJson {
    
    input:
        path("661k.bac120.summary.tsv")
        //tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
    output:
        file("${sample}.gtdb.json")
        publishDir path: "./taxonomy", mode: 'copy', pattern: "${sample}.gtdb.json" 
      
      script:
          """
          for tsv in *.bac120.summary.tsv 
          do 
              python3 /vol/bakrep/linda_test/scripts/ParseToJSON_gtdbtk.py -i \$tsv 
          done
          """

}

process qualityCheck {

    tag "${batch}"    
    conda "/homes/lfenske/miniconda3/envs/checkm"
    errorStrategy 'retry'
    maxRetries 4
    cpus 4
    memory { 40.GB * task.attempt }
    
    input:
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
    output:
        path("${sample}_results.tsv")
        publishDir pattern: "${sample}_results.tsv", path: "checkm/${batch}/", mode: 'copy'

    script:
    """
    mkdir ./temp
    cp ${assemblyPath} ./temp
    checkm lineage_wf ./temp checkm/ -x .gz --tab_table --file "${sample}_results.tsv"
    """    
}

process qcToJson {
    
    input:
        path("*_results.tsv")
        //tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
       
    output:
        file("*.checkm.json")  
        publishDir "./quality_control/", mode: 'copy', pattern: "*.checkm.json"  
      
      script:
          """
          for tsv in ./checkm/*/*.tsv
          do 
              python3 /vol/bakrep/linda_test/scripts/ParseToJSON_checkm.py -i \$tsv -o ./ 
          done
          """

}
        
process annotation {

    tag "${sample}"
    conda "/homes/lfenske/miniconda3/envs/bakta/"
    errorStrategy 'retry'
    maxRetries 4
    memory { 16.GB * task.attempt }
    cpus 8
   
    input:
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)

    output:
        file("${sample}.json")
        publishDir "./annotation/${batch}", saveAs: { filename -> "${sample}.bakta.json" }, mode: 'copy'

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
    taxonomyToJson(taxonomy.out)
    qcToJson(qualityCheck.out)
    
}
