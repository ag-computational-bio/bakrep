nextflow.enable.dsl=2

process taxonomy {
    
    tag "${batch}" 
    conda "/homes/lfenske/miniconda3/envs/gtdbtk-2.1.1" //!!! export GTDBTK_DATA_PATH=/vol/bakrep/database/gtdb/release207v2/ !!!
    errorStrategy { task.exitStatus in 104..143 ? 'retry' : 'ignore' } 
    maxRetries 4
    cpus 8
    memory { 55.GB * task.attempt } // GTDBtk braucht laut Doku etwa 55GB --> vielleicht reicht für jeweils 1 Genom auch weniger
    
    input:
        //path genome_dir // Wenn man nur einen Ordner übergeben will
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
    output:
        path("${sample}.bac120.summary.tsv") 
        //publishDir pattern: "${sample}.bac120.summary.tsv", path: "gtdbtk/${batch}", mode: 'copy'  
        //file("*.log")
        //publishDir "./gtdbtk_logs/${batch}/", saveAs: { filename -> "${sample}.gtdbtk.log" }, mode: 'copy'
        
    script:
    """
    mkdir ./temp
    cp ${assemblyPath} ./temp
    gtdbtk classify_wf --genome_dir ./temp --out_dir "./" --prefix "${sample}" --extension gz --cpus $task.cpus
    """
}

process taxonomyToJson {
    
    input:
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        path("gtdbtk/${batch}/${sample}.bac120.summary.tsv")
        
    output:
        file(".gtdbtk.json")
        publishDir "./taxonomy/${batch}/", saveAs: { filename -> "${sample}.gtdbtk.json" }, mode: 'copy'
      
      script:
          """
              python3 /vol/bakrep/linda_test/scripts/ParseToJSON_gtdbtk.py -i gtdbtk/${batch}/${sample}.bac120.summary.tsv
          """

}

process qualityCheck {

    tag "${batch}"    
    conda "/homes/lfenske/miniconda3/envs/checkm"
    errorStrategy 'retry'
    maxRetries 4
    cpus 8
    memory { 40.GB * task.attempt } // checkm braucht laut Doku ca. 40 GB
    
    input:
        //path genome_dir
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        
    output:
        path("${sample}_results.tsv") // richtig benennen! Damit nicht immer überschrieben wird.
        //publishDir pattern: "${sample}_results.tsv", path: "checkm/${batch}/", mode: 'copy'

    script:
    """
    mkdir ./temp
    cp ${assemblyPath} ./temp
    checkm lineage_wf ./temp checkm/ -x .gz --tab_table --file "${sample}_results.tsv"
    """    
}

process qcToJson {
    
    input:
        tuple val(sample), val(genus), val(species), val(strain), val(batch), path(assemblyPath)
        path("checkm/${batch}/${sample}_results.tsv")
       
    output:
        file(".checkm.json") 
        publishDir "./quality_control/${batch}/", saveAs: { filename -> "${sample}.checkm.json" }, mode: 'copy'
      
      script:
          """
          python3 /vol/bakrep/linda_test/scripts/ParseToJSON_checkm.py -i checkm/${batch}/${sample}_results.tsv
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
    
    // der eingegebene Pfad-String wird hier in einen korrekten Pfad umgewandelt.
    def dataPath = null
    if(params.data != null) {
        dataPath = Path.of(params.data).toAbsolutePath() //Paths.get ist die ältere Variante und klappt deshalb vermutlich nicht...
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
    
    
    //params.genome_dir = "/vol/bakrep/linda_test/no_backup/nextflow/" // Für die Übergabe von einem einzelnen Ordner.
    //genome_dir = Channel.fromPath(params.genome_dir)
 
     
    taxonomy(samples)
    qualityCheck(samples)
    annotation(samples)
    taxonomyToJson(samples, taxonomy.out)
    qcToJson(samples, qualityCheck.out)
    
}
