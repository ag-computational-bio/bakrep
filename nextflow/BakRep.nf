nextflow.enable.dsl=2

/*
        Runs GTDBtk for taxonomic classification. Use classification for Bakta input.
*/

process taxonomy {

        tag "${sample}"
        conda "bioconda::gtdbtk=2.7.2"
        cpus 2
        memory  { 32.GB * task.attempt }
        errorStrategy = { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input:
                tuple val(sample), val(batch), path(assemblyPath)

        output:
                env(gtdbGenus), emit: gtdbGenus
                env(gtdbSpecies), emit: gtdbSpecies
                path("${sample}.gtdbtk.json.gz")
                publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.gtdbtk.json.gz", mode: 'copy'
                stdout()

        script:
                """
                export GTDBTK_DATA_PATH="${params.gtdb}"
                mkdir ./tmp_gtdbtk
                cp ${assemblyPath} ./tmp_gtdbtk
                gtdbtk classify_wf --genome_dir "./tmp_gtdbtk" --out_dir "./" --prefix "${sample}" --extension fa --pplacer_cpus 1
                ParseToJSON_gtdbtk.py -i "${sample}.bac120.summary.tsv" -o "${sample}.gtdbtk.json"
                
                gtdbGenus=\$(cat "${sample}.gtdbtk.json" | jq -r '.classification.genus')
                gtdbSpecies=\$(cat "${sample}.gtdbtk.json" | jq -r '.classification.species | split(" ")[1]')
                echo "${sample}\t\${gtdbGenus}\t\${gtdbSpecies}"

                gzip "${sample}.gtdbtk.json"
                """
}

/*
        Runs CheckM2 for assessing the quality of the genomes.
        CheckM2 is the new version of CheckM which uses a machine learning approach for quality assessement.
*/

process qualityCheck {

        tag "${sample}"
        conda "checkm2=1.1.0"
        cpus 1
        memory { 20.GB * task.attempt }
        errorStrategy = { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input:
                tuple val(sample), val(batch), path(assemblyPath)

        output:
                path("${sample}.checkm2.json.gz")
                publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.checkm2.json.gz", mode: 'copy'

        script:
                """
                mkdir -p ./tmp_qc
                cp ${assemblyPath} ./tmp_qc
                checkm2 predict --input ./tmp_qc --output-directory ./qc -x .fa --database_path ${params.checkm2db}
                ParseToJSON_checkm2.py -i "./qc/quality_report.tsv" -o "${sample}.checkm2.json"
                gzip "${sample}.checkm2.json"
                """
}

process assemblyScan {

        tag "${sample}"
    	conda "bioconda::assembly-scan=1.0.0"
        cpus 1
        memory { 2.GB * task.attempt }
        errorStrategy = { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input: tuple val(sample), val(batch), path(assemblyPath)

        output:
                path("${sample}.assemblyscan.json.gz")
                publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.assemblyscan.json.gz", mode: 'copy'

        script:
                """
                mkdir -p  ./tmp_scan
                cp ${assemblyPath} ./tmp_scan
                assembly-scan ./tmp_scan/* --json > ${sample}.assemblyscan.json
                gzip "${sample}.assemblyscan.json"
                """
}

/*
        Runs mlst for multilocus-sequence-typing.
*/

process mlst {

        tag "${sample}"
        conda "bioconda::mlst=2.35.0"
        cpus 1
        memory { 2.GB * task.attempt }
        errorStrategy = { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input:
                tuple val(sample), val(batch), path(assemblyPath)

        output:
                path("${sample}.mlst.json.gz")
                publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.mlst.json.gz", mode: 'copy'

        script:
                """
                mkdir -p ./tmp_mlst
                cp ${assemblyPath} ./tmp_mlst
                mlst --json "${sample}.mlst.json" --label "${sample}" ./tmp_mlst/*
                gzip "${sample}.mlst.json"
                """
}

/*
        Runs Bakta for annotation. Uses the genus and species classified by GTDB-tk as input.
*/

        process annotation {

        tag "${sample}"
        conda "bioconda::bakta=1.12.0 diamond=2.2.0"
        cpus 4
        memory { 16.GB * task.attempt }
        errorStrategy = { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input:
                tuple val(sample), val(batch), path(assemblyPath)
                val(gtdbGenus)
                val(gtdbSpecies)

        output:
                path("${sample}.*")
                publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.bakta.*.gz", mode: 'copy'
            
        script:
                """
                bakta --db "${params.baktadb}" --prefix "${sample}.bakta" --genus "${gtdbGenus}" --species "${gtdbSpecies}" --keep-contig-headers --threads ${task.cpus} --skip-plot -o "./" --force "${assemblyPath}"
                gzip ${sample}.bakta.* 
                """
}

    process amr {

    tag "${sample}"
    conda "bioconda::ncbi-amrfinderplus=4.2.7"
    cpus 1
    memory { 4.GB * task.attempt }
    errorStrategy = { task.attempt < 5 ? 'retry' : 'ignore' }
    maxRetries 5

    input:
            tuple val(sample), val(batch), path(assemblyPath)
            val(gtdbGenus)
            val(gtdbSpecies)


    output:
            path("${sample}.*")
            publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.amrfinderplus.*.gz", mode: 'copy'

    script:
            """
            amrfinder --database "${params.amrdb}" --plus --organism "${gtdbGenus}"_"${gtdbSpecies}"  -n "${assemblyPath}" -o "${sample}.amrfinderplus.tsv"
            gzip "${sample}.amrfinderplus.tsv"
            """
}  


workflow {
        log.info """\
                 B A K R E P
        ===================================
        setupdir  :   ${params.setupdir}
        samples   :   ${params.samples}
        data      :   ${params.data}
        results   :   ${params.results}
        baktadb   :   ${params.baktadb}
        gtdb      :   ${params.gtdb}
        checkm2db :   ${params.checkm2db}
        amrdb     :   ${params.amrdb}
        """
        .stripIndent()

/*
        Setupdir where the databases and input files are located.
*/
        if(params.setupdir == null) {
    println('Setup directory is null! Please provide the setup directory via --setupdir')
    System.exit(-1)
}
/*
        Option to stop the pipeline in the case the specified output_dir is not empty.
*/
        results = Path.of(params.results).toAbsolutePath()

        if (results != null && results.list().size() != 0) {
    println "!!! WARNING: Directory $results is not empty. Rsync will overwrite existing files. !!!"
    //System.exit(-1)
        }

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
                def batch = sample.substring(3,7)
                def assemblyPath = dataPath.resolve(batch).resolve("${sample}.fa").toAbsolutePath()
                return [sample,batch,assemblyPath]
        } )
        
        taxonomy(samples)
        qualityCheck(samples)
        assemblyScan(samples)
        mlst(samples)
        annotation(samples, taxonomy.out.gtdbGenus, taxonomy.out.gtdbSpecies)
        amr(samples, taxonomy.out.gtdbGenus, taxonomy.out.gtdbSpecies)      
}