nextflow.enable.dsl=2

/*
        Runs GTDBtk for taxonomic classification. Use classification for Bakta input.
*/

process taxonomy {

        tag "${sample}"
	publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.gtdbtk.json.gz", mode: 'link'
	conda "/shared/conda-envs/taxonomy.yaml"
        cpus 4
	memory  { 20.GB * task.attempt }
        errorStrategy { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

input:
    tuple val(sample), val(batch), path(assemblyPath)

output:
    tuple val(sample), val(batch), path(assemblyPath), path("${sample}.gtdbtk.json.gz"), emit: taxonomy


        script:
                """
                export GTDBTK_DATA_PATH="${params.gtdb}"
                mkdir ./tmp_gtdbtk
                cp ${assemblyPath} ./tmp_gtdbtk
                gtdbtk classify_wf --genome_dir "./tmp_gtdbtk" --out_dir "./" --prefix "${sample}" --extension fa --pplacer_cpus ${task.cpus} --cpus ${task.cpus}
                ParseToJSON_gtdbtk.py -i "${sample}.bac120.summary.tsv" -o "${sample}.gtdbtk.json"
                
                export gtdbGenus=\$(jq -r '.classification.genus' "${sample}.gtdbtk.json")
		export gtdbSpecies=\$(jq -r '.classification.species | split(" ")[1]' "${sample}.gtdbtk.json")
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
	publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.checkm2.json.gz", mode: 'link'
	conda "/shared/conda-envs/qualityCheck.yaml"
        cpus 1
        memory { 10.GB * task.attempt }
        errorStrategy { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input:
                tuple val(sample), val(batch), path(assemblyPath)

        output:
                path("${sample}.checkm2.json.gz")

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
	publishDir "${params.results}/${batch}/${sample}/", mode: 'link'
	conda "/shared/conda-envs/assemblyScan.yaml"
        cpus 1
        memory { 1.GB * task.attempt }
        errorStrategy { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input: 
		tuple val(sample), val(batch), path(assemblyPath)

        output:
                path("${sample}.assemblyscan.json.gz")

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
	publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.mlst.json.gz", mode: 'link'
	conda "/shared/conda-envs/mlst.yaml"
        cpus 1
        memory { 1.GB * task.attempt }
        errorStrategy { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5

        input:
                tuple val(sample), val(batch), path(assemblyPath)

        output:
                path("${sample}.mlst.json.gz")

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
	publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.bakta.*.gz", mode: 'link'
	conda "/shared/conda-envs/annotation.yaml"
        cpus 4
        memory { 14.GB * task.attempt }
        errorStrategy { task.attempt < 5 ? 'retry' : 'ignore' }
        maxRetries 5
//	clusterOptions '--exclude=worker-small-[1-29]'

        input:
                tuple val(sample), val(batch), path(assemblyPath), path(taxonomyJson)

        output:
                path("${sample}.*")
            
        script:
                """
                gunzip -c "${taxonomyJson}" > taxonomy.json
                gtdbGenus=\$(jq -r '.classification.genus // empty' taxonomy.json)
                gtdbSpecies=\$(jq -r '(.classification.species // "") | split(" ")[1] // empty' taxonomy.json)
                taxArgs=()
                if [ -n "\$gtdbGenus" ]; then taxArgs+=(--genus "\$gtdbGenus"); fi
                if [ -n "\$gtdbSpecies" ]; then taxArgs+=(--species "\$gtdbSpecies"); fi

                bakta --db "${params.baktadb}" --prefix "${sample}.bakta" \${taxArgs[@]+"\${taxArgs[@]}"} --keep-contig-headers --threads ${task.cpus} --skip-plot -o "./" --force "${assemblyPath}"
                gzip ${sample}.bakta.*
                """
}


    process amr {

	tag "${sample}"
	publishDir "${params.results}/${batch}/${sample}/", pattern: "${sample}.amrfinderplus.*.gz", mode: 'link'
	conda "/shared/conda-envs/amr.yaml"
	cpus 1
	memory { 2.GB * task.attempt }
	errorStrategy { task.attempt < 5 ? 'retry' : 'ignore' }
	maxRetries 5

    input:
            tuple val(sample), val(batch), path(assemblyPath)


    output:
            path("${sample}.*")

    script:
            """
            amrfinder --database "${params.amrdb}" --plus -n "${assemblyPath}" -o "${sample}.amrfinderplus.tsv"
            gzip "${sample}.amrfinderplus.tsv"
            """
}  


workflow {
/*
        Git state of the pipeline for this run (commit id, "-dirty" if there are uncommitted changes).
*/
        def gitCmd   = { String args -> ["git", "-C", projectDir.toString()] + args.tokenize() }
        def commitId = gitCmd("rev-parse --short HEAD").execute().text.trim() ?: 'unknown'
        def dirty    = gitCmd("status --porcelain --untracked-files=no").execute().text.trim()
        def gitState = dirty ? "${commitId}-dirty" : commitId

        log.info """\
                 B A K R E P
        ===================================
        commit    :   ${gitState}
        scriptId  :   ${workflow.scriptId}
        runName   :   ${workflow.runName}
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
        Record pipeline version and command line per run in <results>/_runs/<runName>.txt
*/
        def runInfo = file("${params.results}/_runs/${workflow.runName}.txt")
        runInfo.parent.mkdirs()
        runInfo.text = """\
            start     ${workflow.start}
            commit    ${gitState}
            scriptId  ${workflow.scriptId}
            samples   ${params.samples}
            profile   ${workflow.profile}
            configs   ${workflow.configFiles.unique().join(' ')}
            workDir   ${workflow.workDir}
            cmd       ${workflow.commandLine}
            """.stripIndent()
        if (dirty) runInfo.append("uncommitted:\n${dirty}\n")

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
        .splitCsv( sep: '\t', skip: 0  )
        .map( {
                def sample = it[0]
                def batch = sample.substring(3,7)
                def assemblyPath = dataPath.resolve("${sample}.fa").toAbsolutePath()
                return [sample,batch,assemblyPath]
        } )
        
        taxonomy(samples)
        qualityCheck(samples)
        assemblyScan(samples)
        mlst(samples)
        annotation(taxonomy.out.taxonomy)
        amr(samples)      
}

