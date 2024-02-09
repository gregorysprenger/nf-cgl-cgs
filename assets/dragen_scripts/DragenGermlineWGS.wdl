version 1.0

workflow DragenGermlineWGS {

  input {
    File Fastqs
    Array[String] Names
    String OutputDir

    String DBSNP = "/storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/dbsnp.vcf.gz"
    String Reference = "/storage1/fs1/gtac-mgi/Active/CLE/reference/dragen393_hg38"

    String JobGroup = "/dspencer/adhoc"
    String DragenQueue = "dragen-2"
    String DragenCPU = "20"
    String DragenMEM = "200 G"
    String DragenEnv = "LSF_DOCKER_DRAGEN=y"
    String DragenDocker = "gtac-mgi-dragen(seqfu/oracle8-dragen-4.0.3:latest)"
  }

  scatter(n in Names){
    call dragen_align {
        input: Fastqs=Fastqs,
        Name=n,
        Reference=Reference,
        DBSNP=DBSNP,
        jobGroup=JobGroup,
        queue=DragenQueue,
        mem=DragenMEM,
        env=DragenEnv,
        cpu=DragenCPU,
        docker=DragenDocker,      
        OutputDir=OutputDir + "/" + n
      }
  }
}



task dragen_align {

  input {
    File Fastqs 
    String Name

    String LocalAlignDir = "/staging/tmp/" + Name

    String OutputDir
  
    String Reference
    String DBSNP
    String queue
    String mem
    String env
    String cpu
    String docker
    String jobGroup

  }
  command {
      if [ ! -d "~{LocalAlignDir}" ]; then
        /bin/mkdir ~{LocalAlignDir}
      fi

      if [ ! -d "~{OutputDir}" ]; then
        /bin/mkdir ~{OutputDir}
      fi

      /opt/edico/bin/dragen -r ~{Reference} \
      --fastq-list ~{Fastqs} --fastq-list-sample-id ~{Name} \
      --read-trimmers adapter \
      --trim-adapter-read1 /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/t2t-chm13_adapter1.fa \
      --trim-adapter-read2 /storage1/fs1/gtac-mgi/Active/CLE/reference/dragen_align_inputs/hg38/t2t-chm13_adapter2.fa \
      --enable-map-align true \
      --enable-map-align-output true \
      --enable-bam-indexing true \
      --enable-duplicate-marking true \
      --qc-coverage-ignore-overlaps=true \
      --gc-metrics-enable=true \
      --enable-variant-caller true \
      --vc-combine-phased-variants-distance 3 \
      --dbsnp ~{DBSNP} \
      --enable-sv true \
      --sv-output-contigs true \
      --sv-hyper-sensitivity true \
      --enable-cnv true \
      --cnv-enable-self-normalization true \
      --output-format CRAM \
      --intermediate-results-dir ~{LocalAlignDir} \
      --output-directory ~{OutputDir} \
      --output-file-prefix ~{Name} && \
      rm -Rf ~{LocalAlignDir}
  }

  runtime {
    docker_image: docker
    dragen_env: env
    cpu: cpu
    memory: mem
    queue: queue
    job_group: jobGroup
  }

  output {
      String outdir = "~{OutputDir}"
  }
}
