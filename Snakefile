#! /usr/bin/py
import pandas as pd 
import os

#trim -> fastqc -> mapping -> remove duplicates -> index

configfile: "config.json"
localrules: all, mkdir 

df = pd.read_csv(config["meta_file"], sep='\t', header=0, index_col=0)
sample_ids = list(df.index)
print(df.index)

def get_gz(sample_id):
    dir=config["raw_fastq_gz_dir"]
    print(sample_id) 
    print(dir)
    print (os.path.join(dir,df.loc[sample_id]["ForwardFastqGZ"]))
    return os.path.join(dir,df.loc[sample_id]["ForwardFastqGZ"])
    

rule all:
    input:expand("{dir}/{sample_id}.bracken",dir=config["dir_names"]["Bracken_dir"], sample_id=sample_ids)
    run:
        for sample in sample_ids:
            print("Wrapping up pipeline")

rule mkdir:
    output: touch(config["file_names"]["mkdir_done"])
    params: dirs = list(config["dir_names"].values())
    resources: time_min=10, mem_mb=2000, cpus=1
    shell: "mkdir -p {params.dirs}"

rule porechop: 
    input:
        rules.mkdir.output,
        all_read1 = lambda wildcards: get_gz(wildcards.sample_id)
    resources: time_min=360, mem_mb=2000, cpus=1
    output:
        porechop_read1 = config["dir_names"]["porechop_dir"]+ "/{sample_id}.porechop.output.fastq.gz",
        porechop_stats = config["dir_names"]["porechop_dir"]+ "/{sample_id}.porechop.stats"
    #version: config["tool_version"]["cutadapt"]
#    params:
#        adapter1=lambda wildcards: get_forward_primer(wildcards.sample_id)
    shell: "porechop -i {input.all_read1} -o {output.porechop_read1} > {output.porechop_stats}"

rule Kraken2:
    input:
        read1= rules.porechop.output.porechop_read1
    resources: time_min=360, mem_mb=128000, cpus=1
    output:
        Kraken2_file1 = config["dir_names"]["Kraken_dir"]+ "/{sample_id}.kraken.report",
    shell: "/projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/kraken2_installed/kraken2 --threads 8 --db /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 {input.read1} --report {output} --output {output}"

rule Bracken:
    input:
        r1=rules.Kraken2.output.Kraken2_file1
    resources: time_min=360, mem_mb=64000, cpus=1
    output:
        Bracken_file = config["dir_names"]["Bracken_dir"]+ "/{sample_id}.bracken",
    shell: "/projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_S -r 100 -l S -t 0 ; /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_D -r 100 -l D -t 0 ; /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_O -r 100 -l O -t 0 ; /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_P -r 100 -l P -t 0 ;/projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_C -r 100 -l C -t 0 ; /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_F -r 100 -l F -t 0 ; /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/Bracken/bracken -d /projects/academic/gbcstaff/Core_Projects/Kraken-Metagenomics/kraken2/PlusPFP-01-27-2021 -i {input.r1} -o {output} -w {output}.level_G -r 100 -l G -t 0"
