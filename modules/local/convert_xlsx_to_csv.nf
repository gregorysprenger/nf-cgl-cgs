process CONVERT_XLSX_TO_CSV {
    tag "${meta.id}"
    label 'process_low'

    container 'docker.io/gregorysprenger/pandas-excel:v2.0.1'

    input:
    tuple val(meta), path(spreadsheet)

    output:
    tuple val(meta), path("${meta.id}.csv"), emit: csv
    path("versions.yml")                   , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    """
    #!/usr/bin/env python3

    import platform
    import subprocess
    import pandas as pd

    batch = pd.read_excel("${spreadsheet}".replace("\\\\", ""), engine="openpyxl", skiprows=1)
    batch.to_csv("${meta.id}.csv", index=False)

    # Output version information
    with open("versions.yml", "w") as f:
        f.write(f'"{subprocess.getoutput("echo ${task.process}")}":\\n')
        f.write(f'    python: {platform.python_version()}\\n')
    """
}
