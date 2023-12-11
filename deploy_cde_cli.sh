#!/bin/bash

set -e
cd "$(dirname "$0")"

RESOURCE_NAME_FILES="<file-resource-name>"
RESOURCE_NAME_PYENV="<python-resource-name>"

check_cde_cli() {
    if [ ! -x "./cde" ]; then
        echo "./cde binary not found or not executable. Download the CDE CLI first."
        exit 0
    fi
}

cleanup() {
    echo "cleaning up jobs"
    for file_name in $(ls ./cde_spark_jobs ./cde_airflow_jobs)
    do
        if [[ "$file_name" == *.py && "$file_name" != bonus* ]]; then
            local job_name=${file_name%.*}
            echo "./cde job delete --name $job_name"
            ./cde job delete --name $job_name 
        fi
    done
    echo "cleaning up resources"
    echo "./cde resource delete --name $RESOURCE_NAME_FILES"
    ./cde resource delete --name $RESOURCE_NAME_FILES
    echo "./cde resource delete --name $RESOURCE_NAME_PYENV"
    ./cde resource delete --name $RESOURCE_NAME_PYENV
}

create() {
    echo "creating file resource"
    echo "./cde resource create --type files --name $RESOURCE_NAME_FILES"
    ./cde resource create --type files --name $RESOURCE_NAME_FILES
    echo "creating python resource"
    echo "./cde resource create --type python-env --name $RESOURCE_NAME_PYENV"
    ./cde resource create --type python-env --name $RESOURCE_NAME_PYENV
}

upload_files() {
    echo "uploading file resources"
    for file in ${PWD}/cde_airflow_jobs/* ${PWD}/cde_spark_jobs/* ${PWD}/resources_files/*
    do
        if [[ "$file" != *requirements.txt ]]; then
            echo "./cde resource upload --local-path $file --name $RESOURCE_NAME_FILES"
            ./cde resource upload --local-path $file --name $RESOURCE_NAME_FILES
        fi
    done
}

upload_python() {
    echo "uploading python requirements"
    echo "./cde resource upload --local-path ${PWD}/resources_files/requirements.txt --name $RESOURCE_NAME_PYENV"
    ./cde resource upload --local-path ${PWD}/resources_files/requirements.txt --name $RESOURCE_NAME_PYENV
}

create_spark_jobs() {
    echo "creating spark jobs"
    for file_name in $(ls ./cde_spark_jobs)
    do
        local job_name=${file_name%.*}
        echo "./cde job create --name $job_name --type spark --mount-1-resource $RESOURCE_NAME_FILES --application-file $file_name --python-env-resource-name $RESOURCE_NAME_PYENV"
        ./cde job create --name $job_name --type spark --mount-1-resource $RESOURCE_NAME_FILES --application-file $file_name --python-env-resource-name $RESOURCE_NAME_PYENV
    done
}

create_airflow_jobs() {
    echo "creating airflow jobs"
    for file_name in $(ls ./cde_airflow_jobs)
    do
        if [[ "$file_name" != bonus* ]]; then
            local job_name=${file_name%.*}
            echo "./cde job create --name $job_name --type airflow --mount-1-resource $RESOURCE_NAME_FILES --dag-file $file_name"
            ./cde job create --name $job_name --type airflow --mount-1-resource $RESOURCE_NAME_FILES --dag-file $file_name
        fi
    done
}

main() {
    check_cde_cli
    cleanup
    create
    upload_files
    upload_python
    create_spark_jobs
    create_airflow_jobs
}

main
