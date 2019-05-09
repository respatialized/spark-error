#!/bin/bash


if [ "$1" == "-h" ]; then
    echo "Usage: `basename $0` [conda-dir conda-env sbt-cmd]"
    echo "conda-dir: the path of your anaconda installation (should end in ../conda.sh)"
    echo "conda-env: the path of the conda env with Databricks Connect installation"
    echo "sbt-cmd: the sbt command to execute"
    exit 0
fi

# configuring the connection environment
printf "making Conda path available\n"
source $1
printf "activating conda env and configuring connection\n"
conda activate $2
databricks-connect configure

# create jar location on classpath (if it doesn't exist)
if [ -d "./lib" ]; then
   printf "unmanaged jar directory exists already\n"
else
    printf "creating unmanaged jar directory on classpath\n"
    mkdir lib
fi

# copy jars to env (if not already done)
if [ -z $(diff $(databricks-connect get-jar-dir)/ ./lib) ]; then
    printf "databricks connect jars already on classpath\n"
else
    printf "copying and overwriting jars to classpath from dbconnect path\n"
    cp -f $(databricks-connect get-jar-dir)/*.jar ./lib
fi

printf "configuration complete.\n\n"

# packaging classes
printf "packaging classes for remote execution.\n\n"
sbt "test:package"

# executing job
printf "preparation completed. executing spark job.\n\n"
sbt $3
