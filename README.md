# spark-error

A repo to reproduce the errors observed when trying to execute tests on a Databricks cluster.

## Documentation

Use the included run.sh file to configure the env (`$ ./run.sh -h` will show usage info) and add the necessary jars to the classpath before running anything with sbt.

The smallest cluster the error appeared on was a m4 driver node and 2 m4 worker nodes, so that should serve as a minimal cluster config for testing Databricks Connect.
