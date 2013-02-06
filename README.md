A simple project for getting started with working with Avro data on Hadoop. The goal is to get you
to the point where you can write some simple Avro data files into HDFS.

To build:

	mvn clean package

This will create an `avrostarter-0.1.0-jar-with-dependencies.jar` file in the `target` directory that
may be invoked using:

	java -jar target/avrostarter-0.1.0-jar-with-dependencies.jar <num_calls_to_write> <target_file>

which will write <num_calls_to_write> records into the <target_file> on the local filesystem, overwriting
that file if it already exists.

To examine the code in Eclipse, run:

	mvn eclipse:eclipse

and import the project into Maven as a Java project, with the working directory set to the directory that
contains this README.md file.
