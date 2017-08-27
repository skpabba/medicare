Altus Sample: Medicare Data
===========================

This sample application does some transformations on publicly available
Medicare data, as an illustration of how such transformations might be
done using Spark.

Requirements
------------

* Spark 1.6 or 
* Spark 2.1

Usage
-----

Once built (mvn clean package -pspark21 or mvn clean package -pspark16), the application can be run using any of the standard ways to run a spark application (spark-submit, the Altus Data Engineering API, etc).

* mainClass: com.cloudera.altus.sample.medicare.transform
* arguments:
  * 1: directory containing data files
  * 2: directory to put output files into
  * Either or both locations can be HDFS or S3 paths.

_For your convenience jars for Spark 1.6 and 2.1 are built and are in lib/_

Source Code
-----------

Application source code is published under the Apache Software Licence 2.0.

Data
----

The included data files come from two sources:

* https://data.medicare.gov/data/hospital-compare
  * [hospitals.txt](data/hospitals.txt) (from `Hospital General Information.csv`)
  * [readmissionsDeath.txt](data/readmissionsDeath.txt) (from `Readmissions and Death - Hospital.csv`)
* https://en.wikipedia.org/wiki/List_of_U.S._states_by_GDP_per_capita
  * [gdp.txt](data/gdp.txt)

The Medicare data, as works of the U.S. Government are in the public domain.

The Wikipedia GDP data is published under the
[Creative Commons Attribution-ShareAlike 3.0 License](https://creativecommons.org/licenses/by-sa/3.0/legalcode)
