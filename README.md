# pipelines

### Introduction
This repository contains different examples of pipelines using apache beam python mysql redis, reusable for gcp dataflow. The idea is to have a reference to run pipelines on premise and on cloud.

### Steps
We recommend follow the next steps to try the examples.


Create a Virtual Environment. In the requisites.txt file we specify the libraries that we need.

Activate the virtual environment

Run  wordcount.py example

```
sh
python -m wordcount --input ~/data_lake/fly_small.txt --output ~/data_lake/out/counts
```
python -m mini_wordcount --input ~/data_lake/fly_small.txt --output ~/data_lake/out/counts





### links
* Apache Beam
https://beam.apache.org/
https://beam.apache.org/documentation/programming-guide/
https://beam.apache.org/get-started/quickstart-py/

* Github Apache Beam repo
https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples


* Beam - MySQL Connector
https://pypi.org/project/beam-mysql-connector/
https://github.com/esakik/beam-mysql-connector

* Python, Apache Beam, Relational Database (MySQL, PostgreSQL), Struggle and The Solution
https://pypi.org/project/pysql-beam/
https://yesdeepakverma.medium.com/python-apache-beam-relational-database-mysql-postgresql-struggle-and-the-solution-73e6bf3ccc28


### tutorials, videos, books, ...

* Dataflow Autoscaling Pipelines
By:  Joe Khoury
Course:  47 Minutes
Skillsoft
Library ID
ID: cl_gcde_a11_it_enus

