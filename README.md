# Data Request LGD

---

`java-8` `spring-boot` `oozie` `pig` `postgresql` `jdbi`

---

Spring Boot-based REST API for submitting and monitoring Oozie workflows

Oozie workflows are submitted and monitored by means of `OozieClient` API. They consist of

* a Hive action responsible for creating Pig job output table
* a Pig job that reads some pre-existing Hive tables and poputates the table created on previous step
* a Java action that issues an Impala refresh/invalidate metadata statement on Pig job's output table

Information on monitored Oozie jobs and actions is then stored on `PostgreSQL` database by means of `JDBI` framework 

Secondary module `cdh` contains two sub-modules

* `pig` which contains Oozie workflows, HiveQL scripts, Pig job scripts and Pig job UDFs
* `impala` which contains the jar executing refresh/invalidate statements
