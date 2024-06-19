# Data Pipelines with Airflow

Welcome to the Data Pipelines with Airflow project! This endeavor will provide you with a solid understanding of Apache Airflow's core concepts. Your task involves creating custom operators to execute essential functions like staging data, populating a data warehouse, and validating data through the pipeline.

To begin, we've equipped you with a project template that streamlines imports and includes four unimplemented operators. These operators need your attention to turn them into functional components of a data pipeline. The template also outlines tasks that must be interconnected for a coherent and logical data flow.

A helper class containing all necessary SQL transformations is at your disposal. While you won't have to write the ETL processes, your responsibility lies in executing them using your custom operators.

## Initiating the Airflow Web Server
Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed before proceeding.

To bring up the entire app stack up, we use [docker-compose](https://docs.docker.com/engine/reference/commandline/compose_up/) as shown below

```bash
docker-compose up -d
```
Visit http://localhost:8080 once all containers are up and running.

## Configuring Connections in the Airflow Web Server UI
![Airflow Web Server UI. Credentials: `airflow`/`airflow`.](assets/login.png)

On the Airflow web server UI, use `airflow` for both username and password.
* Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
* Don't forget to start your Redshift cluster via the AWS console.
* After completing these steps, run your DAG to ensure all tasks are successfully executed.

## Getting Started with the Project
1. The project template package comprises three key components:
   * The **DAG template** includes imports and task templates but lacks task dependencies.
   * The **operators** folder with operator templates.
   * A **helper class** for SQL transformations.

1. With these template files, you should see the new DAG in the Airflow UI, with a graph view resembling the screenshot below:
![Project DAG in the Airflow UI](assets/final_project_dag_graph1.png)
You should be able to execute the DAG successfully, but if you check the logs, you will see only `operator not implemented` messages.

## DAG Configuration
In the DAG, add `default parameters` based on these guidelines:
* No dependencies on past runs.
* Tasks are retried three times on failure.
* Retries occur every five minutes.
* Catchup is turned off.
* No email on retry.

Additionally, configure task dependencies to match the flow depicted in the image below:
![Working DAG with correct task dependencies](assets/final_project_dag_graph2.png)

## Developing Operators
To complete the project, build four operators for staging data, transforming data, and performing data quality checks. While you can reuse code from Project 2, leverage Airflow's built-in functionalities like connections and hooks whenever possible to let Airflow handle the heavy lifting.

### Stage Operator
Load any JSON-formatted files from S3 to Amazon Redshift using the stage operator. The operator should create and run a SQL COPY statement based on provided parameters, distinguishing between JSON files. It should also support loading timestamped files from S3 based on execution time for backfills.

### Fact and Dimension Operators
Utilize the provided SQL helper class for data transformations. These operators take a SQL statement, target database, and optional target table as input. For dimension loads, implement the truncate-insert pattern, allowing for switching between insert modes. Fact tables should support append-only functionality.

### Data Quality Operator
Create the data quality operator to run checks on the data using SQL-based test cases and expected results. The operator should raise an exception and initiate task retry and eventual failure if test results don't match expectations.

## Reviewing Starter Code
Before diving into development, familiarize yourself with the following files:
- [plugins/operators/data_quality.py](plugins/operators/data_quality.py)
- [plugins/operators/load_fact.py](plugins/operators/load_fact.py)
- [plugins/operators/load_dimension.py](plugins/operators/load_dimension.py)
- [plugins/operators/stage_redshift.py](plugins/operators/stage_redshift.py)
- [plugins/helpers/sql_queries.py](plugins/helpers/sql_queries.py)
- [dags/final_project.py](dags/final_project.py)

Now you're ready to embark on this exciting journey into the world of Data Pipelines with Airflow!

## Update: How To Run This Dag

2024-06-19: I have completed the project, and have some important comments about running the DAG.

### 1. The DAG assumes the tables already exist in Redshift

I ran the provided create_tables.sql manually in the Redshift SQL editor.  Since the project did not say that the pipeline should include creating the tables.

### 2. The default DAG start date is now()

We were already instructed to set catchup to False in the default args for the dag.  Also, realistically the dag default start date should be now(), because we want to automatically process any new files that come in, since our operator uses the execution date to find files in s3.  However, our data in s3 only includes 2018 files, so we want to have the possibility of backfilling it.  

In this case, we would run the dag once in a backfill command, for a given start and end date (in the past) and load the files timestamped in s3 with that date.

For example, run this command:

`airflow dags backfill -s 2018-11-04 -e 2018-11-04 final_project`

to backfill the data for 2018-11-04.

### 3. The DAG will fail (on purpose) if there is no data for today

If you trigger the dag manually from the UI, to run with the default args, it will take the execution time as now (meaning today, in 2024), and there is no s3 data for today, so the dag will fail, as expected.

In the Stage_events task, you can look at the logs and see the error, for example:

`The specified S3 prefix 'log-data/2024/06/2024-06-19-events.json' does not exist.`

