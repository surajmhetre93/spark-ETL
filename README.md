Rippleshot Data Engineering Take Home.


### Problem Statement
You are working on a project to onboard a new client for Rippleshot. The client is providing two flat files on a daily basis. The client's data must be ingested and available in the Data Warehouse for downstream consumption.

The Data Warehouse consists of parquet files stored in an s3 bucket.

#### Your manager assigns you a JIRA ticket with the following tasks.

- Ingest the new files into the Data Warehouse using PySpark.
- Data should be partitioned by year, month, and day in the s3 bucket.
- Records should be de-duplicated before being ingested into the Data Warehouse.
- We only want the latest account record in the Data Warehouse.
- The product team wants a report of how many transactions happened per account first name and last name, per day.
- The product teams want a report of the top 3 merchants by dollars spent.

### Note 
- The directory `client_data_files` contains the flat files of interest.
- The `Dockerfile` will allow you to run `pyspark` if you do not have it avaialble
on your local system.
  
### Docker and Tests
If you need to use Docker, first build the docker image `docker build --tag take-home .` 

You should only have to do this once.. unless you change the Dockerfile

Use the command `docker-compose up test` to run all unit-tests. 
The container will be tied to your local volume from which you are running the command, and will pick up your changes.

### Running your Code via Docker.
If you want to run your code through `Docker` ensure you have a `main.py` file located in
a `src` folder in the root of this repo.

Running `docker-compose up run` will then run your script at `Spark` on the `Dockerfile` picking
up your code and running it.

