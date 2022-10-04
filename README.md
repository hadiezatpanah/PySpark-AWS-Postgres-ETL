#  PySpark-AWS-Postgres Data Pipeline

## Introduction

This is an End to End solution to read TSV files from AWS S3 and process and import them into postgres relational database.
The scope of this data pipline is build everything that is between raw data and BI tables, including find and fetch the raw data that receives from the app and make it ready for the BI team needs.


## Requirement
The BI team in BRGroup requires 2 tables:
* article_performance: aggregating simple stats on how the article has performed
* user_performance: aggregating simple stats on how the user interacted with the app


Following are few more details regarding the s3-data
  * Each line in the files represent a collected event, the 1st line is the header to help interpret the schema
  * EVENT_NAME column represents the type of event collected 
    * top_news_card_viewed -> A card from Top news section has been viewed by the user
    * my_news_card_viewed -> A card from My news section has been viewed by the user
    * article_viewed -> The user clicked on the card and viewed the article in the app's web viewer
  * The Attributes column is the event payload as a JSON text
    * id = id of the article
    * category = category of the article
    * url
    * title 
    * etc...

The two tables should be created in the Postgres DB that is brought up by executing the docker-compose script in the project folder. They should look as the examples below:

<u>article_performance table</u>

| article_id  | date         | title           | category   | card_views | article_views |
|-------------|--------------|-----------------|------------|------------|---------------|
| id1         |  2020-01-01  | Happy New Year! |  Politics  |  1000      |    22         |

<u>user_performance table</u>

| user_id     | date         | ctr   |
|-------------|--------------|-------|
| id1         |  2020-01-01  |0.15   |

* ctr(click through rate) = number of articles viewed / number of cards viewed
* load the files directly from S3 instead of manually copying them locally 
* create staging tables as necessary for any intermediate steps in the process


## Getting Started

This section explains how to run this App. I have tried to make it very simple. 

### Prerequisites
The required prerequisites are:

* Docker and docker-compose 3
* Internet connection to download required docker images and libraries.

### Installation

* running all containers
   ```sh
   $ sudo docker-compose up -d
   ```

Some more details:
* The docker compose file will create a Postgres container already connected to the ETL container. Postgres instance is used for storing data models and solution.
* After the command prints `BRGroupetl exited with code 0`, the database is in its final state, with the solution.
* In order to read from AWS S3 respective config should be set in `.env` file.

## Stoping Services
Enter the following command to stop the containers:

```bash
$ sudo docker-compose down -v
```

## Author

👤 **Hadi Ezatpanah**

## Version History
* 0.1
    * Initial Release