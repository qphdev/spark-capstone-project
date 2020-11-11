# spark-capstone-project
Spark application for marketing data analysis.
This is a training project to memorize the material from [Spark course on coursera](https://www.coursera.org/learn/scala-spark-big-data/home/welcome). 

### Technologies
Project is created with:
* Scala 2.12.12
* Spark 3.0.1
* sbt 1.3.13

__Note__ that AdoptOpenJDK JDK 8 or AdoptOpenJDK JDK 11 is recommended to use with sbt.

### Setup
```bash
git clone https://github.com/qphdev/spark-capstone-project.git
cd ~/spark-capstone-project

# To build and run project
sbt run

# To run tests
sbt test
```

### Input & output
Datasets to analyze are _mobile-app-clickstream_sample.csv_ and _purchases_sample.csv_, located at src/main/resources/marketing.

The output consists of two small datasets and is written to folders _topChannels_ and _topTenCampaigns_.