# spark-capstone-project
Spark application for marketing data analysis. It is dedicated to answering two questions:
* What are the Top 10 marketing campaigns that bring the biggest revenue?
* What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions with the App in each campaign?

This is a training project to memorize the material from [Spark course on coursera](https://www.coursera.org/learn/scala-spark-big-data/home/welcome). 

### Technologies
Project is created with:
* Scala 2.12.12
* Spark 3.0.1
* sbt 1.3.13

__Note__ that AdoptOpenJDK JDK 8 or AdoptOpenJDK JDK 11 is recommended to use with sbt.

### Setup
To use this project just clone it from github:
```bash
git clone https://github.com/qphdev/spark-capstone-project.git
```

Two command line arguments are expected: path to clickstream dataset and path to purchases dataset.
```bash
cd ~/spark-capstone-project

# To build and run project
sbt "run <clickstream-path> <purchases-path>"

# To run tests
sbt test
```

### Output
The output is two csv files located in "topChannels" and "topTenCampaigns" folders in the root folder of the project. 