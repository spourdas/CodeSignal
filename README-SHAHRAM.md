## Hello 

- this is my documentation on the implementation of "The Match Game"
- The biggest criteria of this project was making it easier for next developers to easily maintain this code

## Goals

- The number one measure was to create this README file to explain the design and configuration
- External configurations will allow fine tuning of  Kafka, EHCache, the feed (such as XBOX), and the application dynamically without recompiling the code
- Correct Java package organization can make the program a lot more readable
- Unit tests not only prove the correctness of the algorithm, it can also be a great documentation on how the program works
- Java methods are broken into smaller chunks to make the program more readable and testable
- By using long and meaningful class, method, and variable names, the program is more understandable
- By making the Consumer class abstract, the functionality is easily extendable
- 
- Finally, I followed these other standard Java practices: 
- 
- Standard project structure, 
- Readability over Reusability
- Standard Code Formatting
- Strict the number of parameters
- Avoid hard coding values

## Third Party Software

- Kafka was chosen to stream the internal movie CSV files to be stored
- Kafka was also used to stream the external feed and compare it with the internal movies
- EHCache was selected to cache the internal movie information. Other technologies tested where Spark, Redis, H2
- Commons CSV and Jackson CSV libraries were both needed
- Commons Text library was brought in to use the Levenshtein algorithm to compare the distance between two strings
- Lombok helps in making value classes more compact by automatically generating constructors, getters, and setters

## System Configuration

Kafka (kafka-consumer.properties, kafka-producer.properties)
- All kafka configurations can be placed in the kafka-consumer and kafka-producer property files to pass to Kafka. 
- This can aid in tuning the performance and difficulties in working with Kafka

### EHCache (cache.properties)

- Based on the amount of data in the movies files, the correct number of buckets in memory can be specified so prevent overflows to disk
- 
- Here are the default values
- cache.movies.max.elements.in.memory=10000
- cache.actors.director.max.elements.in.memory=10000
- cache.movies.visited.max.elements.in.memory=300000
- 
- "cache.movies.names.bucket.size" determines how many movie title string sizes are cached into one bucket. The default size of 3 means that titles with the length of 0,1, and 2 are place into one bucket. Then, tiles with string length of 3, 4, and 5 are place into another bucket and so forth.

### Feed (xbox.properties)

- This file should be named the same as the Java Enumeration external source (XBOX) and it should be in lower case
- The CSV file data and their position are specified here
- Also, the date format specified will allow the program to extract the year the movies were made
- These are the current values:
- 
- header.column.title=Title
- header.column.id=MediaId
- header.column.date=OriginalReleaseDate
- header.column.date.format=M/d/yyyy h:mm:ss a
- header.column.actors=Actors
- header.column.director=Director
- 
- Feed specific Kafka attributes such as id, group, topic, poll timeout, and the number of partitions are specified here:
- 
- kafka.producer.id=xbox-id
- kafka.topic.name=xbox
- kafka.group.name=xbox-group
- kafka.producer.end.signal=XXXXXXXXXXXXXXXXXXXXXXXXX
- kafka.consumer.poll.timeout=100
- kafka.number.of.partitions=1
- 
- The acceptance percentage of string differences (Levenshtein Distance) based on the string length:
- strings.levenshtein.acceptance.threshold.percentage=5

## General Configuration (config.properties)

- CSV column names to Java POJO column name mappings are specified here:
- 
- movies.field.first=id
- movies.field.second=title
- movies.field.third=year

- actors.directors.field.first=id
- actors.directors.field.second=name
- actors.directors.field.third=role
-
- Internal movies Kafka attributes such as poll timeout and the number of partitions:
- 
- kafka.producer.end.signal=XXXXXXXXXXXXXXXXXXXXXXXXX
- kafka.consumer.poll.timeout=100
- kafka.number.of.partitions=1

## Problem Solution

- The internal movies are read line by line
- The movie names are standardized, meaning extra spaces and special characters are removed
- The movies are cached and the keys are the standardized movie titles
- The movie title string length along with the year the movie was made determine what cache the movie is placed into
- By default, "cache.movies.names.bucket.size" is set to 3 in cache.properties. This means three string lengths go into one cache bucket
- The directors and actors are placed into another cache keyed by the movie id. The value is a DTO object that holds a director name along with a list of actors
- For every line from the feed, movie title, year made, director, and actors are extracted
- The feed movie title is standardized meaning extra space and special characters are removed
- Each feed movie title is cached so that one movie is not analyzed more than once
- The movie title and the year made is checked to see if there is an exact match. If so, it will be placed into the list to be returned
- Otherwise, all the movies in cache for that year (within that string length range) are checked
- Each standardized title is compared to the cache using the Levenshtein Distance Algorithm
- Levenshtein Distance is the number of changes needed to change one sequence into another, where each change is a single character modification (deletion, insertion or substitution). 
- A higher distance indicates a greater distance. Zero is the perfect match
- Acceptance Threshold is set to 5 percent by default. 
- The threshold percentage is applied to the movie title string length to determine how many character variances are allowed
- Movies that are in close range are place into a possible match list
- if there is only one movie in the list, that movie is selected and  will be placed into the final list to be returned
- To obtain a score for each movie possibility , the Levenshtein Distance is subtracted from the threshold and added by one 
- This way, a higher score indicates a better match
- The movies are further scored based on their directors and actors (if available on both lists) similar to the above
- The movie with the highest score is selected
- In case of multiple movies at the end, last movie in the list is selected

## Final Analysis

- It is important to know that I assumed that the movie title and the year made were essential in identifying a movie and only slight similarities in the movie titles were allowed
- Most of the movies from the XBOX list that were matched were direct result of exact match on the movie title and the movie year
- There were a few movie titles that were selected based on name similarity 
- None of the XBOX movies had to be matched based on director and actors. This is because there were always only one movie that was similar in name
- Only over 9,000 matches were made between the XBOX list and the internal movie list

## How To Run The Program

- You have to run Zookeeper and Kafka beforehand
- For example, On my Windows machine, here are where the batch files are: C:\Users\spour\kafka\bin\windows
- I created two batch files of my own to make it even simpler
- zoo.bat:
- C:\Users\spour\kafka\bin\windows\zookeeper-server-start.bat C:\Users\spour\kafka\config\zookeeper.properties
- kaf.bat:
- C:\Users\spour\kafka\bin\windows\kafka-server-start.bat C:\Users\spour\kafka\config\server.properties
- I also created docker-compse.yml in case I needed to run Kafka from Docker
- To run the docker compose run in the same directory as the yaml file:
- docker-compose up -d





    
