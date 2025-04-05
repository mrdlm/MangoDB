# MangoDB 

MangoDB is a clone of BitCask written in Java 17. The purpose of this project is to learn how BitCask works and to benchmark its performance when 
implemented in Java. The original BitCask, used as part of Riak, is written in Erlang. 

## How to Run 

`./gradlew build`
`./gradlew run`

## Available Operations

`PUT key value` to add a key-value part
`GET key` to get value for the key

## TODO Feature

### Basics

* deletions 
* compaction

### Optimisations 

* crash recovery 
* multi-threading
