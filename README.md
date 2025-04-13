# MangoDB 

MangoDB is a clone of BitCask written in Java 17. The purpose of this project is to learn how BitCask works and to benchmark its performance when 
implemented in Java. The original BitCask, used as part of Riak, is written in Erlang. 

## How to Run 

`chmod +x run.sh` (only needed once)
`./run.sh`

## How to run integration tests

`pip install -r requirements.txt`
`pytest`

## Available Operations

`PUT key value` to add a key-value part
`GET key` to get value for the key

