# S3 Parquet Reader

A framework utility designed to read a parquet file from S3 and convert to JSONs on local file system

## Setup Instructions
 1. Install brew install 
    ```
    /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    ```
 2. Install Xcode select
    ```
    xcode-select –install
    ``` 
 3. Install Java
    - https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html
 4. Install Apache Spark using brew install
    ```
    brew install apache-spark
    ```

## How to install
```
python -m pip install dist/dokr-0.1-py3-none-any.whl
```

## How to use
Class takes the following parameters:

1. param uri:  Path within the bucket to for parquet data
2. param bucket: Bucket to search look in S3 for can be read from env variable "S3_BUCKET"
3. param access_key: Access key to connect to S3 read from env variable "S3_ACCESS" 
4. param secret_key: Secret key to connect to S3 read from env variable "S3_SECRET" 
5. param destination_dir: Directory to write JSON's to, default is <date_time>_JSON located in this directory

###Recommendations
Set the following environment variables instead of leveraging class variables
```
  S3_ACCESS                 # S3 Access key
  S3_SECRET                 # S3 Secret key
  S3_BUCKET                 # S3 Bucket to look for URI inside
```


Once class has been initialized call method download_and_convert_to_json convert the parquet data into JSON data

###Example:
```
path = "agg_files/historical/DocObject7.parquet"
tmp = Connection(path)
tmp.download_and_convert_to_json()
```

###Other Useful Properties:
Ensures that the URI passed in actually exist returning a boolean
```
tmp.test_connection
```

Gives you the parent directory the parquet data and JSON files have been written to in local storage
```
tmp.parent_destination_path
```

Gives you the parquet directory in local storage
```
tmp.parquet_destination
```

Gives you the path to the directory all JSONs have been written to in local storage
```
tmp.json_destination
```

