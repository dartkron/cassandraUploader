# cassandraUploader

## What is it?

Short application to upload binary files to Cassandra cluster. 
Created to check cassandra speed on inserting raw data to blob field type.

## Prepare
Application depends on [gocql](https://github.com/gocql/gocql) package.

Uploader built to write files to Cassandra table which could be create by following cqlsh command:

    CREATE TABLE stat (first int PRIMARY KEY, second blob);

No matter how many files will be into indicated folder(./files/ by default), but to make it simple better create hundreds of pseudo-random files:

    for i in {1..1000}; do dd if=/dev/urandom of=~/files/${i}.rand bs=5M count=1; done

this will create 1000x5MB files in ./folder. Based on tests, 5MB - optimal size to write to Cassadra: big enough and no to big, to see timeouts.

Now you are ready to load files into your Cassadra cluster. 


## Run

Application accept following command-line arguments:

    -path="./files/": path to directory with blob files to upload
    -servers_list="::1": list of cassandra servers to connect, i.e.: 2001:db8:f:ffff:0:0:0:1,2001:db8:f:ffff:0:0:0:2
    -keyspace="simple_space": keyspace where target table located
    -table="stat": table where blobs will be saved. Should have following structure: first:int, second:blob


but if you're doing everything by this README, just do following steps:
1. scp application to your cassandra server 
2. generate 1000 pseudo-random files into ./files/ directory
3. create simple_space tablespace and stat table 
4. run application without any arguments
 
you should see verbosity output about upload process. 

## My results

In 4 nodes cluster(2x2DC) on comodity VPS(1 VCPU,2 GB RAM, 20GB HDD) i've reached ~20MB/s writes(with EACH_QUORUM).

But if you will start application on multiple nodes in same time,summary throughput will be significally high, like an example:

        2 nodes: 35MB/s
        3 nodes: 45MB/s
