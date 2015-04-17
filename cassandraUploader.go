package main

import (
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"os"
	"strings"
	"time"
)

// function accepting filename and write it to Cassandra
func writeFileToCassandra(table string, iterator int, file string, cluster *gocql.ClusterConfig, totalDuration *time.Duration, totalSize *int64, startChannel chan int) {
	fmt.Println("Proceeding ", iterator, " file... ")
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	// open file
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	// get the file size
	stat, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	// create slice of bytes for new files
	bs := make([]byte, stat.Size())

	// read whole file
	_, err = f.Read(bs)

	if err != nil {
		log.Fatal(err)
	}

	// start counter
	start := time.Now()

	//insert data to Cassandra
	if err := session.Query(`INSERT INTO `+table+` (first,second) VALUES (?, ?)`,
		iterator, bs).Exec(); err != nil {
		log.Println(err)
	}

	// increment elapsed time
	*totalDuration += time.Since(start)
	// increment total size
	*totalSize += stat.Size()
	startChannel <- 1

}

func main() {
	// define variables
	var totalDuration time.Duration
	var totalSize int64
	fileCountCursor := 1
	startChannel := make(chan int, 11)

	// define input flags
	path := flag.String("path", "./files/", "path to directory with blob files to upload")
	servers_list := flag.String("servers_list", "::1", "list of cassandra servers to connect, i.e.: 2001:db8:f:ffff:0:0:0:1,2001:db8:f:ffff:0:0:0:2")
	keyspace := flag.String("keyspace", "simple_space", "keyspace where target table located")
	table := flag.String("table", "stat", "table where blobs will be saved. Should have following structure: first:int, second:blob")
	concurent := flag.Int("concurent", 5, "amount of concurent writes")

	flag.Parse()

	// open directory
	dir, err := os.Open(*path)
	if err != nil {
		log.Fatal(err)
	}
	defer dir.Close()

	// read directory to get attributes
	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		log.Fatal(err)
	}

	// get files count
	files_count := len(fileInfos)

	fmt.Println("Total files count is ", files_count)

	// Cluster definition
	cluster := gocql.NewCluster(strings.Split(*servers_list, ",")...)
	cluster.Keyspace = *keyspace
	// set write consistency to EACH_QUORUM, to provide strong consistency in couple with LOCAL_QUORUM on read
	cluster.Consistency = gocql.Quorum
	// redefine default timeout, because default(600 miliseconds) was not enough on big files
	cluster.Timeout = 5 * time.Second

	// create session to Cassandra cluster

	// follow through list of files and write them to Cassandra into blob filed
	for i, fi := range fileInfos {
		if fileCountCursor > *concurent {
			<-startChannel
		}
		go writeFileToCassandra(*table, i+1, *path+fi.Name(), cluster, &totalDuration, &totalSize, startChannel)
		fileCountCursor++
	}

	// wait until last concurent files will be sent
	for i := 1; i <= *concurent; i++ {
		<-startChannel
	}

	// printing total for whole test
	fmt.Println("Ready!\nTotal duration is", totalDuration, "\nTotal sent", totalSize, "bytes")
	fmt.Println("Average file size", totalSize/int64(files_count), "bytes")
	fmt.Println("Concurency:", *concurent)
	fmt.Printf("Average speed %f MB/s\n", float64(totalSize)/totalDuration.Seconds()/1024/1024*float64(*concurent))

}
