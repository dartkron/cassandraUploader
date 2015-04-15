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
func writeFileToCassandra(table string, iterator int, file string, session *gocql.Session, totalDuration *time.Duration, totalSize *int64) {

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

}

func main() {
	// define variables
	var totalDuration time.Duration
	var totalSize int64

	// define input flags
	path := flag.String("path", "./files/", "path to directory with blob files to upload")
	servers_list := flag.String("servers_list", "::1", "list of cassandra servers to connect, i.e.: 2001:db8:f:ffff:0:0:0:1,2001:db8:f:ffff:0:0:0:2")
	keyspace := flag.String("keyspace", "simple_space", "keyspace where target table located")
	table := flag.String("table", "stat", "table where blobs will be saved. Should have following structure: first:int, second:blob")

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
	cluster.Consistency = gocql.EachQuorum
	// redefine default timeout, because default(600 miliseconds) was not enough on big files
	cluster.Timeout = 3 * time.Second

	// create session to Cassandra cluster
	session, _ := cluster.CreateSession()
	defer session.Close()

	// follow through list of files and write them to Cassandra into blob filed
	for i, fi := range fileInfos {
		fmt.Print("Proceeding ", i+1, " file... ")
		writeFileToCassandra(*table, i+1, *path+fi.Name(), session, &totalDuration, &totalSize)
		fmt.Println("done")
	}

	// printing total for whole test
	fmt.Println("Ready!\nTotal duration is", totalDuration, "\nTotal sent", totalSize, "bytes")
	fmt.Printf("Average duration %f seconds per file\n", totalDuration.Seconds()/float64(files_count))
	fmt.Println("Average file size", totalSize/int64(files_count), "bytes")
	fmt.Printf("Average speed %f MB/s\n", float64(totalSize)/totalDuration.Seconds()/1024/1024)

}
