# go-pg-slowlog

A Go library for parsing and collecting the slowlog of PostgreSQL.

## Usage

```go
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/tkuchiki/go-pg-slowlog/parser"
)

func main() {
	f, err := os.Open("/path/to/postgresql-xxx.log")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	logLinePrefix := "%m [%p]"
	slowLogParser, err := parser.NewPGSlowLogParser(f, logLinePrefix)
	if err != nil {
		log.Fatal(err)
	}

	defer slowLogParser.Stop()
	go slowLogParser.Start()

	for logEntry := range slowLogParser.LogEntryChan() {
		fmt.Println(logEntry.Duration, logEntry.Statement) 
	}
	
	// example output
	// 1.009444s SELECT * FROM singers
	// 1.002257s SELECT * FROM albums 
}
```
