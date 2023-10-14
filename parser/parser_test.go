package parser

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestPGSlowLogParser_Start(t *testing.T) {
	pglog := `2023-10-08 13:14:26.366 GMT [47] LOG:  duration: 1009.444 ms  statement: SELECT
	pg_sleep(1)
2023-10-08 13:16:41.488 GMT [59] LOG:  duration: 1002.257 ms  statement: SELECT * FROM users;
2023-10-08 13:18:58.636 GMT [28] LOG:  checkpoint starting: time
`

	r := strings.NewReader(pglog)

	logLinePrefix := "%m [%p]"
	slowLogParser, err := NewPGSlowLogParser(r, logLinePrefix)
	if err != nil {
		t.Fatal(err)
	}

	slowlogs := make([]*LogEntry, 0)

	defer slowLogParser.Stop()
	go slowLogParser.Start()

	for slowlog := range slowLogParser.LogEntryChan() {
		slowlogs = append(slowlogs, slowlog)
	}

	tests := []struct {
		duration  time.Duration
		statement string
	}{
		{
			duration: time.Duration(1009.444 * float64(time.Millisecond)),
			statement: `SELECT
pg_sleep(1)`,
		},
		{
			duration:  time.Duration(1002.257 * float64(time.Millisecond)),
			statement: `SELECT * FROM users;`,
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("slowlog-%d", i), func(t *testing.T) {
			statement := slowlogs[i].Statement
			if statement != tt.statement {
				t.Fatalf("want: %s, got: %s", tt.statement, statement)
			}

			duration := slowlogs[i].Duration
			if duration != tt.duration {
				t.Fatalf("want: %v, got: %v", tt.duration, duration)
			}
		})
	}

}
