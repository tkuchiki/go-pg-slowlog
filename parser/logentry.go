package parser

import (
	"strconv"
	"strings"
	"time"
)

type LogEntry struct {
	Duration  time.Duration
	Statement string
	ReadBytes int64
}

func durationToFlat64(str string) (float64, error) {
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, err
	}

	return f, nil
}

func NewLogEntry(durationStr, statement string, readBytes int64) (*LogEntry, error) {
	durationf, err := durationToFlat64(durationStr)
	if err != nil {
		return nil, err
	}

	duration := time.Duration(durationf * float64(time.Millisecond))

	return &LogEntry{
		Duration:  duration,
		Statement: strings.TrimRight(statement, "\n"),
		ReadBytes: readBytes,
	}, nil
}

func (le *LogEntry) AppendStatement(statement string) {
	le.Statement += "\n" + strings.Replace(statement, "\t", "", 1)
}

func (le *LogEntry) TrimEndNewline() {
	le.Statement = strings.TrimRight(le.Statement, "\n")
}
