package parser

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

const (
	timestampExpr = `\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.0-9]* [a-zA-Z]+`
	slowLogExpr   = `LOG:\s+duration:\s+([0-9\.]+)\s+ms\s+statement:\s+(.+)`
)

var prefixRegexps = map[string]string{
	"%a": `\S*`,                         // Application name
	"%u": `\S*`,                         // User name
	"%d": `\S*`,                         // Database name
	"%r": `\S*`,                         // Remote host name or IP address, and remote port
	"%h": `\S*`,                         // Remote host name or IP address
	"%b": `(?:\S+|\S+ \S+|\S+ \S+ \S+)`, // Backend type
	"%p": `\d+`,                         // Process ID
	"%P": `\d*`,                         // Process ID of the parallel group leader, if this process is a parallel query worker
	"%t": timestampExpr,                 // Time stamp without milliseconds
	"%m": timestampExpr,                 // Time stamp with milliseconds
	"%n": `\d+[.0-9]*`,                  // Time stamp with milliseconds (as a Unix epoch)
	"%i": `\S*`,                         // Command tag: type of session's current command
	"%e": `\S+`,                         // SQLSTATE error code
	"%c": `\S+`,                         // Session ID
	"%l": `\d+`,                         // Number of the log line for each session or process, starting at 1
	"%s": timestampExpr,                 // Process start time stamp
	"%v": `\S*`,                         // Virtual transaction ID (backendID/localXID)
	"%x": `\S+`,                         // Transaction ID (0 if none is assigned)
	"%q": "",                            // Produces no output, but tells non-session processes to stop at this point in the string; ignored by session processes
	"$Q": `\d*`,                         // Query identifier of the current query. Query identifiers are not computed by default, so this field will be zero unless compute_query_id parameter is enabled or a third-party module that computes query identifiers is configured.
	"%%": "%",                           // Literal %
}

type PGSlowLogParser struct {
	reader       io.Reader
	stopChan     chan bool
	stopped      bool
	logEntryChan chan *LogEntry
	logEntry     *LogEntry
	logPrefixRe  *regexp.Regexp
	slowLogRe    *regexp.Regexp
	readBytes    int64
}

func NewPGSlowLogParser(r io.Reader, logLinePrefix string) (*PGSlowLogParser, error) {
	p := &PGSlowLogParser{
		reader:       r,
		stopChan:     make(chan bool, 1),
		logEntryChan: make(chan *LogEntry),
	}

	logPrefixExpr := p.logLinePrefixToRegexpPattern(logLinePrefix)
	var err error
	p.logPrefixRe, err = regexp.Compile(logPrefixExpr)
	if err != nil {
		return nil, err
	}

	p.slowLogRe = regexp.MustCompile(logPrefixExpr + `\s*` + slowLogExpr)

	return p, nil
}

// %m [%p] -> \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[.0-9]* [a-zA-Z]+ \[\d+\]
func (pg *PGSlowLogParser) logLinePrefixToRegexpPattern(logLinePrefix string) string {
	pattern := regexp.QuoteMeta(logLinePrefix)
	var oldnew []string

	for key, val := range prefixRegexps {
		oldnew = append(oldnew, key, val)
	}

	replacer := strings.NewReplacer(oldnew...)

	return replacer.Replace(pattern)
}

func (pg *PGSlowLogParser) Start() error {
	defer close(pg.logEntryChan)

	r := bufio.NewReader(pg.reader)

LOOP:
	for !pg.stopped {
		select {
		case <-pg.stopChan:
			pg.stopped = true
			break LOOP
		default:
		}

		line, err := r.ReadString('\n')
		readBytes := int64(len(line))
		pg.readBytes += readBytes
		if err != nil {
			if err != io.EOF {
				return err
			}

			err = pg.sendLogEntry(line, readBytes)
			if err != nil {
				return err
			}

			break LOOP
		}

		err = pg.sendLogEntry(line, readBytes)
		if err != nil {
			return err
		}
	}

	err := pg.sendLogEntry("", 0)
	if err != nil {
		return err
	}

	return nil
}

func (pg *PGSlowLogParser) sendLogEntry(line string, readBytes int64) error {
	if line == "" && pg.logEntry != nil {
		pg.logEntry.TrimEndNewline()
		pg.logEntryChan <- pg.logEntry
		pg.logEntry = nil
		return nil
	}

	var err error
	if pg.logPrefixRe.MatchString(line) {
		if pg.logEntry != nil {
			pg.logEntry.TrimEndNewline()
			pg.logEntryChan <- pg.logEntry
			pg.logEntry = nil
		}

		matches := pg.slowLogRe.FindStringSubmatch(line)
		if len(matches) < 3 {
			return nil
		}

		durationStr := matches[1]
		statement := matches[2]
		pg.logEntry, err = NewLogEntry(durationStr, statement, readBytes)
		if err != nil {
			return err
		}
	} else { // multi line statement
		if pg.logEntry == nil {
			return nil
		}

		pg.logEntry.ReadBytes += readBytes

		pg.logEntry.AppendStatement(line)
	}

	return nil
}

func (pg *PGSlowLogParser) Stop() {
	pg.stopChan <- true
	pg.stopped = true
	return
}

func (pg *PGSlowLogParser) LogEntryChan() <-chan *LogEntry {
	return pg.logEntryChan
}

func (pg *PGSlowLogParser) ReadBytes() int64 {
	return pg.readBytes
}
