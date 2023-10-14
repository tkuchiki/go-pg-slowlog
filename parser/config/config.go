package config

import (
	"bufio"
	"io"
	"regexp"
	"strings"
)

type PGConfig map[string]string

var commentRe = regexp.MustCompile(`#\s*.+`)

func LoadPGConfig(r io.Reader) PGConfig {
	scanner := bufio.NewScanner(r)

	pgConf := make(PGConfig)

	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "#") {
			continue
		}

		configs := strings.SplitN(line, "=", 2)
		key := strings.TrimSpace(configs[0])
		val := trimSingleQuote(strings.TrimSpace(configs[1]))
		pgConf[key] = trimComment(val)
	}

	return pgConf
}

func trimSingleQuote(s string) string {
	return strings.TrimSuffix(strings.TrimPrefix(s, "'"), "'")
}

func trimComment(s string) string {
	return commentRe.ReplaceAllString(s, "")
}
