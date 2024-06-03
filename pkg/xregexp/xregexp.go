package xregexp

import "regexp"

func FindMatchGroups(re *regexp.Regexp, s string) (map[string]string, bool) {
	matches := re.FindStringSubmatch(s)
	return getNamedMatches(re, matches), len(matches) > 0
}

func FindAllMatchGroups(re *regexp.Regexp, s string) ([]map[string]string, bool) {
	matches := re.FindAllStringSubmatch(s, -1)
	namedMatches := []map[string]string{}
	for _, groups := range matches {
		namedMatches = append(namedMatches, getNamedMatches(re, groups))
	}

	return namedMatches, len(matches) > 0
}

func getNamedMatches(re *regexp.Regexp, matches []string) map[string]string {
	result := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i < len(matches) {
			result[name] = matches[i]
		}
	}
	return result
}
