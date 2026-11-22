package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/roryq/wrench/pkg/xregexp"
)

var (
	// MigrationFileRegex matches the following patterns:
	// 001.sql
	// 001_name.sql
	// 001_name.up.sql
	// 001_name.generated.sql
	MigrationFileRegex = regexp.MustCompile(`^([0-9]+)(?:_([a-zA-Z0-9_\-]+))?(?:[.]up|[.]generated)?\.sql$`)

	// RepeatableMigrationRegex matches the following patterns:
	// R__name.sql
	// R__name.generated.sql
	RepeatableMigrationRegex = regexp.MustCompile(`(?i)^R__([a-zA-Z0-9_\-]+)(?:[.]generated)?\.sql$`)

	MigrationNameRegex = regexp.MustCompile(`[a-zA-Z0-9_\-]+`)

	dmlAnyRegex = regexp.MustCompile("^(UPDATE|DELETE|INSERT)[\t\n\f\r ].*")

	placeholderRegex = regexp.MustCompile(`\$\{(?P<PlaceholderName>[A-Za-z_]+)\}`)

	// 1. INSERT statements are not supported for partitioned DML. Although not every DML can be partitioned
	// as it must be idempotent. This probably isn't solvable with more regexes.
	// 2. UPDATE or DELETE statements with a SELECT statement in the WHERE clause is not fully partitionable.
	notPartitionedDmlRegex = regexp.MustCompile(`(?is)(?:insert)|(?:update|delete).*select`)
)

const (
	StatementKindDDL            StatementKind = "DDL"
	StatementKindDML            StatementKind = "DML"
	StatementKindPartitionedDML StatementKind = "PartitionedDML"
	// StatementKindConvergentDML repeatedly executes all statements in
	// the migration until no more rows are affected. Each statement is executed
	// in its own transaction, and the concurrency can be configured via the
	// @wrench.Concurrency directive.
	StatementKindConvergentDML StatementKind = "ConvergentDML"
)

type (
	// Migration represents a parsed migration file, e.g. version_name.sql or R__name.sql.
	Migration struct {
		// Version is the version of the migration.
		Version uint

		// Name is the name of the migration.
		Name string

		// FileName is the name of the source file for the migration.
		FileName string

		// Statements is the slice of parsed SQL statements.
		Statements []string

		// RawSQL holds the full migration SQL (after placeholder replacement)
		// when loaded in RawScript mode (e.g. for BigQuery procedural scripts).
		RawSQL string

		Kind StatementKind

		// Directives defines config scoped to a single migration.
		Directives MigrationDirectives

		IsRepeatable bool

		Checksum string
	}

	// MigrationDirectives configures how the migration should be executed.
	MigrationDirectives struct {
		// StatementKind overrides the auto-detected statement kind.
		// This can be used to customise how migrations are executed.
		StatementKind StatementKind
		// Concurrency defines the execution concurrency. Only applicable when
		// StatementKind is StatementKindConvergentDML.
		Concurrency int
	}

	Migrations []*Migration

	StatementKind string

	PlaceholderOptions struct {
		// Placeholders is map of placeholder variable names to placeholder values.
		// These will be used for placeholder replacement if ReplacementEnabled is true.
		Placeholders map[string]string
		// ReplacementEnabled is used to enable or disable placeholder substitution within migration files.
		ReplacementEnabled bool
	}

	// LoadOptions configures how migration files are loaded and parsed.
	LoadOptions struct {
		SkipVersions         []uint
		DetectPartitionedDML bool
		Placeholders         PlaceholderOptions
		// RawScript loads the full SQL file without semicolon splitting or
		// DDL/DML statement-kind validation (used by script-capable drivers like BigQuery).
		RawScript bool
	}
)

// LogApplied emits a structured slog.InfoContext record for an applied migration.
func (m *Migration) LogApplied(ctx context.Context, rowsAffected *int64) {
	msg := "migration applied"
	var attrs []any
	if m.IsRepeatable {
		msg = "repeatable migration applied"
		attrs = []any{"name", m.Name, "file", m.FileName}
	} else {
		attrs = []any{"version", m.Version, "file", m.FileName}
		if m.Name != "" {
			attrs = append(attrs, "name", m.Name)
		}
	}
	if rowsAffected != nil {
		attrs = append(attrs, "rows_affected", *rowsAffected)
	}
	slog.InfoContext(ctx, msg, attrs...)
}

func (ms Migrations) Len() int {
	return len(ms)
}

func (ms Migrations) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Migrations) Less(i, j int) bool {
	if ms[i].IsRepeatable != ms[j].IsRepeatable {
		return !ms[i].IsRepeatable
	}
	if ms[i].IsRepeatable {
		return ms[i].Name < ms[j].Name
	}
	return ms[i].Version < ms[j].Version
}

// LoadMigrations loads and parses migration files from dir using Spanner-compatible statement splitting.
func LoadMigrations(dir string, toSkipSlice []uint, detectPartitionedDML bool, placeholderOptions PlaceholderOptions) (Migrations, error) {
	return LoadMigrationsWithOptions(dir, LoadOptions{
		SkipVersions:         toSkipSlice,
		DetectPartitionedDML: detectPartitionedDML,
		Placeholders:         placeholderOptions,
	})
}

// LoadMigrationsWithOptions loads and parses migration files from dir according to opts.
func LoadMigrationsWithOptions(dir string, opts LoadOptions) (Migrations, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	toSkipMap := map[uint64]bool{}
	for _, skip := range opts.SkipVersions {
		toSkipMap[uint64(skip)] = true
	}

	var migrations Migrations
	for _, f := range files {
		if f.IsDir() {
			continue
		}

		var version uint64
		var name string
		var isRepeatable bool

		if matches := MigrationFileRegex.FindStringSubmatch(f.Name()); matches != nil {
			v, err := strconv.ParseUint(matches[1], 10, 64)
			if err != nil {
				continue
			}
			if v == 0 {
				return nil, fmt.Errorf("migration %s has invalid version 0; versioned migrations must start at 1", f.Name())
			}
			if toSkipMap[v] {
				continue
			}
			version = v
			name = matches[2]
		} else if matches := RepeatableMigrationRegex.FindStringSubmatch(f.Name()); matches != nil {
			isRepeatable = true
			name = matches[1]
		} else {
			continue
		}

		file, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			continue
		}

		// Parse any migration-scoped directives for the migration
		directives, err := ParseMigrationDirectives(string(file))
		if err != nil {
			return nil, err
		}

		var statements []string
		var rawSQL string
		var kind StatementKind
		var checksum string

		if opts.RawScript {
			sqlContent := string(file)
			if opts.Placeholders.ReplacementEnabled {
				replaced, err := ReplacePlaceholders([]string{sqlContent}, opts.Placeholders.Placeholders)
				if err != nil {
					return nil, err
				}
				sqlContent = replaced[0]
			}
			rawSQL = sqlContent
			if trimmed := strings.TrimSpace(sqlContent); trimmed != "" {
				statements = []string{trimmed}
			}

			hash := sha256.New()
			normalized := strings.ReplaceAll(strings.TrimSpace(sqlContent), "\r\n", "\n")
			hash.Write([]byte(normalized))
			checksum = hex.EncodeToString(hash.Sum(nil))
		} else {
			statements, err = ToStatements(file)
			if err != nil {
				return nil, err
			}

			if opts.Placeholders.ReplacementEnabled {
				statements, err = ReplacePlaceholders(statements, opts.Placeholders.Placeholders)
				if err != nil {
					return nil, err
				}
			}

			kind, err = InspectStatementsKind(statements, opts.DetectPartitionedDML)
			if err != nil {
				return nil, err
			}

			hash := sha256.New()
			for _, stmt := range statements {
				// Normalize line endings to LF to ensure consistent checksums across platforms
				normalized := strings.ReplaceAll(stmt, "\r\n", "\n")
				hash.Write([]byte(normalized))
			}
			checksum = hex.EncodeToString(hash.Sum(nil))
		}

		migrations = append(migrations, &Migration{
			Version:      uint(version),
			Name:         name,
			FileName:     f.Name(),
			Statements:   statements,
			RawSQL:       rawSQL,
			Kind:         kind,
			Directives:   directives,
			IsRepeatable: isRepeatable,
			Checksum:     checksum,
		})
	}

	sort.Sort(migrations)
	seen := map[uint]*Migration{}
	seenRepeatable := map[string]*Migration{}
	for _, m := range migrations {
		if m.IsRepeatable {
			if dupe, got := seenRepeatable[m.Name]; got {
				return nil, fmt.Errorf("repeatable migration %s has a duplicate name in file %s", m.Name, dupe.FileName)
			}
			seenRepeatable[m.Name] = m
			continue
		}
		if dupe, got := seen[m.Version]; got {
			return nil, fmt.Errorf("migration %d %s has a duplicate version number of %s", m.Version, m.Name, dupe.Name)
		}
		seen[m.Version] = m
	}

	return migrations, nil
}

// ToStatements parses a migration file into a slice of statements by splitting
// on semicolons while respecting quotes and comments.
//
// The state machine logic is adapted from RemoveCommentsAndTrim, which is
// lifted directly from googleapis/go-sql-spanner.
// https://github.com/googleapis/go-sql-spanner/blob/076c63111370017133f79dd37c0069f68f27d7df/statement_parser.go
func ToStatements(file []byte) ([]string, error) {
	const (
		singleQuote = '\''
		doubleQuote = '"'
		backtick    = '`'
		hyphen      = '-'
		dash        = '#'
		slash       = '/'
		asterisk    = '*'
		semicolon   = ';'
	)

	var statements []string
	var currentStmt strings.Builder

	isInQuoted := false
	isInSingleLineComment := false
	isInMultiLineComment := false
	var startQuote rune
	lastCharWasEscapeChar := false
	isTripleQuoted := false

	runes := []rune(string(file))
	index := 0
	for index < len(runes) {
		c := runes[index]
		if isInQuoted {
			if (c == '\n' || c == '\r') && !isTripleQuoted {
				return nil, fmt.Errorf("statement contains an unclosed literal: %s", string(file))
			} else if c == startQuote {
				if lastCharWasEscapeChar {
					lastCharWasEscapeChar = false
				} else if isTripleQuoted {
					if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
						isInQuoted = false
						startQuote = 0
						isTripleQuoted = false
						currentStmt.WriteRune(c)
						currentStmt.WriteRune(c)
						index += 2
					}
				} else if (startQuote == singleQuote || startQuote == doubleQuote) && len(runes) > index+1 && runes[index+1] == startQuote {
					// escaped quote '' or ""
					if isTripleQuoted {
						// in triple quotes, we only care about the end marker
					} else {
						currentStmt.WriteRune(c)
						c = runes[index+1]
						index++
					}
				} else {
					isInQuoted = false
					startQuote = 0
				}
			} else if c == '\\' {
				lastCharWasEscapeChar = true
			} else {
				lastCharWasEscapeChar = false
			}
			currentStmt.WriteRune(c)
		} else {
			// We are not in a quoted string.
			if isInSingleLineComment {
				if c == '\n' {
					isInSingleLineComment = false
				}
				currentStmt.WriteRune(c)
			} else if isInMultiLineComment {
				if len(runes) > index+1 && c == asterisk && runes[index+1] == slash {
					isInMultiLineComment = false
					currentStmt.WriteRune(c)
					currentStmt.WriteRune(runes[index+1])
					index++
				} else {
					currentStmt.WriteRune(c)
				}
			} else {
				if c == dash || (len(runes) > index+1 && c == hyphen && runes[index+1] == hyphen) {
					// This is a single line comment.
					isInSingleLineComment = true
					currentStmt.WriteRune(c)
				} else if len(runes) > index+1 && c == slash && runes[index+1] == asterisk {
					isInMultiLineComment = true
					currentStmt.WriteRune(c)
					currentStmt.WriteRune(runes[index+1])
					index++
				} else if c == semicolon {
					// End of statement
					stmt, err := RemoveCommentsAndTrim(currentStmt.String())
					if err != nil {
						return nil, err
					}
					if stmt != "" {
						statements = append(statements, stmt)
					}
					currentStmt.Reset()
				} else {
					if c == singleQuote || c == doubleQuote || c == backtick {
						isInQuoted = true
						startQuote = c
						lastCharWasEscapeChar = false
						// Check whether it is a triple-quote.
						if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
							isTripleQuoted = true
							currentStmt.WriteRune(c)
							currentStmt.WriteRune(c)
							index += 2
						}
					}
					currentStmt.WriteRune(c)
				}
			}
		}
		index++
	}

	if isInQuoted {
		return nil, fmt.Errorf("statement contains an unclosed literal: %s", string(file))
	}

	// Handle the last statement if it doesn't end with a semicolon
	stmt, err := RemoveCommentsAndTrim(currentStmt.String())
	if err != nil {
		return nil, err
	}
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements, nil
}

// ReplacePlaceholders substitutes ${KEY} placeholders in each statement.
func ReplacePlaceholders(statements []string, placeholders map[string]string) ([]string, error) {
	replacedStatements := []string{}
	for _, statement := range statements {
		// Check for any placeholder references that are not configured in the placeholders key value map.
		placeholdersInStatement, found := xregexp.FindAllMatchGroups(placeholderRegex, statement)
		if found {
			for _, match := range placeholdersInStatement {
				_, ok := placeholders[match["PlaceholderName"]]
				if !ok {
					return nil, fmt.Errorf("migration statement refers to placeholder variable that is not defined")
				}
			}
		}

		// Perform placeholder replacement.
		for placeholderName, placeholderValue := range placeholders {
			statement = strings.ReplaceAll(statement, fmt.Sprintf("${%s}", placeholderName), placeholderValue)
		}
		replacedStatements = append(replacedStatements, statement)
	}

	return replacedStatements, nil
}

// InspectStatementsKind determines the statement kind for a set of statements.
func InspectStatementsKind(statements []string, detectPartitionedDML bool) (StatementKind, error) {
	kindMap := map[StatementKind]uint64{
		StatementKindDDL:            0,
		StatementKindDML:            0,
		StatementKindPartitionedDML: 0,
	}

	for _, s := range statements {
		kindMap[GetStatementKind(s)]++
	}

	if distinctKind(kindMap, StatementKindDDL) {
		return StatementKindDDL, nil
	}

	// skip further DML type inspection unless detectPartitionedDML is true
	if !detectPartitionedDML && distinctKind(kindMap, StatementKindDML, StatementKindPartitionedDML) {
		return StatementKindDML, nil
	}

	if detectPartitionedDML && distinctKind(kindMap, StatementKindDML) {
		return StatementKindDML, nil
	}

	if detectPartitionedDML && distinctKind(kindMap, StatementKindPartitionedDML) {
		return StatementKindPartitionedDML, nil
	}

	return "", errors.New("Cannot specify DDL and DML in the same migration file")
}

func distinctKind(kindMap map[StatementKind]uint64, kinds ...StatementKind) bool {
	// sum the target statement kinds
	var target uint64
	for _, k := range kinds {
		target = target + kindMap[k]
	}

	// sum all statement kinds
	var total uint64
	for k := range kindMap {
		total = total + kindMap[k]
	}

	return target == total
}

func GetStatementKind(statement string) StatementKind {
	if IsPartitionedDMLOnly(statement) {
		return StatementKindPartitionedDML
	}

	if isDMLAny(statement) {
		return StatementKindDML
	}

	return StatementKindDDL
}

func IsPartitionedDMLOnly(statement string) bool {
	return isDMLAny(statement) && !notPartitionedDmlRegex.Match([]byte(statement))
}

func isDMLAny(statement string) bool {
	return dmlAnyRegex.Match([]byte(statement))
}

// RemoveCommentsAndTrim removes any comments in the query string and trims any
// spaces at the beginning and end of the query. This makes checking what type
// of query a string is a lot easier, as only the first word(s) need to be
// checked after this has been removed.
//
// This function is lifted directly from googleapis/go-sql-spanner.
// https://github.com/googleapis/go-sql-spanner/blob/076c63111370017133f79dd37c0069f68f27d7df/statement_parser.go
func RemoveCommentsAndTrim(sql string) (string, error) {
	const singleQuote = '\''
	const doubleQuote = '"'
	const backtick = '`'
	const hyphen = '-'
	const dash = '#'
	const slash = '/'
	const asterisk = '*'
	isInQuoted := false
	isInSingleLineComment := false
	isInMultiLineComment := false
	var startQuote rune
	lastCharWasEscapeChar := false
	isTripleQuoted := false
	res := strings.Builder{}
	res.Grow(len(sql))
	index := 0
	runes := []rune(sql)
	for index < len(runes) {
		c := runes[index]
		if isInQuoted {
			if (c == '\n' || c == '\r') && !isTripleQuoted {
				return "", fmt.Errorf("statement contains an unclosed literal: %s", sql)
			} else if c == startQuote {
				if lastCharWasEscapeChar {
					lastCharWasEscapeChar = false
				} else if isTripleQuoted {
					if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
						isInQuoted = false
						startQuote = 0
						isTripleQuoted = false
						res.WriteRune(c)
						res.WriteRune(c)
						index += 2
					}
				} else if (startQuote == singleQuote || startQuote == doubleQuote) && len(runes) > index+1 && runes[index+1] == startQuote {
					// escaped quote '' or ""
					if isTripleQuoted {
						// in triple quotes, we only care about the end marker
					} else {
						res.WriteRune(c)
						c = runes[index+1]
						index++
					}
				} else {
					isInQuoted = false
					startQuote = 0
				}
			} else if c == '\\' {
				lastCharWasEscapeChar = true
			} else {
				lastCharWasEscapeChar = false
			}
			res.WriteRune(c)
		} else {
			// We are not in a quoted string.
			if isInSingleLineComment {
				if c == '\n' {
					isInSingleLineComment = false
					// Include the line feed in the result.
					res.WriteRune(c)
				}
			} else if isInMultiLineComment {
				if len(runes) > index+1 && c == asterisk && runes[index+1] == slash {
					isInMultiLineComment = false
					index++
				}
			} else {
				if c == dash || (len(runes) > index+1 && c == hyphen && runes[index+1] == hyphen) {
					// This is a single line comment.
					isInSingleLineComment = true
				} else if len(runes) > index+1 && c == slash && runes[index+1] == asterisk {
					isInMultiLineComment = true
					index++
				} else {
					if c == singleQuote || c == doubleQuote || c == backtick {
						isInQuoted = true
						startQuote = c
						lastCharWasEscapeChar = false
						// Check whether it is a triple-quote.
						if len(runes) > index+2 && runes[index+1] == startQuote && runes[index+2] == startQuote {
							isTripleQuoted = true
							res.WriteRune(c)
							res.WriteRune(c)
							index += 2
						}
					}
					res.WriteRune(c)
				}
			}
		}
		index++
	}
	if isInQuoted {
		return "", fmt.Errorf("statement contains an unclosed literal: %s", sql)
	}
	trimmed := strings.TrimSpace(res.String())
	if len(trimmed) > 0 && trimmed[len(trimmed)-1] == ';' {
		return trimmed[:len(trimmed)-1], nil
	}
	return trimmed, nil
}

// ParseMigrationDirectives extracts migration directives in the format
// @wrench.{key}={value} from the migration preamble.
func ParseMigrationDirectives(migration string) (MigrationDirectives, error) {
	const (
		concurrencyKey   = "Concurrency"
		statementKindKey = "StatementKind"
	)

	// matches a migration directive in the format @wrench.{key}={value}
	directiveRegex := regexp.MustCompile(`(?m)^\s*@wrench[.](?P<Key>\w+)=(?P<Value>\w+)`)
	directiveMatches, _ := xregexp.FindAllMatchGroups(directiveRegex, ExtractPreamble(migration))

	var directives MigrationDirectives
	for _, match := range directiveMatches {
		key, val := match["Key"], match["Value"]
		switch key {
		case statementKindKey:
			directives.StatementKind = StatementKind(val)
		case concurrencyKey:
			concurrency, err := strconv.Atoi(val)
			if err != nil || concurrency < 1 {
				return MigrationDirectives{}, fmt.Errorf("invalid concurrency value: %s", val)
			}
			directives.Concurrency = concurrency
		default:
			return directives, fmt.Errorf("unknown migration directive: %s", key)
		}
	}

	return directives, nil
}

// ExtractPreamble returns all comments from the start of a migration file,
// until the first non-empty non-comment line is encountered.
func ExtractPreamble(migration string) string {
	const (
		blockCommentStart    = "/*"
		blockCommentEnd      = "*/"
		lineCommentPrefix    = "--"
		lineCommentAltPrefix = "#"
	)

	var comments []string
	var blockComment bool
	for line := range strings.SplitSeq(migration, "\n") {
		// Skip empty lines.
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Look for block or line comment start.
		if !blockComment {
			if after, ok := strings.CutPrefix(line, blockCommentStart); ok {
				blockComment = true
				line = after
			} else if after, ok := strings.CutPrefix(line, lineCommentPrefix); ok {
				line = after
			} else if after, ok := strings.CutPrefix(line, lineCommentAltPrefix); ok {
				line = after
			} else {
				// Not in a block comment or line comment, and the line is not
				// empty. Preamble is over.
				break
			}
		}

		// Look for block comment exit.
		if blockComment && strings.Contains(line, blockCommentEnd) {
			line, _, _ = strings.Cut(line, blockCommentEnd)
			blockComment = false
		}

		// Capture non-empty comment lines
		if line = strings.TrimSpace(line); line != "" {
			comments = append(comments, line)
		}
	}

	return strings.Join(comments, "\n")
}
