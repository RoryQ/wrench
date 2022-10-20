package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func main() {
	helpOutput, err := exec.Command("go", "run", "main.go", "--help").Output()
	if err != nil {
		panic(err)
	}

	readme, err := os.ReadFile("README.md")
	if err != nil {
		panic(err)
	}

	format := "<!--usage-shell-->\n```\n%s```"
	re := regexp.MustCompile(fmt.Sprintf(format, "[^`]+"))
	matches := re.FindStringSubmatch(string(readme))
	replaced := strings.ReplaceAll(string(readme), matches[0],
		fmt.Sprintf(format, helpOutput))

	_ = os.WriteFile("README.md", []byte(replaced), 0o644)
}
