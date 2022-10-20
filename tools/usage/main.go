package main

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
)

func main() {
	mainHelpOutputB, err := exec.Command("go", "run", "main.go", "--help").Output()
	if err != nil {
		panic(err)
	}
	mainHelpOutput := string(mainHelpOutputB)
	mainHelpOutput, flagsHelpOutput, _ := strings.Cut(mainHelpOutput, "Flags:")
	migrateHelpOutput := commandHelp("migrate")
	combinedOutput := fmt.Sprintf("%s%sFlags:%s", mainHelpOutput, migrateHelpOutput, flagsHelpOutput)

	readme, err := os.ReadFile("README.md")
	if err != nil {
		panic(err)
	}

	format := "<!--usage-shell-->\n```\n%s```"
	re := regexp.MustCompile(fmt.Sprintf(format, "[^`]+"))
	matches := re.FindStringSubmatch(string(readme))
	replaced := strings.ReplaceAll(string(readme), matches[0],
		fmt.Sprintf(format, combinedOutput))

	_ = os.WriteFile("README.md", []byte((replaced)), 0o644)
}

func commandHelp(command ...string) string {
	command = append([]string{"run", "main.go"}, append(command, "--help")...)
	outputBytes, err := exec.Command("go", command...).Output()
	if err != nil {
		panic(err)
	}
	commandHelpOutput := string(outputBytes)
	commandHelpOutput, _, _ = strings.Cut(commandHelpOutput, "Flags:")
	return commandHelpOutput
}
