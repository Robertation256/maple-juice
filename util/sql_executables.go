package util

import (
	"os"
	"text/template"
	"strings"
	"log"
	"cs425-mp4/config"
)

type FilterMapleTemplateData struct {
	Regex string
}

type JoinMapleTemplateData struct {
	Col string
}

func readTemplateFile(filename string) (string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func generateSourceCode(templateContent string, data interface{}) (string, error) {
	tmpl, err := template.New("main").Parse(templateContent)
	if err != nil {
		return "", err
	}

	var generatedCode strings.Builder
	err = tmpl.Execute(&generatedCode, data)
	if err != nil {
		return "", err
	}

	return generatedCode.String(), nil
}

func writeToFile(filename, content string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(content)
	return err
}

func GenerateFilterMapleExecutables(regex string, executableName string) error{

	// Read template content from template.go
	templateContent, readErr := readTemplateFile(config.TemplateFileDir + "filter_maple_template.go")
	if readErr != nil {
		log.Println("Error reading template file", readErr)
		return readErr
	}

	templateData := FilterMapleTemplateData{Regex: regex}
	sourceCode, generateErr := generateSourceCode(templateContent, templateData)
	if generateErr != nil {
		log.Println("Error generating filter maple executable")
		return generateErr
	}

	writeErr := writeToFile(config.LocalFileDir + executableName, sourceCode)
	if writeErr != nil {
		log.Println("Error writing to output file:")
		return writeErr
	}

	return nil

}

func GenerateJoinMapleExecutables(col string, executableName string) error{

	// Read template content from template.go
	templateContent, readErr := readTemplateFile(config.TemplateFileDir + "join_maple_template.go")
	if readErr != nil {
		log.Println("Error reading template file", readErr)
		return readErr
	}

	templateData := JoinMapleTemplateData{Col: col}
	sourceCode, generateErr := generateSourceCode(templateContent, templateData)
	if generateErr != nil {
		log.Println("Error generating filter maple executable")
		return generateErr
	}

	writeErr := writeToFile(config.LocalFileDir + executableName, sourceCode)
	if writeErr != nil {
		log.Println("Error writing to output file:")
		return writeErr
	}

	return nil

}