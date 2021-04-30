package main

import (
	"bytes"
	"go/format"
	"text/template"
)

type Generator interface {
	FileName() string
	Format() ([]byte, error)
}

type generator struct {
	filename string
	template *template.Template
	data     interface{}
}

func (g *generator) Format() ([]byte, error) {
	buffer := bytes.Buffer{}
	err := g.template.Execute(&buffer, g.data)
	if err != nil {
		return nil, err
	}
	return format.Source(buffer.Bytes())
}

func (g *generator) FileName() string {
	return g.filename
}

func newGenerator(filename string, template *template.Template, data interface{}) Generator {
	return &generator{
		filename: filename,
		template: template,
		data:     data,
	}
}
