package main

import (
	"github.com/mimiro-io/mssqldatalayer/internal"
)

func main() {
	app := internal.CreateLayer()
	app.Run()
}