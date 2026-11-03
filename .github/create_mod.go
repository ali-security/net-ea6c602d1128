package main

import (
	"fmt"
	"os"

	"golang.org/x/mod/module"
	"golang.org/x/mod/zip"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: create_mod <module-path> <version> <dir> <out.zip>")
		os.Exit(2)
	}
	mv := module.Version{Path: os.Args[1], Version: os.Args[2]}
	dir := os.Args[3]
	out := os.Args[4]

	f, err := os.Create(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	if err := zip.CreateFromDir(f, mv, dir); err != nil {
		fmt.Fprintln(os.Stderr, "CreateFromDir:", err)
		os.Exit(1)
	}
	if err := f.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
		os.Exit(1)
	}
	fmt.Println("wrote", out)
}
