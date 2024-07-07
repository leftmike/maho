package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/leftmike/maho/pkg/parser"
)

/*
- add pkg/plan
*/

func main() {
	p := parser.NewParser(bufio.NewReader(os.Stdin), "console")
	for {
		fmt.Print("maho: ")
		stmt, err := p.Parse()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("maho: %s\n", err)
			continue
		}

		fmt.Println(stmt)
	}
}
