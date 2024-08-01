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
-- https://www.scattered-thoughts.net/writing/materialize-decorrelation
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/ops
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/optgen
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/memo
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/norm/rules

- https://pkg.go.dev/github.com/google/btree
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
