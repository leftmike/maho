package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/leftmike/maho/parser"
)

/*
- add pkg/plan
-- https://www.scattered-thoughts.net/writing/materialize-decorrelation
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/ops
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/optgen
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/memo
-- https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/norm/rules
-- IMDb: https://developer.imdb.com/non-commercial-datasets/
-- join order benchmark: https://github.com/gregrahn/join-order-benchmark

- https://pkg.go.dev/github.com/google/btree

- column types
-- logical: engine and above
-- physical: storage
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
