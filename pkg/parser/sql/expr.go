package sql

import (
	"fmt"
	"strings"

	"github.com/leftmike/maho/pkg/types"
)

type Expr interface {
	String() string
	isExpr()
}

type Literal struct {
	Value types.Value
}

type Ref []types.Identifier

type SExpr struct {
	Name types.Identifier
	Args []Expr
}

type Op int

const (
	AddOp Op = iota
	AndOp
	BinaryAndOp
	BinaryOrOp
	ConcatOp
	DivideOp
	EqualOp
	GreaterEqualOp
	GreaterThanOp
	LessEqualOp
	LessThanOp
	LShiftOp
	ModuloOp
	MultiplyOp
	NegateOp
	NoOp
	NotEqualOp
	NotOp
	OrOp
	RShiftOp
	SubtractOp
)

var (
	opNames = []string{
		AddOp:          "+",
		AndOp:          "AND",
		BinaryAndOp:    "&",
		BinaryOrOp:     "|",
		ConcatOp:       "||",
		DivideOp:       "/",
		EqualOp:        "==",
		GreaterEqualOp: ">=",
		GreaterThanOp:  ">",
		LessEqualOp:    "<=",
		LessThanOp:     "<",
		LShiftOp:       "<<",
		ModuloOp:       "%",
		MultiplyOp:     "*",
		NegateOp:       "-",
		NoOp:           "",
		NotEqualOp:     "!=",
		NotOp:          "NOT",
		OrOp:           "OR",
		RShiftOp:       ">>",
		SubtractOp:     "-",
	}
)

type UnaryExpr struct {
	Op   Op
	Expr Expr
}

type BinaryExpr struct {
	Op    Op
	Left  Expr
	Right Expr
}

type SubqueryOp int

const (
	Scalar SubqueryOp = iota
	Exists
	Any
	All
)

type Subquery struct {
	Op     SubqueryOp
	ExprOp Op
	Expr   Expr
	Stmt   Stmt
}

func (l Literal) String() string {
	return types.FormatValue(l.Value)
}

func (_ Literal) isExpr() {}

func (ref Ref) String() string {
	var buf strings.Builder
	buf.WriteString(ref[0].String())
	for idx := 1; idx < len(ref); idx += 1 {
		buf.WriteRune('.')
		buf.WriteString(ref[idx].String())
	}
	return buf.String()
}

func (_ Ref) isExpr() {}

func (se *SExpr) String() string {
	var buf strings.Builder
	buf.WriteString(se.Name.String())
	buf.WriteRune('(')
	for idx, arg := range se.Args {
		if idx > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(arg.String())
	}
	buf.WriteRune(')')
	return buf.String()
}

func (_ *SExpr) isExpr() {}

func (ue *UnaryExpr) String() string {
	if ue.Op == NoOp {
		return ue.Expr.String()
	}
	return fmt.Sprintf("(%s %s)", opNames[ue.Op], ue.Expr)
}

func (_ *UnaryExpr) isExpr() {}

func (be *BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", be.Left, opNames[be.Op], be.Right)
}

func (_ *BinaryExpr) isExpr() {}

func (sq *Subquery) String() string {
	switch sq.Op {
	case Scalar:
		return fmt.Sprintf("(%s)", sq.Stmt)
	case Exists:
		return fmt.Sprintf("EXISTS(%s)", sq.Stmt)
	case Any:
		return fmt.Sprintf("%s %s ANY(%s)", sq.Expr, opNames[sq.ExprOp], sq.Stmt)
	case All:
		return fmt.Sprintf("%s %s ALL(%s)", sq.Expr, opNames[sq.ExprOp], sq.Stmt)
	default:
		panic(fmt.Sprintf("unexpected query expression op; got %v", sq.Op))
	}
}

func (_ *Subquery) isExpr() {}
