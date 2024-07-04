package expr

type Expr interface {
	String() string
	Equal(e Expr) bool
	HasRef() bool
}

// XXX: SExpr
// XXX: ValExpr
// XXX: RefExpr
// XXX: Add Eval()
