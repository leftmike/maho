package stmt

type Stmt interface {
	String() string
}

type Begin struct{}

func (_ *Begin) String() string {
	return "BEGIN"
}

type Commit struct{}

func (_ *Commit) String() string {
	return "COMMIT"
}

type Rollback struct{}

func (_ *Rollback) String() string {
	return "ROLLBACK"
}
