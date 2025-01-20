package jobs

type Source interface {
	isSource()
	construct() (string, construct)
}

type Sink interface {
	isSink()
	construct() (string, construct)
}
