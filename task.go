package DAGTaskScheduler

type taskState uint8

const (
	taskBlocked taskState = iota
	taskReady
	taskRunning
	taskFinished
)

func (s taskState) String() string {
	switch s {
	case taskBlocked:
		return "Blocked"
	case taskReady:
		return "Ready"
	case taskRunning:
		return "Running"
	case taskFinished:
		return "Finished"
	default:
		return ""
	}
}

type task struct {
	id            int
	prevs         []*task
	nexts         []*task
	estimatedTime int64 // ms
	priority      int64
	exec          func()
	state         taskState
}
