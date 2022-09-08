package DAGTaskScheduler

import (
	"log"
	"time"
)

type workerState uint8

const (
	workerIdle workerState = iota
	workerRunning
	workerExit
)

func (s workerState) String() string {
	switch s {
	case workerIdle:
		return "Idle"
	case workerRunning:
		return "Running"
	case workerExit:
		return "Exit"
	default:
		return ""
	}
}

type worker struct {
	id         int
	exec       func(task *task)
	exit       func()
	state      workerState
	exitSignal chan struct{}
}

func NewWorker(id int, exec func(task *task), exit func()) *worker {
	return &worker{
		id:         id,
		exec:       exec,
		exit:       exit,
		state:      workerIdle,
		exitSignal: make(chan struct{}),
	}
}

func (w *worker) Work(readyTasks *priorQ, doneTasks chan *task) {
	for {
		select {
		case <-w.exitSignal:
			// exit
			w.exit()
			w.state = workerExit
			log.Printf("Worker %d exit", w.id)
			return
		default:
			break
		}
		if task0, ok := readyTasks.Pop(); ok {
			w.state = workerRunning
			log.Printf("Worker %d got task %d\n", w.id, task0.id)
			task0.state = taskRunning
			w.exec(task0)
			task0.state = taskFinished
			log.Printf("Worker %d finished task %d\n", w.id, task0.id)
		} else {
			w.state = workerIdle
			time.Sleep(time.Second)
		}
	}
}
