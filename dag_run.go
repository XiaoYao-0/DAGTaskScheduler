package DAGTaskScheduler

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultCapOfDoneTasks = 100
)

type DAG struct {
	Tasks         map[int]*Task
	Dependencies1 map[int][]int
	Dependencies2 [][]int
}

type Task struct {
	Exec          func()
	EstimatedTime int64
}

type DAGRun struct {
	workers                  map[int]*worker
	workersWG                sync.WaitGroup
	tasks                    map[int]*task
	readyTasks               *priorQ
	doneTasks                chan *task
	inDegrees                map[int]int
	numberOfNotFinishedTasks int
	startTime                int64 // ms
	endTime                  int64 // ms
}

func NewDAGRun(dag *DAG) (*DAGRun, error) {
	if dag.Dependencies1 != nil {
		return newDAGRunWithDependencies1(dag.Tasks, dag.Dependencies1)
	}
	if dag.Dependencies2 != nil {
		return newDAGRunWithDependencies2(dag.Tasks, dag.Dependencies2)
	}
	return nil, errors.New("no dependencies")
}

func newDAGRunWithDependencies1(tasks map[int]*Task, dependencies map[int][]int) (*DAGRun, error) {
	// TODO check if there is a loop
	dag := &DAGRun{
		workers:                  make(map[int]*worker),
		workersWG:                sync.WaitGroup{},
		tasks:                    make(map[int]*task),
		readyTasks:               newPriorQ(),
		doneTasks:                make(chan *task, DefaultCapOfDoneTasks),
		inDegrees:                make(map[int]int),
		numberOfNotFinishedTasks: len(tasks),
	}
	for id, task0 := range tasks {
		dag.tasks[id] = &task{
			id:            id,
			estimatedTime: task0.EstimatedTime,
			exec:          task0.Exec,
			state:         taskBlocked,
		}
		dag.inDegrees[id] = 0
	}
	for id, dependency := range dependencies {
		for _, nextID := range dependency {
			dag.tasks[id].nexts = append(dag.tasks[id].nexts, dag.tasks[nextID])
			dag.inDegrees[nextID]++
		}
	}
	var calculatePriority func(task0 *task) int64
	calculatePriority = func(task0 *task) int64 {
		if len(task0.nexts) == 0 {
			task0.priority = 0
			return 0
		}
		var p int64
		for _, next := range task0.nexts {
			p += next.estimatedTime
			p += calculatePriority(next)
		}
		task0.priority = p
		return p
	}
	for id, inDegree := range dag.inDegrees {
		if inDegree == 0 {
			calculatePriority(dag.tasks[id])
			dag.tasks[id].state = taskReady
			dag.readyTasks.Push(dag.tasks[id])
		}
	}
	return dag, nil
}

func newDAGRunWithDependencies2(tasks map[int]*Task, dependencies [][]int) (*DAGRun, error) {
	// TODO check if there is a loop
	// TODO
	return &DAGRun{}, nil
}

func (d *DAGRun) Run() {
	d.startTime = time.Now().UnixMilli()
	done := make(chan struct{})
	go d.scheduler(done)
	<-done
	d.endTime = time.Now().UnixMilli()
	fmt.Printf("DAG tasks done, time consumed = %d ms\n", d.endTime-d.startTime)
	return
}

func (d *DAGRun) RunWithMonitor() {
	d.startTime = time.Now().UnixMilli()
	done := make(chan struct{})
	go d.scheduler(done)
	<-done
	return
}

func (d *DAGRun) scheduler(done chan struct{}) {
	defer func() {
		done <- struct{}{}
	}()
	for {
		if d.numberOfNotFinishedTasks == 0 {
			for _, w := range d.workers {
				w.exitSignal <- struct{}{}
			}
			d.workersWG.Wait()
			return
		}
		select {
		case doneTask := <-d.doneTasks:
			d.numberOfNotFinishedTasks--
			for _, next := range doneTask.nexts {
				d.inDegrees[next.id]--
				if d.inDegrees[next.id] == 0 {
					next.state = taskReady
					d.readyTasks.Push(next)
				}
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func (d *DAGRun) AddWorker(w *worker) error {
	if _, ok := d.workers[w.id]; ok {
		return fmt.Errorf("worker id %d already exists", w.id)
	}
	d.workers[w.id] = w
	d.workersWG.Add(1)
	go func() {
		defer d.workersWG.Done()
		w.Work(d.readyTasks, d.doneTasks)
	}()
	return nil
}

func (d *DAGRun) TerminateWorker(id int) error {
	if _, ok := d.workers[id]; !ok {
		return fmt.Errorf("worker %d doesn't exist", id)
	}
	d.workers[id].exitSignal <- struct{}{}
	return nil
}

func (d *DAGRun) TaskMonitor() string {
	// tasks
	s := "task_id, state\n"
	for _, t := range d.tasks {
		s += fmt.Sprintf("%d, %s\n", t.id, t.state.String())
	}
	return s
}

func (d *DAGRun) WorkerMonitor() string {
	// tasks
	s := "worker_id, state\n"
	for _, w := range d.workers {
		s += fmt.Sprintf("%d, %s\n", w.id, w.state.String())
	}
	return s
}
