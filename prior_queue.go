package DAGTaskScheduler

import "sync"

type items struct {
	tasks []*task
}

func (i items) Push(x *task) {
	i.tasks = append(i.tasks, x)
}

func (i items) Pop() *task {
	return i.tasks[len(i.tasks)-1]
}

func (i items) Len() int {
	return len(i.tasks)
}

func (i items) Swap(x, y int) {
	i.tasks[x], i.tasks[y] = i.tasks[y], i.tasks[x]
}

func (i items) PriorityOver(x, y int) bool {
	return i.tasks[x].priority > i.tasks[y].priority
}

type itemsI interface {
	Push(x *task)
	Pop() *task
	Len() int
	Swap(i, j int)
	PriorityOver(i, j int) bool // item i has priority over item j
}

type priorQ struct {
	items itemsI
	lock  sync.Mutex
}

func newPriorQ() *priorQ {
	return &priorQ{
		items: items{
			tasks: []*task{},
		},
		lock: sync.Mutex{},
	}
}

func (q *priorQ) Pop() (*task, bool) {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.items.Len() == 0 {
		return nil, false
	}
	q.items.Swap(0, q.items.Len()-1)
	top := q.items.Pop()
	q.down(0)
	return top, true
}

func (q *priorQ) Push(item *task) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.items.Push(item)
	q.up(q.items.Len() - 1)
}

func (q *priorQ) up(j int) {
	for {
		if j == 0 {
			return
		}
		i := (j - 1) / 2
		if q.items.PriorityOver(i, j) {
			return
		}
		q.items.Swap(i, j)
		j = i
	}
}

func (q *priorQ) down(i int) {
	for {
		j1 := 2*i + 1
		if j1 >= q.items.Len() {
			return
		}
		j := j1
		if j2 := j1 + 1; j2 < q.items.Len() && q.items.PriorityOver(j2, j1) {
			j = j2
		}
		if q.items.PriorityOver(i, j) {
			return
		}
		q.items.Swap(i, j)
		i = j
	}
}
