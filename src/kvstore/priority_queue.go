package kvstore

import "container/heap"

type item struct {
	term   int
	index  int
	notify chan result
}

type priorityQueue []*item

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	if pq[i].term == pq[j].term {
		return pq[i].index < pq[j].index
	}
	return pq[i].term < pq[j].term
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue) Push(x any) {
	item := x.(*item)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (pq *priorityQueue) pop() *item {
	return heap.Pop(pq).(*item)
}

func (pq *priorityQueue) front() *item {
	if pq.Len() == 0 {
		return nil
	}
	return (*pq)[0]
}

func (pq *priorityQueue) push(i *item) {
	heap.Push(pq, i)
}

func (pq *priorityQueue) init() {
	heap.Init(pq)
}
