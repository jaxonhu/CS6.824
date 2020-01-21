package main

import (
	"mapreduce"
	"container/heap"
	"fmt"
)
func main() {
	items := map[string]string{
		"banana": "banana", "apple": "apple", "pear": "pear",
	}

	// 创建一个优先队列，并将上述元素放入到队列里面，
	// 然后对队列进行初始化以满足优先队列（堆）的不变性。
	pq := make(mapreduce.PriorityQueue, len(items))
	i := 0
	for value, key := range items {
		pq[i] = &mapreduce.Item{
			Value:    value,
			Key: key,
			Index:    i,
		}
		i++
	}
	heap.Init(&pq)

	// 插入一个新元素，然后修改它的优先级。
	item := &mapreduce.Item{
		Value:    "orange",
		Key: "orange",
	}
	heap.Push(&pq, item)

	item = &mapreduce.Item{
		Value:    "apple",
		Key: "apple",
	}
	heap.Push(&pq, item)

	item = &mapreduce.Item{
		Value:    "apple",
		Key: "apple",
	}
	heap.Push(&pq, item)

	item = &mapreduce.Item{
		Value:    "pear",
		Key: "pear",
	}
	heap.Push(&pq, item)

	item = &mapreduce.Item{
		Value:    "orange",
		Key: "orange",
	}
	heap.Push(&pq, item)

	item = &mapreduce.Item{
		Value:    "pear",
		Key: "pear",
	}
	heap.Push(&pq, item)

	// 以降序形式取出并打印队列中的所有元素。
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*mapreduce.Item)
		fmt.Printf("%s:%s \n", item.Key, item.Value)
	}
}
