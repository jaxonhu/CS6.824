package mapreduce

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	// 1. get all intermediate file
	// 2. read all kvs
	// 3. sort by key
	// 4. input into reduceF and get res
	// 5. write res to final file

	intermediates := make([]*json.Decoder, nMap)
	pq := make(PriorityQueue, 0)

	heap.Init(&pq)
	for i :=int(0); i < nMap ; i ++ {
		filename := reduceName(jobName, i, reduceTask)
		f, err := os.Open(filename)
		if err != nil {
			fmt.Printf("read intermediate file %s error : %s", filename, err.Error())
			return
		}
		intermediates[i] = json.NewDecoder(f)
	}
	idx := 0
	for i,decoder := range intermediates {
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Printf("read file %s error: %s", reduceName(jobName, i, reduceTask), err.Error())
			}
			item := Item{
				Value: kv.Value,
				Key:   kv.Key,
				Index: idx,
			}
			idx ++
			heap.Push(&pq, &item)
		}
	}

	fout, err := os.Create(outFile)
	enc := json.NewEncoder(fout)
	if err != nil {
		fmt.Printf("write reduceFile %s error: %s", outFile, err.Error())
		return
	}

	var prevKv Item
	array := make([]string, 0)
	first := true
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*Item)
		key := item.Key
		if first {
			array = append(array, key)
			prevKv = *item
			first = false
			continue
		}
		if key != prevKv.Key {
			reduceRes := reduceF(prevKv.Key, array)
			err := enc.Encode(KeyValue{Key:prevKv.Key, Value:reduceRes})
			if err != nil {
				fmt.Printf("encode output file: %s  key: %s value: %s error: %s", outFile, prevKv.Key, reduceRes, err.Error())
				return
			}
			array = make([]string, 0)
			prevKv = *item
		}
		array = append(array, key)
	}
	if len(array) > 0 {
		reduceRes := reduceF(prevKv.Key, array)
		err := enc.Encode(KeyValue{Key:prevKv.Key, Value:reduceRes})
		if err != nil {
			fmt.Printf("encode output file: %s  key: %s value: %s error: %s", outFile, prevKv.Key, reduceRes, err.Error())
			return
		}
	}

	return
}
