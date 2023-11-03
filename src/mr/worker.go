package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	keepFlag := true
	for keepFlag {
		task := GetTask() // 通过rpc获取任务、类型
		switch task.TaskType {
		case MapTask:
			{
				//fmt.Print("Map Phase!")
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case WaitingTask:
			{
				//fmt.Println("Waiting")
				time.Sleep(time.Second * 5)
			}
		case ReduceTask:
			{
				//fmt.Print("Reduce Phase")
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case ExitTask:
			{
				//fmt.Println("Task clear! Worker is exiting......")
				time.Sleep(time.Second)
				keepFlag = false
			}
		}
	}
	time.Sleep(time.Second)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)
	if ok {
		//fmt.Println("worker get ", reply.TaskType, "task :Id[", reply.TaskId, "]")
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// Map方法
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var kvOuts []KeyValue
	filename := response.FileSlice[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("can't open %v", filename)
	}
	// 通过ioutil获取内容，作为插件的mapf的参数
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("can't read %v", filename)
	}
	file.Close()
	// mapf返回K-V结构体数组
	kvOuts = mapf(filename, string(content))
	reducerNum := response.ReducerNum
	// 创建一个长度为nReduce的二维切片
	hashedKV := make([][]KeyValue, reducerNum)

	for _, kv := range kvOuts {
		hashedKV[ihash(kv.Key)%reducerNum] = append(hashedKV[ihash(kv.Key)%reducerNum], kv)
	}
	for i := 0; i < reducerNum; i++ { // 调用json库，生成中间文件并按规则命名
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		encoder := json.NewEncoder(ofile)
		for _, kv := range hashedKV[i] {
			err := encoder.Encode(kv)
			if err != nil {
				return
			}
		}
		ofile.Close()
	}
}

// shuffle排序kv对需要的方法
type SortedKey []KeyValue

func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }

// shuffle方法，将map完成输出的kv对排序
func shuffle(files []string) []KeyValue {
	var kvOuts []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvOuts = append(kvOuts, kv)
		}
		file.Close()
	}
	sort.Sort(SortedKey(kvOuts))
	return kvOuts
}
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	// 对之前的tmp文件进行shuffle，得到一组排序好的kv数组
	kvOuts := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("ERROR: Failed to create temp file", err)
	}
	// 进行reduce聚合操作，同key项合并，value形成分片不断加入
	i := 0
	for i < len(kvOuts) {
		j := i + 1
		for j < len(kvOuts) && kvOuts[j].Key == kvOuts[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kvOuts[k].Value)
		}
		// 传入插件reducef进行相应的算子，得到结果
		output := reducef(kvOuts[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kvOuts[i].Key, output)
		i = j
	}
	tempFile.Close()
	// 写入完成后，重命名输出的文件
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

// 发送rpc，标注任务完成
func callDone(f *Task) Task {
	args := f
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		// fmt.Println(reply)
	} else {
		// fmt.Printf("call failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
