package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	constructArgs := func(taskNumber int) DoTaskArgs {
		taskArgs := DoTaskArgs{}
		taskArgs.JobName = jobName
		taskArgs.Phase = phase
		taskArgs.TaskNumber = taskNumber
		taskArgs.NumOtherPhase = n_other
		if phase == mapPhase {
			taskArgs.File = mapFiles[taskNumber]
		}
		return taskArgs
	}

	tasks := make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			tasks <- i
		}
	}()

	success := make(chan bool)
	total := 0
	Loop:
		for {
			select {
			case task := <- tasks:
				go func() {
					src := <- registerChan
					var reply ShutdownReply
					taskArgs := constructArgs(task)
					ok := call(src, "Worker.DoTask", &taskArgs, &reply)
					if ok {
						success <- true
						go func() {registerChan <- src}()
					} else {
						tasks <- task
					}
				}()
			case <- success:
				total += 1
			default:
				if total == ntasks {
					break Loop
				}
			}
		}


	fmt.Printf("Schedule: %v done\n", phase)
}
