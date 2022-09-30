package mapreduce

import (
	"fmt"
	"time"
	"math/rand"
)
// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	if(phase == mapPhase){
		workOnMapPhase(nios, ntasks, mr, phase)
	}else{
		workOnReducePhase(nios, ntasks, mr, phase)
	}
	debug("Schedule: %v phase done\n", phase)
	fmt.Println("I am herer")
}

func workOnMapPhase(nios int, ntasks int, mr *Master, phase jobPhase){
	doneWorker := make(chan string, 1)
	waitForWorkerToStart(mr)
	fmt.Println("Worker!", len(mr.workers))
	for index, inputFileName := range mr.files{
		fmt.Println("Beginning scheduling: ")
		var args  DoTaskArgs
		args.JobName = mr.jobName
		args.File = inputFileName
		args.Phase = phase
		args.TaskNumber =index
		args.NumOtherPhase = nios
		fmt.Println("file", inputFileName)
		go callOnWorkerToDoWork(mr, args, doneWorker)
		worker := <-doneWorker
		fmt.Println("worker", worker)
		go setRegisterChannel(worker, mr)
		fmt.Println("End hello ")
	}
}
func workOnReducePhase(nios int, ntasks int, mr *Master, phase jobPhase){
	doneWorker := make(chan string, 1)
	//doneAllWorker := make(chan int, len(mr.files))
	//waitForWorkerToStart(mr)
	index:=0
	for index < nios {
		fmt.Println("Beginning scheduling: ")
		var args  DoTaskArgs
		args.JobName = mr.jobName
		args.Phase = phase
		args.TaskNumber =index
		args.NumOtherPhase = ntasks *2
		if(index == 50){
			break
		}
		go callOnWorkerToDoWork(mr, args, doneWorker)
		worker := <-doneWorker
		fmt.Println("worker", worker)
		go setRegisterChannel(worker, mr)
		fmt.Println("End hello ")
		mr.Lock()
		index++
		mr.Unlock()
	}
}
func callOnWorkerToDoWork(mr *Master, args DoTaskArgs, doneChannel chan string){
	worker := <-mr.registerChannel
	fmt.Println("Hello the problem is here", worker)
	call(worker, "Worker.DoTask", args, nil)
	fmt.Println("End")
	fmt.Println("Workers", mr.workers)
	doneChannel <-worker
}
func waitForWorkerToStart(mr *Master){
	numberWorkers := len(mr.workers)
	 for(numberWorkers == 0){
		numberWorkers = len(mr.workers)
		 if(numberWorkers !=0){
			break;
		 }
		 time.Sleep(1* time.Second)
	 }
}
func setRegisterChannel(worker string, mr *Master){
	if(mr.registerChannel!=nil){
		max :=2
		min :=0
		index := rand.Intn(max-min) + min
		mr.registerChannel <-mr.workers[index]
	}
}
