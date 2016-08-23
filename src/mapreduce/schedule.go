package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce

		for i := 0; i < ntasks; i++ {
			work := <-mr.registerChannel
			arg := DoTaskArgs{mr.jobName, mr.files[i], mapPhase, i, nios}
			go func() {
				ok := call(work, "Worker.DoTask", &arg, new(struct{}))
				if ok {
					mr.registerChannel <- work
				} else {
					i--
				}
			}()
		}

	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
		for i := 0; i < nios; i++ {
			work := <-mr.registerChannel
			arg := DoTaskArgs{mr.jobName, mr.files[i], reducePhase, i, nios}
			go func() {
				ok := call(work, "Worker.DoTask", &arg, new(struct{}))
				if ok {
					mr.registerChannel <- work
				} else {
					i--
				}
			}()
		}

	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}
