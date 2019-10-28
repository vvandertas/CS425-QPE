package main

import (
	"fmt"
	"flag"
	"encoding/json"
	"easyexec"
	"log"
	"time"
	"math/rand"
	"strconv"
	"os/exec"
	"container/list"
	"sync"
	"os"
	"bytes"
)

type job struct {
	gen float64
	start float64
	end float64
	class int
	id int
	cmd string
}

type Config struct {
    JobClassParameters []struct {
        SystemParameters struct { 
            DriverCores        string `json:"driverCores"`
            DriverMemory        string `json:"driverMemory"`
            TotalExecutorCores string `json:"totalExecutorCores"`
            ExecutorCores      string `json:"executorCores"`
            ExecutorMemory     string `json:"executorMemory"`
            PyFiles            string `json:"pyFiles"`
            PropertiesFile     string `json:"propertiesFile"`
            Jars               string `json:"jars"`
            Conf               []string `json:"conf"`
            Action             string `json:"action"`
            DataPath           string `json:"dataPath"`
        } `json:"systemParameters"`
        HyperParameters struct { 
            BatchSize          string `json:"batchSize"`
            MaxEpoch           string `json:"maxEpoch"`
            LearningRate       string `json:"learningRate"`
            LearningrateDecay  string `json:"learningrateDecay"`
        } `json:"hyperParameters"`
        ClassProbability   float64 `json:"classProbability"`
        Priorities         []struct {
            Priority       int `json:"priority"`
            Probability    float64 `json:"probability"`
        } `json:"priorities"`
    } `json:"jobClassParameters"`
    Master             string `json:"master"`
    Lambda             float64 `json:"lambda"`
    Runtime            int `json:"runtime"`
    PreemptJobs        int `json:"preemptJobs"`
}

var jobCmds []string = nil
var jobClassProbabilities []string = nil
var jobPriorities []int = nil
var priorities int = 0
var queues [] *list.List =  nil
var done chan int =  nil
var gen chan int = nil
var wait chan int = nil
var run int = 1
var preempt int = 0
var debug bool = false
var qlock = &sync.Mutex{}
var record bool = false
var recordDir string = ""
var configFileName string = ""
var lambda float64 = 0.001

var outputFileName string = ""

func init() {
	flag.StringVar(&configFileName, "c", "conf.json", "config file")
	flag.StringVar(&outputFileName, "o", "test-output", "output filename")
	flag.BoolVar(&debug, "d", false, "debug logging")
	flag.BoolVar(&record, "r", false, "record output")
}

func generator() {
	meaniat := 1.0/lambda

	// Parse job probabilities
	jobp := make([]float64, len(jobClassProbabilities))
	sum := 0.0
	for i := 0; i < len(jobClassProbabilities); i++ {
		var err error
		jobp[i], err = strconv.ParseFloat(jobClassProbabilities[i], 64)
		if  err != nil {
			log.Fatalf("Job probability must be a float: %s", err.Error())
		}
		sum += jobp[i]
	}

	// Compute CDF
	sump := 0.0
	for i := 0; i < len(jobClassProbabilities); i++ {
		sump += jobp[i]
		jobp[i] = sump
		// Normalize
		jobp[i] /= sum
	}

	// While run is true
	jid := 0
	for run == 1 {
		// Sleep for iat seconds
		iat := rand.ExpFloat64() * meaniat
		if debug {
			log.Printf("[GEN] Waiting %f seconds for new job", iat)
		}
		time.Sleep(time.Duration(iat) * time.Second)
		// Chose class based on CDF
		r := rand.Float64()
		i := 0
		for i = 0; i < len(jobClassProbabilities); i++ {
			if(r < jobp[i]) {
				break;
			}
		}
		// Generate job and set job gen time as well as job class
                log.Print(i)
                log.Print(jobPriorities[i])
		job := &job{gen: float64(time.Now().UnixNano()) / 1000000000.0, class: jobPriorities[i], cmd: jobCmds[i]}
		job.id = jid
		jid++

		// Enqueue job to priority queue
		qlock.Lock()
		if(priorities == 1) {
			queues[0].PushBack(job)
		} else {
			queues[job.class].PushBack(job)
		}
		qlock.Unlock()
		if debug {
			log.Printf("[GEN] Generated new job %d of class %d", job.id, job.class)
		}
		select {
			case gen <- 1: // signal jo generation
				if debug {
					log.Printf("[GEN] Signalled new job")
				}
			default: // do not block on full channel
		}
	}

	// We are done, close the channel
	close(gen)
	wait <- 1
}

func logger(j *job, cmd *exec.Cmd, lid float32) {
	err := cmd.Wait()
	j.end = float64(time.Now().UnixNano()) / 1000000000.0
	done <- 1
	if debug {
		log.Printf("[LOG-%f] Job %d of class %d ended with status %t", lid, j.id, j.class, err == nil)
	}
	// class,success,gen,start,end
	fmt.Printf("%d,%d,%t,%f,%f,%f\n", j.class, j.id, err == nil, j.gen, j.start, j.end)
}

func dispatch(j *job, lid float32) *exec.Cmd {
	j.start = float64(time.Now().UnixNano()) / 1000000000.0
	cmdStr := j.cmd
	if debug {
		log.Printf("[DIS-%f] Starting job %d: %s", lid, j.id, cmdStr)
	}
	cmd := easyexec.Cmd(cmdStr)

	if record {
		outfile, err1 := os.Create(fmt.Sprintf("%s/%f.out",recordDir, lid))
		if err1 != nil {
			panic(err1)
		}
		errfile, err2 := os.Create(fmt.Sprintf("%s/%f.err",recordDir, lid))
		if err2 != nil {
			panic(err2)
		}
		cmd.Stdout = outfile
		cmd.Stderr = errfile
	}

	cmd.Start()

	//Wait for its end and log it
	go logger(j, cmd, lid)

	return cmd
}

func selectJob(max int, remove bool) *job {
	var j *job = nil
	for i := 0; i < max; i++ {
		if debug {
			log.Printf("[SEL] Length of %d is %d", i, queues[i].Len())
		}
		if queues[i].Len() > 0 {
			e := queues[i].Front()
			j = e.Value.(*job)
			if remove {
				queues[i].Remove(e)
			}
			if debug {
				log.Printf("[SEL] Selected job %d from %d", j.id, i)
			}
			break
		}
	}
	return j
}

func scheduler() {
	var runningCmd *exec.Cmd = nil
	var runningJob *job = nil

	lid := float32(0.0)

	for run == 1 {
		if debug {
			log.Printf("[SCH] Starting new cycle running job %v", runningJob)
		}
		select {
		case <- gen:
			if debug {
				log.Printf("[SCH] Recieved gen signal")
			}
			if runningCmd == nil { // No job running -> dispatch next job
				//look for highest priority non empty queue and take job from it
				qlock.Lock()
				j := selectJob(priorities, true)
				qlock.Unlock()
				if j != nil {
					runningJob = j
					runningCmd = dispatch(j, lid+0.1)
					lid++
					if debug {
						log.Printf("[SCH] Dispatched job %d of class %d", j.id, j.class)
					}
				} else {
					log.Fatal("We received a gen event but all queues seem empty!")
				}

			} else if preempt > 0 {
				// Look if a higher priority job is waiting
				qlock.Lock()
				j := selectJob(runningJob.class, true)
				if j != nil {
					// Kill current job and start this one
					runningCmd.Process.Kill()
					// Re-enqueue old running job
					if(priorities == 1) {
						queues[0].PushFront(runningJob)
					} else {
						queues[runningJob.class].PushFront(runningJob)
					}
					qlock.Unlock()
					//runningCmd = dispatch(j, lid+0.2)
					//lid++
					if debug {
						log.Printf("[SCH] Preempting job %d of class %d with job %d of class %d", runningJob.id, runningJob.class, j.id, j.class)
					}
					// Wait for killed job
					<- done
					if debug {
						log.Printf("[SCH] Finished waiting for killed job")
					}
					runningJob = j
					runningCmd = dispatch(j, lid + 0.2)
					lid++
					if debug {
						log.Printf("[SCH] Dispatched job %d of class %d", j.id, j.class)
					}
				} else {
					qlock.Unlock()
					if debug {
						log.Printf("[SCH] Nothing to preempt")
					}
				}
			} else if debug {
				log.Printf("[SCH] Ignored gen signal")
			}

		case <- done:
			if debug {
				log.Printf("[SCH] Received done signal")
			}
			//look for next highest priority job
			qlock.Lock()
			runningJob = selectJob(priorities, true)
			qlock.Unlock()
			if runningJob != nil {
				runningCmd = dispatch(runningJob, lid+0.3)
				lid++
				if debug {
					log.Printf("[SCH] Dispatched job %d of class %d", runningJob.id, runningJob.class)
				}
			} else {
				runningCmd = nil
			}
		}
	}
	close(done)
	wait <- 1
}

func cmd(master string, 
	driverCores string, 
	driverMemory string, 
	totalExecutorCores string,
	executorCores string, 
	executorMemory string, 
	pyFiles string, 
	propertiesFile string, 
	jars string, 
	conf []string, 
	action string, 
	dataPath string, 
	batchSize string, 
	maxEpoch string, 
	learningRate string, 
	learningrateDecay string) string {

	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "spark-submit --master %s ", master)
	fmt.Fprintf(&buffer, "--driver-cores %s ", driverCores)
	fmt.Fprintf(&buffer, "--driver-memory %s ", driverMemory)
	fmt.Fprintf(&buffer, "--total-executor-cores %s ", totalExecutorCores)
	fmt.Fprintf(&buffer, "--executor-cores %s ", executorCores)
	fmt.Fprintf(&buffer, "--executor-memory %s ", executorMemory)
	fmt.Fprintf(&buffer, "--py-files %s ", pyFiles)
	fmt.Fprintf(&buffer, "--properties-file %s ", propertiesFile)
	fmt.Fprintf(&buffer, "--jars %s ", jars)
	for i := range(conf) {
		fmt.Fprintf(&buffer, "--conf % s ", conf[i])
	}
	fmt.Fprintf(&buffer, "--action %s ", action)
	fmt.Fprintf(&buffer, "--dataPath %s ", dataPath)
	fmt.Fprintf(&buffer, "--batchSize %s ", batchSize)
	fmt.Fprintf(&buffer, "--endTriggerNum %s ", maxEpoch)
	fmt.Fprintf(&buffer, "--learningRate %s ", learningRate)
	fmt.Fprintf(&buffer, "--learningrateDecay %s ", learningrateDecay)
	return buffer.String()
}

func contains(array []int, item int) bool {
	for _, e := range array {
		if e == item {
			return true
		}
	}
	return false
}

func main() {
	// Parse cmdline options
	flag.Parse()

	var config Config
	configFile, err := os.Open(configFileName)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&config)

	for i := range(config.JobClassParameters) {
		jobClassParameters := config.JobClassParameters[i]
		classProbability := jobClassParameters.ClassProbability

		for j := range(jobClassParameters.Priorities) {
			systemParameters := jobClassParameters.SystemParameters
			hypterParameters := jobClassParameters.HyperParameters

			classCmd := cmd(
					config.Master, 
					systemParameters.DriverCores, 
					systemParameters.DriverMemory, 
					systemParameters.TotalExecutorCores, 
					systemParameters.ExecutorCores, 
					systemParameters.ExecutorMemory, 
					systemParameters.PyFiles, 
					systemParameters.PropertiesFile,
					systemParameters.Jars, systemParameters.Conf, 
					systemParameters.Action, 
					systemParameters.DataPath, 
					hypterParameters.BatchSize, 
					hypterParameters.MaxEpoch,
					hypterParameters.LearningRate, 
					hypterParameters.LearningrateDecay)
			jobCmds = append(jobCmds, classCmd)

			priority := jobClassParameters.Priorities[j]
			probability := fmt.Sprintf("%f", (classProbability * priority.Probability))
			jobClassProbabilities = append(jobClassProbabilities, probability)

			if !contains(jobPriorities, priority.Priority) {
				priorities = priorities + 1
			}

			jobPriorities = append(jobPriorities, priority.Priority)
		}
	}

	preempt = config.PreemptJobs
	runtime := config.Runtime
	lambda = config.Lambda

	if len(jobCmds) == 0 {
		log.Fatalf("No job class command lines defined!")
	}
	if priorities != 1  && len(jobClassProbabilities) != len(jobCmds) {
		log.Fatalf("The priorities (%d) must be one of eaual to the number of job classes (%d)!", priorities, len(jobCmds))
	}

	if(record) {
		//Create output dir using config file as base
		recordDir = outputFileName//config.Configfile+".res"
		err := os.MkdirAll(recordDir, 0755)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Recording output to %s\n", recordDir)
	}

	log.Printf("Found %d job classes and %d priorities!\n", len(jobCmds), priorities)

	//Create as many queues as priorities
	queues = make([]*list.List, priorities)
	for i := range queues {
		queues[i] =  list.New()
	}

	// Channel for signalling end of job or job generation
	gen = make(chan int, 1)
	done = make(chan int, 1)
	wait = make(chan int, 2)

	run = 1

	// Start scheduler
	log.Printf("Starting the scheduler ...")
	go scheduler()
	log.Printf(" done!\n")

	// Start generator
	log.Printf("Starting the generator ...")
	go generator()
	log.Printf(" done!\n")

	// Run for runtime seconds
	log.Printf("Running for %d seconds\n", runtime)
	time.Sleep(time.Duration(runtime) * time.Second)

	// Wait for shut down
	log.Printf("Shutting down ...")
	run = 0
	<- wait
	<- wait
	log.Printf(" done!\n")
}
