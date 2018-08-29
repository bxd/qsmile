package qsmile

import (
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	pfmt "github.com/x-cray/logrus-prefixed-formatter"
)

var lowerLetterRunes = []rune("abcdefghijklmnopqrstuvwxyz")
var upperLetterRunes = []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
var lg = logrus.New()

func init() {
	fmter := new(pfmt.TextFormatter)
	fmter.TimestampFormat = "06/1/2 15:04:05.000000000"
	fmter.FullTimestamp = true
	fmter.QuoteEmptyFields = true
	fmter.ForceFormatting = true
	lg.SetFormatter(fmter)
	//lg.SetLevel(logrus.WarnLevel)
	lg.SetLevel(logrus.DebugLevel)
	lg.SetOutput(os.Stdout)
	rand.Seed(time.Now().UnixNano())
}

func SetLogLevel(l logrus.Level) {
	if l >= 0 && l <= 6 {
		lg.SetLevel(l)
	} else {
		lg.SetLevel(logrus.WarnLevel)
	}
}

func RandomEnName(max int) string {
	if max < 3 {
		max = 3
	}
	n := rand.Intn(max-2) + 3
	b := make([]rune, n)

	for i := range b {
		b[i] = lowerLetterRunes[rand.Intn(len(lowerLetterRunes))]
	}
	b[0] = upperLetterRunes[rand.Intn(len(upperLetterRunes))]
	return string(b)
}

/*
 * Task
 */
type Task interface {
	Init() bool
	Step() Step
	Done()
}

/*
 * Step
 */
type Step interface {
	HasNext() bool
	Next() interface{}
	PreDo()
	Do()
	PostDo()
	Done()
}

type taskQ chan Task

type Job struct {
	q          taskQ
	labors     []*worker
	lastWorker int
	size       int
	chClose    chan bool
	bOpen      bool
	wg         *sync.WaitGroup
	mtxJob     sync.Mutex
	ticker     *time.Ticker
}

const (
	WORKER_IDLE = iota
	WORKER_BUSY
	WORKER_RETIRE
)

type worker struct {
	name           string
	task           taskQ
	closeInstantly chan bool
	taskDone       chan bool
	state          int
	mtxWorking     sync.Mutex
	wg             *sync.WaitGroup
	ticker         *time.Ticker
}

/*
 * ======Job======
 */
func CreateJob(size int) *Job {
	if size <= 0 {
		size = 1
	}

	j := new(Job)
	j.q = make(taskQ, size)
	j.chClose = make(chan bool, 1)
	j.lastWorker = -1
	j.size = size
	j.wg = &sync.WaitGroup{}

	for i := 0; i < size; i++ {
		j.labors = append(j.labors, j.dispatchWorker(j.wg))
	}
	runtime.SetFinalizer(j, j.clean)
	j.bOpen = true
	return j
}

func (me *Job) clean(o *Job) {
	close(me.chClose)
}

func (me *Job) Kickoff() *Job {
	//me.ticker = time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			select {
			case t := <-me.q:
				lg.Infof("Get a task from Job Q %#v\n", t)
				me.findIdleWorker().assignTask(t)
			case b := <-me.chClose:
				if !b {
					lg.Info("Job dead", me.wg)
					return
				}
			}
			// select {
			// case t := <-me.q:
			// 	lg.Infof("Get a task from Job Q %#v\n", t)
			// 	me.findIdleWorker().assignTask(t)
			// default:
			// 	select {
			// 	case b := <-me.chClose:
			// 		if !b {
			// 			lg.Info("Job dead", me.wg)
			// 			break
			// 		}
			// 		// default:
			// 		// 	<-me.ticker.C
			// 		// 	lg.Infoln("Job living", me.wg)
			// 	}
			// }
		}
	}()
	return me
}

func (me *Job) AddTask(t Task) *Job {
	if t != nil && me.bOpen {
		me.wg.Add(1)
		lg.Infoln("Job Q len ", len(me.q), "Adding task", t)
		me.q <- t
	}
	return me
}

func (me *Job) Stop(immediate bool) {
	me.bOpen = false
	lg.Infoln("Stoping..... wg ", me.wg)
	if !immediate {
		me.wg.Wait()
	}
	for _, v := range me.labors {
		v.stopWork(false)
	}
	me.chClose <- false
	close(me.q)
}

func (me *Job) findIdleWorker() *worker {
	me.mtxJob.Lock()
	defer me.mtxJob.Unlock()
	for {
		if me.lastWorker >= me.size-1 {
			me.lastWorker = -1
		}
		me.lastWorker++
		if WORKER_IDLE == me.labors[me.lastWorker].stat() {
			return me.labors[me.lastWorker]
		}
	}
}

func (me *Job) dispatchWorker(wg *sync.WaitGroup) *worker {
	return new(worker).startWork(wg)
}

/*
 *===========worker========
 */
func (me *worker) startWork(wg *sync.WaitGroup) *worker {
	me.mtxWorking.Lock()
	me.task = make(taskQ, 1)
	me.closeInstantly = make(chan bool, 1)
	me.taskDone = make(chan bool, 1)
	me.state = WORKER_IDLE
	me.wg = wg
	me.name = "♈  " + RandomEnName(10) + "  ♈"
	me.mtxWorking.Unlock()
	//me.ticker = time.NewTicker(500 * time.Millisecond)
	go func() {
		for {
			lg.Debugln(me.name, "worker new loop ......")
			select {
			case t := <-me.task:
				lg.Debugln(me.name, "Before worker exec, task len", len(me.task))
				me.exec(t)
				lg.Debugln(me.name, "In worker startWork routine exec done")
			case r := <-me.closeInstantly:
				lg.Debugln(me.name, "QUIT!!!!!!!!!!!!!!!", r)
				me.sweep(r)
				return
			}
			// select {
			// case r := <-me.closeInstantly:
			// 	lg.Debugln(me.name, "QUIT!!!!!!!!!!!!!!!", r)
			// 	me.sweep(r)
			// 	break
			// default:
			// 	select {
			// 	case t := <-me.task:
			// 		lg.Debugln(me.name, "Before worker exec, task len", len(me.task))
			// 		me.exec(t)
			// 		lg.Debugln(me.name, "In worker startWork routine exec done")
			// 		// case <-me.ticker.C:
			// 		// 	lg.Debugln("\t\t\t\t\t\t", me.name, "still alive")
			// 	}
			// }
		}
	}()
	return me
}

func (me *worker) stopWork(instantly bool) *worker {
	me.closeInstantly <- instantly
	return me
}

func (me *worker) assignTask(t Task) *worker {
	lg.Debugln(me.name, "Worker assign task ", len(me.task), t)
	me.task <- t
	lg.Debugln(me.name, "assign done", len(me.task), t)
	return me
}

func (me *worker) exec(t Task) *worker {
	me.mtxWorking.Lock()
	defer me.mtxWorking.Unlock()
	select {
	case <-me.taskDone:
		lg.Debugln(me.name, "Exec Select me.taskDone")
	default:
		lg.Debugln(me.name, "Exec Select default")
	}
	me.state = WORKER_BUSY
	if t.Init() {
		step := t.Step()
		for {
			if step != nil {
				step.PreDo()
				step.Do()
				step.PostDo()
				step.Done()
				if step.HasNext() {
					o := step.Next()
					if s, ok := o.(Step); ok {
						step = s
					} else {
						lg.Debugln(me.name, "Next step is not a step interface")
						break
					}
				} else {
					step = nil
					lg.Debugln(me.name, "There's no next step")
					break
				}
			}
		}
		go t.Done()
		lg.Debugln(me.name, "Exec t.Done end and wg is ", me.wg)
	}
	me.wg.Done()
	lg.Debugln(me.name, "Exec wg.Done end and wg is ", me.wg)
	me.state = WORKER_IDLE
	me.taskDone <- true
	lg.Debugln(me.name, "Exec end")
	return me
}

func (me *worker) sweep(instantly bool) *worker {
	if !instantly {
		<-me.taskDone
	}
	me.mtxWorking.Lock()
	me.state = WORKER_RETIRE
	close(me.task)
	me.task = nil
	close(me.closeInstantly)
	me.closeInstantly = nil
	close(me.taskDone)
	me.taskDone = nil
	me.mtxWorking.Unlock()
	return me
}

func (me *worker) stat() int {
	return me.state
}
