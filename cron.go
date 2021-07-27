package cron

import (
	"context"
	"sort"
	"sync"
	"time"

  "github.com/google/uuid"
)

type Cron struct {
	entries []*Entry

  stop      chan struct{}
	add     chan *Entry
  remove chan EntryID
  stopJobID chan RunningJobID
  snapshot chan chan []Entry

	running bool
	// logger Logger
  parser    ScheduleParser
	runningMu   sync.Mutex
	location    *time.Location
	jobWaiter   sync.WaitGroup
	runningJobs []*runningJob
  context context.Context
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// Job is an interface for submitterd cron jobs.
type Job interface {
	RunAsync(context.Context)
}

// Schedule descrbes a jobs duty cycle.
type Schedule interface {
	// Next returns the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// EntryID identifies an entry within a Cron instance
type EntryID string

// RunningJobID identifies a running job within a Cron instance
type RunningJobID string

type Entry struct {
	ID EntryID

	Schedule Schedule

	Next time.Time

	Prev time.Time

	WrapperdJob Job

	Job Job
}

func (e Entry) Valid() bool { return e.ID != "" }

type runningJob struct {
  ID RunningJobID
  EntryID EntryID
  Cancel func()
}

// byTime is a wrapper for sorting the entry array by time
// (with zero time at the end).
type byTime []*Entry

func (s byTime) Len() int      { return len(s) }
func (s byTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s byTime) Less(i, j int) bool {
	// Two zero times should return false.
	// Otherwise, zero is "greater" than any other time.
	// (To sort it at the end of the list.)
	if s[i].Next.IsZero() {
		return false
	}
	if s[j].Next.IsZero() {
		return true
	}
	return s[i].Next.Before(s[j].Next)
}

func New() *Cron {
	c := &Cron{
		entries: nil,
		add:     make(chan *Entry),
		running: false,
		// logger: nil,
		runningMu:   sync.Mutex{},
		location:    time.Local,
		runningJobs: nil,
    context: context.Background(),
    parser:    standardParser,
	}
	return c
}

type FuncJob func()

func (f FuncJob) RunAsync(ctx context.Context) { f() }

type FuncJobAsync func(context.Context)

func (f FuncJobAsync) RunAsync(ctx context.Context) { f(ctx) }

func (c *Cron) AddFunc(spec string, cmd func()) (EntryID, error) {
	return c.AddJob(spec, FuncJob(cmd))
}

func (c *Cron) AddFuncAsync(spec string, cmd func(context.Context)) (EntryID, error) {
	return c.AddJob(spec, FuncJobAsync(cmd))
}

func (c *Cron) AddJob(spec string, cmd Job) (EntryID, error) {
  // TODO 
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return "", err
	}
  return  c.Schedule(schedule,cmd), nil
}

// Schedule adds a Job to the Cron to be run on the given schedule.
// The job is wrapped with the configured Chain.
func (c *Cron) Schedule(schedule Schedule, cmd Job) EntryID {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	entry := &Entry{
		ID:         EntryID(uuid.New().String()),
		Schedule:   schedule,
		// WrappedJob: c.chain.Then(cmd),
		Job:        cmd,
	}
	if c.running {
		c.add <- entry
	} else {
		c.entries = append(c.entries, entry)
	}
	return entry.ID
}

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
    replyChan := make(chan []Entry, 1)
    c.snapshot <- replyChan
    return <-replyChan
	}
  return c.entrySnapshot()
}

// Location gets the time zone location
func (c *Cron) Location() *time.Location {
	return c.location
}

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) Entry {
	for _, entry := range c.Entries() {
		if id == entry.ID {
			return entry
		}
	}
	return Entry{}
}

// Remove an entry from being run in the future.
func (c *Cron) Remove(id EntryID) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		c.remove <- id
	} else {
		c.removeEntry(id)
	}
}

// Start the cron scheduler in its own goroutine, or no-op if already started.
func (c *Cron) Start() {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()
	if c.running {
		return
	}
	c.running = true
	go c.run()
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() {
	c.runningMu.Lock()
	if c.running {
		c.runningMu.Unlock()
		return
	}
	c.running = true
	c.runningMu.Unlock()
	c.run()
}

// run the scheduler.. this is private just due to the need to synchronize
// access to the 'running' state variable.
func (c *Cron) run() {
  // TODO Logger
	// c.logger.Info("start")

	// Figure out the next activation times for each entry.
	now := c.now()
	for _, entry := range c.entries {
		entry.Next = entry.Schedule.Next(now)
    // TODO Logger
		// c.logger.Info("schedule", "now", now, "entry", entry.ID, "next", entry.Next)
	}

	for {
		// Determine the next entry to run.
		sort.Sort(byTime(c.entries))

		var timer *time.Timer
		if len(c.entries) == 0 || c.entries[0].Next.IsZero() {
			// If there are no entries yet, just sleep - it still handles new entries
			// and stop requests.
			timer = time.NewTimer(100000 * time.Hour)
		} else {
			timer = time.NewTimer(c.entries[0].Next.Sub(now))
		}

		for {
			select {
			case now = <-timer.C:
				now = now.In(c.location)
        // TODO Logger
				// c.logger.Info("wake", "now", now)

				// Run every entry whose next time was less than now
				for _, e := range c.entries {
					if e.Next.After(now) || e.Next.IsZero() {
						break
					}
					c.startJob(e.ID, e.Job)
					e.Prev = e.Next
					e.Next = e.Schedule.Next(now)
          // TODO Logger
					// c.logger.Info("run", "now", now, "entry", e.ID, "next", e.Next)
				}

			case newEntry := <-c.add:
				timer.Stop()
				now = c.now()
				newEntry.Next = newEntry.Schedule.Next(now)
				c.entries = append(c.entries, newEntry)
        // TODO Logger
				// c.logger.Info("added", "now", now, "entry", newEntry.ID, "next", newEntry.Next)

			case replyChan := <-c.snapshot:
				replyChan <- c.entrySnapshot()
				continue

      case id := <-c.stopJobID:
				timer.Stop()
				now = c.now()
        c.stopJob(id)
        // TODO Logger
				// c.logger.Info("stopJob", id)
				return

			case <-c.stop:
				timer.Stop()
        // TODO Logger
				// c.logger.Info("stop")
				return

			case id := <-c.remove:
				timer.Stop()
				now = c.now()
				c.removeEntry(id)
        // TODO Logger
				// c.logger.Info("removed", "entry", id)
			}

			break
		}
	}
}

// startJob runs the given job in a new goroutine.
func (c *Cron) startJob(id EntryID, j Job) {
	c.runningMu.Lock()
	defer c.runningMu.Unlock()

	c.jobWaiter.Add(1)
  ctx, cancel := context.WithCancel(c.context)
  r := runningJob{
    ID: RunningJobID(uuid.New().String()),
    EntryID: id,
    Cancel: cancel,
  }
  c.runningJobs = append(c.runningJobs, &r)
	go func(ctx context.Context) {
		defer c.jobWaiter.Done()
    defer c.removeRunningJob(r.ID)
		j.RunAsync(ctx)
	}(ctx)
}

func (c *Cron) stopJob(id RunningJobID) {
	for _, r := range c.runningJobs {
		if r.ID == id {
      r.Cancel()
      return
		}
	}

}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return time.Now().In(c.location)
}

// entrySnapshot returns a copy of the current cron entry list.
func (c *Cron) entrySnapshot() []Entry {
	var entries = make([]Entry, len(c.entries))
	for i, e := range c.entries {
		entries[i] = *e
	}
	return entries
}

// runningJobSnapshot returns a copy of the current cron entry list.
func (c *Cron) runningJobSnapshot() []runningJob {
	var runningJobs = make([]runningJob, len(c.runningJobs))
	for i, r := range c.runningJobs {
		runningJobs[i] = *r
	}
	return runningJobs
}

func (c *Cron) removeEntry(id EntryID) {
	var entries []*Entry
	for _, e := range c.entries {
		if e.ID != id {
			entries = append(entries, e)
		}
	}
	c.entries = entries
}

func (c *Cron) removeRunningJob(id RunningJobID){
	var runningJobs []*runningJob
  for _, r := range c.runningJobs {
		if r.ID != id {
			runningJobs = append(runningJobs, r)
		}
  }
  c.runningJobs = runningJobs
}


