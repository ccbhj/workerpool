package xworkers

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"
)

func TestOneQueue(t *testing.T) {
	njob := 10
	pool := NewWorker(1, njob)
	pool.Run()
	wg := &sync.WaitGroup{}
	wg.Add(njob)

	sleepInterval := 1
	startTime := time.Now()

	for i := 0; i < njob; i++ {
		pool.AddJob(func() {
			time.Sleep(time.Duration(sleepInterval) * time.Second)
			wg.Done()
		}, func(interface{}) {})
	}
	// should be done in sleepInterval
	wg.Wait()
	dur := time.Now().Sub(startTime)
	log.Printf("spent: %f", dur.Seconds())
	if int(dur.Seconds()) != int(sleepInterval) {
		t.FailNow()
	}

	// add a extra job
	njob++
	wg.Add(njob)
	startTime = time.Now()
	for i := 0; i < njob; i++ {
		pool.AddJob(func() {
			time.Sleep(time.Duration(sleepInterval) * time.Second)
			wg.Done()
		}, func(interface{}) {})
	}
	// should be done in sleepInterval * 2
	// because the extra one job should wait for a free worker
	wg.Wait()
	dur = time.Now().Sub(startTime)
	log.Printf("spent: %f", dur.Seconds())
	if int(dur.Seconds()) != int(sleepInterval*2) {
		t.FailNow()
	}
	err := pool.CloseAndWait(context.Background())
	if err != nil {
		t.Fatal("fail to close WorkerPool")
	}
}
