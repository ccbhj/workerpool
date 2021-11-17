package xworkers

import (
	"context"
	"sync"
)

type Job func()
type jobChan chan Job

type worker struct {
	workerPool chan jobChan
	quit       chan struct{}
	jobChan    jobChan
	wg         *sync.WaitGroup
}

func newWorker(workerPool chan jobChan, wg *sync.WaitGroup) *worker {
	return &worker{
		workerPool: workerPool,
		jobChan:    make(jobChan),
		quit:       make(chan struct{}, 1),
		wg:         wg,
	}
}

func (w *worker) start() {
	go func() {
		defer func() {
			w.wg.Done()
		}()
		for {
			// register the worker to pool
			select {
			case w.workerPool <- w.jobChan:
				// wait for a coming job
				select {
				case job := <-w.jobChan:
					job()
				case <-w.quit:
					return
				}
			case <-w.quit:
				return
			}

		}
	}()
}

func (w worker) stop() {
	w.quit <- struct{}{}
}

type WorkerPool struct {
	pool     chan jobChan
	jobQueue chan Job
	workers  []*worker
	closeCh  chan struct{}
	wg       *sync.WaitGroup
	runOnce  *sync.Once
}

func NewWorker(queueSize int, maxWorker int) *WorkerPool {
	pool := make(chan jobChan, maxWorker)
	jobQueue := make(chan Job, queueSize)
	closeCh := make(chan struct{})
	workers := make([]*worker, maxWorker)
	wg := &sync.WaitGroup{}
	wg.Add(maxWorker)
	for i := 0; i < maxWorker; i++ {
		workers[i] = newWorker(pool, wg)
	}
	return &WorkerPool{
		pool:     pool,
		jobQueue: jobQueue,
		closeCh:  closeCh,
		workers:  workers,
		wg:       wg,
		runOnce:  &sync.Once{},
	}
}

func (p *WorkerPool) dispatch() {
	for {
		select {
		case job := <-p.jobQueue:
			// if this snippet is wrapped in infinite job queue
			// the jobQueue will be infinite
			go func(j Job) {
				// try to get a worker
				jobChan := <-p.pool
				// send a job to the worker
				jobChan <- job
			}(job)
		case <-p.closeCh:
			return
		}
	}
}

func (p *WorkerPool) Run() {
	p.runOnce.Do(func() {
		for _, wk := range p.workers {
			wk.start()
		}
		go p.dispatch()
	})
}

func (p *WorkerPool) CloseAndWait(ctx context.Context) error {
	doneCh := make(chan struct{})
	go func() {
		// p.closeCh <- struct{}{}
		for _, wk := range p.workers {
			wk.stop()
		}
		p.wg.Wait()
		doneCh <- struct{}{}
	}()

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *WorkerPool) AddJob(job Job, onPanic func(interface{})) {
	p.jobQueue <- wrapSafeJob(job, onPanic)

	// jobChan := <-p.pool
	// jobChan <- wrapSafeJob(job, onPanic)
}

func wrapSafeJob(job Job, onPanic func(interface{})) Job {
	return func() {
		defer func() {
			if r := recover(); r != nil {
				onPanic(r)
			}
		}()
		job()
	}
}
