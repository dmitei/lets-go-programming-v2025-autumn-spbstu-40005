package convcover

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrChanNotFound = errors.New("chan not found")
	ErrPipelineStopped = errors.New("pipeline stopped")
)

const undefinedValue = "undefined"

type HandlerFunc func(ctx context.Context, input <-chan string, output chan<- string) error

type MultiplexerFunc func(ctx context.Context, inputs []<-chan string, output chan<- string) error

type SeparatorFunc func(ctx context.Context, input <-chan string, outputs []chan<- string) error

type Conveyer interface {
	RegisterDecorator(handler HandlerFunc, inputChan, outputChan string)
	RegisterMultiplexer(handler MultiplexerFunc, inputChans []string, outputChan string)
  RegisterSeparator(handler SeparatorFunc, inputChan string, outputChans []string)
	
	Run(ctx context.Context) error
	Send(chanID string, data string) error
	Recv(chanID string) (string, error)
	Close()
}

type pipeline struct {
	mu        sync.RWMutex
	channels  map[string]chan string
	workers   []workerFunc
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	bufferSize int
}

type workerFunc func(ctx context.Context) error

func New(bufferSize int) Conveyer {
	return &pipeline{
		channels:  make(map[string]chan string),
		workers:   make([]workerFunc, 0),
		bufferSize: bufferSize,
	}
}

func (p *pipeline) getOrCreateChannel(name string) chan string {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if ch, exists := p.channels[name]; exists {
		return ch
	}
	
	ch := make(chan string, p.bufferSize)
	p.channels[name] = ch
	return ch
}

func (p *pipeline) getChannel(name string) (chan string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	if ch, exists := p.channels[name]; exists {
		return ch, nil
	}
	
	return nil, ErrChanNotFound
}

func (p *pipeline) RegisterDecorator(handler HandlerFunc, inputChan, outputChan string) {
	inChan := p.getOrCreateChannel(inputChan)
	outChan := p.getOrCreateChannel(outputChan)
	
	worker := func(ctx context.Context) error {
		return handler(ctx, inChan, outChan)
	}
	
	p.addWorker(worker)
}

func (p *pipeline) RegisterMultiplexer(handler MultiplexerFunc, inputChans []string, outputChan string) {
	inChans := make([]<-chan string, len(inputChans))
	for i, name := range inputChans {
		ch := p.getOrCreateChannel(name)
		inChans[i] = ch
	}
	
	outChan := p.getOrCreateChannel(outputChan)
	
	worker := func(ctx context.Context) error {
		return handler(ctx, inChans, outChan)
	}
	
	p.addWorker(worker)
}

func (p *pipeline) RegisterSeparator(handler SeparatorFunc, inputChan string, outputChans []string) {
	inChan := p.getOrCreateChannel(inputChan)
	
	outChans := make([]chan<- string, len(outputChans))
	for i, name := range outputChans {
		ch := p.getOrCreateChannel(name)
		outChans[i] = ch
	}
	
	worker := func(ctx context.Context) error {
		return handler(ctx, inChan, outChans)
	}
	
	p.addWorker(worker)
}

func (p *pipeline) addWorker(worker workerFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers = append(p.workers, worker)
}

func (p *pipeline) Run(ctx context.Context) error {
	p.mu.Lock()
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.mu.Unlock()
	
	errChan := make(chan error, len(p.workers))
	
	for _, worker := range p.workers {
		p.wg.Add(1)
		go func(w workerFunc) {
			defer p.wg.Done()
			if err := w(p.ctx); err != nil {
				select {
				case errChan <- err:
				default:
				}
			}
		}(worker)
	}
	
	select {
	case err := <-errChan:
		p.Close()
		return fmt.Errorf("pipeline error: %w", err)
	case <-p.ctx.Done():
		p.Close()
		return p.ctx.Err()
	case <-func() chan struct{} {
		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()
		return done
	}():
		p.Close()
		return nil
	}
}

func (p *pipeline) Send(chanID string, data string) error {
	if p.ctx == nil || p.ctx.Err() != nil {
		return ErrPipelineStopped
	}
	
	ch, err := p.getChannel(chanID)
	if err != nil {
		return err
	}
	
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case ch <- data:
		return nil
	default:
		return errors.New("channel buffer full")
	}
}

func (p *pipeline) Recv(chanID string) (string, error) {
	if p.ctx == nil || p.ctx.Err() != nil {
		return "", ErrPipelineStopped
	}
	
	ch, err := p.getChannel(chanID)
	if err != nil {
		return "", err
	}
	
	select {
	case <-p.ctx.Done():
		return "", p.ctx.Err()
	case data, ok := <-ch:
		if !ok {
			return undefinedValue, nil
		}
		return data, nil
	}
}

func (p *pipeline) Close() {
	if p.cancel != nil {
		p.cancel()
	}
	
	p.wg.Wait()
	
	p.mu.Lock()
	defer p.mu.Unlock()
	
	for name, ch := range p.channels {
		select {
		case <-ch:
		default:
		}
		close(ch)
		delete(p.channels, name)
	}
	
	p.workers = nil
}
