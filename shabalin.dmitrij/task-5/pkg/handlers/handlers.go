package handlers

import (
	"context"
	"errors"
	"strings"
	"sync"
)

const (
	prefixToAdd   = "decorated: "
	triggerNoDeco = "no decorator"
	triggerNoMult = "no multiplexer"
)

var (
	ErrCantDecorate = errors.New("can't be decorated")
	ErrEmptyInputs  = errors.New("empty inputs")
)

func PrefixDecorator(ctx context.Context, input <-chan string, output chan<- string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}

			if strings.Contains(data, triggerNoDeco) {
				return ErrCantDecorate
			}

			if !strings.HasPrefix(data, prefixToAdd) {
				data = prefixToAdd + data
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- data:
			}
		}
	}
}

func Multiplexer(ctx context.Context, inputs []<-chan string, output chan<- string) error {
	if len(inputs) == 0 {
		return ErrEmptyInputs
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 1)

	processInput := func(input <-chan string) {
		defer wg.Done()
		
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}

				if strings.Contains(data, triggerNoMult) {
					continue
				}

				select {
				case <-ctx.Done():
					return
				case output <- data:
				}
			}
		}
	}

	wg.Add(len(inputs))
	for _, input := range inputs {
		go processInput(input)
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	}
}

func Separator(ctx context.Context, input <-chan string, outputs []chan<- string) error {
	if len(outputs) == 0 {
		return errors.New("no output channels")
	}

	counter := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case data, ok := <-input:
			if !ok {
				return nil
			}

			index := counter % len(outputs)
			counter++

			select {
			case <-ctx.Done():
				return ctx.Err()
			case outputs[index] <- data:
			}
		}
	}
}

func AdvancedSeparator(routingFunc func(string) int) func(context.Context, <-chan string, []chan<- string) error {
	return func(ctx context.Context, input <-chan string, outputs []chan<- string) error {
		if len(outputs) == 0 {
			return errors.New("no output channels")
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case data, ok := <-input:
				if !ok {
					return nil
				}

				index := routingFunc(data)
				if index < 0 || index >= len(outputs) {
					index = 0
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case outputs[index] <- data:
				}
			}
		}
	}
}
