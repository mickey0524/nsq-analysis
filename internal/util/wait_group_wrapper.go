package util

import (
	"sync"
)

// WaitGroupWrapper 包裹 sync.WaitGroup
type WaitGroupWrapper struct {
	sync.WaitGroup
}

// Wrap 包裹执行 cb，多用于 Exit 函数
func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
