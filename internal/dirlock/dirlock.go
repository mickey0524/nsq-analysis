// +build !windows

package dirlock

import (
	"fmt"
	"os"
	"syscall"
)

// DirLock 代表文件锁的数据结构
type DirLock struct {
	dir string
	f   *os.File
}

// New 新建一个文件锁
func New(dir string) *DirLock {
	return &DirLock{
		dir: dir,
	}
}

// Lock 上锁
func (l *DirLock) Lock() error {
	f, err := os.Open(l.dir)
	if err != nil {
		return err
	}
	l.f = f
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return fmt.Errorf("cannot flock directory %s - %s", l.dir, err)
	}
	return nil
}

// Unlock 释放锁
func (l *DirLock) Unlock() error {
	defer l.f.Close()
	return syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
}
