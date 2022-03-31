package local

import (
	"log"
	"sync"
	"testing"
)

//todo 同一个线程可以两次申请读锁
// 同一个线程获得写锁后，读锁一直等待。

type testS struct {
	mu *sync.RWMutex
}

func (t *testS)func1() {
	t.mu.Lock()
	defer t.mu.Unlock()
	log.Println("func2 start")
	t.func2()
	log.Println("func2 end")
}
func (t *testS)func2(){
	t.mu.RLock()
	defer t.mu.RUnlock()
log.Println("this is func2")
}
func TestNewResourceManager(t *testing.T) {
	tS := &testS{mu: &sync.RWMutex{}}
	tS.func1()

}
