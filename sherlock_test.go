package sherlock

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestEtcdLock(t *testing.T) {
	l := NewEtcdLock("TestEtcdLock", nil)
	l.SetNamespace("lock")

	// may return error, but we ignore it
	l.Release()
	if l.Locked() != false {
		t.Errorf("locked should return false")
	}

	if validity, err := l.Acquire(false); err != nil {
		t.Errorf("failed to acquire lock error(%v)", err)
	} else if validity <= 0 {
		t.Errorf("failed to acquire lock")
	}
	if l.Locked() != true {
		t.Errorf("locked should return true")
	}
	if err := l.Release(); err != nil {
		t.Errorf("release lock error(%v)", err)
	}
}

const (
	fpath = "./counter.log"
)

func writer(count int, back chan int) {
	l := NewEtcdLock("TestEtcdLock", nil)

	incr := 0
	for i := 0; i < count; i++ {
		if _, err := l.Acquire(true); err != nil {
			fmt.Println(err)
		} else {
			f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, os.ModePerm)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			buf := make([]byte, 1024)
			n, _ := f.Read(buf)
			num, _ := strconv.ParseInt(strings.TrimRight(string(buf[:n]), "\n"), 10, 64)
			f.WriteAt([]byte(strconv.Itoa(int(num+1))), 0)
			incr += 1
			l.Release()
		}
	}
	back <- incr
}

func init() {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		panic(err)
	}
	f.WriteString("0")
	defer f.Close()
}

func TestSimpleCounter(t *testing.T) {
	routines := 5
	inc := 100
	total := 0
	done := make(chan int, routines)
	for i := 0; i < routines; i++ {
		go writer(inc, done)
	}
	for i := 0; i < routines; i++ {
		singleIncr := <-done
		total += singleIncr
	}

	f, err := os.OpenFile(fpath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := make([]byte, 1024)
	n, _ := f.Read(buf)
	fmt.Printf("Counter value is %s\n", buf[:n])
	counterInFile, _ := strconv.Atoi(string(buf[:n]))
	if total != counterInFile {
		t.Errorf("counter error: increase %d times, but counterInFile is %d", total, counterInFile)
	}
}
