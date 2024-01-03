package main

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/swiss"
)

type Segmenter func(k string, init int) (ret string, next int)

type SegValue[v comparable] struct {
	value v
	done  chan struct{}
}

type SegNode[v comparable] struct {
	mu       sync.RWMutex
	values   []*SegValue[v]
	Children *swiss.Map[string, *SegNode[v]]
}

type Segmap[v comparable] struct {
	Node      *SegNode[v]
	Segmenter Segmenter
}

type SegMapGetValue[v comparable] struct {
	Value v
	Index int
}

func keysegmentfunc(key string, init int) (ret string, next int) {
	if len(key) == 0 || init < 0 || init > len(key)-1 {
		return "", -1
	}

	end := strings.IndexRune(key[init+1:], ' ')
	if end == -1 {
		return key[init:], -1
	}
	return key[init : init+end+1], init + end + 2
}

func (parent *SegNode[v]) NewSegVal(key string, ttl time.Duration, value v, child *SegNode[v], length int) *SegValue[v] {
	seg_val := &SegValue[v]{
		value: value,
		done:  make(chan struct{}),
	}
	if ttl > 0 {
		go func() {
			select {
			case <-time.After(ttl):
				close(seg_val.done)
			case <-seg_val.done:
			}
			fmt.Println("Here")
			child.mu.Lock()
			fmt.Println(child.values, length, key, child.values[:length])
			child.values = append(child.values[:length], child.values[length+1:]...)
			if len(child.values) == 0 {
				parent.Children.Delete(key)
			}
			child.mu.Unlock()
		}()
	}

	return seg_val
}

func NewSegNode[v comparable]() *SegNode[v] {
	return &SegNode[v]{
		Children: swiss.NewMap[string, *SegNode[v]](42),
	}
}

func NewSegmap[v comparable](segmenter Segmenter) *Segmap[v] {
	s := Segmap[v]{}
	s.Node = NewSegNode[v]()
	if segmenter == nil {
		s.Segmenter = keysegmentfunc
	} else {
		s.Segmenter = segmenter
	}

	return &s
}

func (s *Segmap[v]) Put(key string, ttl time.Duration, value ...v) int {
	var parent *SegNode[v]
	segnode := s.Node
	last_key := ""

	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		parent = segnode
		last_key = seg
		segnode.mu.Lock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			child = NewSegNode[v]()
			segnode.Children.Put(seg, child)
		}
		segnode.mu.Unlock()
		segnode = child
	}

	for _, val := range value {
		segnode.values = append(segnode.values, parent.NewSegVal(last_key, ttl, val, segnode, len(segnode.values)))
	}
	return len(segnode.values)
}

func (s *Segmap[v]) Get(key string) []SegMapGetValue[v] {
	segnode := s.Node
	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segnode.mu.RLock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			segnode.mu.RUnlock()
			return nil
		}
		segnode.mu.RUnlock()
		segnode = child
	}
	segnode.mu.RLock()
	defer segnode.mu.RUnlock()
	segval_ret := []SegMapGetValue[v]{}
	for k, val := range segnode.values {
		segval_ret = append(segval_ret, SegMapGetValue[v]{
			Value: val.value,
			Index: k,
		})
	}
	return segval_ret
}

func (s *Segmap[v]) Delete(key string, value v, index int) bool {
	segnode := s.Node
	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segnode.mu.RLock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			segnode.mu.RUnlock()
			return false
		}
		segnode.mu.RUnlock()
		segnode = child
	}

	if len(segnode.values) >= index {
		segnode.mu.RLock()
		if segnode.values[index].value == value {
			close(segnode.values[index].done)
		}
		segnode.mu.RUnlock()
	}
	return false
}

func (s *SegNode[v]) walk() []v {
	if s != nil {
		var result []v
		s.mu.RLock()
		for _, val := range s.values {

			result = append(result, (*val).value)
		}
		s.mu.RUnlock()
		s.Children.Iter(func(k string, seg *SegNode[v]) (stop bool) {
			seg.mu.RLock()
			result = append(result, seg.walk()...)
			seg.mu.RUnlock()
			return false
		})

		return result
	}
	return nil
}

func (s *Segmap[v]) Transverse(key string) []v {
	segnode := s.Node
	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segnode.mu.RLock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			segnode.mu.RUnlock()
			return nil
		}
		segnode.mu.RUnlock()
		segnode = child
	}
	return segnode.walk()
}

func main() {
	segmap := NewSegmap[string](nil)
	segmap.Put("tuutf e f e", time.Duration(2)*time.Second, "first val")
	segmap.Put("tuutf e f e", time.Duration(2)*time.Second, "second val")
	segmap.Put("tuutf e f x", time.Duration(0), "first")
	segmap.Put("tuutf e f x", time.Duration(2)*time.Second, "second")
	segmap.Put("tuutf e f t", time.Duration(2)*time.Second, "third")
	fmt.Println(segmap.Get("tuutf e f e"))
	segmap.Delete("tuutf e f e", "first val", 0)
	time.Sleep(time.Duration(3) * time.Second)
	fmt.Println(segmap.Get("tuutf e f e"))
	fmt.Println(segmap.Transverse("tuutf e f"))
}
