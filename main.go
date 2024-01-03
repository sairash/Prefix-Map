package main

import (
	"fmt"
	"strings"
	"sync"

	"github.com/dolthub/swiss"
)

type Segmenter func(k string, init int) (ret string, next int)

type Segnode[v any] struct {
	Mu       sync.RWMutex
	Value    []v
	Children *swiss.Map[string, *Segnode[v]]
}

type Segmap[v any] struct {
	Node      *Segnode[v]
	Segmenter Segmenter
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

func NewSegNode[v any]() *Segnode[v] {
	return &Segnode[v]{
		Value:    []v{},
		Children: swiss.NewMap[string, *Segnode[v]](42),
	}
}

func NewSegmap[v any](segmenter Segmenter) *Segmap[v] {
	s := Segmap[v]{}
	s.Node = NewSegNode[v]()
	if segmenter == nil {
		s.Segmenter = keysegmentfunc
	} else {
		s.Segmenter = segmenter
	}

	return &s
}

func (s *Segmap[v]) Put(key string, value ...v) int {
	segnode := s.Node
	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segnode.Mu.Lock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			child = NewSegNode[v]()
			segnode.Children.Put(seg, child)
		}
		segnode.Mu.Unlock()
		segnode = child
	}
	segnode.Value = append(segnode.Value, value...)
	return len(segnode.Value)
}

func (s *Segmap[v]) Get(key string) []v {
	segnode := s.Node
	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segnode.Mu.RLock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			segnode.Mu.RUnlock()
			return nil
		}
		segnode.Mu.RUnlock()
		segnode = child
	}
	return segnode.Value
}

func (s *Segnode[v]) walk() []v {
	if s != nil {
		var result []v
		s.Mu.RLock()
		result = append(result, s.Value...)
		s.Mu.RUnlock()
		s.Children.Iter(func(k string, seg *Segnode[v]) (stop bool) {
			seg.Mu.RLock()
			result = append(result, seg.walk()...)
			seg.Mu.RUnlock()
			return false
		})

		return result
	}
	return nil
}

func (s *Segmap[v]) Transverse(key string) []v {

	segnode := s.Node
	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segnode.Mu.RLock()
		child, ok := segnode.Children.Get(seg)
		if !ok {
			segnode.Mu.RUnlock()
			return nil
		}
		segnode.Mu.RUnlock()
		segnode = child
	}
	return segnode.walk()

}

func main() {
	segmap := NewSegmap[string](nil)
	segmap.Put("tuutf e f e", "Hello1")
	segmap.Put("tuutf e f x", "Hello2")
	segmap.Put("tuutf e f t", "Hello3")
	fmt.Println(segmap.Get("tuutf e f e"))
	fmt.Println(segmap.Get("tuutf e f"))
	fmt.Println(segmap.Transverse("tuutf e f"))
}
