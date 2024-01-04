package main

import (
	"crypto/rand"
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
	values   *swiss.Map[string, *SegValue[v]]
	Children *swiss.Map[string, *SegNode[v]]
}

type Segmap[v comparable] struct {
	Node      *SegNode[v]
	Segmenter Segmenter
}

type SegMapGetValue[v comparable] struct {
	Value v
	Uuid  string
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GenerateRandomString(length int) string {

	randomString := make([]byte, length)
	randomBytes := make([]byte, length+(length/4))

	for i, _ := range randomString {
		if _, err := rand.Read(randomBytes); err != nil {
			panic(err)
		}
		randomString[i] = charset[int(randomBytes[i])%len(charset)]
	}

	return string(randomString)
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

func (s *SegNode[v]) GenerateNotUsedUUid(length int) string {
	uuid := GenerateRandomString(length)
	if _, ok := s.values.Get(uuid); !ok {
		return uuid
	} else {
		return s.GenerateNotUsedUUid(length)
	}
}

func (parent *SegNode[v]) NewSegVal(key string, ttl time.Duration, value v, child *SegNode[v], uuid string) *SegValue[v] {
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
			child.mu.Lock()
			child.values.Delete(uuid)
			if child.values.Count() == 0 {
				parent.Children.Delete(key)
			}
			child.mu.Unlock()
		}()
	}

	return seg_val
}

func NewSegNode[v comparable]() *SegNode[v] {
	return &SegNode[v]{
		values:   swiss.NewMap[string, *SegValue[v]](42),
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
		uuid := segnode.GenerateNotUsedUUid(5)
		segnode.values.Put(uuid, parent.NewSegVal(last_key, ttl, val, segnode, uuid))
	}
	return segnode.values.Count()
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
	segnode.values.Iter(func(k string, val *SegValue[v]) (stop bool) {
		segval_ret = append(segval_ret, SegMapGetValue[v]{
			Value: val.value,
			Uuid:  k,
		})
		return false
	})
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

	val, ok := segnode.values.Get(key)

	if ok {
		if val.value == value {
			close(val.done)
		}
	}

	return ok
}

func (s *SegNode[v]) walk() []v {
	if s != nil {
		var result []v
		s.mu.RLock()
		s.values.Iter(func(k string, val *SegValue[v]) (stop bool) {
			result = append(result, val.value)
			return false
		})
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
	segmap.Put("tuutf e f t", time.Duration(2)*time.Second, "last val")
	fmt.Println(segmap.Get("tuutf e f e"))
	fmt.Println(segmap.Transverse("tuutf e f"))
}
