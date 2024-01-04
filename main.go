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

type LastUpdatedMessage[v comparable] struct {
	Value SegData[v]
	Uuid  string
}

type LastUpdated[v comparable] struct {
	mu    sync.RWMutex
	Value map[string][]LastUpdatedMessage[v]
}

type SegData[v comparable] struct {
	Data v
	At   int64
}

type SegValue[v comparable] struct {
	value SegData[v]
	done  chan struct{}
}

type SegNode[v comparable] struct {
	mu       sync.RWMutex
	values   *swiss.Map[string, *SegValue[v]]
	Children *swiss.Map[string, *SegNode[v]]
}

type Segmap[v comparable] struct {
	Node         *SegNode[v]
	Segmenter    Segmenter
	last_updated LastUpdated[v]
}

type SegMapGetValue[v comparable] struct {
	Value SegData[v]
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

func (s *Segmap[v]) CallBack(cb func(map[string][]LastUpdatedMessage[v]), callback_on time.Duration) {
	ticker := time.NewTicker(callback_on)

	go func() {
		for {
			select {
			case <-ticker.C:
				s.last_updated.mu.Lock()
				cb(s.last_updated.Value)
				if len(s.last_updated.Value) > 0 {
					s.last_updated.Value = make(map[string][]LastUpdatedMessage[v])
				}
				s.last_updated.mu.Unlock()
			}
		}
	}()
}

func (s *Segmap[v]) delete(full_key, segmented_key string, ttl time.Duration, value v, parent, child *SegNode[v], uuid string) {
	child.mu.Lock()
	child.values.Delete(uuid)
	if child.values.Count() == 0 {
		parent.Children.Delete(segmented_key)
	}
	child.mu.Unlock()
	s.last_updated.mu.Lock()
	s.last_updated.Value[full_key] = append(s.last_updated.Value[full_key], LastUpdatedMessage[v]{
		Value: SegData[v]{
			Data: value,
			At:   time.Now().Unix(),
		},
		Uuid: uuid,
	})
	s.last_updated.mu.Unlock()
}

func (s *Segmap[v]) NewSegVal(full_key, segmented_key string, ttl time.Duration, value v, parent, child *SegNode[v], uuid string) *SegValue[v] {
	seg_val := &SegValue[v]{
		value: SegData[v]{
			Data: value,
			At:   time.Now().Unix(),
		},
		done: make(chan struct{}),
	}
	if ttl > 0 {
		go func() {
			select {
			case <-time.After(ttl):
				close(seg_val.done)
				s.delete(full_key, segmented_key, ttl, value, parent, child, uuid)
			case <-seg_val.done:
				s.delete(full_key, segmented_key, ttl, value, parent, child, uuid)
			}
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
	s := Segmap[v]{
		last_updated: LastUpdated[v]{
			Value: make(map[string][]LastUpdatedMessage[v]),
		},
	}
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
	segmented_key := ""

	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		parent = segnode
		segmented_key = seg
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
		segnode.values.Put(uuid, s.NewSegVal(key, segmented_key, ttl, val, parent, segnode, uuid))
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

func (s *Segmap[v]) Delete(key, uuid string, value v) bool {
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

	val, ok := segnode.values.Get(uuid)

	if ok {
		if val.value.Data == value {
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
			result = append(result, val.value.Data)
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
	segmap.CallBack(func(m map[string][]LastUpdatedMessage[string]) {
		fmt.Println("Last Deleted: ", m)
	}, time.Duration(2)*time.Second)

	segmap.Put("tuutf e f e", time.Duration(2)*time.Second, "1")
	segmap.Put("tuutf e f e", time.Duration(2)*time.Second, "2")
	segmap.Put("tuutf e f x", time.Duration(3)*time.Second, "3")
	segmap.Put("tuutf e f x", time.Duration(2)*time.Second, "4")
	segmap.Put("tuutf e f t", time.Duration(0)*time.Second, "5")

	val := segmap.Get("tuutf e f x")
	fmt.Println(val[0])
	fmt.Println(segmap.Delete("tuutf e f x", val[0].Uuid, val[0].Value.Data))

	fmt.Println(val)
	time.Sleep(time.Duration(5) * time.Second)
	segmap.Put("tuutf e f t", time.Duration(2)*time.Second, "6")
	fmt.Println(segmap.Get("tuutf e f e"))
	fmt.Println(segmap.Transverse("tuutf e f"))
}
