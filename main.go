package main

import (
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/swiss"
)

type Segmenter func(k string, init int) (ret string, next int)

type LastUpdatedMessage[v comparable] struct {
	Value v
	Uuid  string
}

type LastUpdated[v comparable] struct {
	mu    sync.RWMutex
	Value map[string][]LastUpdatedMessage[v]
}

type SegNode[v comparable] struct {
	mu       sync.RWMutex
	values   *swiss.Map[string, *SegValueData[v]]
	Children *swiss.Map[string, *SegNode[v]]
}

type SegValueData[v comparable] struct {
	till         time.Time
	value        v
	segmentedKey string
	key          string
	uuid         string
}
type segTtl[v comparable] struct {
	values       []*SegValueData[v]
	stop         chan bool
	mu           sync.RWMutex
	last_updated LastUpdated[v]
}

type Segmap[v comparable] struct {
	Node          *SegNode[v]
	Segmenter     Segmenter
	ttlController segTtl[v]
}

type SegMapGetValue[v comparable] struct {
	Value v
	Uuid  string
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func (s *SegNode[v]) GenerateRandomString(length int) string {

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

func spacekeysegmentfunc(key string, init int) (ret string, next int) {
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
	uuid := s.GenerateRandomString(length)
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
				s.ttlController.last_updated.mu.Lock()
				cb(s.ttlController.last_updated.Value)
				if len(s.ttlController.last_updated.Value) > 0 {
					s.ttlController.last_updated.Value = make(map[string][]LastUpdatedMessage[v])
				}
				s.ttlController.last_updated.mu.Unlock()
			}
		}
	}()
}

func (s *Segmap[v]) delete(full_key, segmentedKey string, value v, parent, child *SegNode[v], uuid string) {
	child.mu.Lock()
	child.values.Delete(uuid)
	child.mu.Unlock()
	if child.values.Count() == 0 {
		parent.mu.Lock()
		parent.Children.Delete(segmentedKey)
		parent.mu.Unlock()
	}
	s.ttlController.last_updated.mu.Lock()
	s.ttlController.last_updated.Value[full_key] = append(s.ttlController.last_updated.Value[full_key], LastUpdatedMessage[v]{
		Value: value,
		Uuid:  uuid,
	})
	s.ttlController.last_updated.mu.Unlock()
}

func (s *Segmap[v]) startTTLTimer() {
	for {
		if len(s.ttlController.values) > 0 {
			s.ttlController.stop = make(chan bool)
			fmt.Println("")
			ttl := s.ttlController.values[0]
			select {
			case <-time.After(time.Until(ttl.till)):
				s.ttlController.last_updated.mu.Lock()
				s.ttlController.last_updated.Value[ttl.key] = append(s.ttlController.last_updated.Value[ttl.key], LastUpdatedMessage[v]{
					Value: ttl.value,
					Uuid:  ttl.uuid,
				})
				s.ttlController.last_updated.mu.Unlock()
				s.ttlController.mu.Lock()
				s.ttlController.values = s.ttlController.values[1:]
				s.ttlController.mu.Unlock()
			case <-s.ttlController.stop:
				return
			}
		}
	}
}

func (s *Segmap[v]) addToTTL(ttl *SegValueData[v]) int {
	s.ttlController.mu.RLock()
	defer s.ttlController.mu.RUnlock()

	index := sort.Search(len(s.ttlController.values), func(i int) bool {
		return s.ttlController.values[i].till.After(ttl.till)
	})

	return index
}

func (s *Segmap[v]) NewSegVal(fullKey, segmentedKey, uuid string, ttl time.Duration, value v) *SegValueData[v] {
	segValue := &SegValueData[v]{
		key:          fullKey,
		value:        value,
		segmentedKey: segmentedKey,
		uuid:         uuid,
		till:         time.Now().Add(ttl),
	}

	index := s.addToTTL(segValue)
	s.ttlController.mu.Lock()
	s.ttlController.values = append(s.ttlController.values, nil)
	copy(s.ttlController.values[index+1:], s.ttlController.values[index:])
	s.ttlController.values[index] = segValue

	if index == 0 {
		if s.ttlController.stop != nil {
			close(s.ttlController.stop)
		}
	}

	s.ttlController.mu.Unlock()

	return segValue
}

func NewSegNode[v comparable]() *SegNode[v] {
	return &SegNode[v]{
		values:   swiss.NewMap[string, *SegValueData[v]](42),
		Children: swiss.NewMap[string, *SegNode[v]](42),
	}
}

func NewSegmap[v comparable](segmenter Segmenter) *Segmap[v] {
	s := Segmap[v]{
		ttlController: segTtl[v]{
			values: []*SegValueData[v]{},
			last_updated: LastUpdated[v]{
				Value: make(map[string][]LastUpdatedMessage[v]),
			},
		},
	}

	s.Node = NewSegNode[v]()
	if segmenter == nil {
		s.Segmenter = spacekeysegmentfunc
	} else {
		s.Segmenter = segmenter
	}

	go s.startTTLTimer()

	return &s
}

func (s *Segmap[v]) Put(key string, ttl time.Duration, value ...v) int {
	segnode := s.Node
	segmentedKey := ""

	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		segmentedKey = seg
		child, ok := segnode.Children.Get(seg)
		if !ok {
			segnode.mu.Lock()
			child = NewSegNode[v]()
			segnode.Children.Put(seg, child)
			segnode.mu.Unlock()
		}
		segnode = child
	}

	for _, val := range value {
		uuid := segnode.GenerateNotUsedUUid(5)
		segnode.values.Put(uuid, s.NewSegVal(key, segmentedKey, uuid, ttl, val))
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
	segnode.values.Iter(func(k string, val *SegValueData[v]) (stop bool) {
		segval_ret = append(segval_ret, SegMapGetValue[v]{
			Value: val.value,
			Uuid:  k,
		})
		return false
	})
	return segval_ret
}

// func (s *)

func (s *Segmap[v]) Delete(key, uuid string) bool {
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

	// val, ok := segnode.values.Get(uuid)
	// if ok {

	// 	close(val.done)
	// }

	return false
}

func (s *SegNode[v]) walk() []v {
	if s != nil {
		var result []v
		s.mu.RLock()
		s.values.Iter(func(k string, val *SegValueData[v]) (stop bool) {
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
		segnode.mu.RUnlock()
		if !ok {
			return nil
		}
		segnode = child
		fmt.Println(segnode)
	}
	return segnode.walk()
}

func main() {
	segmap := NewSegmap[string](nil)
	segmap.CallBack(func(m map[string][]LastUpdatedMessage[string]) {
		fmt.Println("Last Deleted: ", m)
	}, time.Duration(2)*time.Second)

	segmap.Put("tuutf e f e ", time.Duration(2)*time.Second, "1")
	segmap.Put("tuutf e f e", time.Duration(2)*time.Second, "2")
	segmap.Put("tuutf e f x", time.Duration(3)*time.Second, "3")
	segmap.Put("tuutf e f x", time.Duration(2)*time.Second, "4")
	segmap.Put("tuutf e f t", time.Duration(0)*time.Second, "5")

	val := segmap.Get("tuutf e f e")
	fmt.Println(val)

	// Delete is not working needs fix!
	// fmt.Println(segmap.Delete("tuutf e f x", val[0].Uuid))

	time.Sleep(time.Duration(5) * time.Second)
	segmap.Put("tuutf e f t", time.Duration(2)*time.Second, "6")
	fmt.Println(segmap.Get("tuutf e f e"))

	// Transverse is not working needs fix!
	fmt.Println(segmap.Transverse("tuutf e f"))
}
