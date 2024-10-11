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
	Value v
	Uuid  string
}

type LastUpdated[v comparable] struct {
	mu    sync.RWMutex
	Value map[string][]LastUpdatedMessage[v]
}

type SegValue[v comparable] struct {
	ttl  *segTtlValue[v]
	done chan struct{}
}

type SegNode[v comparable] struct {
	mu       sync.RWMutex
	values   *swiss.Map[string, *SegValue[v]]
	Children *swiss.Map[string, *SegNode[v]]
}

type segTtlValue[v comparable] struct {
	till          time.Time
	value         v
	segmented_key string
	key           string
	uuid          string
}
type segTtl[v comparable] struct {
	values       []*segTtlValue[v]
	timer        chan bool
	mu           sync.RWMutex
	last_updated LastUpdated[v]
}

type Segmap[v comparable] struct {
	Node           *SegNode[v]
	Segmenter      Segmenter
	ttl_controller segTtl[v]
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
				s.ttl_controller.last_updated.mu.Lock()
				cb(s.ttl_controller.last_updated.Value)
				if len(s.ttl_controller.last_updated.Value) > 0 {
					s.ttl_controller.last_updated.Value = make(map[string][]LastUpdatedMessage[v])
				}
				s.ttl_controller.last_updated.mu.Unlock()
			}
		}
	}()
}

func (s *Segmap[v]) delete(full_key, segmented_key string, value v, parent, child *SegNode[v], uuid string) {
	child.mu.Lock()
	child.values.Delete(uuid)
	child.mu.Unlock()
	if child.values.Count() == 0 {
		parent.mu.Lock()
		parent.Children.Delete(segmented_key)
		parent.mu.Unlock()
	}
	s.ttl_controller.last_updated.mu.Lock()
	s.ttl_controller.last_updated.Value[full_key] = append(s.ttl_controller.last_updated.Value[full_key], LastUpdatedMessage[v]{
		Value: value,
		Uuid:  uuid,
	})
	s.ttl_controller.last_updated.mu.Unlock()
}

func (s *Segmap[v]) new_timer_for_ttl(wait_till time.Time, full_key, uuid string, value v) {
	s.ttl_controller.timer = make(chan bool)
	go func() {
		select {
		case <-time.After(time.Until(wait_till)):
			s.ttl_controller.last_updated.mu.Lock()
			s.ttl_controller.last_updated.Value[full_key] = append(s.ttl_controller.last_updated.Value[full_key], LastUpdatedMessage[v]{
				Value: value,
				Uuid:  uuid,
			})
			s.ttl_controller.last_updated.mu.Unlock()
			s.ttl_controller.mu.Lock()
			s.ttl_controller.values = s.ttl_controller.values[1:]
			s.ttl_controller.mu.Unlock()
			close(s.ttl_controller.timer)
		case <-s.ttl_controller.timer:
			return
		}
		if len(s.ttl_controller.values) > 0 {
			s.new_timer_for_ttl(s.ttl_controller.values[0].till, s.ttl_controller.values[0].key, s.ttl_controller.values[0].uuid, s.ttl_controller.values[0].value)
		}
	}()

}

func (s *Segmap[v]) add_to_ttl(ttl *segTtlValue[v]) int {

	defer s.ttl_controller.mu.RUnlock()
	low := 0
	s.ttl_controller.mu.RLock()

	high := len(s.ttl_controller.values) - 1

	for low <= high {
		mid_index := (low + high) / 2
		comp_value := s.ttl_controller.values[mid_index].till.Sub(ttl.till)

		if comp_value < 0 {
			return mid_index
		} else if comp_value > 0 {
			low = mid_index + 1
		} else {
			high = mid_index + 1
		}
	}
	return low
}

func (s *Segmap[v]) ttl_maker(full_key, segmented_key, uuid string, till time.Time, value v) *segTtlValue[v] {
	ttl := &segTtlValue[v]{
		key:           full_key,
		value:         value,
		segmented_key: segmented_key,
		uuid:          uuid,
		till:          till,
	}

	go func() {
		index := s.add_to_ttl(ttl)
		s.ttl_controller.mu.Lock()
		defer s.ttl_controller.mu.Unlock()
		s.ttl_controller.values = append(s.ttl_controller.values, nil)
		copy(s.ttl_controller.values[index+1:], s.ttl_controller.values[index:])
		s.ttl_controller.values[index] = ttl
		if index == 0 {
			if s.ttl_controller.timer != nil {
				close(s.ttl_controller.timer)
			}
			s.new_timer_for_ttl(till, full_key, uuid, value)
		}
	}()

	return ttl
}

func (s *Segmap[v]) NewSegVal(full_key, segmented_key string, ttl time.Duration, value v, parent, child *SegNode[v], uuid string) *SegValue[v] {
	if ttl > 0 {
		return &SegValue[v]{
			ttl:  s.ttl_maker(full_key, segmented_key, uuid, time.Now().Add(ttl), value),
			done: make(chan struct{}),
		}
		// go func() {
		// 	select {
		// 	case <-time.After(ttl):
		// 		close(seg_val.done)
		// 		s.delete(full_key, segmented_key, value, parent, child, uuid)
		// 	case <-seg_val.done:
		// 		s.delete(full_key, segmented_key, value, parent, child, uuid)
		// 	}
		// }()
	}

	return nil
}

func NewSegNode[v comparable]() *SegNode[v] {
	return &SegNode[v]{
		values:   swiss.NewMap[string, *SegValue[v]](42),
		Children: swiss.NewMap[string, *SegNode[v]](42),
	}
}

func NewSegmap[v comparable](segmenter Segmenter) *Segmap[v] {
	s := Segmap[v]{
		ttl_controller: segTtl[v]{
			values: []*segTtlValue[v]{},
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

	return &s
}

func (s *Segmap[v]) Put(key string, ttl time.Duration, value ...v) int {
	var parent *SegNode[v]
	segnode := s.Node
	segmented_key := ""

	for seg, i := s.Segmenter(key, 0); seg != ""; seg, i = s.Segmenter(key, i) {
		parent = segnode
		segmented_key = seg
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
			Value: val.ttl.value,
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

	val, ok := segnode.values.Get(uuid)
	if ok {

		close(val.done)
	}

	return ok
}

func (s *SegNode[v]) walk() []v {
	if s != nil {
		var result []v
		s.mu.RLock()
		s.values.Iter(func(k string, val *SegValue[v]) (stop bool) {
			result = append(result, val.ttl.value)
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
	// fmt.Println(segmap.Transverse("tuutf e f"))
}
