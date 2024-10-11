package main

import (
	"math/rand"
)

type SegmentedNode[v comparable] struct {
	Parent   *SegmentedNode[v]
	Values   map[int]v
	Children map[string]*SegmentedNode[v]
}

type SegmentedMap[v comparable] struct {
	Node *SegmentedNode[v]
}

func MySegmentedMap[v comparable]() *SegmentedMap[v] {
	return &SegmentedMap[v]{
		Node: &SegmentedNode[v]{
			Parent:   nil,
			Values:   map[int]v{},
			Children: map[string]*SegmentedNode[v]{},
		},
	}
}

func (s *SegmentedMap[v]) Put(key string, value v) {
	parent := s.Node
	var child *SegmentedNode[v]
	for seg, i := spacekeysegmentfunc(key, 0); seg != ""; seg, i = spacekeysegmentfunc(key, i) {
		child = parent.Children[seg]
		if child == nil {
			child = &SegmentedNode[v]{
				Parent:   nil,
				Values:   map[int]v{},
				Children: map[string]*SegmentedNode[v]{},
			}
			child.Parent = parent
			parent.Children[seg] = child
		}
		parent = child
	}
	uuid := rand.Intn(2000)
	parent.Values[uuid] = value
}

func (s *SegmentedMap[v]) Get(key string) map[int]v {
	node := s.Node
	for seg, i := spacekeysegmentfunc(key, 0); seg != ""; seg, i = spacekeysegmentfunc(key, i) {
		node = node.Children[seg]
		if node == nil {
			return nil
		}
	}

	return node.Values
}

func (s *SegmentedNode[v]) walk() []v {
	var result []v
	for _, val := range s.Values {
		result = append(result, val)
	}
	for _, seg := range s.Children {
		if seg != nil {
			result = append(result, seg.walk()...)
		}
	}
	return result
}

func (s *SegmentedMap[v]) Transverse(key string) []v {
	node := s.Node
	for seg, i := spacekeysegmentfunc(key, 0); seg != ""; seg, i = spacekeysegmentfunc(key, i) {
		node = node.Children[seg]
		if node == nil {
			return nil
		}
	}
	return node.walk()
}
