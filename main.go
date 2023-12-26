package main

import (
	"fmt"
	"time"
)

type NodeValues struct {
	Value string
	Done  chan struct{}
}

type Node struct {
	Char       string
	Values     []**NodeValues
	AutoDelete bool
	Children   [27]*Node
}

type Trie struct {
	RootNode *Node
}

func NewTrie() *Trie {
	return &Trie{RootNode: NewNode("\000")}
}

func NewNode(char string) *Node {
	node := &Node{Char: char}
	for i := 0; i < 27; i++ {
		node.Children[i] = nil
	}
	return node
}

func NewNodeValue(value string, has_ttl bool, ttl time.Duration) **NodeValues {
	nv := &NodeValues{Value: value}
	nv.Done = make(chan struct{})
	if has_ttl {
		go func() {
			select {
			case <-time.After(ttl):
				close(nv.Done)
				nv = nil
			case <-nv.Done:
				nv = nil
			}
		}()
	}
	return &nv
}

func (t *Trie) Insert(key string, has_ttl bool, time_to_live int, values ...string) error {
	current := t.RootNode
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if current.Children[index] == nil {
			current.Children[index] = NewNode(string(key[i]))
		}
		current = current.Children[index]
	}
	current.AutoDelete = true

	for _, v := range values {
		nv := NewNodeValue(v, has_ttl, time.Duration(time_to_live)*time.Second)
		current.Values = append(current.Values, nv)
	}

	return nil
}

func delete(current **Node) {

}

func traverse(current **Node) []string {
	var result []string
	if (*current) != nil {
		auto_delete := (*current).AutoDelete

		for _, v := range (*current).Values {
			val := (*v)
			if val != nil {

				result = append(result, val.Value)

				if auto_delete {
					select {
					case <-val.Done:
					default:
						close(val.Done)
					}
				}
			}
		}

		for _, child := range (*current).Children {
			if child != nil {
				result = append(result, traverse(&child)...)
			}
		}

		if auto_delete {
			(*current).Values = nil
			delete(current)
		}
	}
	return result
}

func (t *Trie) SearchThrough(key string) []string {
	current_pointer := &t.RootNode
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if (*current_pointer) == nil || (*current_pointer).Children[index] == nil {
			return []string{}
		}
		current_pointer = &(*current_pointer).Children[index]
	}

	return traverse(current_pointer)
}

func main() {
	new_node := NewTrie()

	new_node.Insert("hel", true, 2, "hel")
	new_node.Insert("hello", true, 2, "hello")
	new_node.Insert("hel", false, 2, "hel")
	new_node.Insert("he", true, 2, "he")
	new_node.Insert("h", true, 2, "h")
	new_node.Insert("se", true, 2, "se")

	time.Sleep(3 * time.Second)
	fmt.Println(new_node.SearchThrough("h"))
	fmt.Println(new_node.SearchThrough("se"))
	fmt.Println(new_node.SearchThrough("h"))
}
