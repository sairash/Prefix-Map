package main

import (
	"fmt"
	"time"
)

type Node struct {
	Char       string
	Values     []string
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

func (node *Node) setTTL(ttl time.Duration) {
	go func() {
		<-time.After(ttl)
		node.Values = nil
	}()
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
	current.Values = append(current.Values, values...)
	if has_ttl {
		current.setTTL(time.Duration(time_to_live) * time.Second)
	}
	return nil
}

func (current *Node) AggrigateAndReturnEveryChildrenValues() []string {
	return_string := []string{}
	if current == nil {
		return []string{}
	} else {
		return_string = append(return_string, current.Values...)
	}
	for _, val := range current.Children {
		if val != nil {
			return_string = append(return_string, val.AggrigateAndReturnEveryChildrenValues()...)
		}
	}

	if current.AutoDelete {
		current.Values = nil
	}
	return return_string
}

func (t *Trie) SearchThrough(key string) []string {
	current := t.RootNode
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if current == nil || current.Children[index] == nil {
			return []string{}
		}

		current = current.Children[index]
	}

	return current.AggrigateAndReturnEveryChildrenValues()
}

func main() {
	new_node := NewTrie()

	new_node.Insert("hel", true, 2, "hel")
	new_node.Insert("hello", true, 2, "hello")
	new_node.Insert("hel", false, 2, "hel")
	new_node.Insert("he", true, 2, "he")
	new_node.Insert("h", true, 2, "h")
	new_node.Insert("se", true, 2, "se")

	fmt.Println(new_node.SearchThrough("h"))
	fmt.Println(new_node.SearchThrough("se"))
	fmt.Println(new_node.SearchThrough("h"))
	time.Sleep(3 * time.Second)
}
