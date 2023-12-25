package main

import (
	"fmt"
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

func (t *Trie) Insert(key string, values ...string) error {
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
	return nil
}

func AggrigateAndReturnEveryChildrenValues(current **Node) []string {
	return_string := []string{}
	current_node := *current
	if current_node == nil {
		return []string{}
	} else {
		return_string = append(return_string, current_node.Values...)
	}
	for _, val := range current_node.Children {
		if val != nil {
			return_string = append(return_string, AggrigateAndReturnEveryChildrenValues(&val)...)
		}
	}

	if current_node.AutoDelete {
		*current = nil
	}
	return return_string
}

func (t *Trie) SearchThrough(key string) []string {
	current_pointer := &t.RootNode
	current := *current_pointer
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if current == nil || current.Children[index] == nil {
			return []string{}
		}

		current_pointer = &current.Children[index]
		current = *current_pointer
	}

	return AggrigateAndReturnEveryChildrenValues(current_pointer)
}

func main() {
	new_node := NewTrie()

	new_node.Insert("hello", "hello")
	new_node.Insert("hel", "hel")
	new_node.Insert("hel", "hel")
	new_node.Insert("he", "he")
	new_node.Insert("h", "h")
	new_node.Insert("se", "se")

	fmt.Println(new_node.SearchThrough("h"))
	fmt.Println(new_node.SearchThrough("se"))
}
