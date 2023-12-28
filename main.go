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
	Values     []**NodeValues
	AutoDelete bool
	Children   [27]*Node
}

type Trie struct {
	RootNode   *Node
	DeleteNode *Node
}

var delteTire *Trie

func NewTrie(needs_delete bool, clean_deleated_nodes_time time.Duration) *Trie {
	new_trie := &Trie{RootNode: NewNode()}
	if needs_delete {
		go new_trie.backgroundDelete(clean_deleated_nodes_time)
	}
	return new_trie
}

func NewNode() *Node {
	node := &Node{}
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

func (t *Trie) Insert(key string, has_ttl bool, time_to_live int, is_auto_delete bool, values ...string) error {
	current := t.RootNode
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if current.Children[index] == nil {
			current.Children[index] = NewNode()
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

func (n *Node) InsertKeyInPointer(key interface{}) (*Node, error) {
	new_node := NewNode()
	switch key := key.(type) {
	case int:
		n.Children[key] = new_node
	case rune:
		n.Children[key-'a'] = new_node
	default:
		return nil, fmt.Errorf("key Can Only be string or int")
	}
	return new_node, nil
}

func (t *Trie) InsertHereIfAnyChildHasValue(key string, has_ttl bool, time_to_live int, values ...string) error {
	current_pointer := t.RootNode
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if current_pointer.Children[index] == nil {
			new_pointer, err := current_pointer.InsertKeyInPointer(index)
			if err != nil {
				return err
			}
			current_pointer = new_pointer
		} else {
			current_pointer = current_pointer.Children[index]
		}
	}

	should_transverse := false

	for _, children := range current_pointer.Children {
		if children != nil {
			should_transverse = true
			break
		}
	}

	if should_transverse {
		all_inner_value := t.traverse(current_pointer, key)
		for _, v := range all_inner_value {
			current_pointer.Values = append(current_pointer.Values, NewNodeValue(v, false, 0))
		}

	}
	return nil
}

func (t *Trie) Delete() {

}

func (t *Trie) backgroundDelete(clean_deleated_nodes_time time.Duration) {
	t.DeleteNode = NewNode()

	time.Sleep(time.Duration(5))
	t.backgroundDelete(clean_deleated_nodes_time)
}

func (t *Trie) traverse(current *Node, key string) []string {
	var result []string
	if current != nil {
		auto_delete := current.AutoDelete

		for _, v := range current.Values {
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

		for k, child := range current.Children {
			if child != nil {
				result = append(result, t.traverse(child, key+string(k))...)
			}
		}

		if auto_delete {
			current.Values = nil
			t.Delete()
		}
	}
	return result
}

func (t *Trie) SearchThrough(key string) []string {
	current_pointer := t.RootNode
	for i := 0; i < len(key); i++ {
		index := key[i] - 'a'
		if current_pointer == nil || current_pointer.Children[index] == nil {
			return []string{}
		}
		current_pointer = current_pointer.Children[index]
	}

	return t.traverse(current_pointer, key)
}

func main() {
	new_node := NewTrie(true, time.Duration(5))

	new_node.Insert("hel", true, 2, true, "hel")
	new_node.Insert("hello", true, 2, true, "hello")
	new_node.Insert("hel", false, 2, true, "hel")
	new_node.Insert("he", true, 2, true, "he")
	new_node.Insert("h", true, 2, true, "h")
	new_node.Insert("se", true, 2, true, "se")
	fmt.Println(new_node.SearchThrough("hello"))
	fmt.Println(new_node.SearchThrough("se"))
	fmt.Println(new_node.SearchThrough("he"))
	fmt.Println(new_node.SearchThrough("h"))
	fmt.Println(new_node.SearchThrough("h"))

	time.Sleep(3 * time.Second)
}
