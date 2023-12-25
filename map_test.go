package main

import (
	"testing"
)

func TestMain(t *testing.T) {
	new_node := NewTrie()

	values := []string{"hello", "he", "he", "h"}
	getting_value := []string{"h", "he", "he", "hello"}

	for _, v := range values {
		new_node.Insert(v, v)
	}

	value := new_node.SearchThrough("h")
	new_value := new_node.SearchThrough("h")

	for k, v := range value {
		if v != getting_value[k] {
			t.Errorf("Unexpected value got expected: %v, got: %v", getting_value[k], v)
		}
	}

	if len(new_value) != 0 {
		t.Errorf("Value mismatch, the expected value was [], got %v", new_value)
	}
}
