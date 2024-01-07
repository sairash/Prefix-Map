package main

import (
	"testing"
)

// var segmap = NewSegmap[string](nil)
// var prefixMap = prefixmap.New()
var newSegmentedMap = MySegmentedMap[string]()

// var m = swiss.NewMap[string, int](42)

// func TestMain(t *testing.T) {
// 	segmap := NewSegmap[string](nil)
// 	segmap.Put("tuutf e f e", time.Duration(2)*time.Second, "1")
// }

func BenchmarkPut(b *testing.B) {

	for i := 0; i < b.N; i++ {
		// m.Put("foo", 1)
		// segmap.Put("tuutf e f e", time.Duration(0), "1")
		newSegmentedMap.Put("tuutf e f e", "1")
		// prefixMap.Insert("tuutfefe", "1")
	}
}

func BenchmarkGet(b *testing.B) {
	// prefixMap.Insert("tuutfefe", "1")
	newSegmentedMap.Put("tuutf e f e", "1")
	// segmap.Put("tuutf e f e", time.Duration(0), "1")
	for i := 0; i < b.N; i++ {
		// m.Put("foo", 1)
		// segmap.Get("tuutf e f e")
		// prefixMap.Get("tuutfefe")
		newSegmentedMap.Get("tuutf e f e")
	}
}

func BenchmarkTra(b *testing.B) {
	// prefixMap.Insert("tuutfefe", "1")
	// prefixMap.Insert("tuutfefx", "1")
	newSegmentedMap.Put("tuutf e f e", "1")
	newSegmentedMap.Put("tuutf e f x", "2")
	for i := 0; i < b.N; i++ {
		// prefixMap.GetByPrefix("tuutfefe")
		newSegmentedMap.Transverse("tuutf e f")
	}
}
