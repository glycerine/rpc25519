package main

import (
	"cmp"
	"iter"
	"slices"
)

// sort any map by its keys
func sorted[K cmp.Ordered, V any](m map[K]V) iter.Seq2[K, V] {

	return func(yield func(K, V) bool) {

		var keys []K
		for k := range m {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			v := m[k]
			if !yield(k, v) {
				return
			}
		}
	} // end seq2 definition
}
