package util

import (
	"sort"

	"github.com/google/go-cmp/cmp"
)

func EqualStringSlice(src, dst []string) bool {
	trans := cmp.Transformer("Sort", func(in []int) []int {
		out := append([]int(nil), in...) // Copy input to avoid mutating it
		sort.Ints(out)
		return out
	})
	return cmp.Equal(src, dst, trans)
}
