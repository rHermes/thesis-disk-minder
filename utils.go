package main

import "fmt"

// makeStrLookupMap create a lookup map for a string slice
func makeStrLookupMap(xs []string) map[string]struct{} {
	m := make(map[string]struct{}, 0)
	for _, x := range xs {
		m[x] = struct{}{}
	}
	return m
}

func generateDefaultTopicNames() []string {
	var xs []string

	for _, sys := range []string{"Flink", "BeamFlink"} {
		for _, query := range []string{"Identity", "Sample", "Projection", "Grep"} {
			for para := 1; para < 3; para++ {
				for run := 1; run < 11; run++ {
					xs = append(xs, fmt.Sprintf("%s%s_%s_%d_%d", OutputTopicPrefix, sys, query, para, run))
				}
			}
		}
	}
	return xs
}
