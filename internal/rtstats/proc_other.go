//go:build !linux

package rtstats

// procStats has nothing to add outside Linux.
func procStats(*Sample) {}
