package util

import (
	"strings"
)

func FindHandlerPos(nameSet string, name string) int {
	for i, item := range strings.Split(nameSet, ",") {
		if item == name {
			return i
		}
	}
	return -1
}

func IsSupportHandler(nameSet []string, name string) bool {
	for _, item := range nameSet {
		if item == name {
			return true
		}
	}
	return false
}
