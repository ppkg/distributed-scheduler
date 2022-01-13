package util

import "strings"

func FindPluginPos(pluginSet string, plugin string) int {
	for i, item := range strings.Split(pluginSet, ",") {
		if item == plugin {
			return i
		}
	}
	return -1
}

func IsSupportPlugin(pluginSet []string, plugin string) bool {
	for _, item := range pluginSet {
		if item == plugin {
			return true
		}
	}
	return false
}
