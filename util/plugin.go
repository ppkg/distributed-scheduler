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
