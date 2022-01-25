package util

import (
	"strings"

	"github.com/ppkg/distributed-scheduler/enum"
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

// 是否为并行任务
func IsParallelTask(name string) bool {
	return strings.Contains(name, enum.ParallelTaskSeparator)
}

// 拆分出并行处理的插件名称
func SplitParallelPlugin(name string) []string {
	return strings.Split(name, enum.ParallelTaskSeparator)
}
