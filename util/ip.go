package util

import (
	"net"

	"github.com/maybgit/glog"
)

// 获取本机ip地址
func GetLocalIp() string {
	ip := "127.0.0.1"
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		glog.Errorf("GetLocalIp 获取本机ip地址失败,err:%+v", err)
		return ip
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ip
}
