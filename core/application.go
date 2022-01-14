package core

import (
	"context"
	"distributed-scheduler/dto"
	"distributed-scheduler/enum"
	"distributed-scheduler/repository"
	"distributed-scheduler/repository/impl"
	"distributed-scheduler/util"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/nacos-group/nacos-sdk-go/clients"
	namingClient "github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/ppkg/glog"
	"github.com/ppkg/kit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"gorm.io/gorm"
)

type ApplicationContext struct {
	conf            Config
	raft            *raft.Raft
	tm              *transport.Manager
	grpcServer      *grpc.Server
	namingClient    namingClient.INamingClient
	heartbeatKeeper heartbeatMap
	Db              *gorm.DB
	Scheduler       *scheduleEngine
	jobContainer    *runningJobContainer
	jobRepo         repository.JobRepository
	taskRepo        repository.TaskRepository
}

func NewApp(opts ...Option) *ApplicationContext {
	instance := &ApplicationContext{
		heartbeatKeeper: heartbeatMap{
			data: make(map[string]*heartbeat),
		},
		jobRepo:  impl.NewJobRepository(),
		taskRepo: impl.NewTaskRepository(),
	}
	instance.initDefaultConfig()
	for _, m := range opts {
		m(&instance.conf)
	}
	instance.initGrpc()
	return instance
}

// 定时检查心跳状态
func (s *ApplicationContext) cronCheckHeartbeatStatus() {
	timer := time.Tick(3 * time.Second)
	for now := range timer {
		for _, item := range s.heartbeatKeeper.All() {
			if now.Sub(item.AccessTime) < 3*time.Second {
				continue
			}
			s.Scheduler.RemoveWorker(item.WorkerNode)
			s.heartbeatKeeper.Remove(item.NodeId)
			glog.Infof("ApplicationContext/checkHeartbeatStatus 心跳断开worker节点(%v,%v)被移除", item.NodeId, item.Endpoint)
		}
	}
}

// 更新心跳数据
func (s *ApplicationContext) UpdateHeartbeat(worker WorkerNode) error {
	oldWorker, ok := s.heartbeatKeeper.Get(worker.NodeId)
	var err error
	if !ok {
		err = s.Scheduler.AddWorker(worker)
		if err != nil {
			return err
		}
		s.heartbeatKeeper.Put(worker)
		glog.Infof("ApplicationContext/checkHeartbeatStatus 新增worker节点(%s,%s),支持插件:%v", worker.NodeId, worker.Endpoint, worker.PluginSet)
		return nil
	}
	trans := cmp.Transformer("Sort", func(in []string) []string {
		out := append([]string(nil), in...)
		sort.Strings(out)
		return out
	})
	// 如果worker节点支持插件集有变动时需要更新索引
	if !cmp.Equal(worker.PluginSet, oldWorker.PluginSet, trans) {
		s.Scheduler.UpdateWorkerIndex(worker)
	}

	oldWorker.AccessTime = time.Now()
	return nil
}

// 监控raft master节点
// 当前节点失去leader身份时需要取消正在运行job
// 当前节点选举为leader身份时需要加载未完成异步job
func (s *ApplicationContext) watchRaftMaster() {
	for isLeader := range s.raft.LeaderCh() {
		if isLeader {
			glog.Infof("ApplicationContext/watchRaftMaster 当前raft节点(%s,%s)获取leader身份", s.conf.Raft.NodeId, s.getPeerAddr())
			s.restartUndoneAsyncJob()
			continue
		}
		glog.Infof("ApplicationContext/watchRaftMaster 当前raft节点(%s,%s)失去leader身份", s.conf.Raft.NodeId, s.getPeerAddr())
		for _, job := range s.jobContainer.GetAll() {
			util.CancelNotify(job.Ctx, job.Job, fmt.Sprintf("当前raft节点(%s,%s)失去leader身份,取消正在运行job", s.conf.Raft.NodeId, s.getPeerAddr()))
			job.Job.Job.Status = enum.SystemExceptionJobStatus
		}
		s.jobContainer.RemoveAll()
		s.Scheduler.NotifyChannel.RemoveAll()
	}
}

// 初始化默认配置
func (s *ApplicationContext) initDefaultConfig() {
	s.conf.AppName = os.Getenv("APP_NAME")
	s.conf.PeerIp = os.Getenv("PEER_IP")
	if s.conf.PeerIp == "" {
		s.conf.PeerIp = util.GetLocalIp()
	}
	port := os.Getenv("APP_PORT")
	if port != "" {
		s.conf.Port, _ = strconv.Atoi(port)
	}
	if s.conf.Port == 0 {
		s.conf.Port = 8080
	}

	threadCount := os.Getenv("SCHEDULER_THREAD_COUNT")
	if threadCount != "" {
		s.conf.SchedulerThreadCount, _ = strconv.Atoi(threadCount)
	}
	if s.conf.SchedulerThreadCount == 0 {
		// worker协程池大小默认1000
		s.conf.SchedulerThreadCount = 1000
	}

	s.conf.Raft.NodeId = os.Getenv("NODE_ID")
	if s.conf.Raft.NodeId == "" {
		s.conf.Raft.NodeId = os.Getenv("HOSTNAME")
	}
	s.conf.Raft.DataDir = os.Getenv("RAFT_DATA_DIR")
	if s.conf.Raft.DataDir == "" {
		s.conf.Raft.DataDir = "./data"
	}

	nacosAddrs := os.Getenv("NACOS_ADDRS")
	if nacosAddrs != "" {
		addrList := strings.Split(nacosAddrs, ",")
		for _, item := range addrList {
			s.appendNacosAddrConfig(item)
		}
	}

	s.conf.Nacos.ClusterName = os.Getenv("NACOS_CLUSTER_NAME")
	s.conf.Nacos.GroupName = os.Getenv("NACOS_GROUP_NAME")
	s.conf.Nacos.Namespace = os.Getenv("NACOS_NAMESPACE")
}

func (s *ApplicationContext) appendNacosAddrConfig(addr string) {
	if addr == "" {
		return
	}
	host, port := parseNacosAddr(addr)
	s.conf.Nacos.Addrs = append(s.conf.Nacos.Addrs, host)
	s.conf.Nacos.Ports = append(s.conf.Nacos.Ports, port)
}

func (s *ApplicationContext) Run() error {
	ctx := context.Background()
	// 初始化调度器引擎
	s.Scheduler = NewScheduler(s.conf.SchedulerThreadCount)
	// 初始化job容器
	s.jobContainer = NewJobContainer()

	// 注册服务(服务发现)
	err := s.initNacos()
	if err != nil {
		glog.Errorf("Application/run 注册服务异常,err:%v", err)
		return err
	}

	// 初始化数据库引擎
	err = s.initDatabase()
	if err != nil {
		glog.Errorf("Application/run 初始化数据库引擎异常,err:%v", err)
		return err
	}

	// 初始化raft选举机制
	err = s.initRaft(ctx, nil)
	if err != nil {
		glog.Errorf("Application/run 初始化raft选举异常,err:%v", err)
		return err
	}

	// 监听服务发现
	err = s.watchServiceDiscovery()
	if err != nil {
		glog.Errorf("Application/run 监听服务发现异常,err:%v", err)
		return err
	}

	// 定时检查worker心跳状态
	go s.cronCheckHeartbeatStatus()
	// 由于nacos服务发现有延迟导致raft选举有点延后，需要延迟3三分钟后再执行监控raft身份变更及重新加载由于服务器奔溃导致出错的job
	time.AfterFunc(3*time.Minute, func() {
		// 监控raft身份并及时处理
		go s.watchRaftMaster()
	})

	// 初始化grpc服务
	err = s.doServe()
	if err != nil {
		glog.Errorf("Application/run 监听grpc服务异常,err:%v", err)
		return err
	}
	return nil
}

// 初始化raft选举
func (s *ApplicationContext) initRaft(ctx context.Context, fsm raft.FSM) error {
	c := raft.DefaultConfig()
	c.LogLevel = "info"
	c.Logger = NewLogger(&hclog.LoggerOptions{
		Name:  "raft",
		Level: hclog.LevelFromString(c.LogLevel),
	})

	c.LocalID = raft.ServerID(s.conf.Raft.NodeId)

	baseDir := filepath.Join(s.conf.Raft.DataDir, s.conf.Raft.NodeId)
	_ = os.MkdirAll(baseDir, os.ModePerm)

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	s.raft, err = raft.NewRaft(c, fsm, ldb, sdb, fss, s.tm.Transport())
	if err != nil {
		return fmt.Errorf("raft.NewRaft: %v", err)
	}

	// 初始化集群
	if isInitCluster, _ := raft.HasExistingState(ldb, sdb, fss); isInitCluster {
		return nil
	}
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(s.conf.Raft.NodeId),
				Address:  raft.ServerAddress(s.getPeerAddr()),
			},
		},
	}
	f := s.raft.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
	}
	return nil
}

// 初始化GRPC服务
func (s *ApplicationContext) initGrpc() {
	s.tm = transport.New(raft.ServerAddress(s.getListenAddr()), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	})
	s.grpcServer = grpc.NewServer()
	s.tm.Register(s.grpcServer)
}

// 注册grpc服务
func (s *ApplicationContext) RegisterGrpc(f func(ctx *ApplicationContext, server *grpc.Server)) *ApplicationContext {
	f(s, s.grpcServer)
	return s
}

func (s *ApplicationContext) doServe() error {
	reflection.Register(s.grpcServer)
	sock, err := net.Listen("tcp", s.getListenAddr())
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	if err := s.grpcServer.Serve(sock); err != nil {
		return fmt.Errorf("failed to grpc serve: %v", err)
	}
	return nil
}

func (s *ApplicationContext) getListenAddr() string {
	return fmt.Sprintf(":%d", s.conf.Port)
}

func (s *ApplicationContext) getPeerAddr() string {
	return fmt.Sprintf("%s:%d", s.conf.PeerIp, s.conf.Port)
}

// 动态添加节点
func (s *ApplicationContext) AddPeer(nodeId, addr string) error {
	serverList := s.raft.GetConfiguration().Configuration().Servers
	for _, item := range serverList {
		if item.ID == raft.ServerID(nodeId) {
			return nil
		}
	}
	rs := s.raft.AddVoter(raft.ServerID(nodeId), raft.ServerAddress(addr), 0, 5*time.Second)
	if err := rs.Error(); err != nil {
		glog.Errorf("Application/AddPeer 当前节点:%s，新增从节点(%s,%s)失败,err:%+v", s.conf.Raft.NodeId, nodeId, addr, err)
		return err
	}
	glog.Infof("Application/AddPeer 当前节点:%s，新增从节点(%s,%s)成功", s.conf.Raft.NodeId, nodeId, addr)
	return nil
}

// 动态删除节点
func (s *ApplicationContext) RemovePeer(nodeId string) error {
	isFound := false
	serverList := s.raft.GetConfiguration().Configuration().Servers
	for _, item := range serverList {
		if item.ID == raft.ServerID(nodeId) {
			isFound = true
			break
		}
	}
	if !isFound {
		return nil
	}

	rs := s.raft.RemoveServer(raft.ServerID(nodeId), 0, 5*time.Second)
	if err := rs.Error(); err != nil {
		glog.Errorf("Application/RemovePeer 当前节点:%s，移除从节点(%s)失败,err:%+v", s.conf.Raft.NodeId, nodeId, err)
		return err
	}
	glog.Infof("Application/RemovePeer 当前节点:%s，移除从节点(%s)成功", s.conf.Raft.NodeId, nodeId)
	return nil
}

// 服务发现，向nacos注册服务
func (s *ApplicationContext) initNacos() error {
	if s.conf.AppName == "" {
		return fmt.Errorf("当前节点:%s，未配置服务名", s.conf.Raft.NodeId)
	}
	if len(s.conf.Nacos.Addrs) == 0 {
		return fmt.Errorf("当前节点:%s，未配置nacos地址", s.conf.Raft.NodeId)
	}

	var err error
	clientConfig := constant.ClientConfig{
		NamespaceId:         s.conf.Nacos.Namespace,
		TimeoutMs:           2000,
		NotLoadCacheAtStart: true,
		LogDir:              "/tmp/nacos/log",
		CacheDir:            "/tmp/nacos/cache",
		RotateTime:          "24h",
		MaxAge:              3,
		LogLevel:            "info",
	}

	serverConfigs := make([]constant.ServerConfig, 0, len(s.conf.Nacos.Addrs))
	for i, host := range s.conf.Nacos.Addrs {
		serverConfigs = append(serverConfigs, constant.ServerConfig{
			IpAddr: host,
			Port:   uint64(s.conf.Nacos.Ports[i]),
		})
	}

	s.namingClient, err = clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		return fmt.Errorf("当前节点:%s，实例化nacos客户端异常:%v", s.conf.Raft.NodeId, err)
	}
	success, err := s.namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          s.conf.PeerIp,
		Port:        uint64(s.conf.Port),
		ServiceName: s.conf.AppName,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    map[string]string{"nodeId": s.conf.Raft.NodeId},
		ClusterName: s.conf.Nacos.ClusterName, // default value is DEFAULT
		GroupName:   s.conf.Nacos.GroupName,   // default value is DEFAULT_GROUP
	})
	if err != nil {
		return fmt.Errorf("当前节点:%s，注册服务发现异常:%v", s.conf.Raft.NodeId, err)
	}
	if !success {
		return fmt.Errorf("当前节点:%s，注册服务(%s)失败", s.conf.Raft.NodeId, s.getPeerAddr())
	}
	return nil
}

// 监听服务发现
func (s *ApplicationContext) watchServiceDiscovery() error {
	return s.namingClient.Subscribe(&vo.SubscribeParam{
		ServiceName: s.conf.AppName,
		GroupName:   s.conf.Nacos.GroupName,
		Clusters: []string{
			s.conf.Nacos.ClusterName,
		},
		SubscribeCallback: func(services []model.SubscribeService, nacosErr error) {
			glog.Infof("Application/watchServiceDiscovery 节点(%s)服务发现回调通知:%s,nacosErr:%v", s.conf.Raft.NodeId, kit.JsonEncode(services), nacosErr)
			future := s.raft.VerifyLeader()
			if future.Error() != nil {
				glog.Infof("当前(%s)不是主节点，不做处理:%v", s.conf.Raft.NodeId, future.Error())
				return
			}
			var err error
			for _, item := range services {
				if item.Metadata == nil {
					continue
				}
				if item.Metadata["nodeId"] == s.conf.Raft.NodeId {
					continue
				}
				// 当服务恢复正常则加入到raft集群里
				if item.Enable && item.Valid {
					err = s.AddPeer(item.Metadata["nodeId"], fmt.Sprintf("%s:%d", item.Ip, item.Port))
					if err != nil {
						glog.Errorf("Application/watchServiceDiscovery 当前节点:%s，service信息:%+v，添加raft节点失败:%v", s.conf.Raft.NodeId, item, err)
					}
				} else {
					// 当服务不正常则从raft集群移除
					err = s.RemovePeer(item.Metadata["nodeId"])
					if err != nil {
						glog.Errorf("Application/watchServiceDiscovery 当前节点:%s，service信息:%+v，移除raft节点失败:%v", s.conf.Raft.NodeId, item, err)
					}
				}
			}
			glog.Infof("Application/watchServiceDiscovery 当前节点:%s,raft集群状态:%s", s.conf.Raft.NodeId, kit.JsonEncode(s.raft.GetConfiguration().Configuration().Servers))
		},
	})
}

// 判断是否为主节点
func (s *ApplicationContext) IsMasterNode() bool {
	future := s.raft.VerifyLeader()
	if future.Error() != nil {
		glog.Infof("ApplicationContext/IsMasterNode 当前(%s)不是主节点，code:%v", s.conf.Raft.NodeId, future.Error())
		return false
	}
	return true
}

// 获取主节点信息
func (s *ApplicationContext) GetMasterNode() dto.NodeInfo {
	leaderServer := s.raft.Leader()
	node := dto.NodeInfo{}
	serverList := s.raft.GetConfiguration().Configuration().Servers
	for _, item := range serverList {
		if strings.HasSuffix(string(item.Address), string(leaderServer)) {
			node.NodeId = string(item.ID)
			node.Endpoint = string(item.Address)
			break
		}
	}

	return node
}

// 应用配置
type Config struct {
	// 应用名称
	AppName string
	// 内部通讯ip
	PeerIp string
	// 应用监听端口号
	Port int

	Raft RaftConfig
	// nacos配置
	Nacos NacosConfig
	// 调度器线程数
	SchedulerThreadCount int
}

type RaftConfig struct {
	// 节点ID
	NodeId string
	// raft数据存储目录
	DataDir string
}

type NacosConfig struct {
	Addrs       []string
	Ports       []int
	Namespace   string
	GroupName   string
	ClusterName string
}

type Option func(conf *Config)

// 配置应用名称
func WithAppNameOption(name string) Option {
	return func(conf *Config) {
		conf.AppName = name
	}
}

func WithPortOption(port int) Option {
	return func(conf *Config) {
		conf.Port = port
	}
}

func WithSchedulerThreadOption(num int) Option {
	return func(conf *Config) {
		conf.SchedulerThreadCount = num
	}
}

// 配置应用节点ID
func WithNodeIdOption(nodeId string) Option {
	return func(conf *Config) {
		conf.Raft.NodeId = nodeId
	}
}

// 配置raft数据存储目录
func WithRaftDirOption(dir string) Option {
	return func(conf *Config) {
		conf.Raft.DataDir = dir
	}
}

// 配置nacos服务地址，格式：域名(ip)+端口号
func WithNacosAddrOption(addr string) Option {
	return func(conf *Config) {
		if addr == "" {
			return
		}
		host, port := parseNacosAddr(addr)
		conf.Nacos.Addrs = append(conf.Nacos.Addrs, host)
		conf.Nacos.Ports = append(conf.Nacos.Ports, port)
	}
}

func WithNacosGroupNameOption(group string) Option {
	return func(conf *Config) {
		conf.Nacos.GroupName = group
	}
}

func WithNacosClusterNameOption(cluster string) Option {
	return func(conf *Config) {
		conf.Nacos.ClusterName = cluster
	}
}

func WithNacosNamespaceOption(namespace string) Option {
	return func(conf *Config) {
		conf.Nacos.Namespace = namespace
	}
}

// 解析nacos地址
func parseNacosAddr(addr string) (string, int) {
	pathInfo := strings.Split(addr, ":")
	port := 8848
	if len(pathInfo) > 1 {
		tmp, _ := strconv.Atoi(pathInfo[1])
		if tmp > 0 {
			port = tmp
		}
	}
	return pathInfo[0], port
}

type heartbeatMap struct {
	data map[string]*heartbeat
	lock sync.RWMutex
}

func (s *heartbeatMap) Put(worker WorkerNode) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.data[worker.NodeId] = &heartbeat{
		WorkerNode: worker,
		AccessTime: time.Now(),
	}
}

func (s *heartbeatMap) Get(nodeId string) (*heartbeat, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	val, ok := s.data[nodeId]
	return val, ok
}

func (s *heartbeatMap) Remove(nodeId string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.data, nodeId)
}

func (s *heartbeatMap) All() []*heartbeat {
	s.lock.RLock()
	defer s.lock.RUnlock()
	list := make([]*heartbeat, 0, len(s.data))
	for _, v := range s.data {
		list = append(list, v)
	}
	return list
}

// 心跳包
type heartbeat struct {
	WorkerNode
	AccessTime time.Time
}
