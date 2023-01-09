package core

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ppkg/distributed-scheduler/enum"
	"github.com/ppkg/distributed-scheduler/repository"
	"github.com/ppkg/distributed-scheduler/repository/impl"
	"github.com/ppkg/distributed-scheduler/util"

	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/maybgit/glog"
	"github.com/nacos-group/nacos-sdk-go/clients"
	configClient "github.com/nacos-group/nacos-sdk-go/clients/config_client"
	namingClient "github.com/nacos-group/nacos-sdk-go/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	nacosModel "github.com/nacos-group/nacos-sdk-go/model"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/ppkg/kit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"gorm.io/gorm"
)

type ApplicationContext struct {
	isLeader bool
	// 是否在拉取worker连接信息
	isPullWorker     bool
	schedulerNodeIds sync.Map

	conf       Config
	raft       *raft.Raft
	tm         *transport.Manager
	grpcServer *grpc.Server
	// nacos服务发现客户端
	namingClient namingClient.INamingClient
	// nacos配置服务客户端
	configClient configClient.IConfigClient
	Db           *gorm.DB
	Scheduler    *scheduleEngine
	jobContainer *runningJobContainer
	jobRepo      repository.JobRepository
	taskRepo     repository.TaskRepository
}

func NewApp(opts ...Option) *ApplicationContext {
	instance := &ApplicationContext{
		jobRepo:  impl.NewJobRepository(),
		taskRepo: impl.NewTaskRepository(),
	}
	instance.initDefaultConfig()
	for _, m := range opts {
		m(&instance.conf)
	}
	if instance.conf.Raft.NodeId == "" {
		instance.conf.Raft.NodeId = fmt.Sprintf("%s:%d", instance.conf.PeerIp, instance.conf.Port)
	}
	instance.initGrpc()
	// 实例化调度引擎
	instance.Scheduler = NewScheduler(instance.conf.SchedulerThreadCount, instance.pullAllWorker)
	return instance
}

// 监控raft leader节点
// 当前节点失去leader身份时需要取消正在运行job
// 当前节点选举为leader身份时需要加载未完成异步job
func (s *ApplicationContext) watchRaftLeader() {
	for isLeader := range s.raft.LeaderCh() {
		s.isLeader = isLeader
		if isLeader {
			s.updateCurrentNacosRole(enum.LeaderRaftRole)
			glog.Infof("ApplicationContext/watchRaftLeader 当前raft节点(%s)获取leader身份", s.conf.Raft.NodeId)

			// 30秒后再次确认自己身份
			time.AfterFunc(30*time.Second, func() {
				if !s.isLeader {
					glog.Infof("ApplicationContext/watchRaftLeader 当前raft节点(%s)已经失去leader身份跳过二次确认leader身份", s.conf.Raft.NodeId)
					return
				}
				s.makeSureLeaderRole()
			})

			// 更新配置中心leaderState值
			s.updateLeaderStateConfig()

			time.AfterFunc(time.Minute, func() {
				// 1分钟后检查是否满足重启job条件
				if !s.isLeader {
					glog.Infof("ApplicationContext/watchRaftLeader 当前raft节点(%s)已经失去leader身份跳过job重启", s.conf.Raft.NodeId)
					return
				}
				s.restartUndoneAsyncJob()
			})
			// 拉取并更新worker节点信息
			s.pullAllWorker()
			continue
		}
		s.updateCurrentNacosRole(enum.FollowerRaftRole)
		glog.Infof("ApplicationContext/watchRaftLeader 当前raft节点(%s)失去leader身份", s.conf.Raft.NodeId)
		for _, job := range s.jobContainer.GetAll() {
			util.CancelNotify(job.Ctx, job.Job, fmt.Sprintf("当前raft节点(%s)失去leader身份,取消正在运行job", s.conf.Raft.NodeId))
			job.Job.Job.Status = int32(enum.SystemExceptionJobStatus)
		}
		s.jobContainer.RemoveAll()
	}
}

// 确认leader身份
func (s *ApplicationContext) makeSureLeaderRole() {
	instance := s.getCurrentNacosInstance()
	if instance == nil {
		return
	}
	if instance.Metadata["role"] == string(enum.LeaderRaftRole) {
		return
	}
	s.updateCurrentNacosRole(enum.LeaderRaftRole)
}

// 更新配置中心leaderState值
func (s *ApplicationContext) updateLeaderStateConfig() {
	_, _ = s.configClient.PublishConfig(vo.ConfigParam{
		DataId:  s.conf.LeaderStateKey,
		Group:   s.conf.Nacos.ServiceGroup,
		Content: fmt.Sprintf("%s,%s", s.conf.Raft.NodeId, time.Now().Format("2006-01-02 15:04:05"))})
}

// 初始化默认配置
func (s *ApplicationContext) initDefaultConfig() {
	s.conf.AppName = os.Getenv("APP_NAME")
	if s.conf.AppName == "" {
		s.conf.AppName = "distributed-scheduler"
	}
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
		// worker协程池大小默认100
		s.conf.SchedulerThreadCount = 100
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
	s.conf.Nacos.ServiceGroup = os.Getenv("NACOS_SERVICE_GROUP")
	if s.conf.Nacos.ServiceGroup == "" {
		s.conf.Nacos.ServiceGroup = "DEFAULT_GROUP"
	}
	s.conf.Nacos.Namespace = os.Getenv("NACOS_NAMESPACE")
	s.conf.WorkerServiceName = "distributed-workder"
	s.conf.LeaderStateKey = "leaderState"
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
	// 初始化job容器
	s.jobContainer = NewJobContainer()

	// 注册服务(服务发现)
	err := s.initNacos()
	if err != nil {
		glog.Errorf("ApplicationContext/run 注册服务异常,err:%v", err)
		return err
	}

	// 初始化数据库引擎
	err = s.initDatabase()
	if err != nil {
		glog.Errorf("ApplicationContext/run 初始化数据库引擎异常,err:%v", err)
		return err
	}

	// 初始化raft选举机制
	err = s.initRaft()
	if err != nil {
		glog.Errorf("ApplicationContext/run 初始化raft选举异常,err:%v", err)
		return err
	}

	// 监听scheduler服务发现
	err = s.watchSchedulerService()
	if err != nil {
		glog.Errorf("ApplicationContext/run 监听scheduler服务发现异常,err:%v", err)
		return err
	}

	// 监听worker服务发现
	err = s.watchWorkerService()
	if err != nil {
		glog.Errorf("ApplicationContext/run 监听worker服务发现异常,err:%v", err)
		return err
	}

	// 监控raft身份变更并及时处理
	go s.watchRaftLeader()

	// 初始化调度器引擎
	err = s.Scheduler.Init()
	if err != nil {
		glog.Errorf("ApplicationContext/run 初始化调度器异常,err:%v", err)
		return err
	}

	glog.Infof("调度器(%s)已启动,endpoint地址:%s", s.conf.AppName, s.conf.Raft.NodeId)
	// 初始化grpc服务
	err = s.doServe()
	if err != nil {
		glog.Errorf("ApplicationContext/run 监听grpc服务异常,err:%v", err)
		return err
	}
	return nil
}

// 全量拉取worker服务信息然后进行更新worker索引
func (s *ApplicationContext) pullAllWorker() {
	if s.isPullWorker {
		return
	}
	s.isPullWorker = true
	defer func() {
		s.isPullWorker = false
	}()

	list := s.getServiceList(s.conf.WorkerServiceName)
	nodeList := make([]WorkerNode, 0, len(list))
	for _, item := range list {
		node := WorkerNode{
			NodeId:       item.Metadata["nodeId"],
			Endpoint:     fmt.Sprintf("%s:%d", item.Ip, item.Port),
			PluginSet:    strings.Split(item.Metadata["pluginSet"], ","),
			JobNotifySet: strings.Split(item.Metadata["jobNotifySet"], ","),
		}
		// 检测grpc是否可以正常通信
		if !s.checkNodeAlive(node.Endpoint) {
			continue
		}
		nodeList = append(nodeList, node)
	}
	s.Scheduler.BatchUpdateWorkerIndex(nodeList)
	glog.Infof("ApplicationContext/pullAllWorker 当前节点:%s，最新worker列表:%s", s.conf.Raft.NodeId, kit.JsonEncode(s.Scheduler.pluginIndexer.GetAllWorker()))
}

// 检查节点连接是否正常
func (s *ApplicationContext) checkNodeAlive(endpoint string) bool {
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// 获取健康的服务列表
func (s *ApplicationContext) getServiceList(serviceName string) []nacosModel.Instance {
	instanceList, err := s.namingClient.SelectInstances(vo.SelectInstancesParam{
		ServiceName: serviceName,
		HealthyOnly: true,
		Clusters: []string{
			s.conf.Nacos.ClusterName,
		},
		GroupName: s.conf.Nacos.ServiceGroup,
	})
	if err == nil {
		return instanceList
	}

	if strings.Contains(err.Error(), "instance list is empty!") {
		return nil
	}
	glog.Errorf("ApplicationContext/getServiceList 从nacos中获取服务列表(%s)异常,err:%+v", serviceName, err)
	return nil
}

// 初始化raft选举
func (s *ApplicationContext) initRaft() error {
	c := raft.DefaultConfig()
	c.LogLevel = "warn"
	c.Logger = NewLogger(&hclog.LoggerOptions{
		Name:  "raft",
		Level: hclog.LevelFromString(c.LogLevel),
	})

	c.LocalID = raft.ServerID(s.endpointToNodeId(s.conf.Raft.NodeId))

	baseDir := filepath.Join(s.conf.Raft.DataDir, s.endpointToNodeId(s.conf.Raft.NodeId))
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

	s.raft, err = raft.NewRaft(c, nil, ldb, sdb, fss, s.tm.Transport())
	if err != nil {
		return fmt.Errorf("raft.NewRaft: %v", err)
	}

	// 初始化集群
	if isInitCluster, _ := raft.HasExistingState(ldb, sdb, fss); isInitCluster {
		return nil
	}
	serviceList := s.getServiceList(s.conf.AppName)
	for _, item := range serviceList {
		// 如果有raft已创建raft集群则当前节点不再重复创建集群
		if enum.RaftRole(item.Metadata["role"]) == enum.LeaderRaftRole {
			return nil
		}
	}
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(s.endpointToNodeId(s.conf.Raft.NodeId)),
				Address:  raft.ServerAddress(s.conf.Raft.NodeId),
			},
		},
	}
	f := s.raft.BootstrapCluster(cfg)
	if err := f.Error(); err != nil {
		return fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
	}

	// 把节点身份信息更新到nacos服务
	for _, item := range serviceList {
		if item.Metadata["nodeId"] != s.conf.Raft.NodeId {
			continue
		}
		item.Metadata["role"] = string(enum.FollowerRaftRole)
		s.updateNacosInstance(item)
	}
	return nil
}

func (s *ApplicationContext) endpointToNodeId(endpoint string) string {
	return strings.ReplaceAll(strings.ReplaceAll(endpoint, ".", "_"), ":", "_")
}

// nacos服务实例
func (s *ApplicationContext) updateNacosInstance(instance nacosModel.Instance) {
	_, err := s.namingClient.UpdateInstance(vo.UpdateInstanceParam{
		Ip:          instance.Ip,
		Port:        instance.Port,
		ClusterName: instance.ClusterName,
		ServiceName: s.conf.Nacos.AppName,
		GroupName:   s.conf.Nacos.ServiceGroup,
		Ephemeral:   true,
		Weight:      instance.Weight,
		Enable:      instance.Enable,
		Metadata:    instance.Metadata,
	})
	if err != nil {
		glog.Errorf("ApplicationContext/updateNacosInstance 更新raft实例异常,err:%+v", err)
	}
}

// 更新当前service身份
func (s *ApplicationContext) updateCurrentNacosRole(role enum.RaftRole) {
	instance := s.getCurrentNacosInstance()
	if instance == nil {
		return
	}
	instance.Metadata["role"] = string(role)
	s.updateNacosInstance(*instance)
}

func (s *ApplicationContext) getCurrentNacosInstance() *nacosModel.Instance {
	serviceList := s.getServiceList(s.conf.AppName)
	// 把节点身份信息更新到nacos服务
	for _, item := range serviceList {
		if item.Metadata["nodeId"] == s.conf.Raft.NodeId {
			return &item
		}
	}
	return nil
}

// 初始化GRPC服务
func (s *ApplicationContext) initGrpc() {
	maxSize := 50 * 1024 * 1024
	s.tm = transport.New(raft.ServerAddress(s.getListenAddr()), []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxSize),
			grpc.MaxCallSendMsgSize(maxSize),
		),
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

// 动态添加节点
func (s *ApplicationContext) AddPeer(nodeId string) error {
	if _, ok := s.schedulerNodeIds.Load(nodeId); ok {
		return nil
	}
	rs := s.raft.AddVoter(raft.ServerID(s.endpointToNodeId(nodeId)), raft.ServerAddress(nodeId), 0, 5*time.Second)
	if err := rs.Error(); err != nil {
		glog.Errorf("ApplicationContext/AddPeer 当前节点:%s，新增从节点(%s)失败,err:%+v", s.conf.Raft.NodeId, nodeId, err)
		return err
	}
	glog.Infof("ApplicationContext/AddPeer 当前节点:%s，新增从节点(%s)成功", s.conf.Raft.NodeId, nodeId)
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
		glog.Errorf("ApplicationContext/RemovePeer 当前节点:%s，移除从节点(%s)失败,err:%+v", s.conf.Raft.NodeId, nodeId, err)
		return err
	}
	glog.Infof("ApplicationContext/RemovePeer 当前节点:%s，移除从节点(%s)成功", s.conf.Raft.NodeId, nodeId)
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

	s.configClient, err = clients.CreateConfigClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		return fmt.Errorf("当前节点:%s，实例化nacos配置服务客户端异常:%v", s.conf.Raft.NodeId, err)
	}

	s.namingClient, err = clients.CreateNamingClient(map[string]interface{}{
		"serverConfigs": serverConfigs,
		"clientConfig":  clientConfig,
	})
	if err != nil {
		return fmt.Errorf("当前节点:%s，实例化nacos服务发现客户端异常:%v", s.conf.Raft.NodeId, err)
	}
	success, err := s.namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          s.conf.PeerIp,
		Port:        uint64(s.conf.Port),
		ServiceName: s.conf.AppName,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata: map[string]string{
			"appName": s.conf.AppName,
			"nodeId":  s.conf.Raft.NodeId,
			"role":    string(enum.FollowerRaftRole),
		},
		ClusterName: s.conf.Nacos.ClusterName,  // default value is DEFAULT
		GroupName:   s.conf.Nacos.ServiceGroup, // default value is DEFAULT_GROUP
	})
	if err != nil {
		return fmt.Errorf("当前节点:%s，注册服务发现异常:%v", s.conf.Raft.NodeId, err)
	}
	if !success {
		return fmt.Errorf("当前节点:%s，注册服务(%s)失败", s.conf.Raft.NodeId, s.conf.Raft.NodeId)
	}
	return nil
}

// 监听worker服务发现
func (s *ApplicationContext) watchWorkerService() error {
	return s.namingClient.Subscribe(&vo.SubscribeParam{
		ServiceName: s.conf.WorkerServiceName,
		GroupName:   s.conf.Nacos.ServiceGroup,
		Clusters: []string{
			s.conf.Nacos.ClusterName,
		},
		SubscribeCallback: func(services []nacosModel.SubscribeService, nacosErr error) {
			glog.Infof("ApplicationContext/watchWorkerService 当前节点:%s，收到worker服务发现通知:%s,%v", s.conf.Raft.NodeId, kit.JsonEncode(services), nacosErr)
			if !s.isLeader {
				return
			}
			s.pullAllWorker()
		},
	})
}

// 监听scheduler服务发现
func (s *ApplicationContext) watchSchedulerService() error {
	return s.namingClient.Subscribe(&vo.SubscribeParam{
		ServiceName: s.conf.AppName,
		GroupName:   s.conf.Nacos.ServiceGroup,
		Clusters: []string{
			s.conf.Nacos.ClusterName,
		},
		SubscribeCallback: func(services []nacosModel.SubscribeService, nacosErr error) {
			glog.Infof("ApplicationContext/watchSchedulerService 当前节点:%s，收到scheduler服务发现通知:%s,%v", s.conf.Raft.NodeId, kit.JsonEncode(services), nacosErr)
			if !s.isLeader {
				return
			}

			serviceList := s.getServiceList(s.conf.AppName)
			serviceMap := make(map[string]struct{}, len(serviceList))
			for _, item := range serviceList {
				serviceMap[fmt.Sprintf("%s:%d", item.Ip, item.Port)] = struct{}{}
			}

			var delNodeIds []string
			s.schedulerNodeIds.Range(func(key, value interface{}) bool {
				nodeId := key.(string)
				if _, ok := serviceMap[nodeId]; ok {
					return true
				}
				// 当服务不正常则从raft集群移除
				err := s.RemovePeer(s.endpointToNodeId(nodeId))
				if err != nil {
					glog.Errorf("ApplicationContext/watchSchedulerService 当前节点:%s，移除节点信息:%s，移除raft节点失败:%+v", s.conf.Raft.NodeId, nodeId, err)
					return true
				}
				delNodeIds = append(delNodeIds, nodeId)
				glog.Errorf("ApplicationContext/watchSchedulerService 当前节点:%s，移除节点信息成功:%s", s.conf.Raft.NodeId, nodeId)
				return true
			})
			if len(delNodeIds) > 0 {
				for _, v := range delNodeIds {
					s.schedulerNodeIds.Delete(v)
				}
			}

			for _, item := range serviceList {
				nodeId := fmt.Sprintf("%s:%d", item.Ip, item.Port)
				if _, ok := s.schedulerNodeIds.Load(nodeId); ok {
					continue
				}

				err := s.AddPeer(nodeId)
				if err != nil {
					glog.Errorf("ApplicationContext/watchSchedulerService 当前节点:%s，新增节点信息:%s，添加raft节点失败:%+v", s.conf.Raft.NodeId, kit.JsonEncode(item), err)
					continue
				}
				s.schedulerNodeIds.Store(nodeId, struct{}{})
				glog.Errorf("ApplicationContext/watchSchedulerService 当前节点:%s，新增节点信息成功:%s", s.conf.Raft.NodeId, kit.JsonEncode(item))
			}
		},
	})
}

// 获取主节点信息
func (s *ApplicationContext) GetLeaderNode() string {
	serviceList := s.getServiceList(s.conf.AppName)
	for _, v := range serviceList {
		if v.Metadata["role"] == string(enum.LeaderRaftRole) {
			return fmt.Sprintf("%s:%d", v.Ip, v.Port)
		}
	}

	return ""
}

// 应用配置
type Config struct {
	// 应用名称
	AppName string
	// 内部通讯ip
	PeerIp string
	// 应用监听端口号
	Port int
	// 工作节点服务名
	WorkerServiceName string
	// raft leader配置中心key
	LeaderStateKey string

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
	// nacos服务发现的配置参数
	Addrs        []string
	Ports        []int
	Namespace    string
	ServiceGroup string
	ClusterName  string
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

func WithNacosServiceGroupOption(group string) Option {
	return func(conf *Config) {
		conf.Nacos.ServiceGroup = group
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
