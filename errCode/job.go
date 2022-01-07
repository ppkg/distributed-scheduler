package errCode

const (
	ErrPluginSetEmpty  = 100200
	ErrPluginUnsupport = 100201

	ErrJobNotFound = 100210
)

func init() {
	regErrCode(map[int]string{
		ErrPluginSetEmpty:  "PluginSet参数不能为空",
		ErrPluginUnsupport: "Plugin插件(%s)系统不支持",

		ErrJobNotFound: "Job不存在",
	})
}
