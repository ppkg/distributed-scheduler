package errCode

const (
	ErrPipelineSetEmpty  = 100200
	ErrPipelineUnsupport = 100201

	ErrJobNotFound = 100210
)

func init() {
	regErrCode(map[int]string{
		ErrPipelineSetEmpty:  "PipelineSet参数不能为空",
		ErrPipelineUnsupport: "Pipeline管道(%s)系统不支持",

		ErrJobNotFound: "Job不存在",
	})
}
