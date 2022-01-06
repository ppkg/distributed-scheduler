package errCode

const (
	ErrPipelineSetEmpty = 100200
	ErrJobNotFound      = 100201
)

func init() {
	regErrCode(map[int]string{
		ErrPipelineSetEmpty: "PipelineSet参数不能为空",
		ErrJobNotFound:      "Job不存在",
	})
}
