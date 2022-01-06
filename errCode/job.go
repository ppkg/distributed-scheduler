package errCode

const (
	ErrPipelineSetEmpty = 100200
)

func init() {
	regErrCode(map[int]string{
		ErrPipelineSetEmpty: "PipelineSet参数不能为空",
	})
}
