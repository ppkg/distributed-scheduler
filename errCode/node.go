package errCode

const (
	ErrNonMasterNode = 100100
)

func init() {
	regErrCode(map[int]string{
		ErrNonMasterNode: "当前不是master节点",
	})
}
