package errCode

const (
	ErrNonLeaderNode = 100100
)

func init() {
	regErrCode(map[int]string{
		ErrNonLeaderNode: "当前不是leader节点",
	})
}
