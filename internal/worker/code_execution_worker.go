package worker

import "time"

type CodeExecutionWorker struct {

}

type CodeExecutionWorkerPayload struct {
	Language string
	SourceCode string
	Stdin string
	TimeLimitMs int
	MemoryLimitKb int
}

func NewCodeExecutionWorker() *CodeExecutionWorker {
	return &CodeExecutionWorker{}
}

func (_ CodeExecutionWorker) Timeout() time.Duration {
	return 60 * time.Second
}