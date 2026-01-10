package models

type TestCase struct {
	Order  int32  `bson:"order"`
	Input  string `bson:"input"`
	Output string `bson:"output"`
}

type Limit struct {
	CpuTime      float32 `bson:"cpu_time"`
	CpuExtraTime float32 `bson:"cpu_extra_time"`
	WallTime     float32 `bson:"wall_time"`
	Memory       int32   `bson:"memory"`
	Stack        int32   `bson:"stack"`
	MaxOpenFiles int32   `bson:"max_open_files"`
	MaxFileSize  float32 `bson:"max_file_size"`
	NetworkAllow bool    `bson:"network_allow"`
}

type SolutionFile struct {
	Name    string `bson:"name"`
	Content string `bson:"content"`
}

type Task struct {
	ID               string         `bson:"_id"`
	SolutionFiles    []SolutionFile `bson:"solution_files"`
	SolutionRunnerID string         `bson:"solution_runner_id"`
	AllowedRunnerIDs []string       `bson:"allowed_runner_ids"`
	CompareID        *string        `bson:"compare_id"`
	TestCases        []TestCase     `bson:"testcases"`
	Limit            *Limit         `bson:"limit"`
}

type UpdateTask struct {
	AllowedRunnerIDs []string       `bson:"allowed_runner_ids"`
	CompareID        *string        `bson:"compare_id"`
	Limit            *Limit         `bson:"limit"`
	SolutionRunnerID *string        `bson:"solution_runner_id"`
	SolutionFiles    []SolutionFile `bson:"solution_files"`
}
