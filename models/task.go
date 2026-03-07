package models

type TestCase struct {
	ID       string `bson:"_id"`
	Order    int32  `bson:"order"`
	Input    string `bson:"input"`
	Output   string `bson:"output"`
	IsHidden bool   `bson:"is_hidden"`
}

type TestCaseGroup struct {
	ID        string     `bson:"_id"`
	Name      string     `bson:"name"`
	Score     int32      `bson:"score"`
	Order     int32      `bson:"order"`
	TestCases []TestCase `bson:"test_cases"`
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

type File struct {
	Name    string `bson:"name"`
	Content string `bson:"content"`
}

type AllowedRunner struct {
	RunnerID string `bson:"runner_id"`
	Files    []File `bson:"files"`
}

type Solution struct {
	RunnerID string `bson:"runner_id"`
	Files    []File `bson:"files"`
}

type Task struct {
	ID             string          `bson:"_id"`
	AllowedRunners []AllowedRunner `bson:"allowed_runners"`
	Solution       *Solution       `bson:"solution"`
	CompareID      *string         `bson:"compare_id"`
	TestCaseGroups []TestCaseGroup `bson:"test_case_groups"`
	Limit          *Limit          `bson:"limit"`
	ResourceFiles  []File          `bson:"resource_files"`
}

type UpdateTask struct {
	AllowedRunners []AllowedRunner `bson:"allowed_runners"`
	CompareID      *string         `bson:"compare_id"`
	Limit          *Limit          `bson:"limit"`
	Solution       *Solution       `bson:"solution"`
	ResourceFiles  []File          `bson:"resource_files"`
}
