package models

type TestCase struct {
	Order  int32  `bson:"order"`
	Input  string `bson:"input"`
	Output string `bson:"output"`
}

type Task struct {
	ID               string     `bson:"_id"`
	Solution         string     `bson:"solution"`
	AllowedRunnerIDs []string   `bson:"allowed_runner_ids"`
	CompareID        string     `bson:"compare_id"`
	TestCases        []TestCase `bson:"test_cases"`
}

type UpdateTask struct {
	Solution         *string    `bson:"solution"`
	AllowedRunnerIDs []string   `bson:"allowed_runner_ids"`
	CompareID        *string    `bson:"compare_id"`
	TestCases        []TestCase `bson:"test_cases"`
}
