package models

type TestCase struct {
	ID     string `bson:"_id"`
	Input  string `bson:"input"`
	Output string `bson:"output"`
}

type Task struct {
	ID        string     `bson:"_id"`
	Solution  string     `bson:"solution"`
	CompareID string     `bson:"compare_id"`
	TestCases []TestCase `bson:"test_cases"`
}

type UpdateTask struct {
	ID        *string     `bson:"_id"`
	Solution  *string     `bson:"solution"`
	CompareID *string     `bson:"compare_id"`
	TestCases *[]TestCase `bson:"test_cases"`
}
