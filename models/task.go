package models

type TestCase struct {
	ID     string `bson:"_id"`
	Input  string `bson:"input"`
	Output string `bson:"output"`
}

type Task struct {
	ID        string     `bson:"_id"`
	TestCases []TestCase `bson:"test_cases"`
}
