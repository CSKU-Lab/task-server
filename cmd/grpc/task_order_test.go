package main

import (
	"testing"

	"github.com/CSKU-Lab/task-service/models"
)

// taskModelToPB must emit groups and their test cases sorted by the persisted
// Order field, regardless of the stored array order. Older writes appended
// groups in goroutine-completion order, so the stored slice cannot be trusted.
func TestTaskModelToPBSortsByOrder(t *testing.T) {
	task := &models.Task{
		ID: "task-1",
		TestCaseGroups: []models.TestCaseGroup{
			{
				ID:    "g-second",
				Order: 1,
				TestCases: []models.TestCase{
					{ID: "b-1", Order: 1, Input: "785"},
					{ID: "b-0", Order: 0, Input: "1678"},
				},
			},
			{
				ID:    "g-first",
				Order: 0,
				TestCases: []models.TestCase{
					{ID: "a-1", Order: 1, Input: "60"},
					{ID: "a-0", Order: 0, Input: "58"},
				},
			},
		},
	}

	pb := taskModelToPB(task)

	if len(pb.GetTestCaseGroups()) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(pb.GetTestCaseGroups()))
	}

	wantGroups := []string{"g-first", "g-second"}
	for i, want := range wantGroups {
		if got := pb.GetTestCaseGroups()[i].GetId(); got != want {
			t.Errorf("group[%d] id = %q, want %q (groups not sorted by Order)", i, got, want)
		}
	}

	// First group's cases must come back ascending by Order: 58 (order 0), 60 (order 1).
	firstCases := pb.GetTestCaseGroups()[0].GetTestCases()
	wantInputs := []string{"58", "60"}
	if len(firstCases) != len(wantInputs) {
		t.Fatalf("expected %d cases in first group, got %d", len(wantInputs), len(firstCases))
	}
	for i, want := range wantInputs {
		if got := firstCases[i].GetInput(); got != want {
			t.Errorf("case[%d] input = %q, want %q (cases not sorted by Order)", i, got, want)
		}
	}

	// The input slice must not be mutated by the sort (taskModelToPB copies first).
	if task.TestCaseGroups[0].ID != "g-second" {
		t.Errorf("input task was mutated: group[0] = %q, want %q", task.TestCaseGroups[0].ID, "g-second")
	}
}
