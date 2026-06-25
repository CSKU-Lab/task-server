package main

import (
	"testing"

	"github.com/CSKU-Lab/task-service/models"
)

func seg(content, segType string) models.Segment {
	return models.Segment{Content: content, Type: segType}
}

func TestAssembleSolutionContent(t *testing.T) {
	tests := []struct {
		name string
		file models.File
		want string
	}{
		{
			// The reported case: an exclude example line plus a hidden input-reading
			// line. The grader must run the hidden line (reads stdin) and drop the
			// exclude example, so output varies with input.
			name: "drops exclude example, keeps hidden input line",
			file: models.File{
				Name: "main.py",
				Segments: []models.Segment{
					seg("C = 25.0\n", "exclude"),
					seg("C = float(input())\n", "hidden"),
					seg("F = 9 / 5 * C + 32\nK = C + 273.15\nprint(F, K)\n", "readonly"),
				},
			},
			want: "C = float(input())\nF = 9 / 5 * C + 32\nK = C + 273.15\nprint(F, K)\n",
		},
		{
			name: "editable uses author reference content, order preserved",
			file: models.File{
				Name: "main.py",
				Segments: []models.Segment{
					seg("import sys\n", "readonly"),
					seg("ans = solve()\n", "editable"),
					seg("print(ans)\n", "hidden"),
				},
			},
			want: "import sys\nans = solve()\nprint(ans)\n",
		},
		{
			name: "all exclude yields empty",
			file: models.File{
				Name:     "main.py",
				Segments: []models.Segment{seg("x", "exclude"), seg("y", "exclude")},
			},
			want: "",
		},
		{
			name: "no segments falls back to flat content",
			file: models.File{
				Name:    "main.py",
				Content: "print('hi')",
			},
			want: "print('hi')",
		},
		{
			name: "no segments and empty content yields empty",
			file: models.File{Name: "main.py"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := assembleSolutionContent(tt.file)
			if got != tt.want {
				t.Fatalf("assembled mismatch\n got: %q\nwant: %q", got, tt.want)
			}
		})
	}
}
