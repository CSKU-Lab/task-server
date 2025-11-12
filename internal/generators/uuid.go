package generators

import "github.com/google/uuid"

func UUID() string {
	id, err := uuid.NewV7()
	if err != nil {
		panic("failed to generate uuid")
	}

	return id.String()
}
