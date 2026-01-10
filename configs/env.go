package configs

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type env map[string]string

func NewEnv() *env {
	if os.Getenv("ENV") == "" {
		log.Fatalln("You forget to set the ENV environment variable!")
	}

	if os.Getenv("ENV") != "docker" {
		log.Println("Loading .env file...")

		err := godotenv.Load()
		if err != nil {
			log.Fatalln("Error loading .env file")
		}
	}

	return &env{
		"MONGO_URI":         os.Getenv("MONGO_URI"),
		"PORT":              os.Getenv("PORT"),
		"DATABASE_NAME":     os.Getenv("DATABASE_NAME"),
		"GRADER_SERVER_URL": os.Getenv("GRADER_SERVER_URL"),
	}
}

func (m *env) Get(key string) string {
	val, exists := (*m)[key]
	if !exists {
		log.Fatalf("Environment variable %s not found!", key)
	}
	return val
}
