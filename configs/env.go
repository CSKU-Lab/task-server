package configs

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type env struct {
	mongoUri string
	port     string
}

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
		mongoUri: os.Getenv("MONGO_URI"),
		port:     os.Getenv("PORT"),
	}
}

func (m *env) GetMongoURI() string {
	if m.mongoUri == "" {
		log.Fatalln("You forget to set the MONGO_URI environment variable!")
	}
	return m.mongoUri
}

func (m *env) GetPort() string {
	if m.port == "" {
		log.Fatalln("You forget to set the PORT environment variable!")
	}
	return m.port
}
