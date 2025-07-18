package config

import (
	"os"
	"sync"

	"github.com/joho/godotenv"
)

var (
	once           sync.Once
	UrlImageUpload string
	UrlImagePublic string
	FolderName     string
)

func LoadEnv() {
	once.Do(func() {
		_ = godotenv.Load()
		UrlImageUpload = os.Getenv("URL_IMAGE_UPLOAD")
		UrlImagePublic = os.Getenv("URL_IMAGE_PUBLIC")
		FolderName = os.Getenv("FOLDER_IMAGE")
	})
}
