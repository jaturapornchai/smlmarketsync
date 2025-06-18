package config

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	_ "github.com/lib/pq"
)

type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
}

type Config struct {
	Database DatabaseConfig `json:"database"`
}

func NewDatabaseConfig() *DatabaseConfig {
	// อ่านไฟล์ smlmarketsync.json
	configPath := "smlmarketsync.json"
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		log.Fatalf("❌ Error: ไม่สามารถอ่านไฟล์ %s: %v\nโปรแกรมจบการทำงาน", configPath, err)
	}

	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("❌ Error: ไม่สามารถแปลงไฟล์ JSON: %v\nโปรแกรมจบการทำงาน", err)
	}

	log.Printf("✅ โหลดการตั้งค่าจาก smlmarketsync.json สำเร็จ: %s:%d", config.Database.Host, config.Database.Port)
	return &config.Database
}

func (config *DatabaseConfig) Connect() (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Password, config.DBName)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %v", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %v", err)
	}

	log.Println("Successfully connected to PostgreSQL database!")
	return db, nil
}
