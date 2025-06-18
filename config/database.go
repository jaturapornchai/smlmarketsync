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

// TableExists ตรวจสอบว่าตารางมีอยู่หรือไม่
func TableExists(db *sql.DB, tableName string) bool {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
	`
	var exists bool
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบตาราง %s: %v", tableName, err)
		return false
	}
	return exists
}

// CreateSyncTable สร้างตาราง sml_market_sync สำหรับเก็บข้อมูล sync
func CreateSyncTable(db *sql.DB) error {
	// table_id 1=ic_inventory_price
	// active_code 1=insert, 2=update, 3=delete
	// row_order_ref = roworder จำนวนในตาราง

	query := `
		CREATE TABLE IF NOT EXISTS sml_market_sync (
			id SERIAL PRIMARY KEY,
			table_id INT NOT NULL,
			active_code INT DEFAULT 0,
			row_order_ref INT DEFAULT 0
		)
	`

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้างตาราง sml_market_sync: %v", err)
	}

	return nil
}

// TriggerExists ตรวจสอบว่า trigger สำหรับตารางที่กำหนดมีอยู่หรือไม่
func TriggerExists(db *sql.DB, tableName string) bool {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.triggers 
			WHERE event_object_table = $1
		)
	`
	var exists bool
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ trigger สำหรับตาราง %s: %v", tableName, err)
		return false
	}
	return exists
}

// CreatePriceTrigger สร้าง trigger สำหรับตาราง ic_inventory_price เพื่อติดตามการเปลี่ยนแปลง
func CreatePriceTrigger(db *sql.DB) error {
	// 1. สร้างฟังก์ชัน trigger
	createFunctionQuery := `
		CREATE OR REPLACE FUNCTION log_price_changes()
		RETURNS TRIGGER AS $$
		BEGIN
			IF TG_OP = 'INSERT' THEN
				-- บันทึกข้อมูลการเพิ่มราคา หลัง Insert
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (1, 1, NEW.roworder);
				
			ELSIF TG_OP = 'UPDATE' THEN
				-- บันทึกข้อมูลการอัพเดทราคา
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (1, 2, NEW.roworder);
				
			ELSIF TG_OP = 'DELETE' THEN
				-- บันทึกข้อมูลการลบราคา
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (1, 3, OLD.roworder);
			END IF;
			
			RETURN NULL; 
		END;
		$$ LANGUAGE plpgsql;
	`
	
	_, err := db.Exec(createFunctionQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้างฟังก์ชัน trigger: %v", err)
	}
	
	// 2. สร้าง trigger ที่ใช้ฟังก์ชันข้างต้น
	createTriggerQuery := `
		DROP TRIGGER IF EXISTS price_changes_trigger ON ic_inventory_price;
		CREATE TRIGGER price_changes_trigger
		AFTER INSERT OR UPDATE OR DELETE ON ic_inventory_price
		FOR EACH ROW EXECUTE FUNCTION log_price_changes();
	`
	
	_, err = db.Exec(createTriggerQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้าง trigger: %v", err)
	}
	
	return nil
}
