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

// PriceTriggerExists ตรวจสอบว่า trigger และ function สำหรับราคาสินค้ามีอยู่หรือไม่
func PriceTriggerExists(db *sql.DB) bool {
	// ตรวจสอบ trigger ที่ชื่อ price_changes_trigger
	triggerQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.triggers 
			WHERE event_object_table = 'ic_inventory_price'
			AND trigger_name = 'price_changes_trigger'
		)
	`
	var triggerExists bool
	err := db.QueryRow(triggerQuery).Scan(&triggerExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ price trigger: %v", err)
		return false
	}

	// ตรวจสอบ function ที่ชื่อ log_price_changes
	functionQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.routines 
			WHERE routine_type = 'FUNCTION'
			AND routine_name = 'log_price_changes'
		)
	`
	var functionExists bool
	err = db.QueryRow(functionQuery).Scan(&functionExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ price function: %v", err)
		return false
	}

	return triggerExists && functionExists
}

// InventoryTriggerExists ตรวจสอบว่า trigger และ function สำหรับสินค้ามีอยู่หรือไม่
func InventoryTriggerExists(db *sql.DB) bool {
	// ตรวจสอบ trigger ที่ชื่อ inventory_changes_trigger
	triggerQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.triggers 
			WHERE event_object_table = 'ic_inventory'
			AND trigger_name = 'inventory_changes_trigger'
		)
	`
	var triggerExists bool
	err := db.QueryRow(triggerQuery).Scan(&triggerExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ inventory trigger: %v", err)
		return false
	}

	// ตรวจสอบ function ที่ชื่อ log_inventory_changes
	functionQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.routines 
			WHERE routine_type = 'FUNCTION'
			AND routine_name = 'log_inventory_changes'
		)
	`
	var functionExists bool
	err = db.QueryRow(functionQuery).Scan(&functionExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ inventory function: %v", err)
		return false
	}

	return triggerExists && functionExists
}

// InventoryBarcodeTriggerExists ตรวจสอบว่า trigger และ function สำหรับ ic_inventory_barcode มีอยู่หรือไม่
func InventoryBarcodeTriggerExists(db *sql.DB) bool {
	// ตรวจสอบ trigger ที่ชื่อ inventory_barcode_changes_trigger
	triggerQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.triggers 
			WHERE event_object_table = 'ic_inventory_barcode'
			AND trigger_name = 'inventory_barcode_changes_trigger'
		)
	`
	var triggerExists bool
	err := db.QueryRow(triggerQuery).Scan(&triggerExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ inventory barcode trigger: %v", err)
		return false
	}

	// ตรวจสอบ function ที่ชื่อ log_inventory_barcode_changes
	functionQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.routines 
			WHERE routine_type = 'FUNCTION'
			AND routine_name = 'log_inventory_barcode_changes'
		)
	`
	var functionExists bool
	err = db.QueryRow(functionQuery).Scan(&functionExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ inventory barcode function: %v", err)
		return false
	}

	return triggerExists && functionExists
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

// CreateInventoryTrigger สร้าง trigger สำหรับตาราง ic_inventory_barcode เพื่อติดตามการเปลี่ยนแปลง
func CreateInventoryTrigger(db *sql.DB) error {
	// 1. สร้างฟังก์ชัน trigger
	createFunctionQuery := `
		CREATE OR REPLACE FUNCTION log_inventory_changes()
		RETURNS TRIGGER AS $$
		BEGIN
			IF TG_OP = 'INSERT' THEN
				-- บันทึกข้อมูลการเพิ่มสินค้า หลัง Insert
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (2, 1, NEW.roworder);
				
			ELSIF TG_OP = 'UPDATE' THEN
				-- บันทึกข้อมูลการอัพเดทสินค้า
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (2, 2, NEW.roworder);

			ELSIF TG_OP = 'DELETE' THEN
				-- บันทึกข้อมูลการลบสินค้า
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (2, 3, OLD.roworder);
			END IF;
			
			RETURN NULL; 
		END;
		$$ LANGUAGE plpgsql;
	`

	_, err := db.Exec(createFunctionQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้างฟังก์ชัน inventory trigger: %v", err)
	}
	// 2. สร้าง trigger ที่ใช้ฟังก์ชันข้างต้น
	createTriggerQuery := `
		DROP TRIGGER IF EXISTS inventory_changes_trigger ON ic_inventory;
		CREATE TRIGGER inventory_changes_trigger
		AFTER INSERT OR UPDATE OR DELETE ON ic_inventory
		FOR EACH ROW EXECUTE FUNCTION log_inventory_changes();
	`

	_, err = db.Exec(createTriggerQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้าง inventory trigger: %v", err)
	}

	return nil
}

// CreateInventoryBarcodeTrigger สร้าง trigger สำหรับตาราง ic_inventory_barcode เพื่อติดตามการเปลี่ยนแปลง
func CreateInventoryBarcodeTrigger(db *sql.DB) error {
	// 1. สร้างฟังก์ชัน trigger
	createFunctionQuery := `
		CREATE OR REPLACE FUNCTION log_inventory_barcode_changes()
		RETURNS TRIGGER AS $$
		BEGIN
			IF TG_OP = 'INSERT' THEN
				-- บันทึกข้อมูลการเพิ่มข้อมูลบาร์โค้ด หลัง Insert
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (3, 1, NEW.roworder);
				
			ELSIF TG_OP = 'UPDATE' THEN
				-- บันทึกข้อมูลการอัพเดทข้อมูลบาร์โค้ด
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (3, 2, NEW.roworder);
				
			ELSIF TG_OP = 'DELETE' THEN
				-- บันทึกข้อมูลการลบข้อมูลบาร์โค้ด
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (3, 3, OLD.roworder);
			END IF;
			
			RETURN NULL; 
		END;
		$$ LANGUAGE plpgsql;
	`

	_, err := db.Exec(createFunctionQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้างฟังก์ชัน inventory barcode trigger: %v", err)
	}

	// 2. สร้าง trigger ที่ใช้ฟังก์ชันข้างต้น
	createTriggerQuery := `
		DROP TRIGGER IF EXISTS inventory_barcode_changes_trigger ON ic_inventory_barcode;
		CREATE TRIGGER inventory_barcode_changes_trigger
		AFTER INSERT OR UPDATE OR DELETE ON ic_inventory_barcode
		FOR EACH ROW EXECUTE FUNCTION log_inventory_barcode_changes();
	`

	_, err = db.Exec(createTriggerQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้าง inventory barcode trigger: %v", err)
	}

	return nil
}

// CustomerTriggerExists ตรวจสอบว่า trigger และ function สำหรับ ar_customer มีอยู่หรือไม่
func CustomerTriggerExists(db *sql.DB) bool {
	// ตรวจสอบ trigger ที่ชื่อ customer_changes_trigger
	triggerQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.triggers 
			WHERE event_object_table = 'ar_customer'
			AND trigger_name = 'customer_changes_trigger'
		)
	`
	var triggerExists bool
	err := db.QueryRow(triggerQuery).Scan(&triggerExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ customer trigger: %v", err)
		return false
	}

	// ตรวจสอบ function ที่ชื่อ log_customer_changes
	functionQuery := `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.routines 
			WHERE routine_type = 'FUNCTION'
			AND routine_name = 'log_customer_changes'
		)
	`
	var functionExists bool
	err = db.QueryRow(functionQuery).Scan(&functionExists)
	if err != nil {
		log.Printf("❌ เกิดข้อผิดพลาดในการตรวจสอบ customer function: %v", err)
		return false
	}

	return triggerExists && functionExists
}

// CreateCustomerTrigger สร้าง trigger สำหรับตาราง ar_customer เพื่อติดตามการเปลี่ยนแปลง
func CreateCustomerTrigger(db *sql.DB) error {
	// 1. สร้างฟังก์ชัน trigger
	createFunctionQuery := `
		CREATE OR REPLACE FUNCTION log_customer_changes()
		RETURNS TRIGGER AS $$
		BEGIN
			IF TG_OP = 'INSERT' THEN
				-- บันทึกข้อมูลการเพิ่มลูกค้า หลัง Insert
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (4, 1, NEW.roworder);
				
			ELSIF TG_OP = 'UPDATE' THEN
				-- บันทึกข้อมูลการอัพเดทลูกค้า
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (4, 2, NEW.roworder);
				
			ELSIF TG_OP = 'DELETE' THEN
				-- บันทึกข้อมูลการลบลูกค้า
				INSERT INTO sml_market_sync (table_id, active_code, row_order_ref)
				VALUES (4, 3, OLD.roworder);
			END IF;
			
			RETURN NULL; 
		END;
		$$ LANGUAGE plpgsql;
	`

	_, err := db.Exec(createFunctionQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้างฟังก์ชัน customer trigger: %v", err)
	}

	// 2. สร้าง trigger ที่ใช้ฟังก์ชันข้างต้น
	createTriggerQuery := `
		DROP TRIGGER IF EXISTS customer_changes_trigger ON ar_customer;
		CREATE TRIGGER customer_changes_trigger
		AFTER INSERT OR UPDATE OR DELETE ON ar_customer
		FOR EACH ROW EXECUTE FUNCTION log_customer_changes();
	`

	_, err = db.Exec(createTriggerQuery)
	if err != nil {
		return fmt.Errorf("ไม่สามารถสร้าง customer trigger: %v", err)
	}

	return nil
}
