package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"strings"
)

type ProductSyncStep struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewProductSyncStep(db *sql.DB) *ProductSyncStep {
	return &ProductSyncStep{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// ExecuteProductSync รันขั้นตอนที่ 1-4: การ sync สินค้า
func (s *ProductSyncStep) ExecuteProductSync() error {
	// ขั้นตอนที่ 1: เตรียมตาราง ic_inventory_barcode_temp
	fmt.Println("=== ขั้นตอนที่ 1: เตรียมตาราง ic_inventory_barcode_temp ผ่าน API ===")
	err := s.PrepareInventoryTempTableViaAPI()
	if err != nil {
		return fmt.Errorf("error preparing temp table: %v", err)
	}

	// ขั้นตอนที่ 2: ดึงข้อมูลสินค้าทั้งหมด
	fmt.Println("=== ขั้นตอนที่ 2: ดึงข้อมูลสินค้าทั้งหมดจากฐานข้อมูลต้นทาง ===")
	inventoryItems, err := s.GetAllInventoryItemsFromSource()
	if err != nil {
		return fmt.Errorf("error getting inventory items: %v", err)
	}

	if len(inventoryItems) == 0 {
		fmt.Println("ไม่มีข้อมูลสินค้าในฐานข้อมูลต้นทาง")
		return nil
	}

	fmt.Printf("พบข้อมูลสินค้าทั้งหมด %d รายการ\n", len(inventoryItems))

	// ขั้นตอนที่ 3: Upload ข้อมูลเป็น batch
	fmt.Println("=== ขั้นตอนที่ 3: Upload ข้อมูลไป ic_inventory_barcode_temp ผ่าน API ===")
	batchSize := 500
	err = s.UploadInventoryItemsBatchViaAPI(inventoryItems, batchSize)
	if err != nil {
		return fmt.Errorf("error uploading inventory items: %v", err)
	}

	// ขั้นตอนที่ 4: ซิงค์ข้อมูลกับตาราง ic_inventory_barcode
	fmt.Println("=== ขั้นตอนที่ 4: ซิงค์ข้อมูลกับตาราง ic_inventory_barcode ===")
	err = s.SyncInventoryData()
	if err != nil {
		return fmt.Errorf("error syncing inventory data: %v", err)
	}

	return nil
}

// PrepareInventoryTempTableViaAPI เตรียมตาราง temp สำหรับสินค้า
func (s *ProductSyncStep) PrepareInventoryTempTableViaAPI() error {
	fmt.Println("กำลังตรวจสอบและเตรียมตาราง ic_inventory_barcode_temp ผ่าน API...")

	// ตรวจสอบว่ามีตารางอยู่หรือไม่
	checkQuery := `
		SELECT COUNT(*) 
		FROM information_schema.tables 
		WHERE table_name = 'ic_inventory_barcode_temp'
	`

	resp, err := s.apiClient.ExecuteSelect(checkQuery)
	if err != nil {
		return fmt.Errorf("error checking temp table existence: %v", err)
	}

	// หากมีตารางอยู่แล้ว ให้ลบออก
	if resp.Success {
		fmt.Println("พบตาราง ic_inventory_barcode_temp อยู่แล้ว กำลัง drop ผ่าน API...")
		dropQuery := "DROP TABLE IF EXISTS ic_inventory_barcode_temp"
		resp, err := s.apiClient.ExecuteCommand(dropQuery)
		if err != nil {
			return fmt.Errorf("error dropping temp table: %v", err)
		}
		if !resp.Success {
			return fmt.Errorf("failed to drop temp table: %s", resp.Message)
		}
		fmt.Println("✅ ลบตาราง ic_inventory_barcode_temp เรียบร้อยแล้ว (ผ่าน API)")
	}

	// สร้างตารางใหม่
	fmt.Println("กำลังสร้างตาราง ic_inventory_barcode_temp ใหม่ผ่าน API...")
	createQuery := `
		CREATE TABLE ic_inventory_barcode_temp (
			ic_code VARCHAR(50) NOT NULL,
			barcode VARCHAR(100) NOT NULL,
			name VARCHAR(255),
			unit_code VARCHAR(20),
			unit_name VARCHAR(100),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (barcode)
		)
	`

	resp, err = s.apiClient.ExecuteCommand(createQuery)
	if err != nil {
		return fmt.Errorf("error creating temp table: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to create temp table: %s", resp.Message)
	}

	fmt.Println("✅ สร้างตาราง ic_inventory_barcode_temp เรียบร้อยแล้ว (ผ่าน API)")
	return nil
}

// GetAllInventoryItemsFromSource ดึงข้อมูลสินค้าทั้งหมด
func (s *ProductSyncStep) GetAllInventoryItemsFromSource() ([]interface{}, error) {
	query := `
		SELECT 
			ic_code, 
			barcode,
			(SELECT name_1 FROM ic_inventory WHERE code=ic_code) as name,
			unit_code,
			(SELECT name_1 FROM ic_unit WHERE code=unit_code) as unit_name 
		FROM ic_inventory_barcode
		WHERE barcode IS NOT NULL AND barcode != ''
		ORDER BY barcode
	`

	fmt.Println("กำลังดึงข้อมูลสินค้าจาก ic_inventory_barcode...")
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing inventory query: %v", err)
	}
	defer rows.Close()

	var items []interface{}
	count := 0

	for rows.Next() {
		var item types.InventoryItem
		err := rows.Scan(
			&item.IcCode,
			&item.Barcode,
			&item.Name,
			&item.UnitCode,
			&item.UnitName,
		)
		if err != nil {
			fmt.Printf("⚠️ ข้ามรายการที่อ่านไม่ได้: %v\n", err)
			continue
		}

		// แปลงเป็น map สำหรับ API
		itemMap := map[string]interface{}{
			"ic_code":   item.IcCode,
			"barcode":   item.Barcode,
			"name":      item.Name,
			"unit_code": item.UnitCode,
			"unit_name": item.UnitName,
		}

		items = append(items, itemMap)
		count++

		// แสดงความคืบหน้าทุกๆ 5000 รายการ
		if count%5000 == 0 {
			fmt.Printf("ดึงข้อมูลสินค้าแล้ว %d รายการ...\n", count)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating inventory rows: %v", err)
	}

	fmt.Printf("ดึงข้อมูลสินค้าจากฐานข้อมูลต้นทางได้ %d รายการ\n", count)
	return items, nil
}

// UploadInventoryItemsBatchViaAPI อัพโหลดข้อมูลแบบ batch
func (s *ProductSyncStep) UploadInventoryItemsBatchViaAPI(items []interface{}, batchSize int) error {
	totalItems := len(items)
	totalBatches := (totalItems + batchSize - 1) / batchSize

	fmt.Printf("กำลัง upload ข้อมูลสินค้าทั้งหมด %d รายการผ่าน API (batch size: %d)\n", totalItems, batchSize)

	for i := 0; i < totalItems; i += batchSize {
		end := i + batchSize
		if end > totalItems {
			end = totalItems
		}

		batchNum := (i / batchSize) + 1
		fmt.Printf("กำลัง upload batch %d/%d (รายการ %d-%d) ผ่าน API\n", batchNum, totalBatches, i+1, end)

		err := s.uploadBatch(items[i:end])
		if err != nil {
			return fmt.Errorf("error uploading batch %d: %v", batchNum, err)
		}

		fmt.Printf("✅ Upload batch %d สำเร็จ (%d รายการ)\n", batchNum, end-i)
	}

	fmt.Printf("✅ Upload ข้อมูลสินค้าทั้งหมด %d รายการเสร็จสิ้น (ผ่าน API)\n", totalItems)
	return nil
}

// uploadBatch อัพโหลดข้อมูล 1 batch
func (s *ProductSyncStep) uploadBatch(batch []interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	var values []string
	for _, item := range batch {
		if itemMap, ok := item.(map[string]interface{}); ok {
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			barcode := fmt.Sprintf("%v", itemMap["barcode"])
			name := fmt.Sprintf("%v", itemMap["name"])
			unitCode := fmt.Sprintf("%v", itemMap["unit_code"])
			unitName := fmt.Sprintf("%v", itemMap["unit_name"])

			// Escape single quotes
			name = strings.ReplaceAll(name, "'", "''")
			unitName = strings.ReplaceAll(unitName, "'", "''")

			value := fmt.Sprintf("('%s', '%s', '%s', '%s', '%s')",
				icCode, barcode, name, unitCode, unitName)
			values = append(values, value)
		}
	}

	if len(values) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
		INSERT INTO ic_inventory_barcode_temp (ic_code, barcode, name, unit_code, unit_name)
		VALUES %s
	`, strings.Join(values, ","))

	resp, err := s.apiClient.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error executing batch insert: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("batch insert failed: %s", resp.Message)
	}

	return nil
}

// SyncInventoryData ซิงค์ข้อมูลระหว่าง temp และ main table
func (s *ProductSyncStep) SyncInventoryData() error {
	// ตรวจสอบและสร้างตาราง ic_inventory_barcode
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory_barcode...")
	err := s.apiClient.CreateInventoryTable()
	if err != nil {
		return fmt.Errorf("error creating inventory table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory_barcode เรียบร้อยแล้ว")

	// เปรียบเทียบและซิงค์ข้อมูล
	fmt.Println("กำลังเปรียบเทียบและซิงค์ข้อมูลระหว่าง 2 ตาราง...")
	insertCount, updateCount, err := s.apiClient.SyncInventoryTableData()
	if err != nil {
		return fmt.Errorf("error syncing inventory data: %v", err)
	}

	fmt.Println("✅ ซิงค์ข้อมูลระหว่าง temp table และ main table เรียบร้อยแล้ว")
	fmt.Printf("📊 สถิติการซิงค์:\n")
	fmt.Printf("   - ข้อมูลใน temp table: %d รายการ\n", insertCount+updateCount)
	fmt.Printf("   - ข้อมูล active ใน main table: %d รายการ\n", insertCount)
	fmt.Printf("   - ข้อมูล inactive ใน main table: %d รายการ\n", updateCount)

	return nil
}
