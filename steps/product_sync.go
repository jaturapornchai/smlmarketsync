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

// ExecuteProductSync รันขั้นตอนที่ 1-3: การ sync สินค้า
func (s *ProductSyncStep) ExecuteProductSync() error {
	// ขั้นตอนที่ 1: เตรียมตาราง barcode
	fmt.Println("=== ขั้นตอนที่ 1: ตรวจสอบตาราง barcode ผ่าน API ===")
	err := s.PrepareInventoryTable()
	if err != nil {
		return fmt.Errorf("error preparing barcode table: %v", err)
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

	// ขั้นตอนที่ 3: Upload ข้อมูลเป็น batch ด้วย UPSERT
	fmt.Println("=== ขั้นตอนที่ 3: Upload และอัพเดทข้อมูลไป ic_inventory_barcode ด้วย UPSERT ===")
	batchSize := 500
	err = s.UploadInventoryItemsBatchViaAPI(inventoryItems, batchSize)
	if err != nil {
		return fmt.Errorf("error uploading inventory items: %v", err)
	}

	fmt.Println("✅ การซิงค์ข้อมูลสินค้าด้วย UPSERT เสร็จสิ้น")
	return nil
}

// PrepareInventoryTable เตรียมตาราง barcode สำหรับสินค้า
func (s *ProductSyncStep) PrepareInventoryTable() error {
	fmt.Println("กำลังตรวจสอบตาราง ic_inventory_barcode ผ่าน API...")
	// ตรวจสอบว่ามีตารางอยู่หรือไม่
	checkQuery := `
		SELECT EXISTS(
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_name = 'ic_inventory_barcode'
		)
	`

	resp, err := s.apiClient.ExecuteSelect(checkQuery)
	if err != nil {
		return fmt.Errorf("error checking table existence: %v", err)
	}

	// ตรวจสอบว่ามีตารางอยู่แล้วหรือไม่
	tableExists := false
	if resp.Success {
		if data, ok := resp.Data.([]interface{}); ok && len(data) > 0 {
			if row, ok := data[0].(map[string]interface{}); ok {
				if exists, ok := row["exists"].(bool); ok {
					tableExists = exists
				}
			}
		}
	}

	// ถ้าไม่มีตาราง ให้สร้างใหม่
	if !tableExists {
		fmt.Println("ไม่พบตาราง ic_inventory_barcode กำลังสร้างตารางใหม่...")
		createQuery := `
			CREATE TABLE ic_inventory_barcode (
				ic_code VARCHAR(50) NOT NULL,
				barcode VARCHAR(100) NOT NULL,
				name VARCHAR(255),
				unit_code VARCHAR(20),
				unit_name VARCHAR(100),
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
				PRIMARY KEY (barcode)
			)
		`

		resp, err = s.apiClient.ExecuteCommand(createQuery)
		if err != nil {
			return fmt.Errorf("error creating table: %v", err)
		}
		if !resp.Success {
			return fmt.Errorf("failed to create table: %s", resp.Message)
		}
		fmt.Println("✅ สร้างตาราง ic_inventory_barcode เรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ พบตาราง ic_inventory_barcode อยู่แล้ว")
	}

	return nil
}

// GetAllInventoryItemsFromSource ดึงข้อมูลสินค้าทั้งหมด
func (s *ProductSyncStep) GetAllInventoryItemsFromSource() ([]interface{}, error) {
	query := `
		SELECT 
			ic_code, 
			barcode,
			coalesce((SELECT name_1 FROM ic_inventory WHERE code=ic_code), 'XX') as name,
			unit_code,
			coalesce((SELECT name_1 FROM ic_unit WHERE code=unit_code), 'XX') as unit_name 
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
		if item.Name == "XX" || item.UnitName == "XX" {
			fmt.Printf("⚠️ รายการที่มีชื่อหรือหน่วยไม่ถูกต้อง: %s\n", item.Barcode)
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

// UploadInventoryItemsBatchViaAPI อัพโหลดข้อมูลแบบ batch ด้วย UPSERT
func (s *ProductSyncStep) UploadInventoryItemsBatchViaAPI(items []interface{}, batchSize int) error {
	totalItems := len(items)
	totalBatches := (totalItems + batchSize - 1) / batchSize

	fmt.Printf("กำลัง upload และอัพเดทข้อมูลสินค้าทั้งหมด %d รายการด้วย UPSERT (batch size: %d)\n", totalItems, batchSize)

	for i := 0; i < totalItems; i += batchSize {
		end := i + batchSize
		if end > totalItems {
			end = totalItems
		}

		batchNum := (i / batchSize) + 1
		fmt.Printf("กำลัง upload และอัพเดทข้อมูล batch %d/%d (รายการ %d-%d) ด้วย UPSERT\n", batchNum, totalBatches, i+1, end)

		err := s.uploadBatch(items[i:end])
		if err != nil {
			return fmt.Errorf("error uploading batch %d: %v", batchNum, err)
		}

		fmt.Printf("✅ UPSERT batch %d สำเร็จ (%d รายการ)\n", batchNum, end-i)
	}

	fmt.Printf("✅ UPSERT ข้อมูลสินค้าทั้งหมด %d รายการเสร็จสิ้น\n", totalItems)
	return nil
}

// uploadBatch อัพโหลดข้อมูล 1 batch ด้วย UPSERT (INSERT ... ON CONFLICT ... DO UPDATE)
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
		INSERT INTO ic_inventory_barcode (ic_code, barcode, name, unit_code, unit_name)
		VALUES %s
		ON CONFLICT (barcode)
		DO UPDATE SET
			ic_code = EXCLUDED.ic_code,
			name = EXCLUDED.name,
			unit_code = EXCLUDED.unit_code,
			unit_name = EXCLUDED.unit_name
		WHERE (
			ic_inventory_barcode.ic_code IS DISTINCT FROM EXCLUDED.ic_code OR
			ic_inventory_barcode.name IS DISTINCT FROM EXCLUDED.name OR
			ic_inventory_barcode.unit_code IS DISTINCT FROM EXCLUDED.unit_code OR
			ic_inventory_barcode.unit_name IS DISTINCT FROM EXCLUDED.unit_name
		)
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

// End of product_sync.go
