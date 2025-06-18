package config

import (
	"fmt"
	"strings"
	"time"
)

// CreatePriceTable สร้างตาราง ic_inventory_price
func (api *APIClient) CreatePriceTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS ic_inventory_price (
			id SERIAL PRIMARY KEY,
			ic_code VARCHAR(50) NOT NULL,
			unit_code VARCHAR(20),
			from_qty DECIMAL(15,6) DEFAULT 0,
			to_qty DECIMAL(15,6) DEFAULT 0,
			from_date DATE,
			to_date DATE,
			sale_type VARCHAR(20),
			sale_price1 DECIMAL(15,6) DEFAULT 0,
			status VARCHAR(20) DEFAULT 'active',
			price_type VARCHAR(20),			cust_code VARCHAR(50),
			sale_price2 DECIMAL(15,6) DEFAULT 0,
			cust_group_1 VARCHAR(50),
			price_mode VARCHAR(20),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(ic_code, unit_code, from_qty, cust_code, price_type)
		)
	`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error creating price table: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to create price table: %s", resp.Message)
	}

	return nil
}

// GetExistingPriceData ดึงข้อมูลราคาสินค้าที่มีอยู่จาก API
func (api *APIClient) GetExistingPriceData() (map[string]map[string]interface{}, error) {
	query := "SELECT ic_code, unit_code, from_qty, sale_price1 FROM ic_inventory_price"

	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return nil, fmt.Errorf("error fetching existing price data: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to fetch existing price data: %s", resp.Message)
	}

	priceMap := make(map[string]map[string]interface{})

	// แปลง response data เป็น slice of map
	if data, ok := resp.Data.([]interface{}); ok {
		for _, row := range data {
			if rowMap, ok := row.(map[string]interface{}); ok {
				icCode := fmt.Sprintf("%v", rowMap["ic_code"])
				unitCode := fmt.Sprintf("%v", rowMap["unit_code"])
				fromQty := fmt.Sprintf("%v", rowMap["from_qty"])

				// สร้าง composite key
				key := fmt.Sprintf("%s|%s|%s", icCode, unitCode, fromQty)
				priceMap[key] = rowMap
			}
		}
	}

	return priceMap, nil
}

// SyncPriceData ซิงค์ข้อมูลราคาสินค้าแบบ batch UPSERT (หยุดทันทีที่พบข้อผิดพลาด)
func (api *APIClient) SyncPriceData(localData []interface{}, existingData map[string]map[string]interface{}) (int, int, error) {
	if len(localData) == 0 {
		return 0, 0, nil
	}

	fmt.Printf("🚀 เริ่มส่งข้อมูลราคาสินค้าทั้งหมด %d รายการแบบ batch UPSERT\n", len(localData))

	// เตรียมข้อมูลสำหรับ batch
	var batchValues []string
	validCount := 0
	skipCount := 0

	// Debug: แสดงตัวอย่างข้อมูลแรกๆ
	debugCount := 0
	for _, item := range localData {
		if itemMap, ok := item.(map[string]interface{}); ok && debugCount < 3 {
			debugCount++
			fmt.Printf("🔍 Debug #%d: ic_code='%v', unit_code='%v', from_qty=%v, sale_price1=%v\n",
				debugCount, itemMap["ic_code"], itemMap["unit_code"], itemMap["from_qty"], itemMap["sale_price1"])
		}
	}

	for i, item := range localData {
		if itemMap, ok := item.(map[string]interface{}); ok {
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			unitCode := fmt.Sprintf("%v", itemMap["unit_code"])

			// ตรวจสอบข้อมูลจำเป็น
			if icCode == "" || icCode == "<nil>" {
				skipCount++
				continue
			} // แปลงข้อมูล
			fromQty := parseFloatValue(itemMap["from_qty"])
			toQty := parseFloatValue(itemMap["to_qty"])
			salePrice1 := parseFloatValue(itemMap["sale_price1"])
			salePrice2 := parseFloatValue(itemMap["sale_price2"])

			fromDate := parseStringValue(itemMap["from_date"])
			toDate := parseStringValue(itemMap["to_date"])
			saleType := parseStringValue(itemMap["sale_type"])
			status := parseStringValue(itemMap["status"])
			priceType := parseStringValue(itemMap["price_type"])
			custCode := parseStringValue(itemMap["cust_code"])
			custGroup1 := parseStringValue(itemMap["cust_group_1"])
			priceMode := parseStringValue(itemMap["price_mode"])

			// Escape single quotes
			icCode = strings.ReplaceAll(icCode, "'", "''")
			unitCode = strings.ReplaceAll(unitCode, "'", "''")
			saleType = strings.ReplaceAll(saleType, "'", "''")
			status = strings.ReplaceAll(status, "'", "''")
			priceType = strings.ReplaceAll(priceType, "'", "''")
			custCode = strings.ReplaceAll(custCode, "'", "''")
			custGroup1 = strings.ReplaceAll(custGroup1, "'", "''")
			priceMode = strings.ReplaceAll(priceMode, "'", "''")

			value := fmt.Sprintf("('%s', '%s', %s, %s, %s, %s, '%s', %s, '%s', '%s', '%s', %s, '%s', '%s')",
				icCode, unitCode, fromQty, toQty,
				nullableDate(fromDate), nullableDate(toDate),
				saleType, salePrice1, status, priceType, custCode,
				salePrice2, custGroup1, priceMode)

			batchValues = append(batchValues, value)
			validCount++
		}

		// แสดงความคืบหน้า
		if (i+1)%2000 == 0 {
			fmt.Printf("⏳ เตรียมข้อมูลแล้ว %d/%d รายการ\n", i+1, len(localData))
		}
	}

	fmt.Printf("📦 เตรียมข้อมูลเสร็จ: %d รายการที่ใช้ได้, ข้าม %d รายการ\n", validCount, skipCount)

	if len(batchValues) == 0 {
		return 0, 0, nil
	}

	// ส่งข้อมูลแบบ batch
	batchSize := 50
	totalBatches := (len(batchValues) + batchSize - 1) / batchSize
	successCount := 0

	fmt.Printf("🚀 กำลังส่งข้อมูล %d รายการแบบ batch UPSERT (ครั้งละ %d รายการ)\n", len(batchValues), batchSize)

	for i := 0; i < len(batchValues); i += batchSize {
		end := i + batchSize
		if end > len(batchValues) {
			end = len(batchValues)
		}

		batchNum := (i / batchSize) + 1
		currentBatchSize := end - i

		fmt.Printf("   📥 กำลังส่ง batch %d/%d (%d รายการ)...\n", batchNum, totalBatches, currentBatchSize)
		err := api.executeBatchUpsertPrice(batchValues[i:end])
		if err != nil {
			fmt.Printf("❌ Batch %d ล้มเหลว: %v\n", batchNum, err)
			// ถ้าพบข้อผิดพลาดให้หยุดทันที
			return successCount, 0, fmt.Errorf("batch %d ล้มเหลว (ยกเลิกทั้งหมด): %v", batchNum, err)
		} else {
			successCount += currentBatchSize
			fmt.Printf("✅ Batch %d สำเร็จ (%d รายการ)\n", batchNum, currentBatchSize)
		}

		if batchNum < totalBatches {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("\n📊 สรุปการซิงค์ราคาสินค้า:\n")
	fmt.Printf("   - ส่งสำเร็จ: %d รายการ\n", successCount)
	fmt.Printf("   - ข้ามเนื่องจากข้อมูลไม่ครบ: %d รายการ\n", skipCount)
	fmt.Printf("   - ล้มเหลว: %d รายการ\n", validCount-successCount)

	return successCount, 0, nil
}

// executeBatchUpsertPrice ทำ batch UPSERT สำหรับราคาสินค้า (ไม่มี retry, ถ้าล้มเหลวจะหยุดทันที)
func (api *APIClient) executeBatchUpsertPrice(values []string) error {
	if len(values) == 0 {
		return nil
	}
	query := fmt.Sprintf(`
		INSERT INTO ic_inventory_price (ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
										sale_type, sale_price1, status, price_type, cust_code, 
										sale_price2, cust_group_1, price_mode)
		VALUES %s
		ON CONFLICT (ic_code, unit_code, from_qty, cust_code, price_type) 
		DO UPDATE SET 
			to_qty = EXCLUDED.to_qty,
			from_date = EXCLUDED.from_date,
			to_date = EXCLUDED.to_date,
			sale_type = EXCLUDED.sale_type,
			sale_price1 = EXCLUDED.sale_price1,
			status = EXCLUDED.status,
			sale_price2 = EXCLUDED.sale_price2,
			cust_group_1 = EXCLUDED.cust_group_1,
			price_mode = EXCLUDED.price_mode,
			updated_at = CURRENT_TIMESTAMP
		WHERE ic_inventory_price.to_qty IS DISTINCT FROM EXCLUDED.to_qty
		   OR ic_inventory_price.sale_price1 IS DISTINCT FROM EXCLUDED.sale_price1
		   OR ic_inventory_price.sale_price2 IS DISTINCT FROM EXCLUDED.sale_price2
		   OR ic_inventory_price.status IS DISTINCT FROM EXCLUDED.status
		   OR ic_inventory_price.from_date IS DISTINCT FROM EXCLUDED.from_date
		   OR ic_inventory_price.to_date IS DISTINCT FROM EXCLUDED.to_date
		   OR ic_inventory_price.sale_type IS DISTINCT FROM EXCLUDED.sale_type
		   OR ic_inventory_price.cust_group_1 IS DISTINCT FROM EXCLUDED.cust_group_1
		   OR ic_inventory_price.price_mode IS DISTINCT FROM EXCLUDED.price_mode;`,
		strings.Join(values, ",")) // ทำเพียงครั้งเดียวไม่มี retry
	var lastErr error

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		lastErr = fmt.Errorf("error executing batch upsert price: %v", err)
		fmt.Printf("❌ ERROR: %v\n", lastErr)
		return lastErr
	}

	if !resp.Success {
		lastErr = fmt.Errorf("batch upsert price failed: %s", resp.Message)
		fmt.Printf("❌ ERROR: %v\n", lastErr)
		return lastErr
	}

	return nil
}

// Helper functions สำหรับ price sync
func parseFloatValue(value interface{}) string {
	if value == nil {
		return "0"
	}

	switch v := value.(type) {
	case float64:
		return fmt.Sprintf("%.6f", v)
	case string:
		if v == "" || v == "<nil>" {
			return "0"
		}
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

func parseStringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func nullableDate(dateStr string) string {
	if dateStr == "" || dateStr == "<nil>" {
		return "NULL"
	}
	return fmt.Sprintf("'%s'", dateStr)
}
