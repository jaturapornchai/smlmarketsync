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
			row_order_ref INT DEFAULT 0,
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
			INDEX idx_row_order_ref (row_order_ref)
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



// SyncPriceData ซิงค์ข้อมูลราคาสินค้าแบบ batch (แยกเป็นการเพิ่มและอัปเดต)
func (api *APIClient) SyncPriceData(syncIds []int, inserts []interface{}, updates []interface{}, deletes []interface{}) {
	if len(inserts) == 0 && len(updates) == 0 && len(deletes) == 0 && len(syncIds) == 0 {
		fmt.Println("ℹ️ ไม่มีข้อมูลที่ต้องดำเนินการ")
		return
	}

	// 1. ลบข้อมูลจาก sml_market_sync ด้วย syncIds
	if len(syncIds) > 0 {
		_, err := api.deleteFromTable("sml_market_sync", "id", toInterfaceSlice(syncIds), false)
		if err != nil {
			return
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องลบจาก sml_market_sync")
	}

	// 2. ลบข้อมูลจาก ic_inventory_price ที่ไม่ต้องการ
	if len(deletes) > 0 {
		fmt.Println("🗑️ กำลังลบข้อมูลที่ไม่ต้องการจาก ic_inventory_price")
		
		// รวบรวม row_order_ref สำหรับการลบ
		var rowOrderRefs []interface{}
		for _, item := range deletes {
			if itemMap, ok := item.(map[string]interface{}); ok {
				if rowOrderRef, exists := itemMap["row_order_ref"]; exists && rowOrderRef != nil {
					rowOrderRefs = append(rowOrderRefs, fmt.Sprintf("%v", rowOrderRef))
				}
			}
		}
		
		if len(rowOrderRefs) > 0 {
			_, err := api.deleteFromTable("ic_inventory_price", "row_order_ref", rowOrderRefs, true)
			if err != nil {
				return
			}
		} else {
			fmt.Println("⚠️ ไม่พบ row_order_ref ที่ต้องการลบ")
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องลบจาก ic_inventory_price")
	}
	
	// 3. ประมวลผล inserts แบบ batch (ครั้งละ 100 รายการ)
	insertCount := 0
	if len(inserts) > 0 {
		count, err := api.processPriceBatch(inserts, 100, false)
		if err != nil {
			return
		}
		insertCount = count
	} else {
		fmt.Println("✅ ไม่มีข้อมูลใหม่ที่ต้องเพิ่ม")
	}
	// 4. ประมวลผล updates แบบ batch (ครั้งละ 100 รายการ)
	updateCount := 0
	if len(updates) > 0 {
		count, err := api.processPriceBatch(updates, 100, true)
		if err != nil {
			return
		}
		updateCount = count
	} else {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องอัปเดต")
	}
	// แสดงผลสรุปข้อมูลที่เตรียมไว้
	fmt.Printf("📦 เตรียมข้อมูลเสร็จ: %d รายการที่ใช้ได้, ข้าม %d รายการ\n", validCount, skipCount)

	if len(batchValues) == 0 {
		return 0, 0, nil
	}

	// ส่งข้อมูลแบบ batch
	batchSize := 50
	totalBatches := (len(batchValues) + batchSize - 1) / batchSize
	successCount := 0

	fmt.Printf("🚀 กำลังส่งข้อมูล %d รายการแบบ batch INSERT (ครั้งละ %d รายการ)\n", len(batchValues), batchSize)

	for i := 0; i < len(batchValues); i += batchSize {
		end := i + batchSize
		if end > len(batchValues) {
			end = len(batchValues)
		}

		batchNum := (i / batchSize) + 1
		currentBatchSize := end - i

		fmt.Printf("   📥 กำลังส่ง batch %d/%d (%d รายการ)...\n", batchNum, totalBatches, currentBatchSize)
		err := api.executeBatchInsertPrice(batchValues[i:end])
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

	// สรุปผลการดำเนินการ
	fmt.Printf("\n📊 สรุปการซิงค์ราคาสินค้า:\n")
	fmt.Printf("   - ลบข้อมูลจาก sml_market_sync: %d รายการ\n", len(syncIds))
	fmt.Printf("   - ลบข้อมูลจาก ic_inventory_price: %d รายการ\n", len(deletes))
	fmt.Printf("   - เพิ่มข้อมูลใหม่: %d/%d รายการ\n", insertCount, len(inserts))
	fmt.Printf("   - อัปเดตข้อมูล: %d/%d รายการ\n", updateCount, len(updates))
}

// executeBatchInsertPrice ทำ batch INSERT สำหรับราคาสินค้า (ไม่มี retry, ถ้าล้มเหลวจะหยุดทันที)
func (api *APIClient) executeBatchInsertPrice(values []string) error {
	if len(values) == 0 {
		return nil
	}
	query := fmt.Sprintf(`
		INSERT INTO ic_inventory_price (ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
										sale_type, sale_price1, status, price_type, cust_code, 
										sale_price2, cust_group_1, price_mode)
		VALUES %s;`,
		strings.Join(values, ",")) // ทำเพียงครั้งเดียวไม่มี retry
	var lastErr error

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		lastErr = fmt.Errorf("error executing batch insert price: %v", err)
		fmt.Printf("❌ ERROR: %v\n", lastErr)
		return lastErr
	}

	if !resp.Success {
		lastErr = fmt.Errorf("batch insert price failed: %s", resp.Message)
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

// prepPriceDataValues เตรียมข้อมูลราคาสินค้าให้อยู่ในรูปแบบที่พร้อมสำหรับคำสั่ง SQL
func prepPriceDataValues(item map[string]interface{}) (string, error) {
	// ตรวจสอบว่ามีข้อมูลจำเป็นครบหรือไม่
	if item["ic_code"] == nil || item["unit_code"] == nil {
		return "", fmt.Errorf("ไม่มี ic_code หรือ unit_code")
	}
	
	// แปลงข้อมูลเป็นรูปแบบสำหรับ SQL
	icCode := fmt.Sprintf("%v", item["ic_code"])
	unitCode := fmt.Sprintf("%v", item["unit_code"])
	fromQty := parseFloatValue(item["from_qty"])
	toQty := parseFloatValue(item["to_qty"])
	fromDate := parseStringValue(item["from_date"])
	toDate := parseStringValue(item["to_date"])
	saleType := parseStringValue(item["sale_type"])
	salePrice1 := parseFloatValue(item["sale_price1"])
	status := parseStringValue(item["status"])
	priceType := parseStringValue(item["price_type"])
	custCode := parseStringValue(item["cust_code"])
	salePrice2 := parseFloatValue(item["sale_price2"])
	custGroup1 := parseStringValue(item["cust_group_1"])
	priceMode := parseStringValue(item["price_mode"])
	
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
		
	return value, nil
}

// deleteFromTable ลบข้อมูลจากตารางที่ระบุ
func (api *APIClient) deleteFromTable(tableName string, idColumn string, ids []interface{}, idIsString bool) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	
	fmt.Printf("🗑️ กำลังลบข้อมูลจากตาราง %s: %d รายการ\n", tableName, len(ids))
	
	// สร้างคำสั่ง DELETE
	deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s IN (", tableName, idColumn)
	
	for i, id := range ids {
		if idIsString {
			// หากเป็น string ต้องใส่เครื่องหมาย quotes
			deleteQuery += fmt.Sprintf("'%v'", id)
		} else {
			// หากเป็นตัวเลข ไม่ต้องใส่เครื่องหมาย quotes
			deleteQuery += fmt.Sprintf("%v", id)
		}
		
		if i < len(ids)-1 {
			deleteQuery += ","
		}
	}
	deleteQuery += ")"
	
	// ทำการลบข้อมูล
	resp, err := api.ExecuteCommand(deleteQuery)
	if err != nil {
		fmt.Printf("❌ ERROR: ไม่สามารถลบข้อมูลจาก %s ได้: %v\n", tableName, err)
		return 0, err
	}
	
	if !resp.Success {
		fmt.Printf("❌ ERROR: ลบข้อมูลจาก %s ล้มเหลว: %s\n", tableName, resp.Message)
		return 0, fmt.Errorf("ลบข้อมูลล้มเหลว: %s", resp.Message)
	}
	
	fmt.Printf("✅ ลบข้อมูลจาก %s เรียบร้อยแล้ว: %d รายการ\n", tableName, len(ids))
	return len(ids), nil
}

// processPriceBatch ประมวลผลข้อมูลราคาสินค้าเป็น batch
func (api *APIClient) processPriceBatch(data []interface{}, batchSize int, isUpdate bool) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	
	operationType := "เพิ่ม"
	if isUpdate {
		operationType = "อัปเดต"
	}
	
	fmt.Printf("🔄 กำลัง%sข้อมูล: %d รายการ (batch ละ %d รายการ)\n", operationType, len(data), batchSize)
	
	// แบ่งเป็น batch
	batchCount := (len(data) + batchSize - 1) / batchSize
	totalProcessed := 0
	
	for b := 0; b < batchCount; b++ {
		start := b * batchSize
		end := start + batchSize
		if end > len(data) {
			end = len(data)
		}
		
		currentBatch := data[start:end]
		fmt.Printf("   📦 ประมวลผล batch ที่ %d/%d (รายการ %d-%d) จากทั้งหมด %d รายการ\n", 
			b+1, batchCount, start+1, end, len(data))
		
		// เตรียมข้อมูลสำหรับ batch
		var batchValues []string
		
		for _, item := range currentBatch {
			if itemMap, ok := item.(map[string]interface{}); ok {
				value, err := prepPriceDataValues(itemMap)
				if err != nil {
					fmt.Printf("⚠️ ข้ามรายการ: %v - %v\n", err, itemMap)
					continue
				}
				batchValues = append(batchValues, value)
			} else {
				fmt.Printf("⚠️ ข้ามรายการที่ไม่ใช่ map: %v\n", item)
			}
		}
		
		// ทำการเพิ่มหรืออัปเดตข้อมูลเป็น batch
		if len(batchValues) > 0 {
			// No WHERE clause needed for simple INSERT
			
			query := fmt.Sprintf(`
				INSERT INTO ic_inventory_price (
					ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
					sale_type, sale_price1, status, price_type, cust_code, 
					sale_price2, cust_group_1, price_mode
				)
				VALUES %s;`,
				strings.Join(batchValues, ","))
			
			resp, err := api.ExecuteCommand(query)
			if err != nil {
				fmt.Printf("❌ ERROR: ไม่สามารถ%sข้อมูล (batch %d) ได้: %v\n", operationType, b+1, err)
				continue
			}
			
			if !resp.Success {
				fmt.Printf("❌ ERROR: %sข้อมูล (batch %d) ล้มเหลว: %s\n", operationType, b+1, resp.Message)
				continue
			}
			
			totalProcessed += len(batchValues)
			fmt.Printf("   ✅ %sข้อมูล batch %d สำเร็จ: %d รายการ\n", operationType, b+1, len(batchValues))
		}
		
		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if b < batchCount-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	
	fmt.Printf("✅ %sข้อมูลเรียบร้อยแล้ว: %d จาก %d รายการ\n", operationType, totalProcessed, len(data))
	return totalProcessed, nil
}

// toInterfaceSlice แปลง slice ของ int เป็น slice ของ interface{}
func toInterfaceSlice(ints []int) []interface{} {
	result := make([]interface{}, len(ints))
	for i, v := range ints {
		result[i] = v
	}
	return result
}
