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
			price_type VARCHAR(20),			
			cust_code VARCHAR(50),
			sale_price2 DECIMAL(15,6) DEFAULT 0,
			cust_group_1 VARCHAR(50),
			price_mode VARCHAR(20)
		)
	`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		// Try to continue even if there's an error, the table might already exist
		fmt.Printf("⚠️ Warning: Error creating price table, continuing anyway: %v\n", err)
		return nil
	}

	if !resp.Success {
		// Try to continue even if there's an error, the table might already exist
		fmt.Printf("⚠️ Warning: Failed to create price table, continuing anyway: %s\n", resp.Message)
		return nil
	}

	return nil
}

// SyncPriceData ซิงค์ข้อมูลราคาสินค้าแบบ batch (แยกเป็นการเพิ่มและลบ)
// activeCode = 2 จะถูกประมวลผลแบบ: ลบก่อน แล้ว insert ใหม่
func (api *APIClient) SyncPriceData(syncIds []int, inserts []interface{}, updates []interface{}, deletes []interface{}) {
	if len(inserts) == 0 && len(updates) == 0 && len(deletes) == 0 && len(syncIds) == 0 {
		fmt.Println("ℹ️ ไม่มีข้อมูลที่ต้องดำเนินการ")
		return
	}

	// 1. ลบข้อมูลจาก sml_market_sync ด้วย syncIds
	if len(syncIds) > 0 {
		_, err := api.deleteFromTable("sml_market_sync", "id", toInterfaceSlice(syncIds), false)
		if err != nil {
			fmt.Printf("⚠️ Warning: ไม่สามารถลบข้อมูลจาก sml_market_sync ได้: %v\n", err)
			// Continue anyway
		} else {
			fmt.Println("✅ ลบข้อมูลจาก sml_market_sync เรียบร้อยแล้ว")
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องลบจาก sml_market_sync")
	}

	// 2. ลบข้อมูลจาก ic_inventory_price ที่ไม่ต้องการ (รวม activeCode = 3 และ activeCode = 2)
	if len(deletes) > 0 {
		fmt.Println("🗑️ กำลังลบข้อมูลจาก ic_inventory_price")

		// รวบรวม row_order_ref สำหรับการลบ
		var rowOrderRefs []interface{}
		for _, item := range deletes {
			rowOrderRefs = append(rowOrderRefs, fmt.Sprintf("%v", item))
		}

		if len(rowOrderRefs) > 0 {
			_, err := api.deleteFromTable("ic_inventory_price", "row_order_ref", rowOrderRefs, false)
			if err != nil {
				fmt.Printf("⚠️ Warning: ไม่สามารถลบข้อมูลจาก ic_inventory_price ได้: %v\n", err)
				// Continue anyway
			} else {
				fmt.Println("✅ ลบข้อมูลจาก ic_inventory_price เรียบร้อยแล้ว")
			}
		} else {
			fmt.Println("⚠️ ไม่พบ row_order_ref ที่ต้องการลบ")
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องลบจาก ic_inventory_price")
	}

	// 3. ประมวลผล inserts แบบ batch (รวมข้อมูลจาก activeCode = 1 และ activeCode = 2)
	insertCount := 0
	if len(inserts) > 0 {
		count, err := api.processPriceBatch(inserts, 100)
		if err != nil {
			fmt.Printf("⚠️ Warning: ไม่สามารถเพิ่มข้อมูลใหม่ได้: %v\n", err)
			// Continue anyway
		} else {
			insertCount = count
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลใหม่ที่ต้องเพิ่ม")
	}

	// สรุปผลการดำเนินการ
	fmt.Printf("\n📊 สรุปการซิงค์ราคาสินค้า:\n")
	fmt.Printf("   - ลบข้อมูลจาก sml_market_sync: %d รายการ\n", len(syncIds))
	fmt.Printf("   - ลบข้อมูลจาก ic_inventory_price: %d รายการ\n", len(deletes))
	fmt.Printf("   - เพิ่มข้อมูลใหม่: %d/%d รายการ\n", insertCount, len(inserts))
	fmt.Printf("   - หมายเหตุ: activeCode = 2 จะถูกลบก่อน แล้ว insert ใหม่\n")
}

// SyncInventoryData ซิงค์ข้อมูลสินค้าแบบ batch (แยกเป็นการเพิ่มและลบ)
// activeCode = 2 จะถูกประมวลผลแบบ: ลบก่อน แล้ว insert ใหม่
func (api *APIClient) SyncInventoryData(inserts []interface{}, updates []interface{}, deletes []interface{}) {
	if len(inserts) == 0 && len(updates) == 0 && len(deletes) == 0 {
		fmt.Println("ℹ️ ไม่มีข้อมูลที่ต้องดำเนินการ")
		return
	}

	// ลบข้อมูลจาก ic_inventory ที่ไม่ต้องการ (รวม activeCode = 3 และ activeCode = 2)
	if len(deletes) > 0 {
		fmt.Println("🗑️ กำลังลบข้อมูลจาก ic_inventory")

		// รวบรวม barcode สำหรับการลบ
		var rowOrderRef []interface{}
		for _, item := range deletes {
			rowOrderRef = append(rowOrderRef, fmt.Sprintf("%v", item))
		}

		if len(rowOrderRef) > 0 {
			_, err := api.deleteFromTable("ic_inventory", "row_order_ref", rowOrderRef, true)
			if err != nil {
				fmt.Printf("⚠️ Warning: ไม่สามารถลบข้อมูลจาก ic_inventory ได้: %v\n", err)
				// Continue anyway
			} else {
				fmt.Println("✅ ลบข้อมูลจาก ic_inventory เรียบร้อยแล้ว")
			}
		} else {
			fmt.Println("⚠️ ไม่พบ code ที่ต้องการลบ")
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องลบจาก ic_inventory")
	}

	// ประมวลผล inserts แบบ batch (รวมข้อมูลจาก activeCode = 1 และ activeCode = 2)
	insertCount := 0
	if len(inserts) > 0 {
		count, err := api.processInventoryInsertBatch(inserts, 100)
		if err != nil {
			fmt.Printf("⚠️ Warning: ไม่สามารถเพิ่มข้อมูลใหม่ได้: %v\n", err)
			// Continue anyway
		} else {
			insertCount = count
		}
	} else {
		fmt.Println("✅ ไม่มีข้อมูลใหม่ที่ต้องเพิ่ม")
	}

	// สรุปผลการดำเนินการ
	fmt.Printf("\n📊 สรุปการซิงค์สินค้า ic_inventory:\n")
	fmt.Printf("   - ลบข้อมูล: %d รายการ\n", len(deletes))
	fmt.Printf("   - เพิ่มข้อมูลใหม่: %d/%d รายการ\n", insertCount, len(inserts))
	fmt.Printf("   - หมายเหตุ: activeCode = 2 จะถูกลบก่อน แล้ว insert ใหม่\n")
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

	// ดึง row_order_ref สำหรับการอ้างอิง
	rowOrderRef := ""
	if item["row_order_ref"] != nil {
		rowOrderRef = fmt.Sprintf("%v", item["row_order_ref"])
	} else {
		return "", fmt.Errorf("ไม่มี row_order_ref")
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

	// Format the values for SQL
	fromDateStr := nullableDate(fromDate)
	toDateStr := nullableDate(toDate)

	value := fmt.Sprintf("(%s, '%s', '%s', %s, %s, %s, %s, '%s', %s, '%s', '%s', '%s', %s, '%s', '%s')",
		rowOrderRef,
		icCode,
		unitCode,
		fromQty,
		toQty,
		fromDateStr,
		toDateStr,
		saleType,
		salePrice1,
		status,
		priceType,
		custCode,
		salePrice2,
		custGroup1,
		priceMode)

	return value, nil
}

// deleteFromTable ลบข้อมูลจากตารางที่ระบุ (แบบ batch เพื่อป้องกัน query ยาว)
func (api *APIClient) deleteFromTable(tableName string, idColumn string, ids []interface{}, idIsString bool) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	fmt.Printf("🗑️ กำลังลบข้อมูลจากตาราง %s: %d รายการ\n", tableName, len(ids))

	batchSize := 1000 // ลบครั้งละ 1,000 รายการเพื่อป้องกัน query ยาว
	totalDeleted := 0
	batchCount := (len(ids) + batchSize - 1) / batchSize

	for b := 0; b < batchCount; b++ {
		start := b * batchSize
		end := start + batchSize
		if end > len(ids) {
			end = len(ids)
		}

		currentBatch := ids[start:end]
		fmt.Printf("   🗑️ ลบ batch ที่ %d/%d (รายการ %d-%d) จากทั้งหมด %d รายการ\n",
			b+1, batchCount, start+1, end, len(ids))

		// สร้างคำสั่ง DELETE สำหรับ batch นี้
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE %s IN (", tableName, idColumn)

		for i, id := range currentBatch {
			if idIsString {
				// หากเป็น string ต้องใส่เครื่องหมาย quotes
				deleteQuery += fmt.Sprintf("'%v'", id)
			} else {
				// หากเป็นตัวเลข ไม่ต้องใส่เครื่องหมาย quotes
				deleteQuery += fmt.Sprintf("%v", id)
			}

			if i < len(currentBatch)-1 {
				deleteQuery += ","
			}
		}
		deleteQuery += ")"

		// ทำการลบข้อมูลสำหรับ batch นี้
		resp, err := api.ExecuteCommand(deleteQuery)
		if err != nil {
			fmt.Printf("❌ ERROR: ไม่สามารถลบข้อมูลจาก %s (batch %d) ได้: %v\n", tableName, b+1, err)
			continue
		}

		if !resp.Success {
			fmt.Printf("❌ ERROR: ลบข้อมูลจาก %s (batch %d) ล้มเหลว: %s\n", tableName, b+1, resp.Message)
			continue
		}

		totalDeleted += len(currentBatch)
		fmt.Printf("   ✅ ลบข้อมูล batch %d สำเร็จ: %d รายการ\n", b+1, len(currentBatch))

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if b < batchCount-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("✅ ลบข้อมูลจาก %s เรียบร้อยแล้ว: %d จาก %d รายการ\n", tableName, totalDeleted, len(ids))
	return totalDeleted, nil
}

// processPriceBatch ประมวลผลข้อมูลราคาสินค้าเป็น batch (เฉพาะ INSERT)
func (api *APIClient) processPriceBatch(data []interface{}, batchSize int) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	fmt.Printf("🔄 กำลังเพิ่มข้อมูล: %d รายการ (batch ละ %d รายการ)\n", len(data), batchSize)

	totalProcessed := 0
	batchCount := (len(data) + batchSize - 1) / batchSize

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

		// ทำการเพิ่มข้อมูลเป็น batch
		if len(batchValues) > 0 {
			query := fmt.Sprintf(`
				INSERT INTO ic_inventory_price (
					row_order_ref, ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
					sale_type, sale_price1, status, price_type, cust_code, 
					sale_price2, cust_group_1, price_mode
				)
				VALUES %s;`,
				strings.Join(batchValues, ","))

			resp, err := api.ExecuteCommand(query)
			if err != nil {
				fmt.Printf("❌ ERROR: ไม่สามารถเพิ่มข้อมูล (batch %d) ได้: %v\n", b+1, err)
				continue
			}

			if !resp.Success {
				fmt.Printf("❌ ERROR: เพิ่มข้อมูล (batch %d) ล้มเหลว: %s\n", b+1, resp.Message)
				continue
			}

			totalProcessed += len(batchValues)
			fmt.Printf("   ✅ เพิ่มข้อมูล batch %d สำเร็จ: %d รายการ\n", b+1, len(batchValues))
		}

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if b < batchCount-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("✅ เพิ่มข้อมูลเรียบร้อยแล้ว: %d จาก %d รายการ\n", totalProcessed, len(data))
	return totalProcessed, nil
}

// processInventoryInsertBatch ประมวลผลข้อมูลสินค้าเป็น batch (เฉพาะ INSERT)
func (api *APIClient) processInventoryInsertBatch(data []interface{}, batchSize int) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	fmt.Printf("🔄 กำลังเพิ่มข้อมูลสินค้า: %d รายการ (batch ละ %d รายการ)\n", len(data), batchSize)

	totalProcessed := 0
	batchCount := (len(data) + batchSize - 1) / batchSize

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
				value, err := prepInventoryDataValues(itemMap)
				if err != nil {
					fmt.Printf("⚠️ ข้ามรายการ: %v - %v\n", err, itemMap)
					continue
				}
				batchValues = append(batchValues, value)
			} else {
				fmt.Printf("⚠️ ข้ามรายการที่ไม่ใช่ map: %v\n", item)
			}
		}

		// ทำการเพิ่มข้อมูลเป็น batch
		if len(batchValues) > 0 {
			query := fmt.Sprintf(`
				INSERT INTO ic_inventory (
					code,name,unit_standard_code,item_type,row_order_ref
				)
				VALUES %s`,
				strings.Join(batchValues, ","))

			resp, err := api.ExecuteCommand(query)
			if err != nil {
				fmt.Printf("❌ ERROR: ไม่สามารถเพิ่มข้อมูลสินค้า (batch %d) ได้: %v\n", b+1, err)
				continue
			}

			if !resp.Success {
				fmt.Printf("❌ ERROR: เพิ่มข้อมูลสินค้า (batch %d) ล้มเหลว: %s\n", b+1, resp.Message)
				continue
			}

			totalProcessed += len(batchValues)
			fmt.Printf("   ✅ เพิ่มข้อมูลสินค้า batch %d สำเร็จ: %d รายการ\n", b+1, len(batchValues))
		}

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if b < batchCount-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("✅ เพิ่มข้อมูลสินค้าเรียบร้อยแล้ว: %d จาก %d รายการ\n", totalProcessed, len(data))
	return totalProcessed, nil
}

// prepInventoryDataValues เตรียมข้อมูลสินค้าให้อยู่ในรูปแบบที่พร้อมสำหรับคำสั่ง SQL
func prepInventoryDataValues(item map[string]interface{}) (string, error) {
	// ตรวจสอบว่ามีข้อมูลจำเป็นครบหรือไม่
	if item["code"] == nil {
		return "", fmt.Errorf("ไม่มี code")
	}

	// แปลงข้อมูลเป็นรูปแบบสำหรับ SQL
	code := fmt.Sprintf("%v", item["code"])
	name := ""
	if item["name"] != nil {
		name = fmt.Sprintf("%v", item["name"])
	}
	unitStandardCode := ""
	if item["unit_standard_code"] != nil {
		unitStandardCode = fmt.Sprintf("%v", item["unit_standard_code"])
	}
	itemType := 0
	if item["item_type"] != nil {
		// ใช้ switch case เพื่อรองรับทั้ง int และ float64
		switch v := item["item_type"].(type) {
		case int:
			itemType = v
		case float64:
			itemType = int(v)
		case int64:
			itemType = int(v)
		default:
			// ถ้าไม่ใช่ตัวเลข ใช้ค่าเริ่มต้น 0
			itemType = 0
		}
	}
	// row_order_ref เป็นค่าเริ่มต้น 0 หากไม่มี
	rowOrderRef := 0
	if item["row_order_ref"] != nil {
		// ใช้ switch case เพื่อรองรับทั้ง int และ float64
		switch v := item["row_order_ref"].(type) {
		case int:
			rowOrderRef = v
		case float64:
			rowOrderRef = int(v)
		case int64:
			rowOrderRef = int(v)
		default:
			// ถ้าไม่ใช่ตัวเลข ใช้ค่าเริ่มต้น 0
			rowOrderRef = 0
		}
	}

	// Escape single quotes
	code = strings.ReplaceAll(code, "'", "''")
	name = strings.ReplaceAll(name, "'", "''")
	unitStandardCode = strings.ReplaceAll(unitStandardCode, "'", "''")

	value := fmt.Sprintf("('%s', '%s', '%s', %d, %d)",
		code, name, unitStandardCode, itemType, rowOrderRef)

	return value, nil
}

// toInterfaceSlice แปลง slice ของ int เป็น slice ของ interface{}
func toInterfaceSlice(ints []int) []interface{} {
	result := make([]interface{}, len(ints))
	for i, v := range ints {
		result[i] = v
	}
	return result
}
