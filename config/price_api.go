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
	fmt.Printf("\n📊 สรุปการซิงค์ราคาสินค้า sml_market_sync:\n")
	fmt.Printf("   - ลบข้อมูล: %d รายการ\n", len(syncIds))
	fmt.Printf("   - ลบข้อมูล: %d รายการ\n", len(deletes))
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

// CreatePriceFormulaTable สร้างตาราง ic_inventory_price_formula
func (api *APIClient) CreatePriceFormulaTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS ic_inventory_price_formula (
			id SERIAL PRIMARY KEY,
			row_order_ref INT DEFAULT 0,
			ic_code VARCHAR(25) NOT NULL DEFAULT '',
			unit_code VARCHAR(25) NOT NULL DEFAULT '',
			sale_type SMALLINT NOT NULL DEFAULT 0,
			price_0 VARCHAR(50) DEFAULT '',
			price_1 VARCHAR(50) DEFAULT '',
			price_2 VARCHAR(50) DEFAULT '',
			price_3 VARCHAR(50) DEFAULT '',
			price_4 VARCHAR(50) DEFAULT '',
			price_5 VARCHAR(50) DEFAULT '',
			price_6 VARCHAR(50) DEFAULT '',
			price_7 VARCHAR(50) DEFAULT '',
			price_8 VARCHAR(50) DEFAULT '',
			price_9 VARCHAR(50) DEFAULT '',
			tax_type SMALLINT NOT NULL DEFAULT 0,
			price_currency SMALLINT DEFAULT 0,
			currency_code VARCHAR(25) DEFAULT ''
		)
	`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		// Try to continue even if there's an error, the table might already exist
		fmt.Printf("⚠️ Warning: Error creating price formula table, continuing anyway: %v\n", err)
		return nil
	}

	if !resp.Success {
		// Try to continue even if there's an error, the table might already exist
		fmt.Printf("⚠️ Warning: Failed to create price formula table, continuing anyway: %s\n", resp.Message)
		return nil
	}

	return nil
}

// SyncPriceFormulaData ซิงค์ข้อมูลสูตรราคาสินค้าแบบ batch (แยกเป็นการเพิ่มและลบ)
// activeCode = 2 จะถูกประมวลผลแบบ: ลบก่อน แล้ว insert ใหม่
func (api *APIClient) SyncPriceFormulaData(syncIds []int, inserts []interface{}, updates []interface{}, deletes []interface{}) {
	if len(inserts) == 0 && len(updates) == 0 && len(deletes) == 0 && len(syncIds) == 0 {
		fmt.Println("ℹ️ ไม่มีข้อมูลสูตรราคาที่ต้องดำเนินการ")
		return
	}

	// 1. ลบข้อมูลจาก sml_market_sync ด้วย syncIds
	if len(syncIds) > 0 {
		_, err := api.deleteFromTable("sml_market_sync", "id", toInterfaceSlice(syncIds), false)
		if err != nil {
			fmt.Printf("⚠️ Warning: ไม่สามารถลบข้อมูลจาก sml_market_sync ได้: %v\n", err)
		}
	}

	// 2. Handle deletes (ลบข้อมูลบน server)
	if len(deletes) > 0 {
		fmt.Printf("🗑️ กำลังลบข้อมูลสูตรราคาสินค้า %d รายการ\n", len(deletes))
		api.executeBatchDeletePriceFormula(deletes)
	}

	// 3. Handle inserts (เพิ่มข้อมูลใหม่)
	if len(inserts) > 0 {
		fmt.Printf("📝 กำลังเพิ่มข้อมูลสูตรราคาสินค้า %d รายการ\n", len(inserts))
		api.executeBatchInsertPriceFormula(inserts)
	}

	// 4. Handle updates (อัพเดทข้อมูล)
	if len(updates) > 0 {
		fmt.Printf("🔄 กำลังอัพเดทข้อมูลสูตรราคาสินค้า %d รายการ\n", len(updates))
		api.executeBatchUpdatePriceFormula(updates)
	}

	fmt.Println("✅ ซิงค์ข้อมูลสูตรราคาสินค้าเสร็จสิ้น")
}

// executeBatchDeletePriceFormula ลบข้อมูลสูตรราคาสินค้าแบบ batch
func (api *APIClient) executeBatchDeletePriceFormula(deletes []interface{}) error {
	if len(deletes) == 0 {
		return nil
	}

	success, err := api.deleteFromTable("ic_inventory_price_formula", "row_order_ref", deletes, true)
	if err != nil {
		fmt.Printf("❌ Error deleting price formula data: %v\n", err)
		return err
	}

	fmt.Printf("✅ ลบข้อมูลสูตรราคาสินค้าสำเร็จ: %d รายการ\n", success)
	return nil
}

// executeBatchInsertPriceFormula เพิ่มข้อมูลสูตรราคาสินค้าแบบ batch
func (api *APIClient) executeBatchInsertPriceFormula(inserts []interface{}) error {
	if len(inserts) == 0 {
		return nil
	}

	const batchSize = 50 // ลดขนาด batch เพราะ field เยอะ
	totalInserted := 0

	for i := 0; i < len(inserts); i += batchSize {
		end := i + batchSize
		if end > len(inserts) {
			end = len(inserts)
		}

		currentBatch := inserts[i:end]
		var values []string
		for _, item := range currentBatch {
			if itemMap, ok := item.(map[string]interface{}); ok {
				// รับค่าเฉพาะ field ที่ต้องการ
				rowOrderRef := fmt.Sprintf("%v", itemMap["row_order_ref"])
				icCode := fmt.Sprintf("%v", itemMap["ic_code"])
				unitCode := fmt.Sprintf("%v", itemMap["unit_code"])
				saleType := fmt.Sprintf("%v", itemMap["sale_type"])
				price0 := fmt.Sprintf("%v", itemMap["price_0"])
				price1 := fmt.Sprintf("%v", itemMap["price_1"])
				price2 := fmt.Sprintf("%v", itemMap["price_2"])
				price3 := fmt.Sprintf("%v", itemMap["price_3"])
				price4 := fmt.Sprintf("%v", itemMap["price_4"])
				price5 := fmt.Sprintf("%v", itemMap["price_5"])
				price6 := fmt.Sprintf("%v", itemMap["price_6"])
				price7 := fmt.Sprintf("%v", itemMap["price_7"])
				price8 := fmt.Sprintf("%v", itemMap["price_8"])
				price9 := fmt.Sprintf("%v", itemMap["price_9"])
				taxType := fmt.Sprintf("%v", itemMap["tax_type"])
				priceCurrency := fmt.Sprintf("%v", itemMap["price_currency"])
				currencyCode := fmt.Sprintf("%v", itemMap["currency_code"])

				// Escape single quotes for string fields
				icCode = strings.ReplaceAll(icCode, "'", "''")
				unitCode = strings.ReplaceAll(unitCode, "'", "''")
				price0 = strings.ReplaceAll(price0, "'", "''")
				price1 = strings.ReplaceAll(price1, "'", "''")
				price2 = strings.ReplaceAll(price2, "'", "''")
				price3 = strings.ReplaceAll(price3, "'", "''")
				price4 = strings.ReplaceAll(price4, "'", "''")
				price5 = strings.ReplaceAll(price5, "'", "''")
				price6 = strings.ReplaceAll(price6, "'", "''")
				price7 = strings.ReplaceAll(price7, "'", "''")
				price8 = strings.ReplaceAll(price8, "'", "''")
				price9 = strings.ReplaceAll(price9, "'", "''")
				currencyCode = strings.ReplaceAll(currencyCode, "'", "''")

				value := fmt.Sprintf("(%s, '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s')",
					rowOrderRef, icCode, unitCode, saleType, price0, price1, price2, price3, price4, price5, price6, price7, price8, price9, taxType, priceCurrency, currencyCode)
				values = append(values, value)
			}
		}

		if len(values) > 0 {
			query := fmt.Sprintf(`
				INSERT INTO ic_inventory_price_formula (row_order_ref, ic_code, unit_code, sale_type, price_0, price_1, price_2, price_3, 
				price_4, price_5, price_6, price_7, price_8, price_9, tax_type, price_currency, currency_code)
				VALUES %s
			`, strings.Join(values, ","))

			resp, err := api.ExecuteCommand(query)
			if err != nil {
				fmt.Printf("❌ Error inserting price formula batch %d-%d: %v\n", i+1, end, err)
				continue
			}

			if !resp.Success {
				fmt.Printf("❌ Failed to insert price formula batch %d-%d: %s\n", i+1, end, resp.Message)
				continue
			}

			totalInserted += len(values)
			fmt.Printf("   ✅ เพิ่มข้อมูลสูตรราคาสินค้า batch สำเร็จ: %d รายการ\n", len(values))
		}

		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("✅ เพิ่มข้อมูลสูตรราคาสินค้าเรียบร้อยแล้ว: %d รายการ\n", totalInserted)
	return nil
}

// executeBatchUpdatePriceFormula อัพเดทข้อมูลสูตรราคาสินค้าแบบ batch
func (api *APIClient) executeBatchUpdatePriceFormula(updates []interface{}) error {
	if len(updates) == 0 {
		return nil
	}

	totalUpdated := 0
	for i, item := range updates {
		if itemMap, ok := item.(map[string]interface{}); ok {
			rowOrderRef := fmt.Sprintf("%v", itemMap["row_order_ref"])
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			unitCode := fmt.Sprintf("%v", itemMap["unit_code"])
			saleType := fmt.Sprintf("%v", itemMap["sale_type"])
			price0 := fmt.Sprintf("%v", itemMap["price_0"])
			price1 := fmt.Sprintf("%v", itemMap["price_1"])
			price2 := fmt.Sprintf("%v", itemMap["price_2"])
			price3 := fmt.Sprintf("%v", itemMap["price_3"])
			price4 := fmt.Sprintf("%v", itemMap["price_4"])
			price5 := fmt.Sprintf("%v", itemMap["price_5"])
			price6 := fmt.Sprintf("%v", itemMap["price_6"])
			price7 := fmt.Sprintf("%v", itemMap["price_7"])
			price8 := fmt.Sprintf("%v", itemMap["price_8"])
			price9 := fmt.Sprintf("%v", itemMap["price_9"])
			taxType := fmt.Sprintf("%v", itemMap["tax_type"])
			priceCurrency := fmt.Sprintf("%v", itemMap["price_currency"])
			currencyCode := fmt.Sprintf("%v", itemMap["currency_code"])

			// Escape single quotes for string fields
			icCode = strings.ReplaceAll(icCode, "'", "''")
			unitCode = strings.ReplaceAll(unitCode, "'", "''")
			price0 = strings.ReplaceAll(price0, "'", "''")
			price1 = strings.ReplaceAll(price1, "'", "''")
			price2 = strings.ReplaceAll(price2, "'", "''")
			price3 = strings.ReplaceAll(price3, "'", "''")
			price4 = strings.ReplaceAll(price4, "'", "''")
			price5 = strings.ReplaceAll(price5, "'", "''")
			price6 = strings.ReplaceAll(price6, "'", "''")
			price7 = strings.ReplaceAll(price7, "'", "''")
			price8 = strings.ReplaceAll(price8, "'", "''")
			price9 = strings.ReplaceAll(price9, "'", "''")
			currencyCode = strings.ReplaceAll(currencyCode, "'", "''")

			updateQuery := fmt.Sprintf(`
				UPDATE ic_inventory_price_formula 
				SET ic_code = '%s', unit_code = '%s', sale_type = %s, price_0 = '%s', price_1 = '%s', price_2 = '%s', 
				    price_3 = '%s', price_4 = '%s', price_5 = '%s', price_6 = '%s', price_7 = '%s', 
				    price_8 = '%s', price_9 = '%s', tax_type = %s, price_currency = %s, 
				    currency_code = '%s'
				WHERE row_order_ref = %s
			`, icCode, unitCode, saleType, price0, price1, price2, price3, price4, price5, price6, price7,
				price8, price9, taxType, priceCurrency, currencyCode, rowOrderRef)

			resp, err := api.ExecuteCommand(updateQuery)
			if err != nil {
				fmt.Printf("❌ Error updating price formula record %d: %v\n", i+1, err)
				continue
			}

			if !resp.Success {
				fmt.Printf("❌ Failed to update price formula record %d: %s\n", i+1, resp.Message)
				continue
			}

			totalUpdated++
		}

		if (i+1)%100 == 0 {
			fmt.Printf("   ⏳ อัพเดทข้อมูลสูตรราคาสินค้าแล้ว: %d/%d รายการ\n", i+1, len(updates))
		}
	}

	fmt.Printf("✅ อัพเดทข้อมูลสูตรราคาสินค้าเรียบร้อยแล้ว: %d รายการ\n", totalUpdated)
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
					code, name, unit_standard_code, item_type, row_order_ref, image_url
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

	imageURL := ""
	if item["image_url"] != nil {
		imageURL = fmt.Sprintf("%v", item["image_url"])
	}
	imageURL = strings.ReplaceAll(imageURL, "'", "''")

	value := fmt.Sprintf("('%s', '%s', '%s', %d, %d, '%s')",
		code, name, unitStandardCode, itemType, rowOrderRef, imageURL)

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
