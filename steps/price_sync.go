package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"strconv"
	"time"
)

type PriceSyncStep struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewPriceSyncStep(db *sql.DB) *PriceSyncStep {
	return &PriceSyncStep{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// ExecutePriceSync รันขั้นตอนที่ 7: การ sync ราคาสินค้า
func (s *PriceSyncStep) ExecutePriceSync() error {
	fmt.Println("=== ซิงค์ข้อมูลราคาสินค้ากับ API ===") // 1. ตรวจสอบและสร้างตาราง ic_inventory_price
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory_price บน API...")
	err := s.apiClient.CreatePriceTable()
	if err != nil {
		return fmt.Errorf("error creating price table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory_price เรียบร้อยแล้ว")
	// 2. ดึงข้อมูลราคาสินค้าจาก local database
	fmt.Println("กำลังดึงข้อมูลราคาสินค้าจากฐานข้อมูล local...")
	syncIds, inserts, updates, deletes, err := s.GetAllPricesFromSource()
	if err != nil {
		return fmt.Errorf("error getting local price data: %v", err)
	}
	if len(syncIds) == 0 {
		fmt.Println("ไม่มีข้อมูลราคาสินค้าใน local database")
		return nil
	}

	// 3. ลบข้อมูลใน sml_market_sync ที่ถูกซิงค์แล้วแบบ batch
	err = s.DeleteSyncRecordsInBatches(syncIds, 100) // ลบครั้งละ 100 รายการ
	if err != nil {
		fmt.Printf("⚠️ Warning: %v\n", err)
		// ทำงานต่อไปถึงแม้จะมีข้อผิดพลาด
	}

	// 4. ซิงค์ข้อมูลไปยัง API
	fmt.Println("กำลังซิงค์ข้อมูลราคาสินค้าไปยัง API...")
	s.apiClient.SyncPriceData(nil, inserts, updates, deletes) // ส่ง nil แทน syncIds เพราะเราลบเองแล้ว
	fmt.Println("✅ ซิงค์ข้อมูลราคาสินค้าเรียบร้อยแล้ว")

	return nil
}

// GetAllPricesFromSource ดึงข้อมูลราคาสินค้าทั้งหมดจากฐานข้อมูลต้นทาง
func (s *PriceSyncStep) GetAllPricesFromSource() ([]int, []interface{}, []interface{}, []interface{}, error) {
	var syndIds []int
	var deletes []interface{}
	var inserts []interface{}
	var updates []interface{}

	querySync := "SELECT id, row_order_ref, active_code FROM sml_market_sync where table_id = 1 ORDER BY active_code DESC"

	rowsSync, errSync := s.db.Query(querySync)
	if errSync != nil {
		return nil, nil, nil, nil, fmt.Errorf("error executing sync query: %v", errSync)
	}
	defer rowsSync.Close()

	for rowsSync.Next() {
		var id, rowOrderRef, activeCode int
		err := rowsSync.Scan(&id, &rowOrderRef, &activeCode)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("error scanning sync row: %v", err)
		}
		syndIds = append(syndIds, id)

		if activeCode != 3 {
			// ดึงข้อมูลดิบ
			queryGetData := `
				SELECT roworder,ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
					sale_type, sale_price1, status, price_type, cust_code, 
					sale_price2, cust_group_1, price_mode
				FROM ic_inventory_price 
				WHERE roworder = $1
			`
			// log queryGetData
			fmt.Printf("Executing query: %s with rowOrderRef: %d\n", queryGetData, rowOrderRef)
			row := s.db.QueryRow(queryGetData, rowOrderRef)
			var price types.PriceItem
			var fromQtyStr, toQtyStr, salePrice1Str, salePrice2Str sql.NullString
			var fromDate, toDate sql.NullString
			err := row.Scan(
				&price.RowOrderRef,
				&price.IcCode,
				&price.UnitCode,
				&fromQtyStr,
				&toQtyStr,
				&fromDate,
				&toDate,
				&price.SaleType,
				&salePrice1Str,
				&price.Status,
				&price.PriceType,
				&price.CustCode,
				&salePrice2Str,
				&price.CustGroup1,
				&price.PriceMode,
			)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("error scanning price row: %v", err)
			}
			// แปลงข้อมูลตัวเลข
			if fromQtyStr.Valid {
				if fromQty, err := strconv.ParseFloat(fromQtyStr.String, 64); err == nil {
					price.FromQty = fromQty
				}
			}
			if toQtyStr.Valid {
				if toQty, err := strconv.ParseFloat(toQtyStr.String, 64); err == nil {
					price.ToQty = toQty
				}
			}
			if salePrice1Str.Valid {
				if salePrice1, err := strconv.ParseFloat(salePrice1Str.String, 64); err == nil {
					price.SalePrice1 = salePrice1
				}
			}
			if salePrice2Str.Valid {
				if salePrice2, err := strconv.ParseFloat(salePrice2Str.String, 64); err == nil {
					price.SalePrice2 = salePrice2
				}
			}
			// แปลงวันที่
			if fromDate.Valid {
				price.FromDate = fromDate.String
			}
			if toDate.Valid {
				price.ToDate = toDate.String
			}
			// แปลงเป็น map สำหรับ API
			priceMap := map[string]interface{}{
				"row_order_ref": price.RowOrderRef,
				"ic_code":       price.IcCode,
				"unit_code":     price.UnitCode,
				"from_qty":      price.FromQty,
				"to_qty":        price.ToQty,
				"from_date":     price.FromDate,
				"to_date":       price.ToDate,
				"sale_type":     price.SaleType,
				"sale_price1":   price.SalePrice1,
				"status":        price.Status,
				"price_type":    price.PriceType,
				"cust_code":     price.CustCode,
				"sale_price2":   price.SalePrice2,
				"cust_group_1":  price.CustGroup1,
				"price_mode":    price.PriceMode,
			} // แยกประเภทตาม active_code
			if activeCode == 1 {
				// activeCode = 1: INSERT ใหม่
				inserts = append(inserts, priceMap)
			}
			if activeCode == 2 {
				// activeCode = 2: DELETE บน server ก่อน แล้ว INSERT ใหม่ (ไม่ใช่ UPDATE)
				deletes = append(deletes, rowOrderRef) // เพิ่มเข้า deletes เพื่อลบบน server ก่อน
				inserts = append(inserts, priceMap)    // เพิ่มเข้า inserts เพื่อ insert ใหม่
			}
		} else if activeCode == 3 {
			deletes = append(deletes, rowOrderRef)
		}
	}

	return syndIds, inserts, updates, deletes, nil
}

// DeleteSyncRecordsInBatches ลบข้อมูลจาก sml_market_sync ในฐานข้อมูลท้องถิ่นแบบแบ่งเป็น batch
func (s *PriceSyncStep) DeleteSyncRecordsInBatches(syncIds []int, batchSize int) error {
	if len(syncIds) == 0 {
		fmt.Println("✅ ไม่มีข้อมูลที่ต้องลบจาก sml_market_sync")
		return nil
	}

	totalItems := len(syncIds)
	fmt.Printf("🗑️ กำลังลบข้อมูลจากตาราง sml_market_sync (local database): %d รายการ (แบ่งเป็น batch ละ %d รายการ)\n",
		totalItems, batchSize)

	// แบ่งเป็น batch
	batchCount := (totalItems + batchSize - 1) / batchSize
	totalDeleted := 0
	successBatches := 0
	failedBatches := 0

	for b := 0; b < batchCount; b++ {
		start := b * batchSize
		end := start + batchSize
		if end > totalItems {
			end = totalItems
		}

		batchIds := syncIds[start:end]
		currentBatchSize := len(batchIds)

		fmt.Printf("   🔄 กำลังลบ batch ที่ %d/%d (รายการ %d-%d) จากทั้งหมด %d รายการ\n",
			b+1, batchCount, start+1, end, totalItems)

		// สร้าง query และ parameter placeholders
		placeholders := make([]string, len(batchIds))
		args := make([]interface{}, len(batchIds))

		for i, id := range batchIds {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = id
		}

		// สร้างคำสั่ง DELETE
		query := fmt.Sprintf("DELETE FROM sml_market_sync WHERE id IN (%s)",
			joinStrings(placeholders, ", "))

		// ทำการลบข้อมูล
		result, err := s.db.Exec(query, args...)
		if err != nil {
			fmt.Printf("   ❌ ERROR: ไม่สามารถลบข้อมูล batch ที่ %d จาก sml_market_sync ได้: %v\n",
				b+1, err)
			failedBatches++
			// ทำ batch ต่อไป
			continue
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			fmt.Printf("   ⚠️ Warning: ไม่สามารถอ่านจำนวนแถวที่ถูกลบได้: %v\n", err)
			rowsAffected = int64(currentBatchSize) // ใช้ขนาดของ batch แทน
		}

		totalDeleted += int(rowsAffected)
		successBatches++
		fmt.Printf("   ✅ ลบข้อมูล batch ที่ %d จาก sml_market_sync สำเร็จ: %d รายการ\n",
			b+1, rowsAffected)

		// หน่วงเวลาเล็กน้อยระหว่าง batch เพื่อลดภาระของ database
		if b < batchCount-1 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// สรุปผลการดำเนินการ
	if failedBatches > 0 {
		fmt.Printf("⚠️ สรุปการลบข้อมูลจาก sml_market_sync: ลบได้ %d/%d รายการ (%d/%d batches สำเร็จ)\n",
			totalDeleted, totalItems, successBatches, batchCount)
		return fmt.Errorf("มีบาง batch ที่ลบไม่สำเร็จ (%d/%d batches ล้มเหลว)",
			failedBatches, batchCount)
	}

	fmt.Printf("✅ ลบข้อมูลจาก sml_market_sync เรียบร้อยแล้ว: %d รายการ (%d batches)\n",
		totalDeleted, batchCount)
	return nil
}

// joinStrings เป็นฟังก์ชันสำหรับรวม string ด้วยตัวคั่น
func joinStrings(elements []string, separator string) string {
	if len(elements) == 0 {
		return ""
	}
	result := elements[0]
	for i := 1; i < len(elements); i++ {
		result += separator + elements[i]
	}
	return result
}
