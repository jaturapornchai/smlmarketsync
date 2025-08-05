package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"strings"
	"time"
)

type CustomerSyncStep struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewCustomerSyncStep(db *sql.DB) *CustomerSyncStep {
	return &CustomerSyncStep{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// ExecuteCustomerSync รันขั้นตอนการ sync ลูกค้า (ตามแบบ Product Sync)
func (s *CustomerSyncStep) ExecuteCustomerSync() error {
	fmt.Println("=== ซิงค์ข้อมูลลูกค้ากับ API ===")

	// 1. ตรวจสอบและสร้างตาราง ar_customer
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ar_customer บน API...")
	err := s.apiClient.CreateCustomerTable()
	if err != nil {
		return fmt.Errorf("error creating customer table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ar_customer เรียบร้อยแล้ว")

	// 2. ดึงข้อมูลลูกค้าจาก local database ผ่าน sml_market_sync
	fmt.Println("กำลังดึงข้อมูลลูกค้าจากฐานข้อมูล local...")
	syncIds, inserts, updates, deletes, err := s.GetAllCustomersFromSource()
	if err != nil {
		return fmt.Errorf("error getting local customer data: %v", err)
	}

	if len(syncIds) == 0 {
		fmt.Println("ไม่มีข้อมูลลูกค้าใน local database")
		return nil
	}

	// 3. ลบข้อมูลใน sml_market_sync ที่ถูกซิงค์แล้วแบบ batch
	err = s.DeleteSyncRecordsInBatches(syncIds, 100) // ลบครั้งละ 100 รายการ
	if err != nil {
		fmt.Printf("⚠️ Warning: %v\n", err)
		// ทำงานต่อไปถึงแม้จะมีข้อผิดพลาด
	}
	// 4. ซิงค์ข้อมูลไปยัง API
	fmt.Println("กำลังซิงค์ข้อมูลลูกค้าไปยัง API...")
	err = s.apiClient.SyncCustomerData(inserts, updates, deletes) // ส่ง inserts, updates, deletes แยกกัน
	if err != nil {
		return fmt.Errorf("error syncing customer data to API: %v", err)
	}
	fmt.Println("✅ ซิงค์ข้อมูลลูกค้าเรียบร้อยแล้ว")
	return nil
}

// GetAllCustomersFromSource ดึงข้อมูลลูกค้าทั้งหมดจากฐานข้อมูลต้นทาง ผ่าน sml_market_sync
func (s *CustomerSyncStep) GetAllCustomersFromSource() ([]int, []interface{}, []interface{}, []interface{}, error) {
	var syncIds []int
	var deletes []interface{}
	var inserts []interface{}
	var updates []interface{}

	// ดึงข้อมูลจาก sml_market_sync สำหรับตาราง customer (table_id = 4)
	querySync := "SELECT id, row_order_ref, active_code FROM sml_market_sync WHERE table_id = 4 ORDER BY active_code DESC"

	rowsSync, errSync := s.db.Query(querySync)
	if errSync != nil {
		return nil, nil, nil, nil, fmt.Errorf("error executing customer sync query: %v", errSync)
	}
	defer rowsSync.Close()

	for rowsSync.Next() {
		var id, rowOrderRef, activeCode int
		err := rowsSync.Scan(&id, &rowOrderRef, &activeCode)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("error scanning customer sync row: %v", err)
		}
		syncIds = append(syncIds, id)

		if activeCode != 3 {
			// ดึงข้อมูลลูกค้าจากตาราง ar_customer (local database)
			queryGetData := `
				SELECT roworder, code, price_level, name_1, name_eng_1, address, telephone 
				FROM ar_customer
				WHERE roworder = $1 AND code IS NOT NULL AND code != ''
			`
			// log queryGetData (following price sync pattern)
			fmt.Printf("Executing customer query: %s with rowOrderRef: %d\n", queryGetData, rowOrderRef)
			row := s.db.QueryRow(queryGetData, rowOrderRef)

			var customer types.CustomerItem
			var priceLevel, name1, nameEng1, address, telephone sql.NullString
			err := row.Scan(
				&customer.RowOrderRef,
				&customer.Code,
				&priceLevel,
				&name1,
				&nameEng1,
				&address,
				&telephone,
			)
			if err != nil {
				if err == sql.ErrNoRows {
					fmt.Printf("⚠️ ไม่พบข้อมูลลูกค้าสำหรับ rowOrderRef: %d\n", rowOrderRef)
					continue
				}
				return nil, nil, nil, nil, fmt.Errorf("error scanning customer row: %v", err)
			}

			// แปลง nullable fields
			if priceLevel.Valid {
				customer.PriceLevel = priceLevel.String
			}
			if name1.Valid {
				customer.Name1 = name1.String
			}
			if nameEng1.Valid {
				customer.NameEng1 = nameEng1.String
			}
			if address.Valid {
				customer.Address = address.String
			}
			if telephone.Valid {
				customer.Telephone = telephone.String
			}

			// แปลงเป็น map สำหรับ API
			customerMap := map[string]interface{}{
				"row_order_ref": customer.RowOrderRef,
				"code":          customer.Code,
				"price_level":   customer.PriceLevel,
				"name_1":        customer.Name1,
				"name_eng_1":    customer.NameEng1,
				"address":       customer.Address,
				"telephone":     customer.Telephone,
			}

			// แยกประเภทตาม active_code
			if activeCode == 1 {
				// activeCode = 1: INSERT ใหม่
				inserts = append(inserts, customerMap)
			}
			if activeCode == 2 {
				// activeCode = 2: DELETE บน server ก่อน แล้ว INSERT ใหม่ (ไม่ใช่ UPDATE)
				deletes = append(deletes, customer.RowOrderRef) // ใช้ row_order_ref เป็น key ในการลบ
				inserts = append(inserts, customerMap)          // เพิ่มเข้า inserts เพื่อ insert ใหม่
			}
		} else if activeCode == 3 {
			deletes = append(deletes, rowOrderRef)
		}
	}

	return syncIds, inserts, updates, deletes, nil
}

// DeleteSyncRecordsInBatches ลบข้อมูลจาก sml_market_sync ในฐานข้อมูลท้องถิ่นแบบแบ่งเป็น batch
func (s *CustomerSyncStep) DeleteSyncRecordsInBatches(syncIds []int, batchSize int) error {
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
			strings.Join(placeholders, ", "))

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
			// ใช้ขนาดของ batch แทน
			rowsAffected = int64(currentBatchSize)
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
