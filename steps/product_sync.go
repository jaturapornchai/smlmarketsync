package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"time"
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

// ExecuteProductSync รันขั้นตอนการ sync สินค้า (ตามแบบ Price Sync)
func (s *ProductSyncStep) ExecuteProductSync() error {
	fmt.Println("=== ซิงค์ข้อมูลสินค้ากับ API ===")

	// 1. ตรวจสอบและสร้างตาราง ic_inventory_barcode
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory บน API...")
	err := s.apiClient.CreateInventoryTable()
	if err != nil {
		return fmt.Errorf("error creating inventory table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory เรียบร้อยแล้ว")

	// 2. ดึงข้อมูลสินค้าจาก local database ผ่าน sml_market_sync
	fmt.Println("กำลังดึงข้อมูลสินค้าจากฐานข้อมูล local...")
	syncIds, inserts, updates, deletes, err := s.GetAllInventoryFromSource()
	if err != nil {
		return fmt.Errorf("error getting local inventory data: %v", err)
	}

	if len(syncIds) == 0 {
		fmt.Println("ไม่มีข้อมูลสินค้าใน local database")
		return nil
	}

	// 3. ลบข้อมูลใน sml_market_sync ที่ถูกซิงค์แล้วแบบ batch
	err = s.DeleteSyncRecordsInBatches(syncIds, 100) // ลบครั้งละ 100 รายการ
	if err != nil {
		fmt.Printf("⚠️ Warning: %v\n", err)
		// ทำงานต่อไปถึงแม้จะมีข้อผิดพลาด
	}

	// 4. ซิงค์ข้อมูลไปยัง API
	fmt.Println("กำลังซิงค์ข้อมูลสินค้าไปยัง API...")
	s.apiClient.SyncInventoryData(inserts, updates, deletes) // ส่ง nil แทน syncIds เพราะเราลบเองแล้ว
	fmt.Println("✅ ซิงค์ข้อมูลสินค้าเรียบร้อยแล้ว")

	return nil
}

// GetAllInventoryFromSource ดึงข้อมูลสินค้าทั้งหมดจากฐานข้อมูลต้นทาง ผ่าน sml_market_sync
func (s *ProductSyncStep) GetAllInventoryFromSource() ([]int, []interface{}, []interface{}, []interface{}, error) {
	var syncIds []int
	var deletes []interface{}
	var inserts []interface{}
	var updates []interface{}

	// ดึงข้อมูลจาก sml_market_sync สำหรับตาราง inventory (table_id = 2)
	querySync := "SELECT id, row_order_ref, active_code FROM sml_market_sync WHERE table_id = 2 ORDER BY active_code DESC"

	rowsSync, errSync := s.db.Query(querySync)
	if errSync != nil {
		return nil, nil, nil, nil, fmt.Errorf("error executing inventory sync query: %v", errSync)
	}
	defer rowsSync.Close()

	for rowsSync.Next() {
		var id, rowOrderRef, activeCode int
		err := rowsSync.Scan(&id, &rowOrderRef, &activeCode)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("error scanning inventory sync row: %v", err)
		}
		syncIds = append(syncIds, id)
		if activeCode != 3 {
			// ดึงข้อมูลสินค้าจากตาราง ic_inventory_barcode (local database)
			queryGetData := `
				SELECT roworder,code,name_1,item_type,unit_standard
				FROM ic_inventory
				WHERE roworder = $1
			`
			// log queryGetData (following price sync pattern)
			fmt.Printf("Executing inventory query: %s with rowOrderRef: %d\n", queryGetData, rowOrderRef)
			row := s.db.QueryRow(queryGetData, rowOrderRef)

			var inventory types.InventoryItem
			err := row.Scan(
				&inventory.RowOrderRef,
				&inventory.IcCode,
				&inventory.Name,
				&inventory.ItemType,
				&inventory.UnitStandardCode,
			)
			if err != nil {
				if err == sql.ErrNoRows {
					fmt.Printf("⚠️ ไม่พบข้อมูลสินค้าสำหรับ barcode: %d\n", rowOrderRef)
					continue
				}
				return nil, nil, nil, nil, fmt.Errorf("error scanning inventory row: %v", err)
			}

			// แปลงเป็น map สำหรับ API
			inventoryMap := map[string]interface{}{
				"code":               inventory.IcCode,
				"name":               inventory.Name,
				"item_type":          inventory.ItemType,
				"unit_standard_code": inventory.UnitStandardCode,
				"row_order_ref":      rowOrderRef,
			}

			// แยกประเภทตาม active_code
			if activeCode == 1 {
				// activeCode = 1: INSERT ใหม่
				inserts = append(inserts, inventoryMap)
			}
			if activeCode == 2 {
				// activeCode = 2: DELETE บน server ก่อน แล้ว INSERT ใหม่ (ไม่ใช่ UPDATE)
				deletes = append(deletes, rowOrderRef)
				inserts = append(inserts, inventoryMap) // เพิ่มเข้า inserts เพื่อ insert ใหม่
			}
		} else if activeCode == 3 {
			deletes = append(deletes, rowOrderRef)
		}
	}

	return syncIds, inserts, updates, deletes, nil
}

// DeleteSyncRecordsInBatches ลบข้อมูลจาก sml_market_sync ในฐานข้อมูลท้องถิ่นแบบแบ่งเป็น batch
func (s *ProductSyncStep) DeleteSyncRecordsInBatches(syncIds []int, batchSize int) error {
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

type ProductBarcodeSyncStep struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewProductBarcodeSyncStep(db *sql.DB) *ProductBarcodeSyncStep {
	return &ProductBarcodeSyncStep{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// ExecuteProductBarcodeSync รันขั้นตอนการ sync ProductBarcode (ตามแบบ Product Sync)
func (s *ProductBarcodeSyncStep) ExecuteProductBarcodeSync() error {
	fmt.Println("=== ซิงค์ข้อมูล ProductBarcode กับ API ===")

	// 1. ตรวจสอบและสร้างตาราง ic_inventory_barcode
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory_barcode บน API...")
	err := s.apiClient.CreateInventoryBarcodeTable()
	if err != nil {
		return fmt.Errorf("error creating inventory barcode table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory_barcode เรียบร้อยแล้ว")

	// 2. ดึงข้อมูล ProductBarcode จาก local database ผ่าน sml_market_sync
	fmt.Println("กำลังดึงข้อมูล ProductBarcode จากฐานข้อมูล local...")
	syncIds, inserts, updates, deletes, err := s.GetAllProductBarcodeFromSource()
	if err != nil {
		return fmt.Errorf("error getting local ProductBarcode data: %v", err)
	}

	if len(syncIds) == 0 {
		fmt.Println("ไม่มีข้อมูล ProductBarcode ใน local database")
		return nil
	}
	// 3. ลบข้อมูลใน sml_market_sync ที่ถูกซิงค์แล้วแบบ batch
	err = s.DeleteSyncRecordsInBatches(syncIds, 100) // ลบครั้งละ 100 รายการ
	if err != nil {
		fmt.Printf("⚠️ Warning: %v\n", err)
		// ทำงานต่อไปถึงแม้จะมีข้อผิดพลาด
	}

	// 4. ซิงค์ข้อมูลไปยัง API
	fmt.Println("กำลังซิงค์ข้อมูล ProductBarcode ไปยัง API...")
	s.apiClient.SyncProductBarcodeData(nil, inserts, updates, deletes) // ส่ง nil แทน syncIds เพราะเราลบเองแล้ว
	fmt.Println("✅ ซิงค์ข้อมูล ProductBarcode เรียบร้อยแล้ว")

	return nil
}

// GetAllProductBarcodeFromSource ดึงข้อมูล ProductBarcode ทั้งหมดจากฐานข้อมูลต้นทาง ผ่าน sml_market_sync
func (s *ProductBarcodeSyncStep) GetAllProductBarcodeFromSource() ([]int, []interface{}, []interface{}, []interface{}, error) {
	var syncIds []int
	var deletes []interface{}
	var inserts []interface{}
	var updates []interface{}

	// ดึงข้อมูลจาก sml_market_sync สำหรับตาราง ProductBarcode (table_id = 3)
	querySync := "SELECT id, row_order_ref, active_code FROM sml_market_sync WHERE table_id = 3 ORDER BY active_code DESC"

	rowsSync, errSync := s.db.Query(querySync)
	if errSync != nil {
		return nil, nil, nil, nil, fmt.Errorf("error executing ProductBarcode sync query: %v", errSync)
	}
	defer rowsSync.Close()

	for rowsSync.Next() {
		var id, rowOrderRef, activeCode int
		err := rowsSync.Scan(&id, &rowOrderRef, &activeCode)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("error scanning ProductBarcode sync row: %v", err)
		}
		syncIds = append(syncIds, id)

		if activeCode != 3 {
			// ดึงข้อมูล ProductBarcode จากตาราง ic_inventory_barcode
			queryGetData := `
				SELECT roworder,ic_code, barcode, 
					coalesce((SELECT name_1 FROM ic_inventory WHERE code=ic_code), 'XX') as name,
					unit_code,
					coalesce((SELECT name_1 FROM ic_unit WHERE code=unit_code), 'XX') as unit_name 
				FROM ic_inventory_barcode
				WHERE roworder = $1
			`
			row := s.db.QueryRow(queryGetData, rowOrderRef)

			var inventory types.BarcodeItem
			err := row.Scan(
				&inventory.RowOrderRef,
				&inventory.IcCode,
				&inventory.Barcode,
				&inventory.Name,
				&inventory.UnitCode,
				&inventory.UnitName,
			)
			if err != nil {
				if err == sql.ErrNoRows {
					fmt.Printf("⚠️ ไม่พบข้อมูล ProductBarcode สำหรับ barcode: %d\n", rowOrderRef)
					continue
				}
				return nil, nil, nil, nil, fmt.Errorf("error scanning ProductBarcode row: %v", err)
			} // แปลงเป็น map สำหรับ API
			inventoryMap := map[string]interface{}{
				"row_order_ref": inventory.RowOrderRef,
				"ic_code":       inventory.IcCode,
				"barcode":       inventory.Barcode,
				"name":          inventory.Name,
				"unit_code":     inventory.UnitCode,
				"unit_name":     inventory.UnitName,
			}

			// แยกประเภทตาม active_code
			if activeCode == 1 {
				// activeCode = 1: INSERT ใหม่
				inserts = append(inserts, inventoryMap)
			}
			if activeCode == 2 {
				// activeCode = 2: DELETE บน server ก่อน แล้ว INSERT ใหม่ (ไม่ใช่ UPDATE)
				deletes = append(deletes, inventory.RowOrderRef) // เพิ่มเข้า deletes เพื่อลบบน server ก่อน
				inserts = append(inserts, inventoryMap)          // เพิ่มเข้า inserts เพื่อ insert ใหม่
			}
		} else if activeCode == 3 {
			deletes = append(deletes, rowOrderRef)
		}
	}

	return syncIds, inserts, updates, deletes, nil
}

// DeleteSyncRecordsInBatches ลบข้อมูลจาก sml_market_sync ในฐานข้อมูลท้องถิ่นแบบแบ่งเป็น batch
func (s *ProductBarcodeSyncStep) DeleteSyncRecordsInBatches(syncIds []int, batchSize int) error {
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
