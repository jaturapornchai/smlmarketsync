package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"strings"
	"time"
)

type PriceFormulaSyncStep struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewPriceFormulaSyncStep(db *sql.DB) *PriceFormulaSyncStep {
	return &PriceFormulaSyncStep{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// ExecutePriceFormulaSync รันขั้นตอนการ sync สูตรราคาสินค้า
func (s *PriceFormulaSyncStep) ExecutePriceFormulaSync() error {
	fmt.Println("=== ซิงค์ข้อมูลสูตรราคาสินค้ากับ API ===") // 1. ตรวจสอบและสร้างตาราง ic_inventory_price_formula
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory_price_formula บน API...")
	err := s.apiClient.CreatePriceFormulaTable()
	if err != nil {
		return fmt.Errorf("error creating price formula table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory_price_formula เรียบร้อยแล้ว")

	// 2. ดึงข้อมูลสูตรราคาสินค้าจาก local database ผ่าน sml_market_sync
	fmt.Println("กำลังดึงข้อมูลสูตรราคาสินค้าจากฐานข้อมูล local...")
	syncIds, inserts, updates, deletes, err := s.GetAllPriceFormulasFromSource()
	if err != nil {
		return fmt.Errorf("error getting local price formula data: %v", err)
	}

	if len(syncIds) == 0 {
		fmt.Println("ไม่มีข้อมูลสูตรราคาสินค้าใน local database")
		return nil
	}

	// 3. ลบข้อมูลใน sml_market_sync ที่ถูกซิงค์แล้วแบบ batch
	err = s.DeleteSyncRecordsInBatches(syncIds, 100) // ลบครั้งละ 100 รายการ
	if err != nil {
		fmt.Printf("⚠️ Warning: %v\n", err)
		// ทำงานต่อไปถึงแม้จะมีข้อผิดพลาด
	}
	// 4. ซิงค์ข้อมูลไปยัง API
	fmt.Println("กำลังซิงค์ข้อมูลสูตรราคาสินค้าไปยัง API...")
	s.apiClient.SyncPriceFormulaData(nil, inserts, updates, deletes) // ส่ง nil แทน syncIds เพราะเราลบเองแล้ว
	fmt.Println("✅ ซิงค์ข้อมูลสูตรราคาสินค้าเรียบร้อยแล้ว")

	return nil
}

// GetAllPriceFormulasFromSource ดึงข้อมูลสูตรราคาสินค้าทั้งหมดจากฐานข้อมูลต้นทาง
func (s *PriceFormulaSyncStep) GetAllPriceFormulasFromSource() ([]int, []interface{}, []interface{}, []interface{}, error) {
	var syndIds []int
	var deletes []interface{}
	var inserts []interface{}
	var updates []interface{}

	querySync := "SELECT id, row_order_ref, active_code FROM sml_market_sync where table_id = 5 ORDER BY active_code DESC"

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
		if activeCode != 3 { // ดึงข้อมูลดิบจาก ic_inventory_price_formula (local table)
			queryGetData := `
				SELECT roworder,COALESCE(ic_code, '') as ic_code, 
				       COALESCE(unit_code, '') as unit_code, 
				       COALESCE(sale_type, 0) as sale_type, 
				       COALESCE(price_0, '0') as price_0, 
				       COALESCE(price_1, '0') as price_1, 
				       COALESCE(price_2, '0') as price_2, 
				       COALESCE(price_3, '0') as price_3,
				       COALESCE(price_4, '0') as price_4, 
				       COALESCE(price_5, '0') as price_5, 
				       COALESCE(price_6, '0') as price_6, 
				       COALESCE(price_7, '0') as price_7, 
				       COALESCE(price_8, '0') as price_8, 
				       COALESCE(price_9, '0') as price_9,
				       COALESCE(tax_type, 0) as tax_type, 
				       COALESCE(price_currency, 0) as price_currency, 
				       COALESCE(currency_code, '') as currency_code
				FROM ic_inventory_price_formula 
				WHERE roworder = $1
			`
			// log queryGetData
			fmt.Printf("Executing query: %s with rowOrderRef: %d\n", queryGetData, rowOrderRef)
			row := s.db.QueryRow(queryGetData, rowOrderRef)
			var priceFormula types.PriceFormulaItem
			err := row.Scan(
				&priceFormula.RowOrderRef,
				&priceFormula.IcCode,
				&priceFormula.UnitCode,
				&priceFormula.SaleType,
				&priceFormula.Price0,
				&priceFormula.Price1,
				&priceFormula.Price2,
				&priceFormula.Price3,
				&priceFormula.Price4,
				&priceFormula.Price5,
				&priceFormula.Price6,
				&priceFormula.Price7,
				&priceFormula.Price8,
				&priceFormula.Price9,
				&priceFormula.TaxType,
				&priceFormula.PriceCurrency,
				&priceFormula.CurrencyCode,
			)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("error scanning price formula row: %v", err)
			} // แปลงเป็น map สำหรับ API
			priceFormulaMap := map[string]interface{}{
				"row_order_ref":  priceFormula.RowOrderRef,
				"ic_code":        priceFormula.IcCode,
				"unit_code":      priceFormula.UnitCode,
				"sale_type":      priceFormula.SaleType,
				"price_0":        priceFormula.Price0,
				"price_1":        priceFormula.Price1,
				"price_2":        priceFormula.Price2,
				"price_3":        priceFormula.Price3,
				"price_4":        priceFormula.Price4,
				"price_5":        priceFormula.Price5,
				"price_6":        priceFormula.Price6,
				"price_7":        priceFormula.Price7,
				"price_8":        priceFormula.Price8,
				"price_9":        priceFormula.Price9,
				"tax_type":       priceFormula.TaxType,
				"price_currency": priceFormula.PriceCurrency,
				"currency_code":  priceFormula.CurrencyCode}
			// แยกประเภทตาม active_code
			if activeCode == 1 {
				// activeCode = 1: INSERT ใหม่
				inserts = append(inserts, priceFormulaMap)
			}
			if activeCode == 2 {
				// activeCode = 2: DELETE บน server ก่อน แล้ว INSERT ใหม่ (ไม่ใช่ UPDATE)
				deletes = append(deletes, rowOrderRef)     // เพิ่มเข้า deletes เพื่อลบบน server ก่อน
				inserts = append(inserts, priceFormulaMap) // เพิ่มเข้า inserts เพื่อ insert ใหม่
			}
		} else if activeCode == 3 {
			deletes = append(deletes, rowOrderRef)
		}
	}

	return syndIds, inserts, updates, deletes, nil
}

// DeleteSyncRecordsInBatches ลบข้อมูลจาก sml_market_sync ในฐานข้อมูลท้องถิ่นแบบแบ่งเป็น batch
func (s *PriceFormulaSyncStep) DeleteSyncRecordsInBatches(syncIds []int, batchSize int) error {
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
