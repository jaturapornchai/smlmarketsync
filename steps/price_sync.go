package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"strconv"
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
	fmt.Println("=== ขั้นตอนที่ 7: ซิงค์ข้อมูลราคาสินค้ากับ API ===")	// 1. ตรวจสอบและสร้างตาราง ic_inventory_price
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory_price บน API...")
	err := s.apiClient.CreatePriceTable()
	if err != nil {
		return fmt.Errorf("error creating price table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory_price เรียบร้อยแล้ว")

	// 2. ดึงข้อมูลราคาสินค้าจาก local database
	fmt.Println("กำลังดึงข้อมูลราคาสินค้าจากฐานข้อมูล local...")
	localData, err := s.GetAllPricesFromSource()
	if err != nil {
		return fmt.Errorf("error getting local price data: %v", err)
	}

	if len(localData) == 0 {
		fmt.Println("ไม่มีข้อมูลราคาสินค้าใน local database")
		return nil
	}

	fmt.Printf("ดึงข้อมูลราคาสินค้าจาก local ได้ %d รายการ\n", len(localData))	// 3. ดึงข้อมูลราคาสินค้าที่มีอยู่จาก API (สำหรับสถิติ)
	fmt.Println("กำลังดึงข้อมูลราคาสินค้าที่มีอยู่จาก API...")
	existingData, err := s.apiClient.GetExistingPriceData()
	if err != nil {
		return fmt.Errorf("error getting existing price data: %v", err)
	}
	fmt.Printf("พบข้อมูลราคาสินค้าใน API อยู่แล้ว %d รายการ\n", len(existingData))	// 4. ซิงค์ข้อมูลโดยส่งทั้งหมดแบบ batch UPSERT
	fmt.Println("กำลังเปรียบเทียบและซิงค์ข้อมูลราคาสินค้า (batch UPSERT)...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ โดยใช้ batch UPSERT\n", len(localData))

	insertCount, updateCount, err := s.apiClient.SyncPriceData(localData, existingData)
	if err != nil {
		return fmt.Errorf("error syncing price data: %v", err)
	}

	fmt.Printf("✅ ซิงค์ข้อมูลราคาสินค้าเรียบร้อยแล้ว (batch UPSERT)\n")
	fmt.Printf("📊 สถิติการซิงค์ราคาสินค้า:\n")
	fmt.Printf("   - ข้อมูลใน local: %d รายการ\n", len(localData))
	fmt.Printf("   - Insert ใหม่: %d รายการ (แบบ batch)\n", insertCount)
	fmt.Printf("   - Update ที่มีอยู่: %d รายการ (แบบ batch)\n", updateCount)
	fmt.Printf("   - ไม่เปลี่ยนแปลง: %d รายการ\n", len(localData)-insertCount-updateCount)

	return nil
}

// GetAllPricesFromSource ดึงข้อมูลราคาสินค้าทั้งหมดจากฐานข้อมูลต้นทาง
func (s *PriceSyncStep) GetAllPricesFromSource() ([]interface{}, error) {
	query := `
		SELECT 
			ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
			sale_type, sale_price1, status, price_type, cust_code, 
			sale_price2, cust_group_1, cust_group_2, price_mode
		FROM ic_inventory_price
		WHERE ic_code IS NOT NULL AND ic_code != ''
		ORDER BY ic_code, unit_code, from_qty
	`

	fmt.Println("กำลังดึงข้อมูลราคาสินค้าจาก ic_inventory_price...")
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing price query: %v", err)
	}
	defer rows.Close()

	var prices []interface{}
	count := 0

	for rows.Next() {
		var price types.PriceItem
		var fromQtyStr, toQtyStr, salePrice1Str, salePrice2Str sql.NullString
		var fromDate, toDate sql.NullString

		err := rows.Scan(
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
			&price.CustGroup2,
			&price.PriceMode,
		)
		if err != nil {
			fmt.Printf("⚠️ ข้ามรายการที่อ่านไม่ได้: %v\n", err)
			continue
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
			"ic_code":      price.IcCode,
			"unit_code":    price.UnitCode,
			"from_qty":     price.FromQty,
			"to_qty":       price.ToQty,
			"from_date":    price.FromDate,
			"to_date":      price.ToDate,
			"sale_type":    price.SaleType,
			"sale_price1":  price.SalePrice1,
			"status":       price.Status,
			"price_type":   price.PriceType,
			"cust_code":    price.CustCode,
			"sale_price2":  price.SalePrice2,
			"cust_group_1": price.CustGroup1,
			"cust_group_2": price.CustGroup2,
			"price_mode":   price.PriceMode,
		}

		prices = append(prices, priceMap)
		count++

		// แสดงความคืบหน้าทุกๆ 1000 รายการ
		if count%1000 == 0 {
			fmt.Printf("ดึงข้อมูลราคาสินค้าแล้ว %d รายการ...\n", count)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating price rows: %v", err)
	}

	fmt.Printf("ดึงข้อมูลราคาสินค้าจากฐานข้อมูลต้นทางได้ %d รายการ\n", count)
	return prices, nil
}
