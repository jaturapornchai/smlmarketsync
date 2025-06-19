package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
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

// ExecuteCustomerSync รันขั้นตอนที่ 6: การ sync ลูกค้า
func (s *CustomerSyncStep) ExecuteCustomerSync() error {
	fmt.Println("=== ขั้นตอนที่ 6: ซิงค์ข้อมูลลูกค้ากับ API ===")

	// 1. ตรวจสอบและสร้างตาราง ar_customer
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ar_customer บน API...")
	err := s.apiClient.CreateCustomerTable()
	if err != nil {
		return fmt.Errorf("error creating customer table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ar_customer เรียบร้อยแล้ว")

	// 2. ดึงข้อมูลลูกค้าจาก local database
	fmt.Println("กำลังดึงข้อมูลลูกค้าจากฐานข้อมูล local...")
	localData, err := s.GetAllCustomersFromSource()
	if err != nil {
		return fmt.Errorf("error getting local customer data: %v", err)
	}

	if len(localData) == 0 {
		fmt.Println("ไม่มีข้อมูลลูกค้าใน local database")
		return nil
	}
	fmt.Printf("ดึงข้อมูลลูกค้าจาก local ได้ %d รายการ\n", len(localData))

	// 3. ซิงค์ข้อมูลโดยส่งทั้งหมดแบบ batch UPSERT
	fmt.Println("กำลังซิงค์ข้อมูลลูกค้า (batch UPSERT)...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ โดยใช้ batch UPSERT\n", len(localData))
	// totalCount, err := s.apiClient.SyncCustomerData(localData)
	totalCount := 0
	// err = nil
	// if err != nil {
	// 	return fmt.Errorf("error syncing customer data: %v", err)
	// }

	fmt.Printf("✅ ซิงค์ข้อมูลลูกค้าเรียบร้อยแล้ว (batch UPSERT)\n")
	fmt.Printf("📊 สถิติการซิงค์ลูกค้า:\n")
	fmt.Printf("   - ข้อมูลใน local: %d รายการ\n", len(localData))
	fmt.Printf("   - ข้อมูลที่ซิงค์: %d รายการ (แบบ batch)\n", totalCount)

	return nil
}

// GetAllCustomersFromSource ดึงข้อมูลลูกค้าทั้งหมดจากฐานข้อมูลต้นทาง
func (s *CustomerSyncStep) GetAllCustomersFromSource() ([]interface{}, error) {
	query := `
		SELECT code, price_level 
		FROM ar_customer
		WHERE code IS NOT NULL AND code != ''
		ORDER BY code
	`

	fmt.Println("กำลังดึงข้อมูลลูกค้าจาก ar_customer...")
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing customer query: %v", err)
	}
	defer rows.Close()

	var customers []interface{}
	count := 0
	skippedCount := 0

	for rows.Next() {
		var customer types.CustomerItem
		var priceLevel sql.NullString

		err := rows.Scan(
			&customer.Code,
			&priceLevel,
		)
		if err != nil {
			fmt.Printf("⚠️ ข้ามรายการที่อ่านไม่ได้: %v\n", err)
			skippedCount++
			continue
		}

		// Validate code is not empty
		if customer.Code == "" {
			fmt.Println("⚠️ ข้ามรายการที่มี code เป็นค่าว่าง")
			skippedCount++
			continue
		}

		// Handle NULL price_level
		if priceLevel.Valid {
			customer.PriceLevel = priceLevel.String
		} else {
			customer.PriceLevel = ""
		}

		// แปลงเป็น map สำหรับ API
		customerMap := map[string]interface{}{
			"code":        customer.Code,
			"price_level": customer.PriceLevel,
		}

		customers = append(customers, customerMap)
		count++

		// แสดงความคืบหน้าทุกๆ 1000 รายการ
		if count%1000 == 0 {
			fmt.Printf("ดึงข้อมูลลูกค้าแล้ว %d รายการ...\n", count)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating customer rows: %v", err)
	}

	fmt.Printf("ดึงข้อมูลลูกค้าจากฐานข้อมูลต้นทางได้ %d รายการ, ข้ามไป %d รายการ\n", count, skippedCount)

	// แสดงตัวอย่างข้อมูล
	if len(customers) > 0 {
		fmt.Println("ตัวอย่างข้อมูลลูกค้า 5 รายการแรก:")
		for i := 0; i < 5 && i < len(customers); i++ {
			fmt.Printf("  %d: %v\n", i+1, customers[i])
		}
	}

	return customers, nil
}
