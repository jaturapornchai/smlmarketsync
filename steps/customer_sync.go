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

	// 3. ดึงข้อมูลลูกค้าที่มีอยู่จาก API (สำหรับสถิติ)
	fmt.Println("กำลังดึงข้อมูลลูกค้าที่มีอยู่จาก API...")
	existingData, err := s.apiClient.GetExistingCustomerData()
	if err != nil {
		return fmt.Errorf("error getting existing customer data: %v", err)
	}
	fmt.Printf("พบข้อมูลลูกค้าใน API อยู่แล้ว %d รายการ\n", len(existingData))

	// 4. ซิงค์ข้อมูลโดยส่งทั้งหมดแบบ batch UPSERT
	fmt.Println("กำลังเปรียบเทียบและซิงค์ข้อมูลลูกค้า (batch UPSERT)...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ โดยใช้ batch UPSERT\n", len(localData))

	insertCount, updateCount, err := s.apiClient.SyncCustomerData(localData, existingData)
	if err != nil {
		return fmt.Errorf("error syncing customer data: %v", err)
	}

	fmt.Printf("✅ ซิงค์ข้อมูลลูกค้าเรียบร้อยแล้ว (batch UPSERT)\n")
	fmt.Printf("📊 สถิติการซิงค์ลูกค้า:\n")
	fmt.Printf("   - ข้อมูลใน local: %d รายการ\n", len(localData))
	fmt.Printf("   - Insert ใหม่: %d รายการ (แบบ batch)\n", insertCount)
	fmt.Printf("   - Update ที่มีอยู่: %d รายการ (แบบ batch)\n", updateCount)
	fmt.Printf("   - ไม่เปลี่ยนแปลง: %d รายการ\n", len(localData)-insertCount-updateCount)

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

	for rows.Next() {
		var customer types.CustomerItem
		err := rows.Scan(
			&customer.Code,
			&customer.PriceLevel,
		)
		if err != nil {
			fmt.Printf("⚠️ ข้ามรายการที่อ่านไม่ได้: %v\n", err)
			continue
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

	fmt.Printf("ดึงข้อมูลลูกค้าจากฐานข้อมูลต้นทางได้ %d รายการ\n", count)
	return customers, nil
}
