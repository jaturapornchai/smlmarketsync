package steps

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"smlmarketsync/types"
	"strconv"
)

type BalanceSyncStep struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewBalanceSyncStep(db *sql.DB) *BalanceSyncStep {
	return &BalanceSyncStep{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// ExecuteBalanceSync รันขั้นตอนที่ 5: การ sync balance
func (s *BalanceSyncStep) ExecuteBalanceSync() error {
	fmt.Println("=== ขั้นตอนที่ 5: ซิงค์ข้อมูล balance กับ API ===")

	// 1. ตรวจสอบและสร้างตาราง ic_balance
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_balance บน API...")
	err := s.apiClient.CreateBalanceTable()
	if err != nil {
		return fmt.Errorf("error creating balance table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_balance เรียบร้อยแล้ว")

	// 2. ดึงข้อมูล balance จาก local database
	fmt.Println("กำลังดึงข้อมูล balance จากฐานข้อมูล local...")
	localData, err := s.GetAllBalanceFromSource()
	if err != nil {
		return fmt.Errorf("error getting local balance data: %v", err)
	}

	if len(localData) == 0 {
		fmt.Println("ไม่มีข้อมูล balance ใน local database")
		return nil
	}

	fmt.Printf("ดึงข้อมูล balance จาก local ได้ %d รายการ\n", len(localData))

	// 3. ดึงข้อมูล balance ที่มีอยู่จาก API (สำหรับสถิติ)
	fmt.Println("กำลังดึงข้อมูล balance ที่มีอยู่จาก API...")
	existingData, err := s.apiClient.GetExistingBalanceData()
	if err != nil {
		return fmt.Errorf("error getting existing balance data: %v", err)
	}
	fmt.Printf("พบข้อมูล balance ใน API อยู่แล้ว %d รายการ\n", len(existingData))

	// 4. ซิงค์ข้อมูลโดยส่งทั้งหมดแบบ batch UPSERT
	fmt.Println("กำลังเปรียบเทียบและซิงค์ข้อมูล balance (เปรียบเทียบ memory)...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ โดยเปรียบเทียบกับ API data ใน memory\n", len(localData))

	insertCount, updateCount, err := s.apiClient.SyncBalanceData(localData, existingData)
	if err != nil {
		return fmt.Errorf("error syncing balance data: %v", err)
	}

	fmt.Printf("✅ ซิงค์ข้อมูล balance เรียบร้อยแล้ว (เปรียบเทียบ memory + batch operations)\n")
	fmt.Printf("📊 สถิติการซิงค์ balance:\n")
	fmt.Printf("   - ข้อมูลใน local: %d รายการ\n", len(localData))
	fmt.Printf("   - Insert ใหม่: %d รายการ (แบบ batch)\n", insertCount)
	fmt.Printf("   - Update ที่มีอยู่: %d รายการ (แบบ batch)\n", updateCount)
	fmt.Printf("   - ไม่เปลี่ยนแปลง: %d รายการ\n", len(localData)-insertCount-updateCount)

	return nil
}

// GetAllBalanceFromSource ดึงข้อมูล balance ทั้งหมดจากฐานข้อมูลต้นทาง
func (s *BalanceSyncStep) GetAllBalanceFromSource() ([]interface{}, error) {
	query := `
		SELECT ic_code, warehouse, ic_unit_code, balance_qty 
		FROM ic_balance
		WHERE ic_code IS NOT NULL AND ic_code != '' 
		  AND warehouse IS NOT NULL AND warehouse != ''
		  AND ic_unit_code IS NOT NULL AND ic_unit_code != ''
		ORDER BY ic_code, warehouse, ic_unit_code
	`

	fmt.Println("กำลังดึงข้อมูล balance จาก ic_balance...")
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing balance query: %v", err)
	}
	defer rows.Close()

	var balances []interface{}
	count := 0

	for rows.Next() {
		var balance types.BalanceItem
		var balanceQtyStr string

		err := rows.Scan(
			&balance.IcCode,
			&balance.Warehouse,
			&balance.UnitCode,
			&balanceQtyStr,
		)
		if err != nil {
			fmt.Printf("⚠️ ข้ามรายการที่อ่านไม่ได้: %v\n", err)
			continue
		}

		// แปลง balance_qty จาก string เป็น float64
		balanceQty, err := strconv.ParseFloat(balanceQtyStr, 64)
		if err != nil {
			fmt.Printf("⚠️ ข้ามรายการที่แปลง balance_qty ไม่ได้: %s -> %v\n", balanceQtyStr, err)
			continue
		}
		balance.BalanceQty = balanceQty

		// แปลงเป็น map สำหรับ API
		balanceMap := map[string]interface{}{
			"ic_code":      balance.IcCode,
			"warehouse":    balance.Warehouse,
			"ic_unit_code": balance.UnitCode,
			"balance_qty":  balance.BalanceQty,
		}

		balances = append(balances, balanceMap)
		count++

		// แสดงความคืบหน้าทุกๆ 2000 รายการ
		if count%2000 == 0 {
			fmt.Printf("ดึงข้อมูล balance แล้ว %d รายการ...\n", count)
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating balance rows: %v", err)
	}

	fmt.Printf("ดึงข้อมูล balance จากฐานข้อมูลต้นทางได้ %d รายการ\n", count)
	return balances, nil
}
