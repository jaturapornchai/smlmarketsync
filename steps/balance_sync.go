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
	fmt.Println("=== ซิงค์ข้อมูล balance กับ API ===")

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
	// 3. ซิงค์ข้อมูลโดยส่งทั้งหมดแบบ batch 
	fmt.Println("กำลังซิงค์ข้อมูล balance...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ\n", len(localData))

	// แสดงตัวอย่างข้อมูลรายการแรก
	if len(localData) > 0 {
		fmt.Printf("ตัวอย่างข้อมูลรายการแรก: %v\n", localData[0])
	} 
	totalCount, err := s.apiClient.SyncInventoryBalanceData(localData)
	if err != nil {
		return fmt.Errorf("error syncing balance data to API: %v", err)
	}
		


	fmt.Printf("✅ ซิงค์ข้อมูล balance เรียบร้อยแล้ว\n")
	fmt.Printf("📊 สถิติการซิงค์ balance:\n")
	fmt.Printf("   - ข้อมูลที่ซิงค์: %d รายการ (แบบ batch)\n", totalCount)

	return nil
}

// GetAllBalanceFromSource ดึงข้อมูล balance ทั้งหมดจากฐานข้อมูลต้นทาง
func (s *BalanceSyncStep) GetAllBalanceFromSource() ([]interface{}, error) {
	query := `
		SELECT 
			itd.item_code AS ic_code,
			itd.wh_code AS warehouse,
			ii.unit_standard AS ic_unit_code,
			COALESCE(SUM(itd.calc_flag * (
				CASE WHEN ((itd.trans_flag IN (70,54,60,58,310,12) OR (itd.trans_flag=66 AND itd.qty>0) OR (itd.trans_flag=14 AND itd.inquiry_type=0) OR (itd.trans_flag=48 AND itd.inquiry_type < 2)) 
						  OR (itd.trans_flag IN (56,68,72,44) OR (itd.trans_flag=66 AND itd.qty<0) OR (itd.trans_flag=46 AND itd.inquiry_type IN (0,2))  
							  OR (itd.trans_flag=16 AND itd.inquiry_type IN (0,2)) OR (itd.trans_flag=311 AND itd.inquiry_type=0)) 
						  AND NOT (itd.doc_ref <> '' AND itd.is_pos = 1))
					 THEN ROUND((itd.qty*itd.stand_value) / itd.divide_value, 2) 
					 ELSE 0 
				END)), 0) AS balance_qty
		FROM ic_trans_detail itd
		INNER JOIN ic_inventory ii ON ii.code = itd.item_code AND ii.item_type NOT IN (1,3)
		WHERE itd.last_status = 0 
		  AND itd.item_type <> 5  
		  AND itd.is_doc_copy = 0
		GROUP BY itd.item_code, itd.wh_code, ii.unit_standard
		HAVING COALESCE(SUM(itd.calc_flag * (
			CASE WHEN ((itd.trans_flag IN (70,54,60,58,310,12) OR (itd.trans_flag=66 AND itd.qty>0) OR (itd.trans_flag=14 AND itd.inquiry_type=0) OR (itd.trans_flag=48 AND itd.inquiry_type < 2)) 
					  OR (itd.trans_flag IN (56,68,72,44) OR (itd.trans_flag=66 AND itd.qty<0) OR (itd.trans_flag=46 AND itd.inquiry_type IN (0,2))  
						  OR (itd.trans_flag=16 AND itd.inquiry_type IN (0,2)) OR (itd.trans_flag=311 AND itd.inquiry_type=0)) 
					  AND NOT (itd.doc_ref <> '' AND itd.is_pos = 1))
				 THEN ROUND((itd.qty*itd.stand_value) / itd.divide_value, 2) 
				 ELSE 0 
			END)), 0) <> 0
		ORDER BY itd.item_code, itd.wh_code
	`

	fmt.Println("กำลังดึงข้อมูล balance จาก ic_trans_detail และ ic_inventory...")
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
			"warehouse":    balance.Warehouse, // Field name in API is 'warehouse'
			"ic_unit_code": balance.UnitCode,  // Field name in API is 'ic_unit_code'
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

	// ตรวจสอบโครงสร้างข้อมูลก่อนส่งกลับ
	if len(balances) > 0 {
		sampleItem := balances[0]
		fmt.Printf("โครงสร้างข้อมูล balance ที่จะส่งไป API (ตัวอย่างรายการแรก): %+v\n", sampleItem)
	}

	return balances, nil
}
