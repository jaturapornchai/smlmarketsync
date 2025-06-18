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

	s.apiClient.SyncPriceData(syncIds,inserts, updates, deletes)
	if err != nil {
		return fmt.Errorf("error syncing price data: %v", err)
	}


	return nil
}

// GetAllPricesFromSource ดึงข้อมูลราคาสินค้าทั้งหมดจากฐานข้อมูลต้นทาง
func (s *PriceSyncStep) GetAllPricesFromSource() ([]interface{}, error) {
	var syndIds []int
	var deletes []interface{}
	var inserts []interface{}
	var updates []interface{}
	
	querySync := "SELECT id,row_order_ref,active_code FROM sml_market_sync order by active_code desc"

	rowsSync, errSync := s.db.Query(query)
	if errSync != nil {
		return nil, fmt.Errorf("error executing sync query: %v", errSync)
	}
	defer rowsSync.Close()

	for rowsSync.Next() {
		var id, rowOrderRef, activeCode int
		err := rowsSync.Scan(&id, &rowOrderRef, &activeCode)
		if err != nil {
			return nil, fmt.Errorf("error scanning sync row: %v", err)
		}
		syndIds = append(syndIds, id)

		if (activeCode < 1 || activeCode > 2) {
			// ดึงข้อมูลดิบ
			queryGetData := `
				SELECT roworder,ic_code, unit_code, from_qty, to_qty, from_date, to_date, 
					sale_type, sale_price1, status, price_type, cust_code, 
					sale_price2, cust_group_1, price_mode
				FROM ic_inventory_price 
				WHERE roworder = $1
			`
			row := s.db.QueryRow(queryGetData, rowOrderRef)
			var price types.PriceItem
			var fromQtyStr, toQtyStr, salePrice1Str, salePrice2Str sql.NullString
			var fromDate, toDate sql.NullString
			err := row.Scan(
				&price.rowOrderRef,
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
				return nil, fmt.Errorf("error scanning price row: %v", err)
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
				"row_order_ref": price.rowOrderRef,
				"ic_code":     price.IcCode,
				"unit_code":   price.UnitCode,
				"from_qty":    price.FromQty,
				"to_qty":      price.ToQty,
				"from_date":   price.FromDate,
				"to_date":     price.ToDate,
				"sale_type":   price.SaleType,
				"sale_price1": price.SalePrice1,
				"status":      price.Status,
				"price_type":  price.PriceType,
				"cust_code":   price.CustCode,
				"sale_price2": price.SalePrice2,
				"cust_group_1": price.CustGroup1,
				"price_mode":   price.PriceMode,
			}
			// แยกประเภทตาม active_code
			if activeCode == 1 {
				inserts = append(inserts, priceMap)
			}
			if activeCode == 2 {
				updates = append(updates, priceMap)
			}
		} else if (activeCode == 3) {
			deletes = append(deletes, rowOrderRef)
		}	
	}

	return syndIds, inserts, updates, deletes, nil
}
