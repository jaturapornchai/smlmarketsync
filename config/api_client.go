/*
API Client Library for SML Market Sync

Table of Contents:
================
1. Core HTTP Client & Basic Types
   - APIClient struct & New function
   - QueryRequest/QueryResponse types
   - ExecuteSelect/ExecuteCommand/executeQuery

2. Database Utility Functions
   - CheckTableExists
   - DropTable

3. Table Creation Functions
   - CreateInventoryBarcodeTable
   - CreateBalanceTable
   - CreateCustomerTable
   - CreateInventoryTable

4. Product/Inventory Sync Functions
   - SyncInventoryBarcodeData
   - GetSyncStatistics
   - SyncInventoryTableData
   - getInventoryCount
   - SyncInventoryData
   - SyncProductBarcodeData
   - SyncPriceData

5. Balance Sync Functions
   - SyncBalanceData
   - executeBatchUpsertBalance
   - insertSingleBalance/updateSingleBalance/upsert            insertQuery := fmt.Sprintf("INSERT INTO ic_balance (ic_code, wh_code, unit_code, balance_qty) VALUES ('%s', '%s', '%s', %s)", icCode, whCode, unitCode, balanceQty)leBalance

6. Customer Sync Functions
   - GetExistingCustomerData
   - SyncCustomerData
   - executeBatchUpsertCustomer

7. Utility/Helper Functions
   - executeBatchInsert
   - executeBatchUpsert
   - executeBatchUpsertForUpdate
*/

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ================================================================================
// 1. CORE HTTP CLIENT & BASIC TYPES
// ================================================================================

const (
	APIBaseURL      = "http://192.168.2.36:8008/v1"
	SelectEndpoint  = "/pgselect"
	CommandEndpoint = "/pgcommand"
)

type APIClient struct {
	client  *http.Client
	baseURL string
}

type QueryRequest struct {
	Query string `json:"query"`
}

type QueryResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
	Message string      `json:"message"`
	Error   string      `json:"error,omitempty"`
}

func NewAPIClient() *APIClient {
	return &APIClient{
		client: &http.Client{
			Timeout: 120 * time.Second, // เพิ่มเป็น 2 นาที สำหรับ batch ขนาดใหญ่
		},
		baseURL: APIBaseURL,
	}
}

// ExecuteSelect ทำการ SELECT query ผ่าน API
func (api *APIClient) ExecuteSelect(query string) (*QueryResponse, error) {
	return api.executeQuery(query, SelectEndpoint)
}

// ExecuteCommand ทำการ execute command (INSERT, UPDATE, DELETE, CREATE, DROP, etc.) ผ่าน API
func (api *APIClient) ExecuteCommand(query string) (*QueryResponse, error) {
	return api.executeQuery(query, CommandEndpoint)
}

func (api *APIClient) executeQuery(query string, endpoint string) (*QueryResponse, error) {
	reqBody := QueryRequest{Query: query}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	url := api.baseURL + endpoint
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// แสดงข้อมูล URL ที่กำลังเรียก
	fmt.Printf("กำลังส่งคำขอไปยัง URL: %s\n", url)

	resp, err := api.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request to %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	// แสดงตัวอย่าง response body สำหรับ debug
	bodySample := string(body)
	if len(bodySample) > 500 {
		bodySample = bodySample[:500] + "..."
	}
	fmt.Printf("ได้รับการตอบกลับ: %s\n", bodySample)

	var response QueryResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %v\nResponse body: %s", err, bodySample)
	}

	if resp.StatusCode != http.StatusOK {
		return &response, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, response.Message)
	}
	return &response, nil
}

// ================================================================================
// 2. DATABASE UTILITY FUNCTIONS
// ================================================================================

// CheckTableExists ตรวจสอบว่าตารางมีอยู่หรือไม่
func (api *APIClient) CheckTableExists(tableName string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '%s')", tableName)

	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return false, err
	}

	if !resp.Success {
		return false, fmt.Errorf("failed to check if table exists: %s", resp.Message)
	}

	if data, ok := resp.Data.([]interface{}); ok && len(data) > 0 {
		if row, ok := data[0].(map[string]interface{}); ok {
			if exists, ok := row["exists"].(bool); ok {
				return exists, nil
			}
		}
	}

	return false, fmt.Errorf("unexpected response format when checking if table exists")
}

// DropTable ลบตารางถ้ามีอยู่
func (api *APIClient) DropTable(tableName string) error {
	exists, err := api.CheckTableExists(tableName)
	if err != nil {
		return err
	}

	if !exists {
		return nil // ไม่มีตารางนี้อยู่แล้ว ถือว่าสำเร็จ
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to drop table: %s", resp.Message)
	}

	return nil
}

// ================================================================================
// 3. TABLE CREATION FUNCTIONS
// ================================================================================

// CreateInventoryTable สร้างตารางสำหรับเก็บข้อมูลสินค้า
func (api *APIClient) CreateInventoryTable() error {
	query := `CREATE TABLE IF NOT EXISTS ic_inventory (
		code VARCHAR(50) NOT NULL,
		name VARCHAR(200),
		unit_standard_code VARCHAR(50),
		item_type int DEFAULT 0, 
		row_order_ref INT DEFAULT 0,
		PRIMARY KEY (code)
	)`

	response, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("failed to create inventory table: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed to create inventory table: %s", response.Message)
	}

	return nil
}

// CreateInventoryBarcodeTable สร้างตารางหลักสำหรับเก็บข้อมูลสินค้าและบาร์โค้ด
func (api *APIClient) CreateInventoryBarcodeTable() error {
	// ตรวจสอบว่ามีตารางนี้อยู่แล้วหรือไม่
	exists, err := api.CheckTableExists("ic_inventory_barcode")
	if err != nil {
		return err
	}

	// ถ้ามีตารางอยู่แล้ว ไม่ต้องทำอะไร
	if exists {
		return nil
	}

	query := `	CREATE TABLE IF NOT EXISTS ic_inventory_barcode (
		ic_code VARCHAR(50) NOT NULL,
		barcode VARCHAR(100) NOT NULL,
		name VARCHAR(200),
		unit_code VARCHAR(50),
		unit_name VARCHAR(100),
		row_order_ref INT DEFAULT 0,
		PRIMARY KEY (barcode)
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create inventory barcode table: %s", resp.Message)
	}

	return nil
}

// CreateBalanceTable สร้างตารางสำหรับเก็บข้อมูล stock balance
func (api *APIClient) CreateBalanceTable() error {
	// ตรวจสอบว่ามีตารางนี้อยู่แล้วหรือไม่
	exists, err := api.CheckTableExists("ic_balance")
	if err != nil {
		return err
	}

	// ถ้ามีตารางอยู่แล้ว ไม่ต้องทำอะไร
	if exists {
		return nil
	}
	query := `
	CREATE TABLE IF NOT EXISTS ic_balance (
		ic_code VARCHAR(50) NOT NULL,
		wh_code VARCHAR(50) NOT NULL,
		unit_code VARCHAR(50) NOT NULL,
		balance_qty NUMERIC(18,3) DEFAULT 0,
		PRIMARY KEY (ic_code, wh_code, unit_code)
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create balance table: %s", resp.Message)
	}

	return nil
}

// CreateCustomerTable สร้างตารางสำหรับเก็บข้อมูลลูกค้า
func (api *APIClient) CreateCustomerTable() error {
	// ตรวจสอบว่ามีตารางนี้อยู่แล้วหรือไม่
	exists, err := api.CheckTableExists("ar_customer")
	if err != nil {
		return err
	}

	// ถ้ามีตารางอยู่แล้ว ไม่ต้องทำอะไร
	if exists {
		return nil
	}
	query := `
	CREATE TABLE IF NOT EXISTS ar_customer (
		code VARCHAR(50) NOT NULL,
		price_level VARCHAR(50),
		row_order_ref INT DEFAULT 0,
		PRIMARY KEY (code)
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create customer table: %s", resp.Message)
	}

	return nil
}

// GetSyncStatistics คืนค่าสถิติการซิงค์ข้อมูล
func (api *APIClient) GetSyncStatistics() (int, int, error) { // จำนวนในตาราง
	queryTemp := "SELECT COUNT(*) AS count FROM ic_inventory_barcode"
	respTemp, err := api.ExecuteSelect(queryTemp)
	if err != nil {
		return 0, 0, err
	}

	tempCount := 0
	if data, ok := respTemp.Data.([]interface{}); ok && len(data) > 0 {
		if row, ok := data[0].(map[string]interface{}); ok {
			if count, ok := row["count"].(float64); ok {
				tempCount = int(count)
			}
		}
	}

	// จำนวนในตารางหลัก
	totalCount, err := api.getInventoryCount()
	if err != nil {
		return 0, 0, err
	}

	return tempCount, totalCount, nil
}

// getInventoryCount นับจำนวนข้อมูลในตาราง ic_inventory_barcode
func (api *APIClient) getInventoryCount() (int, error) {
	query := "SELECT COUNT(*) AS count FROM ic_inventory_barcode"
	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return 0, err
	}

	if data, ok := resp.Data.([]interface{}); ok && len(data) > 0 {
		if row, ok := data[0].(map[string]interface{}); ok {
			if count, ok := row["count"].(float64); ok {
				return int(count), nil
			}
		}
	}

	return 0, fmt.Errorf("failed to get inventory count")
}

// SyncProductBarcodeData ซิงค์ข้อมูล ProductBarcode จาก local ไปยัง API
func (api *APIClient) SyncProductBarcodeData(syncIds []int, inserts []interface{}, updates []interface{}, deletes []interface{}) error {
	fmt.Printf("=== เริ่มซิงค์ข้อมูล ProductBarcode: %d inserts, %d updates, %d deletes ===\n",
		len(inserts), len(updates), len(deletes))

	// Handle deletes first
	if len(deletes) > 0 {
		fmt.Printf("🗑️ กำลังลบข้อมูล ProductBarcode %d รายการ...\n", len(deletes))
		err := api.executeBatchDeleteProductBarcode(deletes)
		if err != nil {
			return fmt.Errorf("error deleting ProductBarcode data: %v", err)
		}
		fmt.Printf("✅ ลบข้อมูล ProductBarcode เรียบร้อยแล้ว\n")
	}

	// Handle inserts
	if len(inserts) > 0 {
		fmt.Printf("📝 กำลังเพิ่มข้อมูล ProductBarcode %d รายการ...\n", len(inserts))
		err := api.executeBatchInsertProductBarcode(inserts)
		if err != nil {
			return fmt.Errorf("error inserting ProductBarcode data: %v", err)
		}
		fmt.Printf("✅ เพิ่มข้อมูล ProductBarcode เรียบร้อยแล้ว\n")
	}

	fmt.Println("✅ ซิงค์ข้อมูล ProductBarcode เสร็จสิ้น")
	return nil
}

// executeBatchInsertProductBarcode เพิ่มข้อมูล ProductBarcode แบบ batch
func (api *APIClient) executeBatchInsertProductBarcode(inserts []interface{}) error {
	if len(inserts) == 0 {
		return nil
	}
	var values []string
	for _, item := range inserts {
		if itemMap, ok := item.(map[string]interface{}); ok {
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			barcode := fmt.Sprintf("%v", itemMap["barcode"])
			name := fmt.Sprintf("%v", itemMap["name"])
			unitCode := fmt.Sprintf("%v", itemMap["unit_code"])
			unitName := fmt.Sprintf("%v", itemMap["unit_name"])
			rowOrderRef := fmt.Sprintf("%v", itemMap["row_order_ref"])

			// Escape single quotes
			name = strings.ReplaceAll(name, "'", "''")
			unitName = strings.ReplaceAll(unitName, "'", "''")

			value := fmt.Sprintf("('%s', '%s', '%s', '%s', '%s', %s)",
				icCode, barcode, name, unitCode, unitName, rowOrderRef)
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
		INSERT INTO ic_inventory_barcode (ic_code, barcode, name, unit_code, unit_name, row_order_ref)
		VALUES %s
	`, strings.Join(values, ","))

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error executing batch insert ProductBarcode: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("batch insert ProductBarcode failed: %s", resp.Message)
	}
	return nil
}

// executeBatchDeleteProductBarcode ลบข้อมูล ProductBarcode แบบ batch
func (api *APIClient) executeBatchDeleteProductBarcode(deletes []interface{}) error {
	if len(deletes) == 0 {
		return nil
	}

	fmt.Printf("🗑️ กำลังลบข้อมูล ProductBarcode %d รายการ...\n", len(deletes))

	// แบ่งเป็น batch เพื่อป้องกัน query ยาวเกินไป
	batchSize := 100
	totalDeleted := 0

	for i := 0; i < len(deletes); i += batchSize {
		end := i + batchSize
		if end > len(deletes) {
			end = len(deletes)
		}

		currentBatch := deletes[i:end]
		var rowOrderRefs []string

		// สร้างรายการ row_order_ref สำหรับลบ
		for _, item := range currentBatch {
			rowOrderRef := fmt.Sprintf("%v", item)
			rowOrderRefs = append(rowOrderRefs, rowOrderRef)
		}

		if len(rowOrderRefs) > 0 {
			query := fmt.Sprintf(`
				DELETE FROM ic_inventory_barcode 
				WHERE row_order_ref IN (%s)
			`, strings.Join(rowOrderRefs, ","))

			resp, err := api.ExecuteCommand(query)
			if err != nil {
				fmt.Printf("⚠️ Warning: ไม่สามารถลบข้อมูล ProductBarcode batch ได้: %v\n", err)
				continue
			}

			if !resp.Success {
				fmt.Printf("⚠️ Warning: ลบข้อมูล ProductBarcode batch ล้มเหลว: %s\n", resp.Message)
				continue
			}

			totalDeleted += len(rowOrderRefs)
			fmt.Printf("   ✅ ลบข้อมูล ProductBarcode batch สำเร็จ: %d รายการ\n", len(rowOrderRefs))
		}

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if end < len(deletes) {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("✅ ลบข้อมูล ProductBarcode เรียบร้อยแล้ว: %d รายการ\n", totalDeleted)
	return nil
}

// SyncCustomerData ซิงค์ข้อมูลลูกค้าจาก local ไปยัง API
func (api *APIClient) SyncCustomerData(inserts []interface{}, updates []interface{}, deletes []interface{}) error {
	fmt.Printf("=== เริ่มซิงค์ข้อมูลลูกค้า: %d inserts, %d updates, %d deletes ===\n",
		len(inserts), len(updates), len(deletes))

	// Handle deletes first
	if len(deletes) > 0 {
		fmt.Printf("🗑️ กำลังลบข้อมูลลูกค้า %d รายการ...\n", len(deletes))
		err := api.executeBatchDeleteCustomer(deletes)
		if err != nil {
			return fmt.Errorf("error deleting customer data: %v", err)
		}
		fmt.Printf("✅ ลบข้อมูลลูกค้าเรียบร้อยแล้ว\n")
	}

	// Handle inserts
	if len(inserts) > 0 {
		fmt.Printf("📝 กำลังเพิ่มข้อมูลลูกค้า %d รายการ...\n", len(inserts))
		err := api.executeBatchInsertCustomer(inserts)
		if err != nil {
			return fmt.Errorf("error inserting customer data: %v", err)
		}
		fmt.Printf("✅ เพิ่มข้อมูลลูกค้าเรียบร้อยแล้ว\n")
	}

	fmt.Println("✅ ซิงค์ข้อมูลลูกค้าเสร็จสิ้น")
	return nil
}

// executeBatchInsertCustomer เพิ่มข้อมูลลูกค้าแบบ batch
func (api *APIClient) executeBatchInsertCustomer(inserts []interface{}) error {
	if len(inserts) == 0 {
		return nil
	}
	var values []string
	for _, item := range inserts {
		if itemMap, ok := item.(map[string]interface{}); ok {
			code := fmt.Sprintf("%v", itemMap["code"])
			priceLevel := fmt.Sprintf("%v", itemMap["price_level"])
			rowOrderRef := fmt.Sprintf("%v", itemMap["row_order_ref"])
			// Escape single quotes
			priceLevel = strings.ReplaceAll(priceLevel, "'", "''")
			// สร้างค่า value สำหรับ insert
			// ใช้ row_order_ref เป็น key ในการ insert
			if rowOrderRef == "" {
				return fmt.Errorf("row_order_ref is required")
			}
			if code == "" {
				return fmt.Errorf("code is required")
			}
			if priceLevel == "" {
				return fmt.Errorf("price_level is required")
			}
			value := fmt.Sprintf("('%s', '%s', '%s')", code, priceLevel, rowOrderRef)
			values = append(values, value)
		}
	}
	if len(values) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
		INSERT INTO ar_customer (code, price_level, row_order_ref)
		VALUES %s
	`, strings.Join(values, ","))

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error executing batch insert customer: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("batch insert customer failed: %s", resp.Message)
	}
	return nil
}

// executeBatchDeleteCustomer ลบข้อมูลลูกค้าแบบ batch
func (api *APIClient) executeBatchDeleteCustomer(deletes []interface{}) error {
	if len(deletes) == 0 {
		return nil
	}

	fmt.Printf("🗑️ กำลังลบข้อมูลลูกค้า %d รายการ...\n", len(deletes))

	// แบ่งเป็น batch เพื่อป้องกัน query ยาวเกินไป
	batchSize := 100
	totalDeleted := 0

	for i := 0; i < len(deletes); i += batchSize {
		end := i + batchSize
		if end > len(deletes) {
			end = len(deletes)
		}

		currentBatch := deletes[i:end]
		var rowOrderRefs []string

		// สร้างรายการ code สำหรับลบ
		for _, item := range currentBatch {
			code := fmt.Sprintf("'%v'", item) // ใช้ code เป็น key ในการลบ
			rowOrderRefs = append(rowOrderRefs, code)
		}

		if len(rowOrderRefs) > 0 {
			query := fmt.Sprintf(`
				DELETE FROM ar_customer 
				WHERE row_order_ref IN (%s)
			`, strings.Join(rowOrderRefs, ","))

			resp, err := api.ExecuteCommand(query)
			if err != nil {
				fmt.Printf("⚠️ Warning: ไม่สามารถลบข้อมูลลูกค้า batch ได้: %v\n", err)
				continue
			}

			if !resp.Success {
				fmt.Printf("⚠️ Warning: ลบข้อมูลลูกค้า batch ล้มเหลว: %s\n", resp.Message)
				continue
			}

			totalDeleted += len(rowOrderRefs)
			fmt.Printf("   ✅ ลบข้อมูลลูกค้า batch สำเร็จ: %d รายการ\n", len(rowOrderRefs))
		}

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if end < len(deletes) {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("✅ ลบข้อมูลลูกค้าเรียบร้อยแล้ว: %d รายการ\n", totalDeleted)
	return nil
}

func (api *APIClient) SyncInventoryBalanceData(data []interface{}) (int, error) {
	fmt.Printf("🔄 กำลัง sync ข้อมูล balance %d รายการ\n", len(data))

	// ดึงข้อมูลเดิมจาก server มาไว้ใน memory ใช้ API (แบบแบ่งหน้า)
	fmt.Println("📥 กำลังดึงข้อมูล balance จาก server มาเก็บใน memory")
	serverDataMap := make(map[string]map[string]interface{})

	batchSize := 10000
	offset := 0
	totalFetched := 0

	for {
		// ดึงข้อมูลครั้งละ 10,000 รายการ
		query := fmt.Sprintf("SELECT ic_code, wh_code, unit_code, balance_qty FROM ic_balance LIMIT %d OFFSET %d", batchSize, offset)
		resp, err := api.ExecuteSelect(query)

		if err != nil {
			if offset == 0 {
				fmt.Printf("⚠️ Warning: ไม่สามารถดึงข้อมูลจาก server: %v (จะทำการ insert ทั้งหมด)\n", err)
				break
			} else {
				fmt.Printf("⚠️ Warning: Error ดึงข้อมูล batch ที่ offset %d: %v\n", offset, err)
				break
			}
		}

		if !resp.Success || resp.Data == nil {
			fmt.Printf("📊 ไม่มีข้อมูลเพิ่มเติม หรือ response ไม่สำเร็จ\n")
			break
		}

		// Parse ข้อมูลจาก server
		batchCount := 0
		if rows, ok := resp.Data.([]interface{}); ok {
			for _, row := range rows {
				if rowMap, ok := row.(map[string]interface{}); ok {
					icCode := fmt.Sprintf("%v", rowMap["ic_code"])
					whCode := fmt.Sprintf("%v", rowMap["wh_code"])
					unitCode := fmt.Sprintf("%v", rowMap["unit_code"])

					// สร้าง key สำหรับ map (ic_code + wh_code + unit_code)
					key := fmt.Sprintf("%s|%s|%s", icCode, whCode, unitCode)
					serverDataMap[key] = rowMap
					batchCount++
				}
			}
		}

		totalFetched += batchCount
		fmt.Printf("📊 ดึงข้อมูล batch ที่ %d: %d รายการ (รวม %d รายการ)\n", (offset/batchSize)+1, batchCount, totalFetched)

		// ถ้าได้ข้อมูลน้อยกว่า batchSize แสดงว่าหมดแล้ว
		if batchCount < batchSize {
			break
		}

		offset += batchSize
		time.Sleep(200 * time.Millisecond)
	}

	fmt.Printf("📊 ดึงข้อมูลจาก server เสร็จสิ้น: %d รายการทั้งหมด\n", totalFetched)
	// สร้าง map สำหรับข้อมูล local
	localDataMap := make(map[string]map[string]interface{})
	for _, item := range data {
		if itemMap, ok := item.(map[string]interface{}); ok {
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			// ตรวจสอบชื่อฟิลด์ warehouse หรือ wh_code
			whCode := ""
			if whCodeVal, exists := itemMap["wh_code"]; exists {
				whCode = fmt.Sprintf("%v", whCodeVal)
			} else if warehouseVal, exists := itemMap["warehouse"]; exists {
				whCode = fmt.Sprintf("%v", warehouseVal)
			}
			// ตรวจสอบชื่อฟิลด์ unit_code หรือ ic_unit_code
			unitCode := ""
			if unitCodeVal, exists := itemMap["unit_code"]; exists {
				unitCode = fmt.Sprintf("%v", unitCodeVal)
			} else if icUnitCodeVal, exists := itemMap["ic_unit_code"]; exists {
				unitCode = fmt.Sprintf("%v", icUnitCodeVal)
			} // ข้าม record ที่มีข้อมูลไม่ครบ
			if icCode == "<nil>" || icCode == "" || whCode == "<nil>" || whCode == "" || unitCode == "<nil>" || unitCode == "" {
				continue
			}

			key := fmt.Sprintf("%s|%s|%s", icCode, whCode, unitCode)
			// สร้าง normalized item สำหรับการ sync
			normalizedItem := map[string]interface{}{
				"ic_code":     icCode,
				"wh_code":     whCode,
				"unit_code":   unitCode,
				"balance_qty": itemMap["balance_qty"],
			}
			localDataMap[key] = normalizedItem
		}
	}

	// เปรียบเทียบข้อมูลและแยกประเภท insert/update/delete
	var insertsData []map[string]interface{}
	var updatesData []map[string]interface{}
	var deletesKeys []string
	// ตรวจสอบข้อมูลใน local เปรียบเทียบกับ server
	for key, localItem := range localDataMap {
		if serverData, exists := serverDataMap[key]; exists {
			// มีข้อมูลทั้งใน local และ server - ตรวจสอบการเปลี่ยนแปลง
			serverIcCode := fmt.Sprintf("%v", serverData["ic_code"])
			serverWhCode := fmt.Sprintf("%v", serverData["wh_code"])
			serverUnitCode := fmt.Sprintf("%v", serverData["unit_code"])

			newIcCode := fmt.Sprintf("%v", localItem["ic_code"])
			newWhCode := fmt.Sprintf("%v", localItem["wh_code"])
			newUnitCode := fmt.Sprintf("%v", localItem["unit_code"])

			// แปลง balance_qty เป็นตัวเลขเพื่อเปรียบเทียบ
			serverBalanceQtyFloat, serverErr := strconv.ParseFloat(fmt.Sprintf("%v", serverData["balance_qty"]), 64)
			localBalanceQtyFloat, localErr := strconv.ParseFloat(fmt.Sprintf("%v", localItem["balance_qty"]), 64)

			balanceQtyChanged := false
			if serverErr != nil || localErr != nil {
				// ถ้าแปลงไม่ได้ ให้เปรียบเทียบเป็น string
				balanceQtyChanged = fmt.Sprintf("%v", serverData["balance_qty"]) != fmt.Sprintf("%v", localItem["balance_qty"])
			} else {
				// เปรียบเทียบเป็นตัวเลข (ใช้ความแม่นยำ 0.001)
				balanceQtyChanged = math.Abs(serverBalanceQtyFloat-localBalanceQtyFloat) > 0.001
			}

			// ตรวจสอบการเปลี่ยนแปลงในทั้ง 4 ฟิลด์
			if serverIcCode != newIcCode ||
				serverWhCode != newWhCode ||
				serverUnitCode != newUnitCode ||
				balanceQtyChanged {
				// ข้อมูลเปลี่ยนแปลง ต้อง update
				updatesData = append(updatesData, localItem)
			}
		} else {
			// มีใน local แต่ไม่มีใน server - ต้อง insert
			insertsData = append(insertsData, localItem)
		}
	} // ตรวจสอบข้อมูลใน server ที่ไม่มีใน local - ต้อง delete
	for key := range serverDataMap {
		if _, exists := localDataMap[key]; !exists {
			// มีใน server แต่ไม่มีใน local - ต้อง delete
			deletesKeys = append(deletesKeys, key)
		}
	}

	fmt.Printf("📋 การวิเคราะห์ข้อมูล: Insert %d รายการ, Update %d รายการ, Delete %d รายการ\n",
		len(insertsData), len(updatesData), len(deletesKeys))

	successCount := 0

	// ทำการ DELETE ข้อมูลที่ไม่มีใน local
	if len(deletesKeys) > 0 {
		fmt.Printf("🗑️ กำลังลบข้อมูลที่ไม่มีใน local %d รายการ\n", len(deletesKeys))
		deleteCount := 0
		for i, key := range deletesKeys {
			parts := strings.Split(key, "|")
			if len(parts) == 3 {
				icCode := parts[0]
				whCode := parts[1]
				unitCode := parts[2]
				deleteQuery := fmt.Sprintf("DELETE FROM ic_balance WHERE ic_code = '%s' AND wh_code = '%s' AND unit_code = '%s'", icCode, whCode, unitCode)

				resp, err := api.ExecuteCommand(deleteQuery)
				if err != nil {
					fmt.Printf("❌ Error deleting record %d: %v\n", i+1, err)
					continue
				}

				if !resp.Success {
					fmt.Printf("❌ Failed to delete record %d: %s\n", i+1, resp.Message)
					continue
				}

				deleteCount++
				if (i+1)%100 == 0 {
					fmt.Printf("⏳ Delete แล้ว %d/%d รายการ\n", i+1, len(deletesKeys))
				}
			}
		}
		fmt.Printf("✅ Delete เสร็จสิ้น: %d รายการสำเร็จ\n", deleteCount)
		successCount += deleteCount
	}
	// ทำการ INSERT ข้อมูลใหม่
	if len(insertsData) > 0 {
		fmt.Printf("➕ กำลัง insert ข้อมูลใหม่ %d รายการ\n", len(insertsData))
		insertCount := 0
		for i, itemMap := range insertsData {
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			whCode := fmt.Sprintf("%v", itemMap["wh_code"])
			unitCode := fmt.Sprintf("%v", itemMap["unit_code"])
			balanceQty := fmt.Sprintf("%v", itemMap["balance_qty"])

			if i < 5 { // แสดงข้อมูลตัวอย่าง 5 รายการแรก
				fmt.Printf("📝 กำลัง insert: ic_code='%s', wh_code='%s', unit_code='%s', balance_qty=%s\n", icCode, whCode, unitCode, balanceQty)
			}

			insertQuery := fmt.Sprintf("INSERT INTO ic_balance (ic_code, wh_code, unit_code, balance_qty) VALUES ('%s', '%s', '%s', %s)", icCode, whCode, unitCode, balanceQty)

			resp, err := api.ExecuteCommand(insertQuery)
			if err != nil {
				fmt.Printf("❌ Error inserting record %d: %v\n", i+1, err)
				continue
			}

			if !resp.Success {
				fmt.Printf("❌ Failed to insert record %d: %s\n", i+1, resp.Message)
				continue
			}

			insertCount++
			if (i+1)%100 == 0 {
				fmt.Printf("⏳ Insert แล้ว %d/%d รายการ\n", i+1, len(insertsData))
			}
		}
		fmt.Printf("✅ Insert เสร็จสิ้น: %d รายการสำเร็จ\n", insertCount)
		successCount += insertCount
	}

	// ทำการ UPDATE ข้อมูลที่เปลี่ยนแปลง
	if len(updatesData) > 0 {
		fmt.Printf("🔄 กำลัง update ข้อมูลที่เปลี่ยนแปลง %d รายการ\n", len(updatesData))
		updateCount := 0
		for i, itemMap := range updatesData {
			icCode := fmt.Sprintf("%v", itemMap["ic_code"])
			whCode := fmt.Sprintf("%v", itemMap["wh_code"])
			unitCode := fmt.Sprintf("%v", itemMap["unit_code"])
			balanceQty := fmt.Sprintf("%v", itemMap["balance_qty"])
			updateQuery := fmt.Sprintf("UPDATE ic_balance SET balance_qty = %s WHERE ic_code = '%s' AND wh_code = '%s' AND unit_code = '%s'", balanceQty, icCode, whCode, unitCode)

			resp, err := api.ExecuteCommand(updateQuery)
			if err != nil {
				fmt.Printf("❌ Error updating record %d: %v\n", i+1, err)
				continue
			}

			if !resp.Success {
				fmt.Printf("❌ Failed to update record %d: %s\n", i+1, resp.Message)
				continue
			}

			updateCount++
			if (i+1)%100 == 0 {
				fmt.Printf("⏳ Update แล้ว %d/%d รายการ\n", i+1, len(updatesData))
			}
		}
		fmt.Printf("✅ Update เสร็จสิ้น: %d รายการสำเร็จ\n", updateCount)
		successCount += updateCount
	}
	fmt.Printf("🎉 Sync balance เสร็จสิ้นทั้งหมด: %d รายการสำเร็จ (Delete: %d, Insert: %d, Update: %d)\n", successCount, len(deletesKeys), len(insertsData), len(updatesData))
	return successCount, nil
}
