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

5. Balance Sync Functions
   - GetExistingBalanceData
   - SyncBalanceData
   - executeBatchUpsertBalance
   - insertSingleBalance/updateSingleBalance/upsertSingleBalance

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
	"net/http"
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
	// ลบตารางเดิมก่อนถ้ามี
	err := api.DropTable("ic_inventory_barcode")
	if err != nil {
		return fmt.Errorf("error dropping existing barcode table: %v", err)
	}

	query := `	CREATE TABLE ic_inventory_barcode (
		ic_code VARCHAR(50) NOT NULL,
		barcode VARCHAR(100) NOT NULL,
		name VARCHAR(200),
		unit_code VARCHAR(50),
		unit_name VARCHAR(100),
		PRIMARY KEY (ic_code, barcode)
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create inventory temp table: %s", resp.Message)
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

	query := `	CREATE TABLE ic_inventory_barcode (
		ic_code VARCHAR(50) NOT NULL,
		barcode VARCHAR(100) NOT NULL,
		name VARCHAR(200),
		unit_code VARCHAR(50),
		unit_name VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (ic_code, barcode)
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
	CREATE TABLE ic_balance (
		ic_code VARCHAR(50) NOT NULL,
		wh_code VARCHAR(50) NOT NULL,
		unit_code VARCHAR(50) NOT NULL,
		balance_qty NUMERIC(18,3) DEFAULT 0,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
	CREATE TABLE ar_customer (
		code VARCHAR(50) NOT NULL,
		price_level VARCHAR(50),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

// ================================================================================
// 4. PRODUCT/INVENTORY SYNC FUNCTIONS
// ================================================================================

// SyncInventoryBarcodeData ซิงค์ข้อมูลบาร์โค้ดสินค้าจากตารางชั่วคราวไปยังตารางหลัก
func (api *APIClient) SyncInventoryBarcodeData() (int, int, error) {
	// สร้าง INSERT/UPDATE สำหรับข้อมูลใหม่
	query := `
	INSERT INTO ic_inventory_barcode (ic_code, barcode, name, unit_code, unit_name)
	SELECT 
		t.ic_code, 
		t.barcode, 
		t.name, 
		t.unit_code, 
		t.unit_name	FROM 
		ic_inventory_barcode t
	ON CONFLICT (ic_code, barcode) 
	DO UPDATE SET
		name = EXCLUDED.name,
		unit_code = EXCLUDED.unit_code,
		unit_name = EXCLUDED.unit_name
	WHERE (
		ic_inventory_barcode.ic_code IS DISTINCT FROM EXCLUDED.ic_code OR
		ic_inventory_barcode.name IS DISTINCT FROM EXCLUDED.name OR
		ic_inventory_barcode.unit_code IS DISTINCT FROM EXCLUDED.unit_code OR
		ic_inventory_barcode.unit_name IS DISTINCT FROM EXCLUDED.unit_name
	)
	`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return 0, 0, err
	}

	if !resp.Success {
		return 0, 0, fmt.Errorf("failed to sync inventory barcode data: %s", resp.Message)
	}

	// นับจำนวนที่มีการ insert และ update
	affectedCount := 0
	message := resp.Message
	if strings.Contains(message, "affected") {
		fmt.Sscanf(message, "%d rows affected", &affectedCount)
	}

	// ตรวจสอบจำนวนข้อมูลในตารางหลัก
	totalCount, err := api.getInventoryCount()
	if err != nil {
		return affectedCount, 0, err
	}

	return affectedCount, totalCount, nil
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

// ================================================================================
// 5. BALANCE SYNC FUNCTIONS
// ================================================================================

// GetExistingBalanceData ดึงข้อมูล balance ที่มีอยู่ในระบบ (สำหรับข้อมูลเท่านั้น ไม่ได้ใช้ในการเปรียบเทียบแล้ว)
func (api *APIClient) GetExistingBalanceData() (map[string]map[string]float64, error) {
	query := "SELECT ic_code, wh_code, unit_code, balance_qty FROM ic_balance"
	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to get existing balance data: %s", resp.Message)
	}

	// สร้าง map เพื่อเก็บข้อมูล
	result := make(map[string]map[string]float64)

	// วนลูปผลลัพธ์
	data, ok := resp.Data.([]interface{})
	if !ok {
		return result, nil // ไม่มีข้อมูล
	}

	for _, item := range data {
		row, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		icCode, ok1 := row["ic_code"].(string)
		whCode, ok2 := row["wh_code"].(string)
		unitCode, ok3 := row["unit_code"].(string)
		balanceQty, ok4 := row["balance_qty"].(float64)

		if !ok1 || !ok2 || !ok3 || !ok4 {
			continue
		}

		// สร้าง key สำหรับ product และ warehouse
		productKey := icCode
		warehouseKey := whCode + ":" + unitCode

		// เพิ่มข้อมูลลงใน map
		if _, exists := result[productKey]; !exists {
			result[productKey] = make(map[string]float64)
		}
		result[productKey][warehouseKey] = balanceQty
	}

	return result, nil
}

// SyncBalanceData ซิงค์ข้อมูล balance โดยใช้ batch upsert
func (api *APIClient) SyncBalanceData(localData []interface{}) (int, error) {
	// สร้าง slice สำหรับเก็บค่า values ที่จะ upsert
	var values []string
	batchSize := 1000 // จำนวน records ใน 1 batch
	totalCount := 0
	processedCount := 0
	fmt.Println("เริ่มประมวลผลข้อมูล balance ทั้งหมด", len(localData), "รายการ")

	// แสดงตัวอย่างข้อมูลรายการแรก (ถ้ามี)
	if len(localData) > 0 {
		fmt.Printf("ตัวอย่างข้อมูลรายการที่ 1: %v\n", localData[0])
	}

	for i, item := range localData {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			fmt.Printf("ข้ามรายการที่ %d: ไม่สามารถแปลงเป็น map ได้\n", i+1)
			continue
		}

		// Get field values and check if they exist
		icCode, ok1 := itemMap["ic_code"].(string)
		warehouse, ok2 := itemMap["warehouse"].(string)
		unitCode, ok3 := itemMap["ic_unit_code"].(string)
		balanceQty, ok4 := itemMap["balance_qty"].(float64)

		if !ok1 || !ok2 || !ok3 || !ok4 {
			fmt.Printf("ข้ามรายการที่ %d: ข้อมูลไม่ครบ ic_code=%v, warehouse=%v, ic_unit_code=%v, balance_qty=%v\n",
				i+1, itemMap["ic_code"], itemMap["warehouse"], itemMap["ic_unit_code"], itemMap["balance_qty"])
			continue
		}
		processedCount++

		// Map field names for SQL query (warehouse -> wh_code, ic_unit_code -> unit_code)
		valueStr := fmt.Sprintf("('%s', '%s', '%s', %f, NOW())",
			icCode, warehouse, unitCode, balanceQty)
		values = append(values, valueStr)

		// ถ้าครบ batch size หรือเป็นรายการสุดท้าย ให้ทำการ upsert
		if len(values) >= batchSize || i == len(localData)-1 {
			if len(values) > 0 {
				fmt.Printf("กำลัง UPSERT batch ที่ %d ขนาด %d รายการ...\n", (i/batchSize)+1, len(values))
				err := api.executeBatchUpsertBalance(values)
				if err != nil {
					fmt.Printf("❌ เกิดข้อผิดพลาดในการ UPSERT batch: %v\n", err)
					return totalCount, err
				}
				fmt.Printf("✅ UPSERT batch ที่ %d เสร็จสิ้น\n", (i/batchSize)+1)
				totalCount += len(values)
				values = []string{} // reset batch
			}
		}
	}

	fmt.Printf("ประมวลผลข้อมูลทั้งหมด %d รายการ, UPSERT สำเร็จ %d รายการ\n", processedCount, totalCount)
	return totalCount, nil
}

// executeBatchUpsertBalance ทำการ upsert ข้อมูล balance เป็น batch
func (api *APIClient) executeBatchUpsertBalance(values []string) error {
	if len(values) == 0 {
		return nil
	}

	// ใช้ชื่อคอลัมน์ที่ตรงกันระหว่าง warehouse/wh_code และ unit_code/ic_unit_code
	query := fmt.Sprintf(`
	INSERT INTO ic_balance (ic_code, wh_code, unit_code, balance_qty, updated_at)
	VALUES %s
	ON CONFLICT (ic_code, wh_code, unit_code) 
	DO UPDATE SET
		balance_qty = EXCLUDED.balance_qty,
		updated_at = EXCLUDED.updated_at
	WHERE (
		ic_balance.ic_code IS DISTINCT FROM EXCLUDED.ic_code OR
		ic_balance.wh_code IS DISTINCT FROM EXCLUDED.wh_code OR
		ic_balance.unit_code IS DISTINCT FROM EXCLUDED.unit_code OR
		ic_balance.balance_qty IS DISTINCT FROM EXCLUDED.balance_qty
	)
	`, strings.Join(values, ","))

	// แสดงตัวอย่างของ query สำหรับการ debug (แสดงเฉพาะบางส่วน)
	querySample := query
	if len(query) > 500 {
		querySample = query[:500] + "..."
	}
	fmt.Printf("Query UPSERT: %s\n", querySample)
	fmt.Printf("กำลัง UPSERT ข้อมูล %d รายการเข้าตาราง ic_balance...\n", len(values))
	resp, err := api.ExecuteCommand(query)
	if err != nil {
		fmt.Printf("❌ เกิดข้อผิดพลาดในการ UPSERT: %v\n", err)
		// Show the first value for debugging
		if len(values) > 0 {
			fmt.Printf("ตัวอย่างค่าที่พยายาม UPSERT: %s\n", values[0])
		}
		return err
	}

	if !resp.Success {
		fmt.Printf("❌ UPSERT ไม่สำเร็จ: %s\n", resp.Message)
		return fmt.Errorf("failed to batch upsert balance data: %s", resp.Message)
	}

	// นับจำนวนที่มีการ insert และ update
	affectedCount := 0
	message := resp.Message
	if strings.Contains(message, "affected") {
		fmt.Sscanf(message, "%d rows affected", &affectedCount)
		fmt.Printf("✅ UPSERT สำเร็จ: มีการอัพเดท %d รายการ\n", affectedCount)
	} else {
		fmt.Printf("✅ UPSERT สำเร็จ แต่ไม่สามารถระบุจำนวนรายการที่อัพเดทได้\n")
	}

	return nil
}

// ================================================================================
// 6. CUSTOMER SYNC FUNCTIONS
// ================================================================================

// SyncCustomerData ซิงค์ข้อมูลลูกค้าโดยใช้ batch upsert
func (api *APIClient) SyncCustomerData(localData []interface{}) (int, error) {
	// สร้าง slice สำหรับเก็บค่า values ที่จะ upsert
	var values []string
	batchSize := 50 // ลดขนาด batch เพื่อป้องกันปัญหา (เดิม 1000)
	totalCount := 0
	batchCount := 0

	for i := 0; i < len(localData); i++ {
		item := localData[i]
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			fmt.Printf("⚠️ ข้ามรายการที่ %d: ไม่สามารถแปลงเป็น map ได้\n", i)
			continue
		}

		code, hasCode := itemMap["code"].(string)
		if !hasCode || code == "" {
			fmt.Printf("⚠️ ข้ามรายการที่ %d: ไม่มี code หรือ code เป็นค่าว่าง\n", i)
			continue
		}

		// Escape special characters in code
		code = strings.ReplaceAll(code, "'", "''")
		// Filter out non-printable characters
		var filteredCode strings.Builder
		for _, r := range code {
			if r >= 32 && r < 127 || r >= 3585 && r <= 3675 { // ASCII printable and Thai characters
				filteredCode.WriteRune(r)
			}
		}
		code = filteredCode.String()

		priceLevel, hasPriceLevel := itemMap["price_level"].(string)
		// Sanitize strings
		if !hasPriceLevel {
			priceLevel = ""
		}

		// Escape special characters in price_level
		priceLevel = strings.ReplaceAll(priceLevel, "'", "''")
		// Filter out non-printable characters
		var filteredPriceLevel strings.Builder
		for _, r := range priceLevel {
			if r >= 32 && r < 127 || r >= 3585 && r <= 3675 { // ASCII printable and Thai characters
				filteredPriceLevel.WriteRune(r)
			}
		}
		priceLevel = filteredPriceLevel.String()

		// Skip invalid characters or empty codes
		if code == "" {
			continue
		}

		totalCount++

		// สร้าง value string สำหรับ batch upsert
		valueStr := fmt.Sprintf("('%s', '%s', NOW())",
			code, priceLevel)
		values = append(values, valueStr)

		// ถ้าครบ batch size หรือเป็นรายการสุดท้าย ให้ทำการ upsert
		if len(values) >= batchSize || i == len(localData)-1 {
			if len(values) > 0 {
				batchCount++
				fmt.Printf("กำลัง UPSERT batch ที่ %d ขนาด %d รายการ...\n", batchCount, len(values))

				// Show sample values for debugging
				sampleCount := 5
				if len(values) < sampleCount {
					sampleCount = len(values)
				}
				fmt.Printf("ตัวอย่างข้อมูล %d รายการแรกใน batch:\n", sampleCount)
				for j := 0; j < sampleCount; j++ {
					fmt.Printf("  - %s\n", values[j])
				}

				err := api.executeBatchUpsertCustomer(values)
				if err != nil {
					return totalCount, err
				}
				fmt.Printf("✅ UPSERT batch ที่ %d เสร็จสิ้น\n", batchCount)
				values = []string{} // reset batch
			}
		}
	}

	return totalCount, nil
}

// executeBatchUpsertCustomer ทำการ upsert ข้อมูลลูกค้าเป็น batch
func (api *APIClient) executeBatchUpsertCustomer(values []string) error {
	if len(values) == 0 {
		return nil
	}

	// Check for empty values that might cause SQL errors
	var validValues []string
	for _, value := range values {
		if value != "('', '', NOW())" && value != "(NULL, NULL, NOW())" {
			validValues = append(validValues, value)
		}
	}

	if len(validValues) == 0 {
		fmt.Println("ไม่มีข้อมูลที่ถูกต้องสำหรับ UPSERT หลังจากกรองข้อมูลที่ไม่ถูกต้องออก")
		return nil
	}

	query := fmt.Sprintf(`
	INSERT INTO ar_customer (code, price_level, created_at)
	VALUES %s
	ON CONFLICT (code) 
DO UPDATE SET 
    price_level = EXCLUDED.price_level
WHERE 
    ar_customer.price_level IS DISTINCT FROM EXCLUDED.price_level;
	`, strings.Join(validValues, ","))

	// Debug output for query
	queryPreview := query
	if len(query) > 500 {
		queryPreview = query[:500] + "..."
	}
	fmt.Printf("UPSERT Query: %s\n", queryPreview)

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		fmt.Printf("❌ เกิดข้อผิดพลาดในการ UPSERT: %v\n", err)

		// More detailed error info for debugging
		if resp != nil {
			fmt.Printf("API response: success=%v, message=%s\n", resp.Success, resp.Message)
		}

		return err
	}

	if !resp.Success {
		fmt.Printf("❌ UPSERT ล้มเหลว: %s\n", resp.Message)
		return fmt.Errorf("failed to batch upsert customer data: %s", resp.Message)
	}

	fmt.Printf("✅ UPSERT สำเร็จ\n")

	return nil
}

// ================================================================================
// 7. UTILITY/HELPER FUNCTIONS
// ================================================================================

// executeBatchInsert ทำการ insert ข้อมูลเป็น batch
func (api *APIClient) executeBatchInsert(tableName string, columns []string, values []string) error {
	if len(values) == 0 {
		return nil
	}

	columnsStr := strings.Join(columns, ", ")
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName, columnsStr, strings.Join(values, ","))

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to batch insert: %s", resp.Message)
	}

	return nil
}
