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
	CREATE TABLE IF NOT EXISTS ar_customer (
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
