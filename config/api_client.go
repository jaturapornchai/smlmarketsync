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

	resp, err := api.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error executing request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %v", err)
	}

	var response QueryResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling response: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &response, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, response.Message)
	}

	return &response, nil
}

// CheckDatabaseExists ตรวจสอบว่าตารางมีอยู่หรือไม่
func (api *APIClient) CheckTableExists(tableName string) (bool, error) {
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = '%s')", tableName)

	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return false, err
	}

	if !resp.Success {
		return false, fmt.Errorf("table check failed: %s", resp.Message)
	}

	// ตรวจสอบผลลัพธ์จาก API response ตามรูปแบบที่เห็นใน debug
	if data, ok := resp.Data.([]interface{}); ok && len(data) > 0 {
		if row, ok := data[0].(map[string]interface{}); ok {
			if exists, ok := row["exists"].(bool); ok {
				return exists, nil
			}
		}
	}

	return false, nil
}

// DropTable ลบตาราง
func (api *APIClient) DropTable(tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to drop table %s: %s", tableName, resp.Message)
	}

	return nil
}

// CreateInventoryTempTable สร้างตาราง ic_inventory_barcode_temp
func (api *APIClient) CreateInventoryTempTable() error {
	query := `
	CREATE TABLE ic_inventory_barcode_temp (
		ic_code VARCHAR(50),
		barcode VARCHAR(50) PRIMARY KEY,
		name VARCHAR(255),
		unit_code VARCHAR(20),
		unit_name VARCHAR(100),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create ic_inventory_barcode_temp table: %s", resp.Message)
	}

	return nil
}

// CreateInventoryBarcodeTable สร้างตาราง ic_inventory_barcode ถ้าไม่มี
func (api *APIClient) CreateInventoryBarcodeTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS ic_inventory_barcode (
		ic_code VARCHAR(50),
		barcode VARCHAR(50) PRIMARY KEY,
		name VARCHAR(255),
		unit_code VARCHAR(20),
		unit_name VARCHAR(100),
		price DECIMAL(15,4) DEFAULT 0,
		status INTEGER DEFAULT 1,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create ic_inventory_barcode table: %s", resp.Message)
	}

	return nil
}

// CreateBalanceTable สร้างตาราง ic_balance ถ้าไม่มี
func (api *APIClient) CreateBalanceTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS ic_balance (
		ic_code VARCHAR(50),
		warehouse VARCHAR(50),
		ic_unit_code VARCHAR(20),
		balance_qty DECIMAL(15,4) DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (ic_code, warehouse)
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create ic_balance table: %s", resp.Message)
	}

	return nil
}

// CreateCustomerTable สร้างตาราง ar_customer ถ้าไม่มี
func (api *APIClient) CreateCustomerTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS ar_customer (
		code VARCHAR(50) PRIMARY KEY,
		price_level VARCHAR(20),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create ar_customer table: %s", resp.Message)
	}

	return nil
}

// SyncInventoryBarcodeData เปรียบเทียบและซิงค์ข้อมูลระหว่าง temp table และ main table
func (api *APIClient) SyncInventoryBarcodeData() error {
	// 1. Insert new barcodes from temp to main table
	insertQuery := `
		INSERT INTO ic_inventory_barcode (ic_code, barcode, name, unit_code, unit_name, price, status)
		SELECT t.ic_code, t.barcode, t.name, t.unit_code, t.unit_name, 0, 1
		FROM ic_inventory_barcode_temp t
		LEFT JOIN ic_inventory_barcode m ON t.barcode = m.barcode
		WHERE m.barcode IS NULL`

	resp, err := api.ExecuteCommand(insertQuery)
	if err != nil {
		return fmt.Errorf("error inserting new barcodes: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to insert new barcodes: %s", resp.Message)
	}

	// 2. Update status to 0 for barcodes that exist in main table but not in temp
	updateQuery := `
		UPDATE ic_inventory_barcode 
		SET status = 0, updated_at = CURRENT_TIMESTAMP
		WHERE barcode NOT IN (SELECT barcode FROM ic_inventory_barcode_temp)
		AND status = 1`

	resp, err = api.ExecuteCommand(updateQuery)
	if err != nil {
		return fmt.Errorf("error updating inactive barcodes: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to update inactive barcodes: %s", resp.Message)
	}

	return nil
}

// GetSyncStatistics ดึงสถิติการซิงค์
func (api *APIClient) GetSyncStatistics() (map[string]int, error) {
	stats := make(map[string]int)

	// นับจำนวน record ใน temp table
	tempCountQuery := "SELECT COUNT(*) FROM ic_inventory_barcode_temp"
	resp, err := api.ExecuteSelect(tempCountQuery)
	if err != nil {
		return nil, err
	}

	if resp.Success && len(resp.Data.([]interface{})) > 0 {
		if row, ok := resp.Data.([]interface{})[0].(map[string]interface{}); ok {
			if count, ok := row["count"].(float64); ok {
				stats["temp_count"] = int(count)
			}
		}
	}

	// นับจำนวน active records ใน main table
	activeCountQuery := "SELECT COUNT(*) FROM ic_inventory_barcode WHERE status = 1"
	resp, err = api.ExecuteSelect(activeCountQuery)
	if err != nil {
		return nil, err
	}

	if resp.Success && len(resp.Data.([]interface{})) > 0 {
		if row, ok := resp.Data.([]interface{})[0].(map[string]interface{}); ok {
			if count, ok := row["count"].(float64); ok {
				stats["active_count"] = int(count)
			}
		}
	}

	// นับจำนวน inactive records ใน main table
	inactiveCountQuery := "SELECT COUNT(*) FROM ic_inventory_barcode WHERE status = 0"
	resp, err = api.ExecuteSelect(inactiveCountQuery)
	if err != nil {
		return nil, err
	}

	if resp.Success && len(resp.Data.([]interface{})) > 0 {
		if row, ok := resp.Data.([]interface{})[0].(map[string]interface{}); ok {
			if count, ok := row["count"].(float64); ok {
				stats["inactive_count"] = int(count)
			}
		}
	}

	return stats, nil
}

// GetExistingBalanceData ดึงข้อมูลทั้งหมดจากตาราง ic_balance บน API
func (api *APIClient) GetExistingBalanceData() (map[string]map[string]float64, error) {
	query := "SELECT ic_code, warehouse, balance_qty FROM ic_balance"

	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to get balance data: %s", resp.Message)
	}

	// สร้าง map สำหรับเก็บข้อมูล existing balance [ic_code][warehouse] = balance_qty
	existingData := make(map[string]map[string]float64)

	if data, ok := resp.Data.([]interface{}); ok {
		for _, row := range data {
			if rowMap, ok := row.(map[string]interface{}); ok {
				icCode, _ := rowMap["ic_code"].(string)
				warehouse, _ := rowMap["warehouse"].(string)
				balanceQty, _ := rowMap["balance_qty"].(float64)

				if existingData[icCode] == nil {
					existingData[icCode] = make(map[string]float64)
				}
				existingData[icCode][warehouse] = balanceQty
			}
		}
	}

	return existingData, nil
}

// SyncBalanceData ซิงค์ข้อมูล balance โดยส่งทั้งหมดแบบ batch UPSERT ไม่ต้องเปรียบเทียบ
func (api *APIClient) SyncBalanceData(localData []interface{}, existingData map[string]map[string]float64) (int, int, error) {
	totalCount := len(localData)
	skipCount := 0

	fmt.Printf("� เริ่มส่งข้อมูล balance ทั้งหมด %d รายการแบบ batch UPSERT (ไม่เปรียบเทียบ)\n", totalCount)

	// เตรียมข้อมูลสำหรับ batch upsert
	var batchValues []string
	validCount := 0

	// Process local data และเตรียม batch values
	for i, item := range localData {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		icCode, _ := itemMap["ic_code"].(string)
		warehouse, _ := itemMap["warehouse"].(string)
		unitCode, _ := itemMap["ic_unit_code"].(string)
		balanceQty, _ := itemMap["balance_qty"].(float64)

		// ตรวจสอบความถูกต้องของข้อมูลก่อนส่ง
		if icCode == "" || warehouse == "" {
			fmt.Printf("⚠️ ข้ามรายการที่ข้อมูลไม่ครบ: ic_code='%s', warehouse='%s'\n", icCode, warehouse)
			skipCount++
			continue
		}

		// Debug แสดงข้อมูลรายการแรก
		if i < 3 {
			fmt.Printf("🔍 Debug #%d: ic_code='%s', warehouse='%s', unit_code='%s', balance_qty=%f\n",
				i+1, icCode, warehouse, unitCode, balanceQty)
		}

		// Escape single quotes สำหรับ SQL
		icCodeEsc := strings.ReplaceAll(icCode, "'", "''")
		warehouseEsc := strings.ReplaceAll(warehouse, "'", "''")
		unitCodeEsc := strings.ReplaceAll(unitCode, "'", "''")

		// เตรียม value สำหรับ batch insert
		value := fmt.Sprintf("('%s', '%s', '%s', %f)", icCodeEsc, warehouseEsc, unitCodeEsc, balanceQty)
		batchValues = append(batchValues, value)
		validCount++

		// แสดง progress ทุกๆ 2000 รายการ
		if (i+1)%2000 == 0 {
			fmt.Printf("⏳ เตรียมข้อมูลแล้ว %d/%d รายการ\n", i+1, totalCount)
		}
	}

	fmt.Printf("📦 เตรียมข้อมูลเสร็จ: %d รายการที่ใช้ได้, ข้าม %d รายการ\n", validCount, skipCount)

	if len(batchValues) == 0 {
		return 0, 0, fmt.Errorf("ไม่มีข้อมูลที่ถูกต้องสำหรับส่ง")
	}

	// Execute batch UPSERT
	batchSize := 100 // ทำทีละ 100 รายการเพื่อความเสถียร
	totalBatches := (len(batchValues) + batchSize - 1) / batchSize
	successCount := 0

	fmt.Printf("🚀 กำลังส่งข้อมูล %d รายการแบบ batch UPSERT (ครั้งละ %d รายการ)\n", len(batchValues), batchSize)

	for i := 0; i < len(batchValues); i += batchSize {
		end := i + batchSize
		if end > len(batchValues) {
			end = len(batchValues)
		}

		batchNum := (i / batchSize) + 1
		currentBatchSize := end - i

		fmt.Printf("   📥 กำลังส่ง batch %d/%d (%d รายการ)...\n", batchNum, totalBatches, currentBatchSize)

		err := api.executeBatchUpsertBalance(batchValues[i:end])
		if err != nil {
			fmt.Printf("❌ Batch %d ล้มเหลว: %v\n", batchNum, err)
			// ไม่ return error เพื่อให้ทำ batch ต่อไปได้
		} else {
			successCount += currentBatchSize
			fmt.Printf("✅ Batch %d สำเร็จ (%d รายการ)\n", batchNum, currentBatchSize)
		}

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if batchNum < totalBatches {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// สรุปผลลัพธ์
	fmt.Printf("\n📊 สรุปการซิงค์ Balance:\n")
	fmt.Printf("   - ส่งสำเร็จ: %d รายการ\n", successCount)
	fmt.Printf("   - ข้ามเนื่องจากข้อมูลไม่ครบ: %d รายการ\n", skipCount)
	fmt.Printf("   - ล้มเหลว: %d รายการ\n", validCount-successCount)

	return successCount, 0, nil
}

// insertSingleBalance ทำ INSERT รายการเดียว
func (api *APIClient) insertSingleBalance(icCode, warehouse, unitCode string, balanceQty float64) error {
	// Escape single quotes สำหรับ SQL
	icCodeEsc := strings.ReplaceAll(icCode, "'", "''")
	warehouseEsc := strings.ReplaceAll(warehouse, "'", "''")
	unitCodeEsc := strings.ReplaceAll(unitCode, "'", "''")

	query := fmt.Sprintf(`
		INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
		VALUES ('%s', '%s', '%s', %f)`,
		icCodeEsc, warehouseEsc, unitCodeEsc, balanceQty)

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error executing insert: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("insert failed: %s", resp.Message)
	}

	return nil
}

// updateSingleBalance ทำ UPDATE รายการเดียว
func (api *APIClient) updateSingleBalance(icCode, warehouse, unitCode string, balanceQty float64) error {
	// Escape single quotes สำหรับ SQL
	icCodeEsc := strings.ReplaceAll(icCode, "'", "''")
	warehouseEsc := strings.ReplaceAll(warehouse, "'", "''")
	unitCodeEsc := strings.ReplaceAll(unitCode, "'", "''")

	query := fmt.Sprintf(`
		UPDATE ic_balance 
		SET ic_unit_code = '%s', balance_qty = %f, updated_at = CURRENT_TIMESTAMP 
		WHERE ic_code = '%s' AND warehouse = '%s'`,
		unitCodeEsc, balanceQty, icCodeEsc, warehouseEsc)

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error executing update: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("update failed: %s", resp.Message)
	}

	return nil
}

// upsertSingleBalance ทำ UPSERT รายการเดียว (INSERT หรือ UPDATE ขึ้นอยู่กับว่ามี ic_code+warehouse หรือไม่)
func (api *APIClient) upsertSingleBalance(icCode, warehouse, unitCode string, balanceQty float64) error {
	// Escape single quotes สำหรับ SQL
	icCodeEsc := strings.ReplaceAll(icCode, "'", "''")
	warehouseEsc := strings.ReplaceAll(warehouse, "'", "''")
	unitCodeEsc := strings.ReplaceAll(unitCode, "'", "''")
	// ใช้ PostgreSQL syntax: INSERT ... ON CONFLICT พร้อมตรวจสอบข้อมูลเปลี่ยนแปลงหรือไม่
	query := fmt.Sprintf(`
		INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
		VALUES ('%s', '%s', '%s', %f)
		ON CONFLICT (ic_code, warehouse) 
		DO UPDATE SET 
			ic_unit_code = EXCLUDED.ic_unit_code,
			balance_qty = EXCLUDED.balance_qty,
			updated_at = CURRENT_TIMESTAMP
		WHERE (ic_balance.ic_unit_code IS DISTINCT FROM EXCLUDED.ic_unit_code 
			OR ic_balance.balance_qty IS DISTINCT FROM EXCLUDED.balance_qty)`,
		icCodeEsc, warehouseEsc, unitCodeEsc, balanceQty)

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error executing upsert: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("upsert failed: %s", resp.Message)
	}

	return nil
}

// executeBatchInsert ทำ batch insert พร้อม retry mechanism
func (api *APIClient) executeBatchInsert(insertValues []string) error {
	if len(insertValues) == 0 {
		return nil
	}

	insertQuery := fmt.Sprintf(`
		INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
		VALUES %s`,
		strings.Join(insertValues, ","))

	// เพิ่ม retry mechanism สำหรับ API call
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		resp, err := api.ExecuteCommand(insertQuery)
		if err != nil {
			lastErr = fmt.Errorf("error executing batch insert (attempt %d/%d): %v", retry+1, maxRetries, err)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond) // Exponential backoff
				continue
			}
			return lastErr
		}

		if !resp.Success {
			lastErr = fmt.Errorf("batch insert failed (attempt %d/%d): %s", retry+1, maxRetries, resp.Message)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond)
				continue
			}
			return lastErr
		}

		// Success!
		return nil
	}

	return lastErr
}

// executeBatchUpsert ทำ batch upsert สำหรับ insert หรือ update
func (api *APIClient) executeBatchUpsert(values []string, isInsert bool) error {
	if len(values) == 0 {
		return nil
	}

	var query string
	if isInsert {
		// สำหรับ insert ใหม่ ใช้ INSERT ... ON DUPLICATE KEY UPDATE
		query = fmt.Sprintf(`
			INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
			VALUES %s
			ON DUPLICATE KEY UPDATE 
				ic_unit_code = VALUES(ic_unit_code),
				balance_qty = VALUES(balance_qty),
				updated_at = CURRENT_TIMESTAMP`,
			strings.Join(values, ","))
	} else {
		// สำหรับ update ที่มีอยู่แล้ว ใช้ INSERT ... ON DUPLICATE KEY UPDATE
		query = fmt.Sprintf(`
			INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
			VALUES %s
			ON DUPLICATE KEY UPDATE 
				ic_unit_code = VALUES(ic_unit_code),
				balance_qty = VALUES(balance_qty),
				updated_at = CURRENT_TIMESTAMP`,
			strings.Join(values, ","))
	}

	// Debug: แสดงขนาดของ query ที่ส่งไป
	fmt.Printf("🔍 Query size: %d characters, %d values\n", len(query), len(values))

	// เพิ่ม retry mechanism สำหรับ API call
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		resp, err := api.ExecuteCommand(query)
		if err != nil {
			lastErr = fmt.Errorf("error executing batch upsert (attempt %d/%d): %v", retry+1, maxRetries, err)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond) // Exponential backoff
				continue
			}
			return lastErr
		}

		if !resp.Success {
			lastErr = fmt.Errorf("batch upsert failed (attempt %d/%d): %s", retry+1, maxRetries, resp.Message)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond)
				continue
			}
			return lastErr
		}

		// Success!
		return nil
	}

	return lastErr
}

// executeBatchUpsertForUpdate ทำ batch update โดยใช้ INSERT ... ON DUPLICATE KEY UPDATE
func (api *APIClient) executeBatchUpsertForUpdate(updateBatch []string) error {
	if len(updateBatch) == 0 {
		return nil
	}

	// ใช้ INSERT ... ON DUPLICATE KEY UPDATE สำหรับ PostgreSQL เราจะใช้ INSERT ... ON CONFLICT
	query := fmt.Sprintf(`
		INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
		VALUES %s
		ON CONFLICT (ic_code, warehouse) 
		DO UPDATE SET 
			ic_unit_code = EXCLUDED.ic_unit_code,
			balance_qty = EXCLUDED.balance_qty,
			updated_at = CURRENT_TIMESTAMP`,
		strings.Join(updateBatch, ","))

	// เพิ่ม retry mechanism สำหรับ API call
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		resp, err := api.ExecuteCommand(query)
		if err != nil {
			lastErr = fmt.Errorf("error executing batch upsert for update (attempt %d/%d): %v", retry+1, maxRetries, err)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond) // Exponential backoff
				continue
			}
			return lastErr
		}

		if !resp.Success {
			lastErr = fmt.Errorf("batch upsert for update failed (attempt %d/%d): %s", retry+1, maxRetries, resp.Message)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 500 * time.Millisecond)
				continue
			}
			return lastErr
		}

		// Success!
		return nil
	}

	return lastErr
}

// executeBatchUpsertBalance ทำ batch UPSERT สำหรับ balance โดยใช้ PostgreSQL ON CONFLICT
func (api *APIClient) executeBatchUpsertBalance(values []string) error {
	if len(values) == 0 {
		return nil
	}
	// ใช้ PostgreSQL syntax: INSERT ... ON CONFLICT พร้อมตรวจสอบข้อมูลเปลี่ยนแปลงหรือไม่
	query := fmt.Sprintf(`
		INSERT INTO ic_balance (ic_code, warehouse, ic_unit_code, balance_qty)
		VALUES %s
		ON CONFLICT (ic_code, warehouse) 
		DO UPDATE SET 
			ic_unit_code = EXCLUDED.ic_unit_code,
			balance_qty = EXCLUDED.balance_qty,
			updated_at = CURRENT_TIMESTAMP
		WHERE (ic_balance.ic_unit_code IS DISTINCT FROM EXCLUDED.ic_unit_code 
			OR ic_balance.balance_qty IS DISTINCT FROM EXCLUDED.balance_qty)`,
		strings.Join(values, ","))

	// เพิ่ม retry mechanism สำหรับ API call
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		resp, err := api.ExecuteCommand(query)
		if err != nil {
			lastErr = fmt.Errorf("error executing batch upsert balance (attempt %d/%d): %v", retry+1, maxRetries, err)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 300 * time.Millisecond) // Exponential backoff
				continue
			}
			return lastErr
		}

		if !resp.Success {
			lastErr = fmt.Errorf("batch upsert balance failed (attempt %d/%d): %s", retry+1, maxRetries, resp.Message)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 300 * time.Millisecond)
				continue
			}
			return lastErr
		}

		// Success!
		return nil
	}

	return lastErr
}

// SyncCustomerData ซิงค์ข้อมูลลูกค้าโดยส่งทั้งหมดแบบ batch UPSERT
func (api *APIClient) SyncCustomerData(localData []interface{}, existingData map[string]string) (int, int, error) {
	totalCount := len(localData)
	skipCount := 0

	fmt.Printf("🚀 เริ่มส่งข้อมูลลูกค้าทั้งหมด %d รายการแบบ batch UPSERT\n", totalCount)

	// เตรียมข้อมูลสำหรับ batch upsert
	var batchValues []string
	validCount := 0

	// Process local data และเตรียม batch values
	for i, item := range localData {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		code, _ := itemMap["code"].(string)
		priceLevel, _ := itemMap["price_level"].(string)

		// ตรวจสอบความถูกต้องของข้อมูลก่อนส่ง
		if code == "" {
			fmt.Printf("⚠️ ข้ามรายการที่ข้อมูลไม่ครบ: code='%s'\n", code)
			skipCount++
			continue
		}

		// Debug แสดงข้อมูลรายการแรก
		if i < 3 {
			fmt.Printf("🔍 Debug #%d: code='%s', price_level='%s'\n",
				i+1, code, priceLevel)
		}

		// Escape single quotes สำหรับ SQL
		codeEsc := strings.ReplaceAll(code, "'", "''")
		priceLevelEsc := strings.ReplaceAll(priceLevel, "'", "''")

		// เตรียม value สำหรับ batch insert
		value := fmt.Sprintf("('%s', '%s')", codeEsc, priceLevelEsc)
		batchValues = append(batchValues, value)
		validCount++

		// แสดง progress ทุกๆ 2000 รายการ
		if (i+1)%2000 == 0 {
			fmt.Printf("⏳ เตรียมข้อมูลแล้ว %d/%d รายการ\n", i+1, totalCount)
		}
	}

	fmt.Printf("📦 เตรียมข้อมูลเสร็จ: %d รายการที่ใช้ได้, ข้าม %d รายการ\n", validCount, skipCount)

	if len(batchValues) == 0 {
		return 0, 0, fmt.Errorf("ไม่มีข้อมูลที่ถูกต้องสำหรับส่ง")
	}

	// Execute batch UPSERT
	batchSize := 200 // ทำทีละ 200 รายการสำหรับลูกค้า (ข้อมูลน้อยกว่า balance)
	totalBatches := (len(batchValues) + batchSize - 1) / batchSize
	successCount := 0

	fmt.Printf("🚀 กำลังส่งข้อมูล %d รายการแบบ batch UPSERT (ครั้งละ %d รายการ)\n", len(batchValues), batchSize)

	for i := 0; i < len(batchValues); i += batchSize {
		end := i + batchSize
		if end > len(batchValues) {
			end = len(batchValues)
		}

		batchNum := (i / batchSize) + 1
		currentBatchSize := end - i

		fmt.Printf("   📥 กำลังส่ง batch %d/%d (%d รายการ)...\n", batchNum, totalBatches, currentBatchSize)

		err := api.executeBatchUpsertCustomer(batchValues[i:end])
		if err != nil {
			fmt.Printf("❌ Batch %d ล้มเหลว: %v\n", batchNum, err)
			// ไม่ return error เพื่อให้ทำ batch ต่อไปได้
		} else {
			successCount += currentBatchSize
			fmt.Printf("✅ Batch %d สำเร็จ (%d รายการ)\n", batchNum, currentBatchSize)
		}

		// หน่วงเวลาเล็กน้อยระหว่าง batch
		if batchNum < totalBatches {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// สรุปผลลัพธ์
	fmt.Printf("\n📊 สรุปการซิงค์ลูกค้า:\n")
	fmt.Printf("   - ส่งสำเร็จ: %d รายการ\n", successCount)
	fmt.Printf("   - ข้ามเนื่องจากข้อมูลไม่ครบ: %d รายการ\n", skipCount)
	fmt.Printf("   - ล้มเหลว: %d รายการ\n", validCount-successCount)

	return successCount, 0, nil
}

// executeBatchUpsertCustomer ทำ batch UPSERT สำหรับลูกค้าโดยใช้ PostgreSQL ON CONFLICT
func (api *APIClient) executeBatchUpsertCustomer(values []string) error {
	if len(values) == 0 {
		return nil
	}

	// ใช้ PostgreSQL syntax: INSERT ... ON CONFLICT พร้อมตรวจสอบข้อมูลเปลี่ยนแปลงหรือไม่
	query := fmt.Sprintf(`
		INSERT INTO ar_customer (code, price_level)
		VALUES %s
		ON CONFLICT (code) 
		DO UPDATE SET 
			price_level = EXCLUDED.price_level,
			updated_at = CURRENT_TIMESTAMP
		WHERE ar_customer.price_level IS DISTINCT FROM EXCLUDED.price_level`,
		strings.Join(values, ","))

	// เพิ่ม retry mechanism สำหรับ API call
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		resp, err := api.ExecuteCommand(query)
		if err != nil {
			lastErr = fmt.Errorf("error executing batch upsert customer (attempt %d/%d): %v", retry+1, maxRetries, err)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 300 * time.Millisecond) // Exponential backoff
				continue
			}
			return lastErr
		}

		if !resp.Success {
			lastErr = fmt.Errorf("batch upsert customer failed (attempt %d/%d): %s", retry+1, maxRetries, resp.Message)
			if retry < maxRetries-1 {
				time.Sleep(time.Duration(retry+1) * 300 * time.Millisecond)
				continue
			}
			return lastErr
		}

		// Success!
		return nil
	}

	return lastErr
}

// GetExistingCustomerData ดึงข้อมูลลูกค้าที่มีอยู่จาก API
func (api *APIClient) GetExistingCustomerData() (map[string]string, error) {
	query := "SELECT code, price_level FROM ar_customer"

	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return nil, fmt.Errorf("error fetching existing customer data: %v", err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("failed to fetch existing customer data: %s", resp.Message)
	}

	customerMap := make(map[string]string)

	// แปลง response data เป็น slice of map
	if data, ok := resp.Data.([]interface{}); ok {
		for _, row := range data {
			if rowMap, ok := row.(map[string]interface{}); ok {
				code := ""
				priceLevel := ""

				if codeVal, exists := rowMap["code"]; exists && codeVal != nil {
					code = fmt.Sprintf("%v", codeVal)
				}

				if priceLevelVal, exists := rowMap["price_level"]; exists && priceLevelVal != nil {
					priceLevel = fmt.Sprintf("%v", priceLevelVal)
				}

				if code != "" {
					customerMap[code] = priceLevel
				}
			}
		}
	}

	return customerMap, nil
}

// SyncInventoryTableData ซิงค์ข้อมูลระหว่าง temp table และ main table
func (api *APIClient) SyncInventoryTableData() (int, int, error) {
	// ใช้ PostgreSQL MERGE หรือ INSERT ... ON CONFLICT เพื่อซิงค์ข้อมูล
	query := `
		-- เริ่มจาก temp table แล้วซิงค์กับ main table
		INSERT INTO ic_inventory_barcode (ic_code, barcode, name, unit_code, unit_name, status, created_at, updated_at)
		SELECT ic_code, barcode, name, unit_code, unit_name, 'active', created_at, CURRENT_TIMESTAMP
		FROM ic_inventory_barcode_temp
		ON CONFLICT (barcode) 
		DO UPDATE SET 
			ic_code = EXCLUDED.ic_code,
			name = EXCLUDED.name,
			unit_code = EXCLUDED.unit_code,
			unit_name = EXCLUDED.unit_name,
			status = 'active',
			updated_at = CURRENT_TIMESTAMP
		WHERE ic_inventory_barcode.ic_code IS DISTINCT FROM EXCLUDED.ic_code
		   OR ic_inventory_barcode.name IS DISTINCT FROM EXCLUDED.name
		   OR ic_inventory_barcode.unit_code IS DISTINCT FROM EXCLUDED.unit_code
		   OR ic_inventory_barcode.unit_name IS DISTINCT FROM EXCLUDED.unit_name
		   OR ic_inventory_barcode.status IS DISTINCT FROM 'active';
		
		-- อัปเดตข้อมูลที่ไม่มีใน temp table ให้เป็น inactive
		UPDATE ic_inventory_barcode 
		SET status = 'inactive', updated_at = CURRENT_TIMESTAMP
		WHERE barcode NOT IN (SELECT barcode FROM ic_inventory_barcode_temp)
		  AND status = 'active';
	`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return 0, 0, fmt.Errorf("error syncing inventory data: %v", err)
	}

	if !resp.Success {
		return 0, 0, fmt.Errorf("failed to sync inventory data: %s", resp.Message)
	}

	// ดึงสถิติหลังจากซิงค์
	activeCount, err := api.getInventoryCount("active")
	if err != nil {
		return 0, 0, err
	}

	inactiveCount, err := api.getInventoryCount("inactive")
	if err != nil {
		return 0, 0, err
	}

	return activeCount, inactiveCount, nil
}

// getInventoryCount ดึงจำนวนข้อมูลตามสถานะ
func (api *APIClient) getInventoryCount(status string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM ic_inventory_barcode WHERE status = '%s'", status)

	resp, err := api.ExecuteSelect(query)
	if err != nil {
		return 0, fmt.Errorf("error getting inventory count: %v", err)
	}

	if !resp.Success {
		return 0, fmt.Errorf("failed to get inventory count: %s", resp.Message)
	}

	// แปลงผลลัพธ์
	if data, ok := resp.Data.([]interface{}); ok && len(data) > 0 {
		if row, ok := data[0].(map[string]interface{}); ok {
			if countVal, exists := row["count"]; exists {
				if count, ok := countVal.(float64); ok {
					return int(count), nil
				}
			}
		}
	}

	return 0, nil
}

// CreateInventoryTable สร้างตาราง ic_inventory_barcode
func (api *APIClient) CreateInventoryTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS ic_inventory_barcode (
			ic_code VARCHAR(50) NOT NULL,
			barcode VARCHAR(100) NOT NULL,
			name VARCHAR(255),
			unit_code VARCHAR(20),
			unit_name VARCHAR(100),
			status VARCHAR(20) DEFAULT 'active',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (barcode)
		)
	`

	resp, err := api.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("error creating inventory table: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("failed to create inventory table: %s", resp.Message)
	}

	return nil
}
