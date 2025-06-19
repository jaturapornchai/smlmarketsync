package models

import (
	"database/sql"
	"fmt"
	"smlmarketsync/config"
	"strings"
	"time"
)

type Product struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Price       float64   `json:"price"`
	Stock       int       `json:"stock"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ProductBarcode struct {
	Barcode  string         `json:"barcode"`
	Name     sql.NullString `json:"name"`
	UnitCode string         `json:"unit_code"`
	UnitName sql.NullString `json:"unit_name"`
}

// InventoryItem สำหรับข้อมูลที่จะ upload ไป ic_inventory_barcode
type InventoryItem struct {
	IcCode   string `json:"ic_code"`
	Barcode  string `json:"barcode"`
	Name     string `json:"name"`
	UnitCode string `json:"unit_code"`
	UnitName string `json:"unit_name"`
}

// BalanceItem สำหรับข้อมูล ic_balance
type BalanceItem struct {
	IcCode     string  `json:"ic_code"`
	Warehouse  string  `json:"warehouse"`
	UnitCode   string  `json:"ic_unit_code"`
	BalanceQty float64 `json:"balance_qty"`
}

// CustomerItem สำหรับข้อมูล ar_customer
type CustomerItem struct {
	Code       string `json:"code"`
	PriceLevel string `json:"price_level"`
}

type ProductRepository struct {
	db        *sql.DB
	apiClient *config.APIClient
}

func NewProductRepository(db *sql.DB) *ProductRepository {
	return &ProductRepository{
		db:        db,
		apiClient: config.NewAPIClient(),
	}
}

// GetAllProductBarcodes ดึงข้อมูลบาร์โค้ดสินค้าทั้งหมด
func (r *ProductRepository) GetAllProductBarcodes() ([]ProductBarcode, error) {
	query := `
		SELECT 
			barcode,
			(SELECT name_1 FROM ic_inventory WHERE code=ic_code) as name,
			unit_code,
			(SELECT name_1 FROM ic_unit WHERE code=unit_code) as unit_name 
		FROM ic_inventory_barcode
		ORDER BY barcode`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var productBarcodes []ProductBarcode
	for rows.Next() {
		var pb ProductBarcode
		err := rows.Scan(&pb.Barcode, &pb.Name, &pb.UnitCode, &pb.UnitName)
		if err != nil {
			return nil, err
		}
		productBarcodes = append(productBarcodes, pb)
	}

	return productBarcodes, nil
}

// CreateTable สร้างตารางสินค้า
func (r *ProductRepository) CreateTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS products (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL,
		description TEXT,
		price DECIMAL(10,2) NOT NULL,
		stock INTEGER NOT NULL DEFAULT 0,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	_, err := r.db.Exec(query)
	return err
}

// GetAll ดึงข้อมูลสินค้าทั้งหมด
func (r *ProductRepository) GetAll() ([]Product, error) {
	query := `SELECT id, name, description, price, stock, created_at, updated_at FROM products ORDER BY id`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.Price, &p.Stock, &p.CreatedAt, &p.UpdatedAt)
		if err != nil {
			return nil, err
		}
		products = append(products, p)
	}

	return products, nil
}

// GetByID ดึงข้อมูลสินค้าตาม ID
func (r *ProductRepository) GetByID(id int) (*Product, error) {
	query := `SELECT id, name, description, price, stock, created_at, updated_at FROM products WHERE id = $1`

	var p Product
	err := r.db.QueryRow(query, id).Scan(&p.ID, &p.Name, &p.Description, &p.Price, &p.Stock, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}

	return &p, nil
}

// Create เพิ่มสินค้าใหม่
func (r *ProductRepository) Create(product *Product) error {
	query := `
		INSERT INTO products (name, description, price, stock) 
		VALUES ($1, $2, $3, $4) 
		RETURNING id, created_at, updated_at`

	err := r.db.QueryRow(query, product.Name, product.Description, product.Price, product.Stock).
		Scan(&product.ID, &product.CreatedAt, &product.UpdatedAt)

	return err
}

// Update อัปเดตข้อมูลสินค้า
func (r *ProductRepository) Update(product *Product) error {
	query := `
		UPDATE products 
		SET name = $1, description = $2, price = $3, stock = $4, updated_at = CURRENT_TIMESTAMP 
		WHERE id = $5
		RETURNING updated_at`

	err := r.db.QueryRow(query, product.Name, product.Description, product.Price, product.Stock, product.ID).
		Scan(&product.UpdatedAt)

	return err
}

// Delete ลบสินค้า
func (r *ProductRepository) Delete(id int) error {
	query := `DELETE FROM products WHERE id = $1`
	_, err := r.db.Exec(query, id)
	return err
}

// UploadInventoryItemsBatch upload ข้อมูลสินค้าเป็น batch
func (r *ProductRepository) UploadInventoryItemsBatch(items []InventoryItem, batchSize int) error {
	totalItems := len(items)
	fmt.Printf("กำลัง upload ข้อมูลสินค้าทั้งหมด %d รายการ (batch size: %d)\n", totalItems, batchSize)

	for i := 0; i < totalItems; i += batchSize {
		end := i + batchSize
		if end > totalItems {
			end = totalItems
		}

		batch := items[i:end]
		fmt.Printf("กำลัง upload batch %d/%d (รายการ %d-%d)\n",
			(i/batchSize)+1, (totalItems+batchSize-1)/batchSize, i+1, end)

		err := r.uploadBatchViaAPI(batch)
		if err != nil {
			return fmt.Errorf("error uploading batch %d: %v", (i/batchSize)+1, err)
		}

		fmt.Printf("✅ Upload batch %d สำเร็จ\n", (i/batchSize)+1)
	}

	fmt.Printf("✅ Upload ข้อมูลสินค้าทั้งหมด %d รายการเสร็จสิ้น\n", totalItems)
	return nil
}

// UploadInventoryItemsBatchViaAPI upload ข้อมูลสินค้าเป็น batch ผ่าน API
func (r *ProductRepository) UploadInventoryItemsBatchViaAPI(items []InventoryItem, batchSize int) error {
	totalItems := len(items)
	fmt.Printf("กำลัง upload ข้อมูลสินค้าทั้งหมด %d รายการผ่าน API (batch size: %d)\n", totalItems, batchSize)

	for i := 0; i < totalItems; i += batchSize {
		end := i + batchSize
		if end > totalItems {
			end = totalItems
		}

		batch := items[i:end]
		batchNum := (i / batchSize) + 1
		totalBatches := (totalItems + batchSize - 1) / batchSize

		fmt.Printf("กำลัง upload batch %d/%d (รายการ %d-%d) ผ่าน API\n",
			batchNum, totalBatches, i+1, end)

		err := r.uploadBatchViaAPI(batch)
		if err != nil {
			return fmt.Errorf("error uploading batch %d via API: %v", batchNum, err)
		}

		fmt.Printf("✅ Upload batch %d สำเร็จ (%d รายการ)\n", batchNum, len(batch))
	}

	fmt.Printf("✅ Upload ข้อมูลสินค้าทั้งหมด %d รายการเสร็จสิ้น (ผ่าน API)\n", totalItems)
	return nil
}

// uploadBatchViaAPI upload หนึ่ง batch ของข้อมูลผ่าน API
func (r *ProductRepository) uploadBatchViaAPI(items []InventoryItem) error {
	if len(items) == 0 {
		return nil
	}

	// สร้าง bulk insert query แบบ VALUES literal
	var valueStrings []string
	for _, item := range items {
		// Escape single quotes ใน string values
		icCode := strings.ReplaceAll(item.IcCode, "'", "''")
		barcode := strings.ReplaceAll(item.Barcode, "'", "''")
		name := strings.ReplaceAll(item.Name, "'", "''")
		unitCode := strings.ReplaceAll(item.UnitCode, "'", "''")
		unitName := strings.ReplaceAll(item.UnitName, "'", "''")

		valueString := fmt.Sprintf(
			"('%s', '%s', '%s', '%s', '%s')",
			icCode, barcode, name, unitCode, unitName,
		)
		valueStrings = append(valueStrings, valueString)
	}
	query := fmt.Sprintf(`
		INSERT INTO ic_inventory_barcode 
		(ic_code, barcode, name, unit_code, unit_name)
		VALUES %s
		`,
		strings.Join(valueStrings, ","))

	// ใช้ API client แทน direct database connection
	resp, err := r.apiClient.ExecuteCommand(query)
	if err != nil {
		return fmt.Errorf("API command error: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("API command failed: %s", resp.Message)
	}

	return nil
}

// SyncWithMainTable ขั้นตอนที่ 4: ซิงค์ข้อมูลกับ main table
func (r *ProductRepository) SyncWithMainTable() error {
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_inventory_barcode...")

	// สร้างตาราง ic_inventory_barcode ถ้าไม่มี
	err := r.apiClient.CreateInventoryBarcodeTable()
	if err != nil {
		return fmt.Errorf("error creating ic_inventory_barcode table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_inventory_barcode เรียบร้อยแล้ว")
	fmt.Println("กำลังซิงค์ข้อมูลสินค้า...")
	// ซิงค์ข้อมูล
	insertCount, totalCount, err := r.apiClient.SyncInventoryBarcodeData()
	if err != nil {
		return fmt.Errorf("error syncing data: %v", err)
	}
	fmt.Printf("   - จำนวนรายการที่อัปเดต: %d\n   - จำนวนรายการทั้งหมด: %d\n", insertCount, totalCount)
	fmt.Println("กำลังตรวจสอบสถิติการซิงค์...") // ดึงสถิติการซิงค์
	count, _, err := r.apiClient.GetSyncStatistics()
	if err != nil {
		fmt.Printf("⚠️ ไม่สามารถดึงสถิติได้: %v\n", err)
		return nil
	}

	fmt.Printf("\n📊 สถิติการซิงค์:\n")
	fmt.Printf("   - จำนวนรายการที่ซิงค์: %d รายการ\n", count)

	return nil
}

// GetBalanceDataFromLocal ดึงข้อมูล balance จากฐานข้อมูล local
func (r *ProductRepository) GetBalanceDataFromLocal() ([]interface{}, error) {
	fmt.Println("กำลังดึงข้อมูล balance จากฐานข้อมูล local...")

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
		ORDER BY itd.item_code, itd.wh_code`

	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error querying balance data: %v", err)
	}
	defer rows.Close()

	var results []interface{}
	for rows.Next() {
		var icCode, warehouse, unitCode string
		var balanceQty float64

		err := rows.Scan(&icCode, &warehouse, &unitCode, &balanceQty)
		if err != nil {
			return nil, fmt.Errorf("error scanning balance row: %v", err)
		}

		item := map[string]interface{}{
			"ic_code":      icCode,
			"warehouse":    warehouse,
			"ic_unit_code": unitCode,
			"balance_qty":  balanceQty,
		}
		results = append(results, item)
	}

	fmt.Printf("ดึงข้อมูล balance จาก local ได้ %d รายการ\n", len(results))
	return results, nil
}

/*// SyncBalanceWithAPI ขั้นตอนที่ 5: ซิงค์ข้อมูล balance กับ API แบบ batch
func (r *ProductRepository) SyncBalanceWithAPI() error {
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ic_balance บน API...")

	// สร้างตาราง ic_balance ถ้าไม่มี
	err := r.apiClient.CreateBalanceTable()
	if err != nil {
		return fmt.Errorf("error creating ic_balance table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ic_balance เรียบร้อยแล้ว")

	// ดึงข้อมูล balance จาก local
	localData, err := r.GetBalanceDataFromLocal()
	if err != nil {
		return fmt.Errorf("error getting local balance data: %v", err)
	}

	if len(localData) == 0 {
		fmt.Println("ไม่มีข้อมูล balance ใน local database")
		return nil
	}
	// ซิงค์ข้อมูล balance
	fmt.Println("กำลังซิงค์ข้อมูล balance...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ\n", len(localData))

	totalCount, err := r.apiClient.SyncBalanceData(localData)
	if err != nil {
		return fmt.Errorf("error syncing balance data: %v", err)
	}
	fmt.Printf("✅ ซิงค์ข้อมูล balance เรียบร้อยแล้ว\n")
	fmt.Printf("📊 สถิติการซิงค์ balance:\n")
	fmt.Printf("   - ข้อมูลที่ซิงค์: %d รายการ (แบบ batch)\n", totalCount)

	return nil
}
*/
// GetAllCustomersFromSource ดึงข้อมูลลูกค้าทั้งหมดจากฐานข้อมูลต้นทาง
func (r *ProductRepository) GetAllCustomersFromSource() ([]interface{}, error) {
	query := `
		SELECT code, price_level 
		FROM ar_customer
		WHERE code IS NOT NULL AND code != ''
		ORDER BY code
	`

	fmt.Println("กำลังดึงข้อมูลลูกค้าจาก ar_customer...")
	rows, err := r.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("error executing customer query: %v", err)
	}
	defer rows.Close()

	var customers []interface{}
	count := 0

	for rows.Next() {
		var customer CustomerItem
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

	fmt.Printf("ดึงข้อมูลลูกค้าจากฐานข้อมูลต้นทางได้ %d รายการ\n", len(customers))
	return customers, nil
}

/*// SyncCustomerWithAPI ซิงค์ข้อมูลลูกค้ากับ API
func (r *ProductRepository) SyncCustomerWithAPI() error {
	// 1. ตรวจสอบและสร้างตาราง ar_customer บน API
	fmt.Println("กำลังตรวจสอบและสร้างตาราง ar_customer บน API...")
	err := r.apiClient.CreateCustomerTable()
	if err != nil {
		return fmt.Errorf("error creating customer table: %v", err)
	}
	fmt.Println("✅ ตรวจสอบ/สร้างตาราง ar_customer เรียบร้อยแล้ว")

	// 2. ดึงข้อมูลลูกค้าจาก local database
	fmt.Println("กำลังดึงข้อมูลลูกค้าจากฐานข้อมูล local...")
	localData, err := r.GetAllCustomersFromSource()
	if err != nil {
		return fmt.Errorf("error getting local customer data: %v", err)
	}

	if len(localData) == 0 {
		fmt.Println("ไม่มีข้อมูลลูกค้าใน local database")
		return nil
	}

	fmt.Printf("ดึงข้อมูลลูกค้าจาก local ได้ %d รายการ\n", len(localData))

	// 3. ดึงข้อมูลลูกค้าที่มีอยู่จาก API (ไม่จำเป็นสำหรับ batch UPSERT)
	fmt.Println("กำลังดึงข้อมูลลูกค้าที่มีอยู่จาก API...")
	existingData, err := r.apiClient.GetExistingCustomerData()
	if err != nil {
		return fmt.Errorf("error getting existing customer data: %v", err)
	}
	fmt.Printf("พบข้อมูลลูกค้าใน API อยู่แล้ว %d รายการ\n", len(existingData))

	// 4. ซิงค์ข้อมูลโดยส่งทั้งหมดแบบ batch UPSERT
	fmt.Println("กำลังเปรียบเทียบและซิงค์ข้อมูลลูกค้า (batch UPSERT)...")
	fmt.Printf("📦 จะประมวลผลข้อมูล %d รายการ โดยใช้ batch UPSERT\n", len(localData))

	insertCount, updateCount, err := r.apiClient.SyncCustomerData(localData, existingData)
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
*/