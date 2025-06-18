package main

import (
	"fmt"
	"log"
	"smlmarketsync/config"
	"smlmarketsync/models"
)

func main() {
	fmt.Println("=== โปรแกรมซิงค์ข้อมูลสินค้าไป ic_inventory_barcode_temp ===")

	// เชื่อมต่อฐานข้อมูลต้นทาง (Read-only สำหรับดึงข้อมูล)
	dbConfig := config.NewDatabaseConfig()
	db, err := dbConfig.Connect()
	if err != nil {
		log.Fatal("Failed to connect to source database:", err)
	}
	defer db.Close()

	// สร้าง repository และ API client
	productRepo := models.NewProductRepository(db)

	// 1. เตรียมตาราง ic_inventory_barcode_temp ผ่าน API
	fmt.Println("\n=== ขั้นตอนที่ 1: เตรียมตาราง ic_inventory_barcode_temp ผ่าน API ===")
	err = productRepo.PrepareInventoryTempTableViaAPI()
	if err != nil {
		log.Fatalf("Error preparing ic_inventory_barcode_temp table via API: %v", err)
	}

	// 2. ดึงข้อมูลสินค้าทั้งหมดจากฐานข้อมูลต้นทาง
	fmt.Println("\n=== ขั้นตอนที่ 2: ดึงข้อมูลสินค้าทั้งหมดจากฐานข้อมูลต้นทาง ===")
	inventoryItems, err := productRepo.GetAllInventoryItemsFromSource()
	if err != nil {
		log.Fatalf("Error reading inventory items from source: %v", err)
	}

	if len(inventoryItems) == 0 {
		fmt.Println("ไม่มีข้อมูลสินค้าในฐานข้อมูลต้นทาง")
		return
	}

	fmt.Printf("พบข้อมูลสินค้าทั้งหมด %d รายการ\n", len(inventoryItems))
	// 3. Upload ข้อมูลเป็น batch ผ่าน API
	fmt.Println("\n=== ขั้นตอนที่ 3: Upload ข้อมูลไป ic_inventory_barcode_temp ผ่าน API ===")
	batchSize := 500 // Upload ครั้งละ 500 รายการ เพื่อความเร็วและเสถียรภาพ
	err = productRepo.UploadInventoryItemsBatchViaAPI(inventoryItems, batchSize)
	if err != nil {
		log.Fatalf("Error uploading inventory items via API: %v", err)
	}
	// 4. ซิงค์ข้อมูลกับ main table
	fmt.Println("\n=== ขั้นตอนที่ 4: ซิงค์ข้อมูลกับตาราง ic_inventory_barcode ===")
	err = productRepo.SyncWithMainTable()
	if err != nil {
		log.Fatalf("Error syncing with main table: %v", err)
	}
	// 5. ซิงค์ข้อมูล balance กับ API
	fmt.Println("\n=== ขั้นตอนที่ 5: ซิงค์ข้อมูล balance กับ API ===")
	err = productRepo.SyncBalanceWithAPI()
	if err != nil {
		log.Fatalf("Error syncing balance with API: %v", err)
	}

	// 6. ซิงค์ข้อมูลลูกค้ากับ API
	fmt.Println("\n=== ขั้นตอนที่ 6: ซิงค์ข้อมูลลูกค้ากับ API ===")
	err = productRepo.SyncCustomerWithAPI()
	if err != nil {
		log.Fatalf("Error syncing customer with API: %v", err)
	}

	fmt.Printf("\n✅ การซิงค์ข้อมูลเสร็จสิ้นทุกขั้นตอน!\n")
	fmt.Printf("รวมจำนวนสินค้าที่ประมวลผล: %d รายการ\n", len(inventoryItems))
	fmt.Println("ข้อมูลถูกซิงค์ครบทุกตาราง: ic_inventory_barcode_temp, ic_inventory_barcode, ic_balance, และ ar_customer")
}
