package main

import (
	"fmt"
	"log"
	"smlmarketsync/config"
	"smlmarketsync/steps"
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

	// ขั้นตอนที่ 1-4: Sync สินค้า (Product/Inventory)
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync สินค้า (Steps 1-4)")
	productStep := steps.NewProductSyncStep(db)
	err = productStep.ExecuteProductSync()
	if err != nil {
		log.Fatalf("❌ Error in product sync steps: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync สินค้า เสร็จสิ้น")

	// ขั้นตอนที่ 5: Sync Balance
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync balance (Step 5)")
	balanceStep := steps.NewBalanceSyncStep(db)
	err = balanceStep.ExecuteBalanceSync()
	if err != nil {
		log.Fatalf("❌ Error in balance sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync balance เสร็จสิ้น")

	// ขั้นตอนที่ 6: Sync Customer
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ลูกค้า (Step 6)")
	customerStep := steps.NewCustomerSyncStep(db)
	err = customerStep.ExecuteCustomerSync()
	if err != nil {
		log.Fatalf("❌ Error in customer sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ลูกค้า เสร็จสิ้น")
	// ขั้นตอนที่ 7: Sync Price
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ราคาสินค้า (Step 7)")
	priceStep := steps.NewPriceSyncStep(db)
	err = priceStep.ExecutePriceSync()
	if err != nil {
		log.Fatalf("❌ Error in price sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ราคาสินค้า เสร็จสิ้น")

	fmt.Println("\n🎉 การซิงค์ข้อมูลเสร็จสิ้นทุกขั้นตอน!")
	fmt.Println("ข้อมูลถูกซิงค์ครบทุกตาราง: ic_inventory_barcode_temp, ic_inventory_barcode, ic_balance, ar_customer, และ ic_inventory_price")
	fmt.Println("รวมถึงการตรวจสอบความถูกต้องและทำความสะอาดข้อมูลแล้ว")
}
