package main

import (
	"fmt"
	"log"
	"smlmarketsync/config"
	"smlmarketsync/steps"
)

func main() {
	fmt.Println("=== โปรแกรมซิงค์ข้อมูลสินค้าไป ic_inventory_barcode ===")

	// เชื่อมต่อฐานข้อมูลต้นทาง (Read-only สำหรับดึงข้อมูล)
	dbConfig := config.NewDatabaseConfig()
	db, err := dbConfig.Connect()
	if err != nil {
		log.Fatal("Failed to connect to source database:", err)
	}
	defer db.Close()

	// ตรวจสอบ บน database ว่ามี Table sml_market_sync หรือไม่
	if !config.TableExists(db, "sml_market_sync") {
		// สร้างตาราง sml_market_sync ถ้ายังไม่มี
		err = config.CreateSyncTable(db)
		if err != nil {
			log.Fatalf("Failed to create sml_market_sync table: %v", err)
		}
		fmt.Println("✅ ตาราง sml_market_sync ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ ตาราง sml_market_sync มีอยู่แล้ว")
	}
	// ตรวจสอบ บน database ว่ามี ใน table ic_inventory_price มี tigger หรือไม่
	if !config.PriceTriggerExists(db) {
		// สร้าง trigger สำหรับ ic_inventory_price ถ้ายังไม่มี
		err = config.CreatePriceTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ic_inventory_price: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ic_inventory_price ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ic_inventory_price มีอยู่แล้ว")
	} // ตรวจสอบ บน database ว่ามี ใน table ic_inventory_price_formula มี tigger หรือไม่
	if !config.PriceFormulaTriggerExists(db) {
		// สร้าง trigger สำหรับ ic_inventory_price_formula ถ้ายังไม่มี
		err = config.CreatePriceFormulaTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ic_inventory_price_formula: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ic_inventory_price_formula ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ic_inventory_price_formula มีอยู่แล้ว")
	}

	// ตรวจสอบ บน database ว่ามี ใน table ic_inventory มี tigger หรือไม่
	if !config.InventoryTriggerExists(db) {
		// สร้าง trigger สำหรับ ic_inventory_barcode ถ้ายังไม่มี
		err = config.CreateInventoryTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ic_inventory: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ic_inventory ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ic_inventory มีอยู่แล้ว")
	}

	// ตรวจสอบ บน database ว่ามี ใน table ic_inventory_barcode มี tigger หรือไม่
	if !config.InventoryBarcodeTriggerExists(db) {
		// สร้าง trigger สำหรับ ic_inventory_barcode ถ้ายังไม่มี
		err = config.CreateInventoryBarcodeTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ic_inventory_barcode: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ic_inventory_barcode ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ic_inventory_barcode มีอยู่แล้ว")
	}

	// ตรวจสอบ บน database ว่ามี ใน table ar_customer มี tigger หรือไม่
	if !config.CustomerTriggerExists(db) {
		// สร้าง trigger สำหรับ ar_customer ถ้ายังไม่มี
		err = config.CreateCustomerTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ar_customer: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ar_customer ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ar_customer มีอยู่แล้ว")
	}

	// Sync Data Start
	fmt.Println("🔄 เริ่มขั้นตอนการซิงค์ข้อมูล...")
	// Sync สินค้า (Product/Inventory)
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync สินค้า")
	productStep := steps.NewProductSyncStep(db)
	err = productStep.ExecuteProductSync()
	if err != nil {
		log.Fatalf("❌ Error in product sync steps: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync สินค้า เสร็จสิ้น")
	// Sync Price
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ราคาสินค้า")
	priceStep := steps.NewPriceSyncStep(db)
	err = priceStep.ExecutePriceSync()
	if err != nil {
		log.Fatalf("❌ Error in price sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ราคาสินค้า เสร็จสิ้น")

	// Sync Price Formula
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync สูตรราคาสินค้า")
	priceFormulaStep := steps.NewPriceFormulaSyncStep(db)
	err = priceFormulaStep.ExecutePriceFormulaSync()
	if err != nil {
		log.Fatalf("❌ Error in price formula sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync สูตรราคาสินค้า เสร็จสิ้น")

	// Sync ProductBarcode
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ProductBarcode")
	productBarcodeStep := steps.NewProductBarcodeSyncStep(db)
	err = productBarcodeStep.ExecuteProductBarcodeSync()
	if err != nil {
		log.Fatalf("❌ Error in ProductBarcode sync steps: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ProductBarcode เสร็จสิ้น")

	// Sync Customer
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ลูกค้า")
	customerStep := steps.NewCustomerSyncStep(db)
	err = customerStep.ExecuteCustomerSync()
	if err != nil {
		log.Fatalf("❌ Error in customer sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ลูกค้า เสร็จสิ้น")

	// Sync Balance
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync balance")
	balanceStep := steps.NewBalanceSyncStep(db)
	err = balanceStep.ExecuteBalanceSync()
	if err != nil {
		log.Fatalf("❌ Error in balance sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync balance เสร็จสิ้น")

	fmt.Println("\n🎉 การซิงค์ข้อมูลเสร็จสิ้นทุกขั้นตอน!")
	fmt.Println("ข้อมูลถูกซิงค์ครบทุกตาราง: ic_inventory_barcode, ic_balance, ar_customer, ic_inventory_price, และ ic_inventory_price_formula")
}
