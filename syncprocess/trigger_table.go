package syncprocess

import (
	"database/sql"
	"fmt"
	"log"
	"smlmarketsync/config"
)

func VerifyTriggerDB(db *sql.DB) {

	// ตรวจสอบ บน database ว่ามี Table sml_market_sync หรือไม่
	if !config.TableExists(db, "sml_market_sync") {
		// สร้างตาราง sml_market_sync ถ้ายังไม่มี
		err := config.CreateSyncTable(db)
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
		err := config.CreatePriceTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ic_inventory_price: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ic_inventory_price ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ic_inventory_price มีอยู่แล้ว")
	} // ตรวจสอบ บน database ว่ามี ใน table ic_inventory_price_formula มี tigger หรือไม่
	if !config.PriceFormulaTriggerExists(db) {
		// สร้าง trigger สำหรับ ic_inventory_price_formula ถ้ายังไม่มี
		err := config.CreatePriceFormulaTrigger(db)
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
		err := config.CreateInventoryTrigger(db)
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
		err := config.CreateInventoryBarcodeTrigger(db)
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
		err := config.CreateCustomerTrigger(db)
		if err != nil {
			log.Fatalf("Failed to create trigger for ar_customer: %v", err)
		}
		fmt.Println("✅ Trigger สำหรับ ar_customer ถูกสร้างเรียบร้อยแล้ว")
	} else {
		fmt.Println("✅ Trigger สำหรับ ar_customer มีอยู่แล้ว")
	}

}
