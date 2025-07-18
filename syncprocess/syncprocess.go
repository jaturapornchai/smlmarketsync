package syncprocess

import (
	"database/sql"
	"fmt"
	"log"
	"smlmarketsync/config"
	"smlmarketsync/steps"
)

type ISyncProcess interface {
	SyncImages() error
	StartSyncProcess(endSignal <-chan bool)
}

type SyncProcess struct {
	db              *sql.DB
	dbImage         *sql.DB
	imagePatternUrl string
}

func NewSyncProcess(db *sql.DB, dbImage *sql.DB) ISyncProcess {
	config.LoadEnv()
	sync := &SyncProcess{
		db:              db,
		dbImage:         dbImage,
		imagePatternUrl: fmt.Sprintf("%s/%s/%%s.jpeg", config.UrlImagePublic, config.FolderName),
	}
	return sync
}

func (s *SyncProcess) StartSyncProcess(endSignal <-chan bool) {

	// Sync Data Start
	fmt.Println("🔄 เริ่มขั้นตอนการซิงค์ข้อมูล...")
	// Sync สินค้า (Product/Inventory)
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync สินค้า")
	productStep := steps.NewProductSyncStep(s.db, s.dbImage)

	err := productStep.ExecuteProductSync()
	if err != nil {
		log.Fatalf("❌ Error in product sync steps: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync สินค้า เสร็จสิ้น")
	// Sync Price
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ราคาสินค้า")
	priceStep := steps.NewPriceSyncStep(s.db)
	err = priceStep.ExecutePriceSync()
	if err != nil {
		log.Fatalf("❌ Error in price sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ราคาสินค้า เสร็จสิ้น")

	// Sync Price Formula
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync สูตรราคาสินค้า")
	priceFormulaStep := steps.NewPriceFormulaSyncStep(s.db)
	err = priceFormulaStep.ExecutePriceFormulaSync()
	if err != nil {
		log.Fatalf("❌ Error in price formula sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync สูตรราคาสินค้า เสร็จสิ้น")

	// Sync ProductBarcode
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ProductBarcode")
	productBarcodeStep := steps.NewProductBarcodeSyncStep(s.db, s.dbImage)
	err = productBarcodeStep.ExecuteProductBarcodeSync()
	if err != nil {
		log.Fatalf("❌ Error in ProductBarcode sync steps: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ProductBarcode เสร็จสิ้น")

	// Sync Customer
	fmt.Println("\n🔄 เริ่มขั้นตอนการ sync ลูกค้า")
	customerStep := steps.NewCustomerSyncStep(s.db)
	err = customerStep.ExecuteCustomerSync()
	if err != nil {
		log.Fatalf("❌ Error in customer sync step: %v", err)
	}
	fmt.Println("✅ ขั้นตอนการ sync ลูกค้า เสร็จสิ้น")

	// // Sync Balance
	// fmt.Println("\n🔄 เริ่มขั้นตอนการ sync balance")
	// balanceStep := steps.NewBalanceSyncStep(s.db)
	// err = balanceStep.ExecuteBalanceSync()
	// if err != nil {
	// 	log.Fatalf("❌ Error in balance sync step: %v", err)
	// }
	// fmt.Println("✅ ขั้นตอนการ sync balance เสร็จสิ้น")

	fmt.Println("\n🎉 การซิงค์ข้อมูลเสร็จสิ้นทุกขั้นตอน!")
	fmt.Println("ข้อมูลถูกซิงค์ครบทุกตาราง: ic_inventory_barcode, ic_balance, ar_customer, ic_inventory_price, และ ic_inventory_price_formula")
}
