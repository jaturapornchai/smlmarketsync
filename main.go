package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"smlmarketsync/config"
	"smlmarketsync/syncprocess"
	"syscall"
	"time"
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

	syncprocess.VerifyTriggerDB(db)

	dbImage, err := dbConfig.ConnectDBImage()

	if err != nil {
		log.Fatal("Failed to connect to image database:", err)
	}
	defer dbImage.Close()

	// ตรวจสอบและสร้าง trigger สำหรับ images
	if !config.ImagesTriggerExists(dbImage) {
		if err := config.CreateImagesTrigger(dbImage); err != nil {
			log.Fatalf("❌ สร้าง images trigger ไม่สำเร็จ: %v", err)
		}
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				fmt.Println("Exiting...")
				return
			default:
				start := time.Now()

				process := syncprocess.NewSyncProcess(db, dbImage)
				process.StartSyncProcess(done) // Run the task

				// // sync รูปภาพ
				// err := process.SyncImages()
				// if err != nil {
				// 	log.Printf("❌ SyncImages error: %v", err)
				// }

				// Measure the time taken to run DoSomething
				elapsed := time.Since(start)

				if elapsed < 15*time.Second {
					fmt.Printf("Job completed in %s, waiting for the next interval...\n", elapsed)
					// Sleep for the remaining time to ensure 5-second intervals
					time.Sleep(15*time.Second - elapsed)
				}
			}
		}
	}()

	// Wait for exit signal
	<-exitChan
	fmt.Println("Received exit signal, stopping ticker...")

	// Notify the goroutine to stop
	done <- true

}
