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

	syncProcess := syncprocess.NewSyncProcess(db, dbImage)

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

				syncProcess.StartSyncProcess(done) // Run the task

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
