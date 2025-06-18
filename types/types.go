package types

import (
	"database/sql"
	"time"
)

// Product struct for basic product information
type Product struct {
	ID          int       `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Price       float64   `json:"price"`
	Stock       int       `json:"stock"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ProductBarcode struct for barcode data
type ProductBarcode struct {
	Barcode  string         `json:"barcode"`
	Name     sql.NullString `json:"name"`
	UnitCode string         `json:"unit_code"`
	UnitName sql.NullString `json:"unit_name"`
}

// InventoryItem สำหรับข้อมูลที่จะ upload ไป ic_inventory_barcode_temp
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
