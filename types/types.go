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

// InventoryItem สำหรับข้อมูลที่จะ upload ไป ic_inventory
type InventoryItem struct {
	RowOrderRef      int    `json:"row_order_ref"`
	IcCode           string `json:"ic_code"`
	Name             string `json:"name"`
	ItemType         int    `json:"item_type"`
	UnitStandardCode string `json:"unit_standard_code"`
}

type BarcodeItem struct {
	RowOrderRef int    `json:"row_order_ref"`
	IcCode      string `json:"ic_code"`
	Barcode     string `json:"barcode"`
	Name        string `json:"name"`
	UnitCode    string `json:"unit_code"`
	UnitName    string `json:"unit_name"`
}

// BalanceItem สำหรับข้อมูล ic_balance
type BalanceItem struct {
	IcCode     string  `json:"ic_code"`
	Warehouse  string  `json:"warehouse"`    // wh_code in database
	UnitCode   string  `json:"ic_unit_code"` // unit_code in database
	BalanceQty float64 `json:"balance_qty"`
}

// CustomerItem สำหรับข้อมูล ar_customer
type CustomerItem struct {
	RowOrderRef int    `json:"row_order_ref"`
	Code        string `json:"code"`
	PriceLevel  string `json:"price_level"`
}

// PriceItem สำหรับข้อมูล ic_inventory_price
type PriceItem struct {
	RowOrderRef int     `json:"row_order_ref"`
	IcCode      string  `json:"ic_code"`
	UnitCode    string  `json:"unit_code"`
	FromQty     float64 `json:"from_qty"`
	ToQty       float64 `json:"to_qty"`
	FromDate    string  `json:"from_date"`
	ToDate      string  `json:"to_date"`
	SaleType    string  `json:"sale_type"`
	SalePrice1  float64 `json:"sale_price1"`
	Status      string  `json:"status"`
	PriceType   string  `json:"price_type"`
	CustCode    string  `json:"cust_code"`
	SalePrice2  float64 `json:"sale_price2"`
	CustGroup1  string  `json:"cust_group_1"`
	PriceMode   string  `json:"price_mode"`
}

// PriceFormulaItem สำหรับข้อมูล ic_price_formula
type PriceFormulaItem struct {
	RowOrderRef int     `json:"row_order_ref"`
	IcCode        string `json:"ic_code"`
	UnitCode      string `json:"unit_code"`
	SaleType      int    `json:"sale_type"`
	Price0        string `json:"price_0"`
	Price1        string `json:"price_1"`
	Price2        string `json:"price_2"`
	Price3        string `json:"price_3"`
	Price4        string `json:"price_4"`
	Price5        string `json:"price_5"`
	Price6        string `json:"price_6"`
	Price7        string `json:"price_7"`
	Price8        string `json:"price_8"`
	Price9        string `json:"price_9"`
	TaxType       int    `json:"tax_type"`
	PriceCurrency int    `json:"price_currency"`
	CurrencyCode  string `json:"currency_code"`
}
