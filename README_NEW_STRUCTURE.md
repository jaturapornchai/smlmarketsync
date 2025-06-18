# SML Market Sync - โครงสร้างโค้ดใหม่

โปรแกรม Go สำหรับซิงค์ข้อมูลจากฐานข้อมูล PostgreSQL ไปยัง API โดยแยกโค้ดตาม steps เพื่อให้จัดการง่ายขึ้น

## 📁 โครงสร้างไฟล์ใหม่

```
smlmarketsync/
├── main.go                    # Main orchestrator
├── go.mod                     # Go module dependencies
├── smlmarketsync.json         # Database configuration
├── config/
│   ├── database.go            # Database connection
│   └── api_client.go          # API client functions
├── types/
│   └── types.go               # Shared data structures
├── steps/
│   ├── product_sync.go        # Steps 1-4: Product/Inventory sync
│   ├── balance_sync.go        # Step 5: Balance sync
│   └── customer_sync.go       # Step 6: Customer sync
└── models/
    └── product.go             # Legacy models (เก็บไว้สำหรับ backward compatibility)
```

## 🔄 ขั้นตอนการ Sync

### Steps 1-4: Product/Inventory Sync (`steps/product_sync.go`)
- **Step 1**: เตรียมตาราง `ic_inventory_barcode_temp`
- **Step 2**: ดึงข้อมูลสินค้าจาก local database
- **Step 3**: Upload ข้อมูลแบบ batch ไป temp table
- **Step 4**: Sync ข้อมูลระหว่าง temp table และ main table

### Step 5: Balance Sync (`steps/balance_sync.go`)
- ดึงข้อมูล balance จาก `ic_balance`
- Sync แบบ batch UPSERT ไปยัง API

### Step 6: Customer Sync (`steps/customer_sync.go`)
- ดึงข้อมูลลูกค้าจาก `ar_customer`
- Sync แบบ batch UPSERT ไปยัง API

## 🏗️ ประโยชน์ของโครงสร้างใหม่

### 1. **แยกตาม Responsibility**
- แต่ละ step มี responsibility ชัดเจน
- ง่ายต่อการ debug และแก้ไข
- สามารถเทสต์แต่ละ step แยกกันได้

### 2. **Code Reusability**
- Types ถูกแยกออกมาใน `types/types.go`
- API client functions รวมอยู่ใน `config/api_client.go`
- แต่ละ step สามารถใช้ซ้ำได้

### 3. **Maintainability**
- โค้ดสั้นลง อ่านง่ายขึ้น
- แต่ละไฟล์มีขนาดเหมาะสม (~150-300 บรรทัด)
- ลดความซับซ้อนของไฟล์เดียวใหญ่ๆ

### 4. **Scalability**
- เพิ่ม step ใหม่ได้ง่าย
- แก้ไข business logic แยกตาม domain
- สามารถรัน step เดียวได้ (future enhancement)

## 🚀 การใช้งาน

### รันทั้งหมด
```bash
go run .
```

### Build executable
```bash
go build .
```

## ⚙️ Configuration

การตั้งค่าเหมือนเดิมใน `smlmarketsync.json`:
```json
{
  "host": "192.168.2.213",
  "port": 5432,
  "user": "postgres",
  "password": "your_password",
  "dbname": "your_database",
  "sslmode": "disable"
}
```

## 📊 Performance

### Batch Sizes:
- **Products**: 500 รายการ/batch
- **Balance**: 100 รายการ/batch  
- **Customer**: 200 รายการ/batch

### Timeout:
- API timeout: 120 วินาที (เพิ่มขึ้นสำหรับ batch ขนาดใหญ่)

## 🔧 Future Enhancements

1. **Single Step Execution**: รัน step เดียวผ่าน command line flags
2. **Parallel Processing**: รัน steps บางส่วนแบบ parallel
3. **Configuration per Step**: กำหนด batch size แยกตาม step
4. **Monitoring & Metrics**: เพิ่ม metrics สำหรับติดตาม performance
5. **Recovery**: Resume จากจุดที่ล้มเหลว

## 📝 Migration Notes

- ไฟล์เก่า `models/product.go` ยังคงเก็บไว้สำหรับ backward compatibility
- หาก code เก่ามี dependency กับ `models/product.go` ยังสามารถใช้งานได้ปกติ
- แนะนำให้ migrate ไปใช้โครงสร้างใหม่ทีละส่วน
