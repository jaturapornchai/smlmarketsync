# SML Market Sync

โปรแกรมซิงค์ข้อมูลสินค้าและ balance จากฐานข้อมูล PostgreSQL local ไปยัง API ปลายทางแบบ real-time

## คุณสมบัติ

- ✅ ซิงค์ข้อมูลสินค้า (`ic_inventory_barcode`) แบบ batch (500 รายการ/batch)
- ✅ ซิงค์ข้อมูล balance (`ic_balance`) แบบ UPSERT batch (100 รายการ/batch)
- ✅ ใช้ PostgreSQL `INSERT ... ON CONFLICT` สำหรับ UPSERT อัตโนมัติ
- ✅ ตรวจสอบข้อมูลเปลี่ยนแปลงก่อน UPDATE (ประหยัด I/O)
- ✅ Retry mechanism และ error handling
- ✅ Progress tracking แบบ real-time
- ✅ เชื่อมต่อฐานข้อมูลผ่าน configuration file

## การติดตั้ง

1. Clone repository:
   ```bash
   git clone https://github.com/YOUR_USERNAME/smlmarketsync.git
   cd smlmarketsync
   ```

2. ติดตั้ง dependencies:
   ```bash
   go mod download
   ```

3. สร้างไฟล์ `smlmarketsync.json` สำหรับการตั้งค่า:
   ```json
   {
     "database": {
       "host": "your_host",
       "port": 5432,
       "user": "your_user",
       "password": "your_password", 
       "dbname": "your_database",
       "sslmode": "disable"
     }
   }
   ```

## การใช้งาน

```bash
go run .
```

หรือ build เป็น executable:
```bash
go build -o smlmarketsync.exe
./smlmarketsync.exe
```

## โครงสร้างโปรแกรม

### ขั้นตอนการทำงาน

1. **เตรียมตาราง temp** - สร้าง/ลบ `ic_inventory_barcode_temp`
2. **ดึงข้อมูลสินค้า** - จากตาราง `ic_inventory_barcode` local
3. **Upload สินค้า** - ส่งข้อมูลแบบ batch ไปยัง temp table
4. **ซิงค์สินค้า** - เปรียบเทียบและซิงค์ระหว่าง temp กับ main table
5. **ซิงค์ balance** - ส่งข้อมูล `ic_balance` แบบ UPSERT batch

### API Endpoints

- `POST /v1/pgselect` - สำหรับ SELECT queries
- `POST /v1/pgcommand` - สำหรับ INSERT/UPDATE/DELETE queries

### Database Schema

#### ic_inventory_barcode
```sql
- ic_code VARCHAR(50)
- barcode VARCHAR(50) PRIMARY KEY
- name VARCHAR(255)
- unit_code VARCHAR(20)
- unit_name VARCHAR(100)
- price DECIMAL(15,4)
- status INTEGER
```

#### ic_balance
```sql
- ic_code VARCHAR(50)
- warehouse VARCHAR(50)
- ic_unit_code VARCHAR(20)
- balance_qty DECIMAL(15,4)
- PRIMARY KEY (ic_code, warehouse)
```

## ประสิทธิภาพ

- **เวลาการทำงาน**: ~3-4 นาที สำหรับ 18,000+ สินค้า และ 15,000+ balance
- **Batch Size**: 
  - สินค้า: 500 รายการ/batch
  - Balance: 100 รายการ/batch
- **Memory Usage**: ต่ำ เนื่องจากใช้ streaming และ batch processing

## เทคโนโลยี

- **Go 1.21+**
- **PostgreSQL** 
- **REST API** (JSON)
- **Batch Processing**
- **UPSERT Operations**

## การพัฒนา

### โครงสร้างไฟล์

```
├── main.go                 # Entry point
├── config/
│   ├── api_client.go      # API client และ batch operations
│   └── database.go        # Database configuration
├── models/
│   └── product.go         # Product models และ business logic
├── go.mod
├── go.sum
└── README.md
```

### การ Build

```bash
# Development
go run .

# Production build
go build -ldflags="-s -w" -o smlmarketsync.exe

# Cross-platform build
GOOS=linux GOARCH=amd64 go build -o smlmarketsync-linux
```

## License

MIT License

## การตั้งค่า

### การติดตั้ง dependencies
```bash
go mod tidy
```

### การรันโปรแกรม
```bash
go run main.go
```

หรือ build แล้วรัน:
```bash
go build -o product-reader.exe
./product-reader.exe
```

## การกำหนดค่าฐานข้อมูล

ใช้ไฟล์ `smlmarketsync.json` สำหรับกำหนดค่าการเชื่อมต่อฐานข้อมูล:
```json
{
  "database": {
    "host": "your_host",
    "port": "your_port", 
    "user": "your_user",
    "password": "your_password",
    "dbname": "your_database"
  }
}
```

## ผลลัพธ์ที่ได้

โปรแกรมจะแสดงผล:
1. ข้อความเริ่มต้นโปรแกรม
2. สถานะการเชื่อมต่อฐานข้อมูล
3. จำนวนสินค้าทั้งหมด (ถ้ามี)
4. ตารางแสดงข้อมูลสินค้า:
   - ID สินค้า
   - ชื่อสินค้า
   - รายละเอียด
   - ราคา (บาท)
   - จำนวนคงเหลือ
5. มูลค่ารวมของสินค้าทั้งหมด
6. ข้อความจบการทำงาน

## โครงสร้างโปรเจกต์

```
.
├── main.go              # Entry point ของแอปพลิเคชัน
├── go.mod               # Go module dependencies
├── go.sum               # Go module checksums
├── smlmarketsync.json   # Configuration file
├── config/
│   └── database.go      # Database configuration
├── models/
│   └── product.go       # Product model
└── handlers/
    └── product.go       # Product handlers
```
