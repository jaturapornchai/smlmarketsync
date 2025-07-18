package syncprocess

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"smlmarketsync/config"
	"strings"
)

// SyncImages อ่าน log จาก sml_market_sync (table_id=6) แล้ว sync รูปภาพ
func (s *SyncProcess) SyncImages() error {
	rows, err := s.dbImage.Query(`SELECT id, active_code, row_order_ref FROM sml_market_sync WHERE table_id = 6 ORDER BY id`)
	if err != nil {
		log.Printf("❌ Query sml_market_sync error: %v", err)
		return err
	}
	defer rows.Close()

	found := false

	for rows.Next() {
		found = true
		var id, activeCode, rowOrderRef int
		if err := rows.Scan(&id, &activeCode, &rowOrderRef); err != nil {
			log.Printf("❌ Scan row error: %v", err)
			continue
		}

		log.Printf("🔎 พบ log รูปภาพ id=%d active_code=%d row_order_ref=%d", id, activeCode, rowOrderRef)

		switch activeCode {
		case 1, 2: // insert/update
			var imageID, guidCode string
			var imageFile []byte
			err := s.dbImage.QueryRow(`SELECT image_id, guid_code, image_file FROM images WHERE roworder = $1`, rowOrderRef).
				Scan(&imageID, &guidCode, &imageFile)
			if err != nil {
				continue
			}

			url, err := uploadImageToMinIO(imageFile, guidCode)
			if err != nil {
				log.Printf("❌ Upload image_id=%s error: %v", imageID, err)
				continue
			}

			shouldBeUrl := fmt.Sprintf(s.imagePatternUrl, guidCode)

			if url != shouldBeUrl {
				log.Printf("🔄 Updating image URL for row_order_ref=%d from %s to %s", rowOrderRef, url, shouldBeUrl)
				url = shouldBeUrl
			} else {
				log.Printf("✅ Image URL for row_order_ref=%d is already correct: %s", rowOrderRef, url)
			}

		case 3: // delete
			var imageID, imageURL string
			err := s.dbImage.QueryRow(`SELECT image_id, image_url FROM images WHERE roworder = $1`, rowOrderRef).
				Scan(&imageID, &imageURL)
			if err == nil && imageURL != "" {
				deleteImageFromMinIO(imageURL)

			}
		}

		_, _ = s.dbImage.Exec(`DELETE FROM sml_market_sync WHERE id = $1`, id)
	}

	if !found {
		log.Println("ℹ️ ไม่พบ log รูปภาพใน sml_market_sync")
	}
	return nil
}

// uploadImageToMinIO อัปโหลดรูปไป MinIO

func uploadImageToMinIO(imageFile []byte, guidCode string) (string, error) {
	// ใช้ค่าจาก config
	filename := guidCode + ".jpeg"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	_ = writer.WriteField("folderName", config.FolderName)

	// ระบุ Content-Type เป็น image/jpeg
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, "file", filename))
	h.Set("Content-Type", "image/jpeg")
	part, err := writer.CreatePart(h)
	if err != nil {
		return "", fmt.Errorf("create form file error: %v", err)
	}
	_, err = io.Copy(part, bytes.NewReader(imageFile))
	if err != nil {
		return "", fmt.Errorf("copy image bytes error: %v", err)
	}
	writer.Close()

	log.Printf("Uploading image: %s, size: %d bytes", filename, len(imageFile))

	req, err := http.NewRequest("POST", config.UrlImageUpload+"/upload", body)
	if err != nil {
		return "", fmt.Errorf("create request error: %v", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http post error: %v", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	log.Println("========== MinIO Raw Response ==========")
	log.Println(string(respBody))
	log.Println("========================================")

	var result struct {
		URL string `json:"url"`
	}
	_ = json.Unmarshal(respBody, &result)
	return result.URL, nil
}

// deleteImageFromMinIO ลบรูปที่ MinIO
func deleteImageFromMinIO(imageURL string) error {
	// ตัวอย่างการแยก folderName/filename จาก url
	parts := strings.Split(imageURL, "/")
	if len(parts) < 2 {
		return fmt.Errorf("invalid image url")
	}
	folderName := parts[len(parts)-2]
	filename := parts[len(parts)-1]
	url := fmt.Sprintf("%s/files/%s/%s", config.UrlImageUpload, folderName, filename)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
