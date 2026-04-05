# EPİAŞ Veri Botu — Web Uygulaması

## 📁 Klasör Yapısı
```
epias_app/
├── main.py            ← FastAPI backend (tüm mantık burada)
├── requirements.txt   ← Gerekli kütüphaneler
├── outputs/           ← İndirilebilir Excel dosyaları (otomatik oluşur)
└── static/
    └── index.html     ← Web arayüzü
```

## 🚀 Kurulum ve Çalıştırma

### 1. Kütüphaneleri Kur
```bash
pip install -r requirements.txt
```

### 2. Uygulamayı Başlat
```bash
uvicorn main:app --reload
```

### 3. Tarayıcıda Aç
```
http://localhost:8000
```

---

## 🌐 API Endpointleri

| Method | URL | Açıklama |
|--------|-----|----------|
| POST | `/api/gop/start` | GÖP işlemini başlatır, job_id döner |
| POST | `/api/kgup/start` | KGÜP işlemini başlatır, job_id döner |
| GET  | `/api/job/{job_id}` | İşin durumunu sorgular |
| GET  | `/api/download/{filename}` | Hazır Excel dosyasını indirir |

---

## ⚙️ Notlar
- Şifre ve kullanıcı adını `main.py` içindeki `USERNAME` / `PASSWORD` değişkenlerinden değiştir.
- Üretilen Excel dosyaları `outputs/` klasörüne kaydedilir.
- Birden fazla kullanıcı aynı anda farklı raporlar çalıştırabilir (her iş ayrı job_id alır).