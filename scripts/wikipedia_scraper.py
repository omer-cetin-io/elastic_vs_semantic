import requests
import os
from fpdf import FPDF
import re
import time

# Wikipedia API için headers
HEADERS = {
    'User-Agent': 'WikipediaPDFScraper/1.0 (https://example.com/contact) Python/requests'
}

# Gerekli klasörü oluştur
output_dir = "wikipedia_pdfs"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Data & Cloud Technologies ile ilgili arama terimleri - daha spesifik
search_terms = [
    "cloud computing", "big data", "data science", "machine learning",
    "artificial intelligence", "data analytics", "relational database", "data warehouse",
    "business intelligence", "apache spark", "hadoop ecosystem", "kubernetes",
    "docker container", "microservices architecture", "amazon web services", "microsoft azure", "google cloud platform",
    "data mining", "nosql database", "structured query language", "mongodb",
    "postgresql", "redis database", "elasticsearch", "apache kafka",
    "stream processing", "extract transform load", "data pipeline", "data lake",
    "data governance", "cloud security", "serverless computing", "containerization technology",
    "devops methodology", "continuous integration", "infrastructure as code", "terraform software",
    "ansible automation", "jenkins automation", "git version control", "version control system",
    "application programming interface", "representational state transfer", "graphql query language", "json format",
    "python programming language", "java programming language", "scala programming", "r statistical computing",
    "data visualization", "tableau software", "microsoft power bi", "apache airflow", "distributed computing", "computer cluster"
]

def clean_text(text):
    """Metni PDF için temizle"""
    # HTML etiketlerini kaldır
    text = re.sub(r'<.*?>', '', text)
    # Özel karakterleri temizle
    text = re.sub(r'[^\w\s.,;:!?()-]', '', text)
    # Çoklu boşlukları tek boşluğa çevir
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def test_wikipedia_api():
    """Wikipedia API'sinin çalışıp çalışmadığını test et"""
    try:
        # Basit bir test
        test_url = "https://en.wikipedia.org/api/rest_v1/page/summary/Python_(programming_language)"
        response = requests.get(test_url, headers=HEADERS, timeout=10)
        print(f"Test URL: {test_url}")
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Title: {data.get('title', 'N/A')}")
            print("✓ Wikipedia API çalışıyor")
            return True
        else:
            print(f"✗ Wikipedia API yanıt vermiyor: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ API test hatası: {e}")
        return False

def search_wikipedia_simple(term):
    """Basit Wikipedia arama"""
    try:
        print(f"  -> Arama yapılıyor: {term}")
        
        # Wikipedia search API'sini kullan
        search_url = "https://en.wikipedia.org/w/api.php"
        search_params = {
            'action': 'query',
            'format': 'json',
            'list': 'search',
            'srsearch': term,
            'srlimit': 1,
            'srprop': 'snippet'
        }
        
        search_response = requests.get(search_url, params=search_params, headers=HEADERS, timeout=15)
        print(f"  -> Search API Status: {search_response.status_code}")
        
        if search_response.status_code == 403:
            print(f"  -> 403 Hatası - alternatif yöntem deneniyor...")
            # Alternatif: Direkt REST API dene
            return try_direct_rest_api(term)
            
        if search_response.status_code != 200:
            print(f"  -> Search API hatası: {search_response.status_code}")
            return None
            
        search_data = search_response.json()
        
        if not search_data.get('query', {}).get('search'):
            print(f"  -> Arama sonucu bulunamadı")
            return None
            
        # İlk sonucu al
        first_result = search_data['query']['search'][0]
        page_title = first_result['title']
        print(f"  -> Bulunan sayfa: {page_title}")
        
        # Sayfa özet bilgisini al
        summary_url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{page_title.replace(' ', '_')}"
        summary_response = requests.get(summary_url, headers=HEADERS, timeout=15)
        
        if summary_response.status_code == 200:
            summary_data = summary_response.json()
            print(f"  -> ✓ Özet alındı")
            return summary_data
        else:
            print(f"  -> Özet alınamadı: {summary_response.status_code}")
            # Yine de temel bilgileri döndür
            return {
                'title': page_title,
                'extract': f"Wikipedia article about {page_title}"
            }
            
    except Exception as e:
        print(f"  -> Hata: {e}")
        return None

def try_direct_rest_api(term):
    """Direkt REST API ile deneme"""
    try:
        # Terimi URL-safe hale getir
        term_variants = [
            term.replace(" ", "_"),
            term.title().replace(" ", "_"),
            term.replace(" ", ""),
        ]
        
        for variant in term_variants:
            url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{variant}"
            response = requests.get(url, headers=HEADERS, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('type') != 'disambiguation':
                    print(f"  -> ✓ Direkt bulundu: {variant}")
                    return data
        
        print(f"  -> Direkt yöntem de başarısız")
        return None
        
    except Exception as e:
        print(f"  -> Direkt API hatası: {e}")
        return None

def get_full_article(title):
    """Tam makale içeriğini al"""
    try:
        url = f"https://en.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'format': 'json',
            'titles': title,
            'prop': 'extracts',
            'explaintext': True,
            'exsectionformat': 'plain'
        }
        response = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if response.status_code == 200:
            data = response.json()
            pages = data['query']['pages']
            for page_id in pages:
                if 'extract' in pages[page_id]:
                    return pages[page_id]['extract']
        return None
    except Exception as e:
        print(f"  -> Tam makale alınamadı: {e}")
        return None

def create_pdf(title, content, filename):
    """PDF oluştur"""
    try:
        pdf = FPDF()
        pdf.add_page()
        
        # Türkçe karakterler için font ayarla
        pdf.set_font('Arial', 'B', 16)
        
        # Başlık ekle
        pdf.cell(0, 10, title.encode('latin1', 'replace').decode('latin1'), 0, 1, 'C')
        pdf.ln(10)
        
        # İçerik ekle
        pdf.set_font('Arial', '', 12)
        
        if content:
            # İçeriği satırlara böl
            lines = content.split('\n')
            for line in lines:
                if line.strip():
                    # Uzun satırları böl
                    cleaned_line = clean_text(line)
                    # Latin1 karakterlerine dönüştür
                    safe_line = cleaned_line.encode('latin1', 'replace').decode('latin1')
                    
                    # 80 karakterden uzun satırları böl
                    while len(safe_line) > 80:
                        split_pos = safe_line.rfind(' ', 0, 80)
                        if split_pos == -1:
                            split_pos = 80
                        
                        pdf.cell(0, 6, safe_line[:split_pos], 0, 1)
                        safe_line = safe_line[split_pos:].strip()
                    
                    if safe_line:
                        pdf.cell(0, 6, safe_line, 0, 1)
        
        # PDF'i kaydet
        pdf.output(os.path.join(output_dir, filename))
        return True
    except Exception as e:
        print(f"PDF oluşturulamadı {filename}: {e}")
        return False

def main():
    """Ana fonksiyon"""
    print("Wikipedia Data & Cloud Technologies PDF Scraper başlatılıyor...")
    print(f"PDF'ler {output_dir} klasörüne kaydedilecek.")
    
    # İlk olarak API'yi test et
    print("\n" + "="*50)
    print("Wikipedia API testi yapılıyor...")
    if not test_wikipedia_api():
        print("UYARI: Wikipedia API'sinde sorun olabilir. Devam ediliyor...")
    
    print("\n" + "="*50)
    print("Ana işlem başlıyor...")
    
    successful_downloads = 0
    
    for i, term in enumerate(search_terms[:50]):  # İlk 50 terimi al
        print(f"\n[{i+1}/50] '{term}' aranıyor...")
        
        # Wikipedia'da ara
        summary = search_wikipedia_simple(term)
        
        if summary and 'title' in summary:
            title = summary['title']
            print(f"  -> ✓ Bulundu: {title}")
            
            # Tam makaleyi al
            full_content = get_full_article(title)
            
            if not full_content and 'extract' in summary:
                # Eğer tam makale alınamazsa özet kullan
                full_content = summary['extract']
                print(f"  -> Tam makale yerine özet kullanılıyor")
            
            if full_content:
                # Dosya adını oluştur
                safe_filename = re.sub(r'[^\w\s-]', '', title)
                safe_filename = re.sub(r'[-\s]+', '-', safe_filename)
                filename = f"{i+1:02d}_{safe_filename[:50]}.pdf"
                
                # PDF oluştur
                if create_pdf(title, full_content, filename):
                    print(f"  -> ✓ PDF oluşturuldu: {filename}")
                    successful_downloads += 1
                else:
                    print(f"  -> ✗ PDF oluşturulamadı: {filename}")
            else:
                print(f"  -> ✗ İçerik alınamadı: {title}")
        else:
            print(f"  -> ✗ Sonuç bulunamadı: {term}")
        
        # API'yi yormamak için kısa bekleme
        time.sleep(1)  # 403 hatalarını önlemek için biraz daha bekle
    
    print(f"\n{'='*50}")
    print(f"İşlem tamamlandı!")
    print(f"Başarılı indirme: {successful_downloads}/50")
    print(f"PDF'ler '{output_dir}' klasöründe")
    
    if successful_downloads == 0:
        print("\nHİÇBİR PDF İNDİRİLEMEDİ!")
        print("Olası nedenler:")
        print("- İnternet bağlantısı problemi")
        print("- Wikipedia API geçici olarak kapalı")
        print("- Güvenlik duvarı Wikipedia'yı engelliyor")

if __name__ == "__main__":
    main()