import os
import json
from elasticsearch import Elasticsearch
from PyPDF2 import PdfReader
import re
from datetime import datetime

# Data & Cloud Technologies ile ilgili 10 kelime
SEARCH_KEYWORDS = [
    "microservices",
    "containerization", 
    "data pipeline",
    "machine learning",
    "distributed computing",
    "cloud storage",
    "api gateway",
    "data warehouse",
    "kubernetes",
    "nosql database"
]

# Elasticsearch bağlantısı
ES_HOST = "localhost:9200"
INDEX_NAME = "wikipedia_pdfs"

class WikipediaPDFSearcher:
    def __init__(self, pdf_directory="wikipedia_pdfs", es_host=ES_HOST):
        self.pdf_directory = pdf_directory
        self.es = Elasticsearch([es_host])
        self.index_name = INDEX_NAME
        
    def setup_elasticsearch_index(self):
        """Elasticsearch index'ini oluştur"""
        try:
            # Index varsa sil
            if self.es.indices.exists(index=self.index_name):
                self.es.indices.delete(index=self.index_name)
                print(f"✓ Eski index silindi: {self.index_name}")
            
            # Yeni index oluştur
            index_mapping = {
                "mappings": {
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "content": {
                            "type": "text", 
                            "analyzer": "standard"
                        },
                        "filename": {
                            "type": "keyword"
                        },
                        "created_at": {
                            "type": "date"
                        },
                        "page_count": {
                            "type": "integer"
                        }
                    }
                }
            }
            
            self.es.indices.create(index=self.index_name, body=index_mapping)
            print(f"✓ Yeni index oluşturuldu: {self.index_name}")
            return True
            
        except Exception as e:
            print(f"✗ Index oluşturma hatası: {e}")
            return False
    
    def extract_text_from_pdf(self, pdf_path):
        """PDF'den metin çıkar"""
        try:
            reader = PdfReader(pdf_path)
            text = ""
            
            for page in reader.pages:
                text += page.extract_text() + "\n"
            
            # Temizleme
            text = re.sub(r'\s+', ' ', text)  # Çoklu boşlukları tek boşluğa çevir
            text = text.strip()
            
            return text, len(reader.pages)
            
        except Exception as e:
            print(f"✗ PDF okuma hatası ({pdf_path}): {e}")
            return None, 0
    
    def index_pdfs(self):
        """PDF'leri Elasticsearch'e indexle"""
        if not os.path.exists(self.pdf_directory):
            print(f"✗ PDF klasörü bulunamadı: {self.pdf_directory}")
            return False
        
        pdf_files = [f for f in os.listdir(self.pdf_directory) if f.endswith('.pdf')]
        
        if not pdf_files:
            print(f"✗ PDF dosyası bulunamadı: {self.pdf_directory}")
            return False
        
        print(f"📁 {len(pdf_files)} PDF dosyası bulundu")
        
        success_count = 0
        
        for pdf_file in pdf_files:
            pdf_path = os.path.join(self.pdf_directory, pdf_file)
            print(f"📄 İşleniyor: {pdf_file}")
            
            # PDF'den metin çıkar
            content, page_count = self.extract_text_from_pdf(pdf_path)
            
            if content:
                # Başlığı dosya adından çıkar
                title = pdf_file.replace('.pdf', '').replace('_', ' ')
                title = re.sub(r'^\d+\s*', '', title)  # Başındaki sayıları kaldır
                
                # Elasticsearch'e kaydet
                doc = {
                    "title": title,
                    "content": content,
                    "filename": pdf_file,
                    "created_at": datetime.now(),
                    "page_count": page_count
                }
                
                try:
                    response = self.es.index(
                        index=self.index_name,
                        body=doc
                    )
                    print(f"  ✓ İndexlendi: {response['_id']}")
                    success_count += 1
                    
                except Exception as e:
                    print(f"  ✗ İndexleme hatası: {e}")
            
        print(f"\n📊 İndexleme tamamlandı: {success_count}/{len(pdf_files)} başarılı")
        return success_count > 0
    
    def search_keyword(self, keyword, size=10):
        """Belirli bir kelimeyi ara"""
        try:
            query = {
                "query": {
                    "multi_match": {
                        "query": keyword,
                        "fields": ["title^2", "content"],  # title'a 2x ağırlık
                        "type": "best_fields",
                        "fuzziness": "AUTO"  # Yazım hatalarına tolerans
                    }
                },
                "highlight": {
                    "fields": {
                        "content": {
                            "fragment_size": 150,
                            "number_of_fragments": 3
                        }
                    }
                },
                "_source": ["title", "filename", "page_count"]
            }
            
            response = self.es.search(
                index=self.index_name,
                body=query,
                size=size
            )
            
            return response
            
        except Exception as e:
            print(f"✗ Arama hatası ({keyword}): {e}")
            return None
    
    def print_search_results(self, keyword, results):
        """Arama sonuçlarını yazdır"""
        if not results or results['hits']['total']['value'] == 0:
            print(f"❌ '{keyword}' için sonuç bulunamadı\n")
            return
        
        total = results['hits']['total']['value']
        print(f"🔍 '{keyword}' için {total} sonuç bulundu:")
        print("-" * 50)
        
        for i, hit in enumerate(results['hits']['hits'], 1):
            source = hit['_source']
            score = hit['_score']
            
            print(f"{i}. {source['title']}")
            print(f"   📁 Dosya: {source['filename']}")
            print(f"   📄 Sayfa: {source['page_count']} | Skor: {score:.2f}")
            
            # Highlight'ları göster
            if 'highlight' in hit:
                print("   💡 İlgili bölümler:")
                for fragment in hit['highlight']['content']:
                    clean_fragment = re.sub(r'<[^>]+>', '', fragment)  # HTML etiketleri kaldır
                    print(f"      • ...{clean_fragment}...")
            
            print()
    
    def search_all_keywords(self):
        """Tüm kelimeler için arama yap"""
        print("🚀 Wikipedia PDF'lerinde kelime arama başlıyor...")
        print("=" * 60)
        
        results_summary = {}
        
        for keyword in SEARCH_KEYWORDS:
            print(f"\n{'='*20} ARAMA: {keyword.upper()} {'='*20}")
            
            results = self.search_keyword(keyword, size=5)  # Her kelime için top 5
            
            if results:
                total_hits = results['hits']['total']['value']
                results_summary[keyword] = total_hits
                self.print_search_results(keyword, results)
            else:
                results_summary[keyword] = 0
                print(f"❌ '{keyword}' için arama yapılamadı\n")
        
        # Özet rapor
        print("\n" + "="*60)
        print("📊 ARAMA ÖZETİ")
        print("="*60)
        
        for keyword, count in sorted(results_summary.items(), key=lambda x: x[1], reverse=True):
            print(f"{keyword:20} : {count:3} sonuç")
        
        total_found = sum(results_summary.values())
        print(f"\n📈 Toplam sonuç: {total_found}")
    
    def run(self):
        """Ana çalıştırma fonksiyonu"""
        print("🔧 Elasticsearch Wikipedia PDF Arama Sistemi")
        print("=" * 50)
        
        # 1. Index oluştur
        if not self.setup_elasticsearch_index():
            return False
        
        # 2. PDF'leri indexle
        if not self.index_pdfs():
            return False
        
        # Biraz bekle (indexleme tamamlansın)
        import time
        print("⏳ İndexleme tamamlanması bekleniyor...")
        time.sleep(2)
        
        # 3. Arama yap
        self.search_all_keywords()
        
        return True

def main():
    """Ana fonksiyon"""
    # Elasticsearch bağlantısını kontrol et
    try:
        es_test = Elasticsearch([ES_HOST])
        if not es_test.ping():
            print("❌ Elasticsearch bağlantısı başarısız!")
            print(f"Elasticsearch {ES_HOST} adresinde çalışıyor mu?")
            print("\nElasticsearch kurulum:")
            print("1. Docker: docker run -p 9200:9200 -e discovery.type=single-node elasticsearch:8.11.0")
            print("2. Brew: brew install elasticsearch && brew services start elasticsearch")
            return
    except Exception as e:
        print(f"❌ Elasticsearch bağlantı hatası: {e}")
        return
    
    # Arama sistemini başlat
    searcher = WikipediaPDFSearcher()
    searcher.run()

if __name__ == "__main__":
    main()