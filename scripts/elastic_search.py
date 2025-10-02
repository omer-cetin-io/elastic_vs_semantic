import os
import sqlite3
from PyPDF2 import PdfReader
import re

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

class WikipediaPDFSearcher:
    def __init__(self, pdf_directory="wikipedia_pdfs"):
        self.pdf_directory = pdf_directory
        self.db_path = "wikipedia_search.db"
        
    def setup_database(self):
        """SQLite veritabanını oluştur"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Eski tabloları sil
            cursor.execute('DROP TABLE IF EXISTS documents')
            cursor.execute('DROP TABLE IF EXISTS documents_fts')
            
            # Ana tablo
            cursor.execute('''
                CREATE TABLE documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    content TEXT,
                    filename TEXT,
                    page_count INTEGER
                )
            ''')
            
            # Full-text search tablosu
            cursor.execute('''
                CREATE VIRTUAL TABLE documents_fts USING fts5(
                    title, content, filename
                )
            ''')
            
            conn.commit()
            conn.close()
            
            print(f"✓ Veritabanı hazırlandı: {self.db_path}")
            return True
            
        except Exception as e:
            print(f"✗ Veritabanı hatası: {e}")
            return False
    
    def extract_text_from_pdf(self, pdf_path):
        """PDF'den metin çıkar"""
        try:
            reader = PdfReader(pdf_path)
            text = ""
            
            for page in reader.pages:
                text += page.extract_text() + "\n"
            
            # Temizleme
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
            
            return text, len(reader.pages)
            
        except Exception as e:
            print(f"✗ PDF okuma hatası ({pdf_path}): {e}")
            return None, 0
    
    def index_pdfs(self):
        """PDF'leri veritabanına kaydet"""
        if not os.path.exists(self.pdf_directory):
            print(f"✗ PDF klasörü bulunamadı: {self.pdf_directory}")
            return False
        
        pdf_files = [f for f in os.listdir(self.pdf_directory) if f.endswith('.pdf')]
        
        if not pdf_files:
            print(f"✗ PDF dosyası bulunamadı: {self.pdf_directory}")
            return False
        
        print(f"📁 {len(pdf_files)} PDF dosyası bulundu")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        success_count = 0
        
        for pdf_file in pdf_files:
            pdf_path = os.path.join(self.pdf_directory, pdf_file)
            print(f"📄 İşleniyor: {pdf_file}")
            
            # PDF'den metin çıkar
            content, page_count = self.extract_text_from_pdf(pdf_path)
            
            if content:
                # Başlığı dosya adından çıkar
                title = pdf_file.replace('.pdf', '').replace('_', ' ')
                title = re.sub(r'^\d+\s*', '', title)
                
                # Ana tabloya kaydet
                cursor.execute('''
                    INSERT INTO documents (title, content, filename, page_count)
                    VALUES (?, ?, ?, ?)
                ''', (title, content, pdf_file, page_count))
                
                # FTS tablosuna kaydet
                cursor.execute('''
                    INSERT INTO documents_fts (title, content, filename)
                    VALUES (?, ?, ?)
                ''', (title, content, pdf_file))
                
                print(f"  ✓ Kaydedildi")
                success_count += 1
                
        conn.commit()
        conn.close()
        
        print(f"\n📊 İndexleme tamamlandı: {success_count}/{len(pdf_files)} başarılı")
        return success_count > 0
    
    def search_keyword(self, keyword, limit=5):
        """Belirli bir kelimeyi ara"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # FTS5 ile arama
            cursor.execute('''
                SELECT d.title, d.filename, d.page_count, d.content
                FROM documents_fts fts
                JOIN documents d ON d.title = fts.title
                WHERE fts MATCH ?
                LIMIT ?
            ''', (keyword, limit))
            
            results = cursor.fetchall()
            conn.close()
            
            # Snippet oluştur
            processed_results = []
            for title, filename, page_count, content in results:
                snippet = self.create_snippet(content, keyword)
                processed_results.append((title, filename, page_count, snippet))
            
            return processed_results
            
        except Exception as e:
            print(f"✗ Arama hatası ({keyword}): {e}")
            return []
    
    def create_snippet(self, content, keyword, max_length=200):
        """İçerikten alakalı snippet oluştur"""
        if not content:
            return ""
        
        content_lower = content.lower()
        keyword_lower = keyword.lower()
        
        pos = content_lower.find(keyword_lower)
        if pos == -1:
            return content[:max_length] + "..." if len(content) > max_length else content
        
        start = max(0, pos - max_length//2)
        end = min(len(content), pos + max_length//2)
        
        snippet = content[start:end]
        if start > 0:
            snippet = "..." + snippet
        if end < len(content):
            snippet = snippet + "..."
            
        return snippet
    
    def print_search_results(self, keyword, results):
        """Arama sonuçlarını yazdır"""
        if not results:
            print(f"❌ '{keyword}' için sonuç bulunamadı\n")
            return
        
        print(f"🔍 '{keyword}' için {len(results)} sonuç bulundu:")
        print("-" * 50)
        
        for i, (title, filename, page_count, snippet) in enumerate(results, 1):
            print(f"{i}. {title}")
            print(f"   📁 Dosya: {filename}")
            print(f"   📄 Sayfa: {page_count}")
            
            if snippet and snippet.strip():
                print(f"   💡 İlgili bölüm:")
                print(f"      • {snippet}")
            
            print()
    
    def search_all_keywords(self):
        """Tüm kelimeler için arama yap"""
        print("🚀 Wikipedia PDF'lerinde kelime arama başlıyor...")
        print("=" * 60)
        
        results_summary = {}
        
        for keyword in SEARCH_KEYWORDS:
            print(f"\n{'='*20} ARAMA: {keyword.upper()} {'='*20}")
            
            results = self.search_keyword(keyword, limit=5)
            
            if results:
                results_summary[keyword] = len(results)
                self.print_search_results(keyword, results)
            else:
                results_summary[keyword] = 0
                print(f"❌ '{keyword}' için sonuç bulunamadı\n")
        
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
        print("🔧 SQLite Wikipedia PDF Arama Sistemi")
        print("=" * 50)
        
        if not self.setup_database():
            return False
        
        if not self.index_pdfs():
            return False
        
        print("⏳ İndexleme tamamlandı, arama başlıyor...")
        
        self.search_all_keywords()
        return True

def main():
    print("📚 Wikipedia PDF Arama Sistemi (SQLite FTS5)")
    print("🔧 Elasticsearch gerekmez - Yerel SQLite ile çalışır")
    print("=" * 50)
    
    searcher = WikipediaPDFSearcher()
    
    if searcher.run():
        print(f"\n✅ Arama tamamlandı! Veritabanı: wikipedia_search.db")
    else:
        print("\n❌ Arama sistemi çalıştırılamadı!")

if __name__ == "__main__":
    main()