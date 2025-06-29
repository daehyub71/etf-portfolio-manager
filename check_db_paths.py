# ==========================================
# check_db_paths.py - 데이터베이스 파일 경로 확인
# ==========================================

import os
import sqlite3
import pandas as pd
from pathlib import Path
import glob

def find_all_db_files():
    """모든 .db 파일 찾기"""
    print("🔍 모든 .db 파일 검색")
    print("=" * 60)
    
    # 검색할 경로들
    search_paths = [
        ".",                    # 현재 디렉토리
        "..",                   # 상위 디렉토리
        "data/",               # data 디렉토리
        "web/",                # web 디렉토리
        "core/",               # core 디렉토리
    ]
    
    all_db_files = []
    
    for search_path in search_paths:
        if os.path.exists(search_path):
            pattern = os.path.join(search_path, "*.db")
            db_files = glob.glob(pattern)
            for db_file in db_files:
                abs_path = os.path.abspath(db_file)
                all_db_files.append(abs_path)
    
    # 중복 제거
    all_db_files = list(set(all_db_files))
    
    print(f"📁 발견된 .db 파일들 ({len(all_db_files)}개):")
    for i, db_file in enumerate(all_db_files, 1):
        size_mb = os.path.getsize(db_file) / 1024 / 1024
        print(f"{i}. {db_file}")
        print(f"   크기: {size_mb:.2f} MB")
        print(f"   수정시간: {pd.Timestamp.fromtimestamp(os.path.getmtime(db_file))}")
        print()
    
    return all_db_files

def check_db_content(db_path):
    """개별 DB 파일 내용 확인"""
    print(f"\n🔍 {db_path} 내용 확인:")
    print("-" * 50)
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 테이블 목록
        tables = pd.read_sql_query("""
            SELECT name FROM sqlite_master WHERE type='table'
        """, conn)['name'].tolist()
        
        print(f"📋 테이블: {', '.join(tables)}")
        
        # 각 테이블의 행 수 확인
        for table in tables:
            try:
                count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", conn).iloc[0]['count']
                print(f"  - {table}: {count:,}개")
                
                # etf_info나 etf_master인 경우 추가 정보
                if table in ['etf_info', 'etf_master']:
                    # 최근 업데이트 시간 확인
                    try:
                        if table == 'etf_info':
                            latest = pd.read_sql_query(f"SELECT MAX(last_updated) as latest FROM {table}", conn).iloc[0]['latest']
                        else:
                            latest = pd.read_sql_query(f"SELECT MAX(updated_at) as latest FROM {table}", conn).iloc[0]['latest']
                        print(f"    → 최근 업데이트: {latest}")
                    except:
                        pass
                        
                    # AUM 통계
                    try:
                        aum_stats = pd.read_sql_query(f"""
                            SELECT 
                                COUNT(CASE WHEN aum > 0 THEN 1 END) as aum_count,
                                SUM(COALESCE(aum, 0)) as total_aum
                            FROM {table}
                        """, conn)
                        aum_info = aum_stats.iloc[0]
                        print(f"    → AUM 보유: {aum_info['aum_count']:,}개, 총 AUM: {aum_info['total_aum']:,}억원")
                    except:
                        pass
                        
            except Exception as e:
                print(f"  - {table}: 조회 실패 ({e})")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ DB 접근 실패: {e}")

def check_module_paths():
    """모듈들이 사용하는 DB 경로 확인"""
    print("\n🔧 모듈별 DB 경로 확인")
    print("=" * 60)
    
    # 1. main.py에서 사용하는 경로
    print("1. main.py (SafeETFLauncher):")
    print(f"   기본 경로: 'etf_universe.db'")
    print(f"   절대 경로: {os.path.abspath('etf_universe.db')}")
    
    # 2. 대시보드에서 확인하는 경로들
    print("\n2. dashboard.py가 확인하는 경로들:")
    dashboard_paths = [
        "etf_universe.db",
        "../etf_universe.db", 
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "etf_universe.db")
    ]
    
    for i, path in enumerate(dashboard_paths, 1):
        abs_path = os.path.abspath(path)
        exists = os.path.exists(path)
        print(f"   {i}. {path}")
        print(f"      → {abs_path}")
        print(f"      → 존재: {'✅' if exists else '❌'}")
        if exists:
            size_mb = os.path.getsize(path) / 1024 / 1024
            print(f"      → 크기: {size_mb:.2f} MB")
    
    # 3. 현재 작업 디렉토리
    print(f"\n3. 현재 작업 디렉토리: {os.getcwd()}")
    print(f"4. 스크립트 위치: {os.path.dirname(os.path.abspath(__file__))}")

def suggest_fix():
    """해결 방법 제안"""
    print("\n🔧 해결 방법 제안")
    print("=" * 60)
    
    print("방법 1: 통합 업데이트 (권장)")
    print("  # 현재 디렉토리에서 새로 업데이트")
    print("  python main.py update --max-etfs 683 --force")
    print()
    
    print("방법 2: 대시보드 경로 수정")
    print("  # 가장 큰 DB 파일의 경로를 dashboard.py에 하드코딩")
    print()
    
    print("방법 3: 파일 이동/복사")
    print("  # 가장 큰 DB 파일을 현재 디렉토리로 복사")
    print("  # Windows: copy large_file.db etf_universe.db")
    print("  # Mac/Linux: cp large_file.db etf_universe.db")

def main():
    """메인 실행"""
    print("🔍 ETF 데이터베이스 경로 진단 도구")
    print("=" * 60)
    
    # 1. 모든 DB 파일 찾기
    db_files = find_all_db_files()
    
    if not db_files:
        print("❌ .db 파일을 찾을 수 없습니다!")
        return
    
    # 2. 각 DB 파일 내용 확인
    print("\n" + "=" * 60)
    print("📊 각 DB 파일 내용 분석")
    print("=" * 60)
    
    for db_file in db_files:
        check_db_content(db_file)
    
    # 3. 모듈 경로 확인
    check_module_paths()
    
    # 4. 해결 방법 제안
    suggest_fix()
    
    # 5. 결론
    print("\n" + "=" * 60)
    print("🎯 진단 결과 요약")
    print("=" * 60)
    
    # 가장 큰 파일과 가장 많은 데이터를 가진 파일 찾기
    largest_file = None
    largest_size = 0
    most_data_file = None
    most_data_count = 0
    
    for db_file in db_files:
        size = os.path.getsize(db_file)
        if size > largest_size:
            largest_size = size
            largest_file = db_file
        
        try:
            conn = sqlite3.connect(db_file)
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)['name'].tolist()
            
            total_count = 0
            for table in tables:
                if table in ['etf_info', 'etf_master']:
                    count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table}", conn).iloc[0]['count']
                    total_count += count
            
            if total_count > most_data_count:
                most_data_count = total_count
                most_data_file = db_file
                
            conn.close()
        except:
            pass
    
    print(f"📁 가장 큰 파일: {largest_file} ({largest_size/1024/1024:.2f} MB)")
    print(f"📊 가장 많은 데이터: {most_data_file} ({most_data_count:,}개 ETF)")
    
    if largest_file == most_data_file:
        print(f"✅ 권장: {largest_file} 사용")
    else:
        print(f"⚠️ 파일 불일치 - 확인 필요")

if __name__ == "__main__":
    main()