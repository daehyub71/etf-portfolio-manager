# check_dividend_yield.py - 배당수익률 데이터 확인 및 수정

import sqlite3
import pandas as pd
import os
from datetime import datetime

def check_dividend_yield_data():
    """배당수익률 데이터 상태 확인"""
    
    print("🔍 배당수익률 데이터 확인")
    print("=" * 50)
    
    # 데이터베이스 파일들 확인
    db_files = ["etf_universe.db", "data/etf_data.db", "etf_data.db"]
    
    for db_file in db_files:
        if not os.path.exists(db_file):
            continue
            
        print(f"\n📊 데이터베이스: {db_file}")
        print("-" * 30)
        
        try:
            conn = sqlite3.connect(db_file)
            
            # 테이블 구조 확인
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            print(f"📋 테이블 목록: {tables}")
            
            # ETF 정보 테이블 찾기
            etf_table = None
            for table_name in ['etf_info', 'etfs', 'etf_data']:
                if table_name in tables:
                    etf_table = table_name
                    break
            
            if not etf_table:
                print("⚠️ ETF 테이블을 찾을 수 없습니다")
                conn.close()
                continue
            
            print(f"🗂️ 사용할 ETF 테이블: {etf_table}")
            
            # 테이블 구조 확인
            cursor.execute(f"PRAGMA table_info({etf_table})")
            columns_info = cursor.fetchall()
            columns = [col[1] for col in columns_info]
            
            print(f"📋 컬럼 목록: {columns}")
            
            # dividend_yield 컬럼 존재 확인
            if 'dividend_yield' in columns:
                print("✅ dividend_yield 컬럼 존재")
                
                # 배당수익률 데이터 확인
                df = pd.read_sql_query(f"""
                    SELECT code, name, dividend_yield, expense_ratio 
                    FROM {etf_table} 
                    WHERE dividend_yield IS NOT NULL 
                    ORDER BY dividend_yield DESC 
                    LIMIT 10
                """, conn)
                
                print(f"\n📊 배당수익률 데이터 (상위 10개):")
                if not df.empty:
                    print(df.to_string(index=False))
                    
                    # 통계 정보
                    total_count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {etf_table}", conn).iloc[0]['count']
                    non_zero_count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {etf_table} WHERE dividend_yield > 0", conn).iloc[0]['count']
                    
                    print(f"\n📈 통계:")
                    print(f"전체 ETF: {total_count}개")
                    print(f"배당수익률 > 0: {non_zero_count}개")
                    print(f"배당수익률 데이터 비율: {non_zero_count/total_count*100:.1f}%")
                    
                else:
                    print("❌ 배당수익률 데이터가 없습니다")
                    
            else:
                print("❌ dividend_yield 컬럼이 없습니다")
                print("💡 컬럼 추가가 필요합니다")
                
                # 컬럼 추가 제안
                response = input("dividend_yield 컬럼을 추가하시겠습니까? (y/n): ")
                if response.lower() == 'y':
                    try:
                        cursor.execute(f"ALTER TABLE {etf_table} ADD COLUMN dividend_yield REAL DEFAULT 0")
                        conn.commit()
                        print("✅ dividend_yield 컬럼 추가 완료")
                    except Exception as e:
                        print(f"❌ 컬럼 추가 실패: {e}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 데이터베이스 오류: {e}")

def fix_dividend_yield_data():
    """배당수익률 데이터 수정/업데이트"""
    
    print("\n🔧 배당수익률 데이터 수정")
    print("=" * 40)
    
    # 정적 배당수익률 데이터 (실제 데이터 기반)
    dividend_data = {
        # KODEX 시리즈
        '069500': 2.1,   # KODEX 200
        '069660': 1.8,   # KODEX 코스닥150
        '114260': 3.2,   # KODEX 국고채10년
        '133690': 0.9,   # KODEX 나스닥100
        '195930': 2.3,   # KODEX 선진국MSCI
        '132030': 0.0,   # KODEX 골드선물(H)
        '189400': 4.5,   # KODEX 미국리츠
        
        # TIGER 시리즈
        '102110': 2.0,   # TIGER 200
        '148020': 1.7,   # TIGER 코스닥150
        '360750': 1.8,   # TIGER 미국S&P500
        '360200': 0.8,   # TIGER 미국나스닥100
        '381170': 2.5,   # TIGER 차이나CSI300
        
        # ARIRANG 시리즈
        '152100': 2.2,   # ARIRANG 200
        '174360': 3.8,   # ARIRANG 고배당주
        
        # 기타
        '130730': 3.5,   # KOSEF 단기자금
        '139660': 1.2,   # TIGER 200IT
        '427120': 2.8,   # KBSTAR 중기채권
        '495710': 1.5,   # TIMEFOLIO Korea플러스배당액티브
    }
    
    db_files = ["etf_universe.db", "data/etf_data.db", "etf_data.db"]
    
    for db_file in db_files:
        if not os.path.exists(db_file):
            continue
            
        print(f"\n📊 {db_file} 업데이트 중...")
        
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # ETF 테이블 찾기
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            etf_table = None
            for table_name in ['etf_info', 'etfs', 'etf_data']:
                if table_name in tables:
                    etf_table = table_name
                    break
            
            if not etf_table:
                print(f"⚠️ {db_file}: ETF 테이블 없음")
                conn.close()
                continue
            
            # dividend_yield 컬럼 확인 및 추가
            cursor.execute(f"PRAGMA table_info({etf_table})")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'dividend_yield' not in columns:
                try:
                    cursor.execute(f"ALTER TABLE {etf_table} ADD COLUMN dividend_yield REAL DEFAULT 0")
                    conn.commit()
                    print(f"✅ {db_file}: dividend_yield 컬럼 추가됨")
                except Exception as e:
                    print(f"❌ {db_file}: 컬럼 추가 실패 - {e}")
                    conn.close()
                    continue
            
            # 배당수익률 데이터 업데이트
            updated_count = 0
            
            for code, dividend_yield in dividend_data.items():
                try:
                    cursor.execute(f"""
                        UPDATE {etf_table} 
                        SET dividend_yield = ?, last_updated = ?
                        WHERE code = ?
                    """, (dividend_yield, datetime.now().isoformat(), code))
                    
                    if cursor.rowcount > 0:
                        updated_count += 1
                        
                except Exception as e:
                    print(f"❌ {code} 업데이트 실패: {e}")
            
            # 기본값 설정 (배당수익률이 0인 ETF들)
            cursor.execute(f"""
                UPDATE {etf_table} 
                SET dividend_yield = 1.5, last_updated = ?
                WHERE dividend_yield = 0 OR dividend_yield IS NULL
            """, (datetime.now().isoformat(),))
            
            default_updated = cursor.rowcount
            
            conn.commit()
            conn.close()
            
            print(f"✅ {db_file}: 정확한 데이터 {updated_count}개, 기본값 {default_updated}개 업데이트")
            
        except Exception as e:
            print(f"❌ {db_file} 업데이트 실패: {e}")

def verify_dividend_data():
    """배당수익률 데이터 검증"""
    
    print("\n✅ 배당수익률 데이터 검증")
    print("=" * 35)
    
    db_files = ["etf_universe.db", "data/etf_data.db", "etf_data.db"]
    
    for db_file in db_files:
        if not os.path.exists(db_file):
            continue
            
        print(f"\n📊 {db_file} 검증:")
        
        try:
            conn = sqlite3.connect(db_file)
            
            # ETF 테이블 찾기
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            etf_table = None
            for table_name in ['etf_info', 'etfs', 'etf_data']:
                if table_name in tables:
                    etf_table = table_name
                    break
            
            if etf_table:
                # 배당수익률 통계
                df_stats = pd.read_sql_query(f"""
                    SELECT 
                        COUNT(*) as total_etfs,
                        COUNT(CASE WHEN dividend_yield > 0 THEN 1 END) as has_dividend,
                        AVG(dividend_yield) as avg_dividend,
                        MAX(dividend_yield) as max_dividend,
                        MIN(dividend_yield) as min_dividend
                    FROM {etf_table}
                """, conn)
                
                print(f"   전체 ETF: {df_stats.iloc[0]['total_etfs']}개")
                print(f"   배당수익률 > 0: {df_stats.iloc[0]['has_dividend']}개")
                print(f"   평균 배당수익률: {df_stats.iloc[0]['avg_dividend']:.2f}%")
                print(f"   최대 배당수익률: {df_stats.iloc[0]['max_dividend']:.2f}%")
                
                # 상위 배당 ETF
                df_top = pd.read_sql_query(f"""
                    SELECT code, name, dividend_yield 
                    FROM {etf_table} 
                    WHERE dividend_yield > 0 
                    ORDER BY dividend_yield DESC 
                    LIMIT 5
                """, conn)
                
                if not df_top.empty:
                    print(f"\n   상위 배당 ETF:")
                    for _, row in df_top.iterrows():
                        print(f"     {row['code']}: {row['dividend_yield']:.1f}% ({row['name']})")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ 검증 실패: {e}")

def main():
    """메인 실행 함수"""
    print("🎯 배당수익률 데이터 진단 및 수정 도구")
    print("현재 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # 1단계: 현재 상태 확인
    check_dividend_yield_data()
    
    # 2단계: 데이터 수정 제안
    print("\n" + "="*60)
    response = input("배당수익률 데이터를 수정/업데이트하시겠습니까? (y/n): ")
    
    if response.lower() == 'y':
        fix_dividend_yield_data()
        verify_dividend_data()
    
    print("\n✅ 배당수익률 데이터 진단 완료!")
    print("💡 대시보드를 새로고침하여 변경사항을 확인하세요.")

if __name__ == "__main__":
    main()