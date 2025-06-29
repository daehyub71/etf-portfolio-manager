# quick_db_check.py - 빠른 데이터베이스 확인 스크립트 (수정된 버전)

import sqlite3
import pandas as pd
from datetime import datetime
import os

def quick_check(db_path="etf_universe.db"):
    """데이터베이스 빠른 확인"""
    
    print("🔍 ETF 데이터베이스 빠른 확인")
    print("=" * 50)
    
    # 데이터베이스 파일들 확인
    db_files = ["etf_universe.db", "data/portfolio_data.db", "data/etf_data.db", "portfolio_data.db", "etf_data.db"]
    existing_files = []
    
    for db_file in db_files:
        if os.path.exists(db_file):
            existing_files.append(db_file)
    
    if not existing_files:
        print(f"❌ 데이터베이스 파일을 찾을 수 없습니다")
        print("확인한 위치:", db_files)
        return
    
    print(f"📁 발견된 데이터베이스 파일: {len(existing_files)}개")
    
    for db_file in existing_files:
        print(f"\n{'='*60}")
        print(f"🗄️ 데이터베이스: {db_file}")
        print(f"{'='*60}")
        
        # 파일 크기
        file_size = os.path.getsize(db_file)
        print(f"📁 파일 크기: {file_size:,} bytes ({file_size/1024:.1f} KB)")
        
        try:
            conn = sqlite3.connect(db_file)
            
            # 테이블 목록과 구조 확인
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            print(f"🗂️ 테이블 수: {len(tables)}개")
            
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                
                # 테이블 구조도 간단히 확인
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                col_names = [col[1] for col in columns[:5]]  # 처음 5개 컬럼만
                col_summary = ", ".join(col_names)
                if len(columns) > 5:
                    col_summary += f"... ({len(columns)}개 컬럼)"
                
                print(f"   - {table_name}: {count:,}개 레코드 ({col_summary})")
            
            # 샘플 데이터 확인
            print(f"\n📊 샘플 데이터:")
            for table in tables:
                table_name = table[0]
                try:
                    # 각 테이블의 샘플 데이터 확인
                    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                    rows = cursor.fetchall()
                    if rows:
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        print(f"   📋 {table_name}:")
                        for i, row in enumerate(rows):
                            row_data = []
                            for j, value in enumerate(row[:3]):  # 처음 3개 컬럼만
                                if j < len(columns) and columns[j] in ['name', 'code', 'symbol', 'etf_code']:
                                    row_data.append(f"{columns[j]}={value}")
                                elif j < len(columns) and 'price' in columns[j].lower() and value:
                                    row_data.append(f"{columns[j]}=₩{value:,.0f}")
                                elif value:
                                    if j < len(columns):
                                        row_data.append(f"{columns[j]}={value}")
                            print(f"      {i+1}: {', '.join(row_data)}")
                except:
                    continue
            
            conn.close()
            
        except Exception as e:
            print(f"❌ 데이터베이스 오류: {e}")
    
    print(f"\n✅ 데이터베이스 확인 완료")

def show_all_etfs(db_path="etf_universe.db"):
    """모든 ETF 목록 표시"""
    
    try:
        # 여러 데이터베이스 파일 시도
        db_files = [db_path, "etf_universe.db", "data/etf_data.db", "etf_data.db"]
        
        df = None
        used_db_file = None
        
        for db_file in db_files:
            if os.path.exists(db_file):
                try:
                    conn = sqlite3.connect(db_file)
                    
                    # 테이블 이름 확인
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    all_tables = [row[0] for row in cursor.fetchall()]
                    
                    print(f"📊 데이터베이스: {db_file}")
                    print(f"🗂️ 발견된 테이블: {all_tables}")
                    
                    # ETF 정보 테이블 찾기 (우선순위)
                    etf_table = None
                    for table_name in ['etfs', 'etf_info', 'etf_data']:
                        if table_name in all_tables:
                            etf_table = table_name
                            break
                    
                    if not etf_table:
                        print("⚠️ ETF 관련 테이블을 찾을 수 없습니다.")
                        conn.close()
                        continue
                    
                    print(f"📋 사용할 테이블: {etf_table}")
                    
                    # 테이블 구조 확인
                    cursor.execute(f"PRAGMA table_info({etf_table});")
                    columns = cursor.fetchall()
                    available_columns = [col[1] for col in columns]
                    
                    print(f"📋 사용 가능한 컬럼: {available_columns[:10]}...")
                    
                    # 실제 데이터베이스 구조에 맞는 컬럼 매핑
                    column_mappings = {
                        'symbol': ['symbol', 'code', 'etf_code'],
                        'name': ['name', 'etf_name'],
                        'category': ['category', 'asset_class'],
                        'current_price': ['current_price', 'close_price', 'price', 'market_price'],
                        'aum': ['aum', 'total_assets'],
                        'expense_ratio': ['expense_ratio'],
                        'last_updated': ['last_updated', 'update_date']
                    }
                    
                    # 실제 사용할 컬럼 결정
                    select_parts = []
                    
                    for display_col, possible_cols in column_mappings.items():
                        found_col = None
                        for possible_col in possible_cols:
                            if possible_col in available_columns:
                                found_col = possible_col
                                break
                        
                        if found_col:
                            select_parts.append(f"{found_col} as {display_col}")
                        else:
                            select_parts.append(f"NULL as {display_col}")
                    
                    # 쿼리 실행
                    query = f"SELECT {', '.join(select_parts)} FROM {etf_table} ORDER BY name"
                    
                    print(f"🔍 실행할 쿼리: {query[:100]}...")
                    
                    try:
                        df = pd.read_sql_query(query, conn)
                        used_db_file = db_file
                        conn.close()
                        
                        # DataFrame 유효성 검사 (수정된 부분)
                        if df is not None and not df.empty:
                            print(f"✅ 데이터 로드 성공: {len(df)}개 레코드")
                            break
                        else:
                            print("⚠️ 테이블이 비어있습니다.")
                            df = None
                    except Exception as query_error:
                        print(f"⚠️ 쿼리 실행 실패: {query_error}")
                        conn.close()
                        df = None
                        continue
                        
                except Exception as e:
                    print(f"⚠️ {db_file} 처리 중 오류: {e}")
                    if 'conn' in locals():
                        conn.close()
                    continue
        
        # 결과 확인 및 출력 (수정된 부분)
        if df is None or df.empty:
            print("📭 사용 가능한 ETF 데이터를 찾을 수 없습니다.")
            return
        
        print(f"\n📋 전체 ETF 목록 ({len(df)}개) - 출처: {used_db_file}")
        print("=" * 100)
        print(f"{'심볼':<10} {'ETF명':<35} {'카테고리':<20} {'현재가':<15} {'AUM':<15}")
        print("-" * 100)
        
        for idx, row in df.iterrows():
            symbol = str(row['symbol']) if pd.notna(row['symbol']) else "N/A"
            name = str(row['name'])[:33] if pd.notna(row['name']) else "N/A"
            category = str(row['category'])[:18] if pd.notna(row['category']) else "N/A"
            
            # 현재가 처리
            if pd.notna(row['current_price']) and row['current_price'] != 0:
                try:
                    price = f"₩{float(row['current_price']):,.0f}"
                except:
                    price = str(row['current_price'])
            else:
                price = "N/A"
            
            # AUM 처리    
            if pd.notna(row['aum']) and row['aum'] != 0:
                try:
                    aum_val = float(row['aum'])
                    if aum_val > 1e12:
                        aum = f"{aum_val/1e12:.1f}조"
                    elif aum_val > 1e8:
                        aum = f"{aum_val/1e8:.0f}억"
                    else:
                        aum = f"₩{aum_val:,.0f}"
                except:
                    aum = str(row['aum'])
            else:
                aum = "N/A"
            
            print(f"{symbol:<10} {name:<35} {category:<20} {price:<15} {aum:<15}")
        
        print(f"\n📊 총 {len(df)}개 ETF 정보 표시 완료")
        
    except Exception as e:
        print(f"❌ ETF 목록 조회 실패: {e}")
        print("📋 디버그 정보:")
        
        # 디버그: 모든 가능한 DB 파일의 테이블 구조 출력
        db_files = [db_path, "etf_universe.db", "data/etf_data.db", "etf_data.db"]
        for db_file in db_files:
            if os.path.exists(db_file):
                try:
                    conn = sqlite3.connect(db_file)
                    cursor = conn.cursor()
                    
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = cursor.fetchall()
                    
                    print(f"   📂 {db_file}:")
                    for table_name in [t[0] for t in tables]:
                        if 'etf' in table_name.lower():  # ETF 관련 테이블만
                            cursor.execute(f"PRAGMA table_info({table_name});")
                            columns = cursor.fetchall()
                            col_names = [col[1] for col in columns[:8]]  # 처음 8개만
                            print(f"      🗂️ {table_name}: {', '.join(col_names)}{'...' if len(columns) > 8 else ''}")
                    
                    conn.close()
                except Exception as debug_e:
                    print(f"      ❌ {db_file} 분석 실패: {debug_e}")

def show_latest_prices(db_path="etf_universe.db", symbol=None):
    """최신 가격 데이터 표시 (수정된 버전)"""
    try:
        # 여러 데이터베이스 파일 시도
        db_files = [db_path, "etf_universe.db", "data/etf_data.db", "etf_data.db"]
        conn = None
        df = None
        
        for db_file in db_files:
            if os.path.exists(db_file):
                try:
                    conn = sqlite3.connect(db_file)
                    
                    # 가격 테이블 확인
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    all_tables = [row[0] for row in cursor.fetchall()]
                    
                    # 가격 관련 테이블 찾기
                    price_table = None
                    for table_name in ['etf_price_history', 'etf_prices', 'price_history', 'prices']:
                        if table_name in all_tables:
                            price_table = table_name
                            break
                    
                    if not price_table:
                        conn.close()
                        continue
                    
                    # 테이블 구조 확인
                    cursor.execute(f"PRAGMA table_info({price_table});")
                    columns = cursor.fetchall()
                    available_columns = [col[1] for col in columns]
                    
                    print(f"📊 데이터베이스: {db_file}, 테이블: {price_table}")
                    print(f"📋 사용 가능한 컬럼: {', '.join(available_columns[:10])}")
                    
                    # 컬럼 매핑 개선
                    symbol_col = None
                    for col_name in ['code', 'symbol', 'etf_code']:
                        if col_name in available_columns:
                            symbol_col = col_name
                            break
                    
                    if not symbol_col:
                        print(f"⚠️ {db_file}에서 심볼 컬럼을 찾을 수 없습니다.")
                        conn.close()
                        continue
                    
                    date_col = 'date'
                    close_col = 'close_price' if 'close_price' in available_columns else 'close'
                    
                    if symbol:
                        # 특정 ETF의 최근 데이터
                        query = f"""
                        SELECT {date_col} as date, 
                               {close_col} as close_price
                        FROM {price_table} 
                        WHERE {symbol_col} = ?
                        ORDER BY {date_col} DESC 
                        LIMIT 10
                        """
                        
                        df = pd.read_sql_query(query, conn, params=(symbol,))
                        
                        if not df.empty:
                            print(f"\n📈 {symbol} 최근 10일 가격 데이터")
                            print("=" * 50)
                            print(f"{'날짜':<12} {'종가':<15}")
                            print("-" * 30)
                            
                            for _, row in df.iterrows():
                                date_str = str(row['date'])
                                close_price = f"₩{row['close_price']:>12,.0f}" if pd.notna(row['close_price']) else "N/A"
                                print(f"{date_str:<12} {close_price:<15}")
                            
                            print(f"\n📊 총 {len(df)}개 레코드 표시")
                            conn.close()
                            return
                        else:
                            print(f"⚠️ {symbol}에 대한 가격 데이터를 찾을 수 없습니다.")
                    else:
                        # 모든 ETF의 최신 가격 - 간단한 쿼리 사용
                        query = f"""
                        SELECT DISTINCT {symbol_col} as symbol
                        FROM {price_table} 
                        ORDER BY {symbol_col}
                        LIMIT 20
                        """
                        
                        symbols_df = pd.read_sql_query(query, conn)
                        
                        if not symbols_df.empty:
                            print(f"\n📈 최신 가격 데이터 (상위 20개 ETF)")
                            print("=" * 60)
                            print(f"{'심볼':<10} {'최신 날짜':<12} {'종가':<15}")
                            print("-" * 40)
                            
                            for _, row in symbols_df.iterrows():
                                etf_symbol = row['symbol']
                                
                                # 각 ETF의 최신 데이터 조회
                                latest_query = f"""
                                SELECT {date_col} as date, {close_col} as close_price
                                FROM {price_table} 
                                WHERE {symbol_col} = ?
                                ORDER BY {date_col} DESC 
                                LIMIT 1
                                """
                                
                                latest_df = pd.read_sql_query(latest_query, conn, params=(etf_symbol,))
                                
                                if not latest_df.empty:
                                    latest_row = latest_df.iloc[0]
                                    date_str = str(latest_row['date'])
                                    close_price = f"₩{latest_row['close_price']:>12,.0f}" if pd.notna(latest_row['close_price']) else "N/A"
                                    print(f"{etf_symbol:<10} {date_str:<12} {close_price:<15}")
                            
                            conn.close()
                            return
                    
                    conn.close()
                    
                except Exception as e:
                    print(f"⚠️ {db_file} 처리 중 오류: {e}")
                    if conn:
                        conn.close()
                    continue
        
        print("📭 가격 데이터를 찾을 수 없습니다.")
        
    except Exception as e:
        print(f"❌ 가격 데이터 조회 실패: {e}")
        
        # 디버그 정보
        print("📋 디버그 정보:")
        db_files = ["etf_universe.db", "data/etf_data.db", "etf_data.db"]
        for db_file in db_files:
            if os.path.exists(db_file):
                try:
                    conn = sqlite3.connect(db_file)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    tables = cursor.fetchall()
                    
                    print(f"   📂 {db_file}:")
                    for table_name in [t[0] for t in tables]:
                        if 'price' in table_name.lower():
                            cursor.execute(f"PRAGMA table_info({table_name});")
                            columns = cursor.fetchall()
                            col_names = [col[1] for col in columns]
                            print(f"      🗂️ {table_name}: {col_names}")
                            
                            # 샘플 데이터도 확인
                            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                            sample = cursor.fetchone()
                            if sample:
                                print(f"         샘플: {sample[:3]}...")
                    
                    conn.close()
                except Exception as debug_e:
                    print(f"      ❌ {db_file} 분석 실패: {debug_e}")

def main():
    """메인 함수"""
    import sys
    
    print("🚀 ETF 데이터베이스 빠른 확인 도구 (수정 버전)")
    print("현재 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # 명령줄 인수 처리
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "check":
            quick_check()
        elif command == "list":
            show_all_etfs()
        elif command == "prices":
            symbol = sys.argv[2].upper() if len(sys.argv) > 2 else None
            show_latest_prices(symbol=symbol)
        elif command in ["help", "-h", "--help"]:
            print("\n사용법:")
            print("  python quick_db_check.py [command] [options]")
            print("\n명령어:")
            print("  check   - 데이터베이스 전체 상태 확인")
            print("  list    - 전체 ETF 목록 표시")
            print("  prices  - 최신 가격 데이터 (prices [ETF심볼])")
            print("  help    - 도움말")
            print("\n예시:")
            print("  python quick_db_check.py check")
            print("  python quick_db_check.py list") 
            print("  python quick_db_check.py prices 069500")
        else:
            print(f"❌ 알 수 없는 명령어: {command}")
            print("사용법: python quick_db_check.py [check|list|prices|help] [ETF심볼]")
            print("도움말: python quick_db_check.py help")
    else:
        # 기본 실행: 전체 체크
        quick_check()
        print("\n" + "="*50)
        show_all_etfs()

if __name__ == "__main__":
    main()