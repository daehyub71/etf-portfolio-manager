# db_explorer.py - ETF 데이터베이스 탐색기

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import argparse
import os
import sys
from pathlib import Path

# 시각화 라이브러리 (선택사항)
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    plt.rcParams['font.family'] = ['Malgun Gothic', 'AppleGothic', 'sans-serif']
    plt.rcParams['axes.unicode_minus'] = False
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    print("⚠️ matplotlib/seaborn 없음 - 차트 기능 비활성화")

class ETFDatabaseExplorer:
    """ETF 데이터베이스 탐색 및 분석 도구"""
    
    def __init__(self, db_path="etf_universe.db"):
        self.db_path = db_path
        self.conn = None
        self.etf_table = None
        self.price_table = None
        self.available_databases = []
        
        # 여러 데이터베이스 파일 확인
        possible_dbs = [
            db_path,
            "etf_universe.db",
            "data/etf_data.db", 
            "etf_data.db",
            "data/portfolio_data.db",
            "portfolio_data.db"
        ]
        
        for db_file in possible_dbs:
            if os.path.exists(db_file):
                self.available_databases.append(db_file)
        
        if not self.available_databases:
            print(f"❌ 데이터베이스 파일을 찾을 수 없습니다")
            return
        
        self.connect()
        
    def connect(self):
        """데이터베이스 연결 및 테이블 구조 파악"""
        try:
            for db_file in self.available_databases:
                try:
                    conn = sqlite3.connect(db_file)
                    cursor = conn.cursor()
                    
                    # 모든 테이블 조회
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    all_tables = [row[0] for row in cursor.fetchall()]
                    
                    print(f"🔍 {db_file} 테이블들: {all_tables}")
                    
                    # ETF 정보 테이블 찾기 (우선순위: etfs > etf_info > 기타)
                    etf_table = None
                    for table_candidate in ['etfs', 'etf_info', 'etf_data']:
                        if table_candidate in all_tables:
                            etf_table = table_candidate
                            break
                    
                    # 가격 테이블 찾기
                    price_table = None
                    for table_candidate in ['etf_prices', 'prices', 'price_data']:
                        if table_candidate in all_tables:
                            price_table = table_candidate
                            break
                    
                    # 적어도 하나의 테이블이 있으면 연결
                    if etf_table or price_table:
                        self.conn = conn
                        self.db_path = db_file
                        self.etf_table = etf_table
                        self.price_table = price_table
                        
                        print(f"✅ 데이터베이스 연결 성공: {db_file}")
                        print(f"   - ETF 테이블: {self.etf_table}")
                        print(f"   - 가격 테이블: {self.price_table}")
                        
                        # 테이블 구조 파악
                        self._analyze_table_structure()
                        return True
                    else:
                        print(f"⚠️ {db_file}에서 ETF 관련 테이블을 찾을 수 없음")
                        conn.close()
                        
                except Exception as e:
                    print(f"⚠️ {db_file} 연결 시도 실패: {e}")
                    if 'conn' in locals():
                        conn.close()
                    continue
            
            print("❌ 적절한 데이터베이스 구조를 찾을 수 없습니다")
            print("🔍 시도한 파일들:")
            for db_file in self.available_databases:
                print(f"   - {db_file}")
            return False
                
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            return False
    
    def _analyze_table_structure(self):
        """테이블 구조 분석"""
        try:
            cursor = self.conn.cursor()
            
            # ETF 테이블 구조 분석
            if self.etf_table:
                cursor.execute(f"PRAGMA table_info({self.etf_table});")
                self.etf_columns = [col[1] for col in cursor.fetchall()]
                
                # 컬럼 매핑 설정
                self.symbol_col = 'symbol' if 'symbol' in self.etf_columns else 'code'
                self.name_col = 'name'
                self.current_price_col = 'current_price' if 'current_price' in self.etf_columns else None
                
            # 가격 테이블 구조 분석
            if self.price_table:
                cursor.execute(f"PRAGMA table_info({self.price_table});")
                self.price_columns = [col[1] for col in cursor.fetchall()]
                
                # 가격 테이블 컬럼 매핑
                self.price_symbol_col = 'symbol' if 'symbol' in self.price_columns else 'etf_code'
                self.open_col = 'open' if 'open' in self.price_columns else 'open_price'
                self.high_col = 'high' if 'high' in self.price_columns else 'high_price'
                self.low_col = 'low' if 'low' in self.price_columns else 'low_price'
                self.close_col = 'close' if 'close' in self.price_columns else 'close_price'
                
        except Exception as e:
            print(f"⚠️ 테이블 구조 분석 실패: {e}")
    
    def get_table_info(self):
        """데이터베이스 테이블 정보 조회"""
        try:
            print("\n" + "="*60)
            print("📊 데이터베이스 정보")
            print("="*60)
            print(f"📁 연결된 DB: {self.db_path}")
            
            cursor = self.conn.cursor()
            
            # 모든 사용 가능한 데이터베이스 정보
            print(f"\n🗂️ 사용 가능한 데이터베이스 파일:")
            for db_file in self.available_databases:
                file_size = os.path.getsize(db_file)
                print(f"   - {db_file}: {file_size:,} bytes ({file_size/1024:.1f} KB)")
            
            # 현재 데이터베이스의 테이블 목록
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            print(f"\n🗂️ 테이블 목록:")
            for table in tables:
                table_name = table[0]
                print(f"\n📋 테이블: {table_name}")
                
                # 테이블 구조 조회
                cursor.execute(f"PRAGMA table_info({table_name});")
                columns = cursor.fetchall()
                
                print("   컬럼 정보:")
                for col in columns:
                    col_id, col_name, col_type, not_null, default, pk = col
                    pk_mark = " (PK)" if pk else ""
                    null_mark = " NOT NULL" if not_null else ""
                    default_mark = f" DEFAULT {default}" if default else ""
                    print(f"   - {col_name}: {col_type}{pk_mark}{null_mark}{default_mark}")
                
                # 레코드 수 조회
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                print(f"   📈 레코드 수: {count:,}개")
                
        except Exception as e:
            print(f"❌ 테이블 정보 조회 실패: {e}")
    
    def get_etf_list(self, show_details=True):
        """ETF 목록 조회"""
        try:
            if not self.etf_table:
                print("📭 ETF 정보 테이블이 없습니다.")
                return None
            
            # 실제 테이블 구조 확인
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.etf_table});")
            columns = cursor.fetchall()
            available_columns = [col[1] for col in columns]
            
            print(f"🔍 디버그: 테이블 {self.etf_table}의 컬럼들: {available_columns}")
            
            # 컬럼명 동적 매핑
            symbol_col = None
            name_col = None
            category_col = None
            price_col = None
            
            # 심볼/코드 컬럼 찾기
            for col in ['symbol', 'code', 'etf_code']:
                if col in available_columns:
                    symbol_col = col
                    break
            
            # 이름 컬럼 찾기
            for col in ['name', 'etf_name']:
                if col in available_columns:
                    name_col = col
                    break
            
            # 카테고리 컬럼 찾기
            for col in ['category', 'asset_class']:
                if col in available_columns:
                    category_col = col
                    break
            
            # 가격 컬럼 찾기
            for col in ['current_price', 'close_price', 'price']:
                if col in available_columns:
                    price_col = col
                    break
            
            if not symbol_col or not name_col:
                print(f"❌ 필수 컬럼(심볼, 이름)을 찾을 수 없습니다.")
                print(f"   사용 가능한 컬럼: {available_columns}")
                return None
            
            # 기본 쿼리 구성
            select_parts = [f"{symbol_col} as symbol", f"{name_col} as name"]
            
            if category_col:
                select_parts.append(f"{category_col} as category")
            else:
                select_parts.append("NULL as category")
            
            # 추가 정보 컬럼들
            optional_columns = {
                'expense_ratio': 'expense_ratio',
                'total_assets': 'aum',
                'volume': 'volume',
                'last_updated': 'last_updated'
            }
            
            for db_col, alias in optional_columns.items():
                if db_col in available_columns:
                    select_parts.append(f"{db_col} as {alias}")
                else:
                    select_parts.append(f"NULL as {alias}")
            
            # 현재가 정보 처리
            if price_col:
                select_parts.append(f"{price_col} as current_price")
                if 'price_change_pct' in available_columns:
                    select_parts.append("price_change_pct")
                else:
                    select_parts.append("NULL as price_change_pct")
            elif self.price_table:
                # 별도 가격 테이블에서 최신 가격 가져오기
                base_query = f"""
                SELECT {', '.join(select_parts)},
                       p.{self.close_col} as current_price,
                       p.volume as current_volume,
                       p.date as price_date
                FROM {self.etf_table} e
                LEFT JOIN (
                    SELECT {self.price_symbol_col}, {self.close_col}, volume, date,
                           ROW_NUMBER() OVER (PARTITION BY {self.price_symbol_col} ORDER BY date DESC) as rn
                    FROM {self.price_table}
                ) p ON e.{symbol_col} = p.{self.price_symbol_col} AND p.rn = 1
                ORDER BY COALESCE(e.total_assets, 0) DESC
                """
            else:
                select_parts.extend(["NULL as current_price", "NULL as price_change_pct"])
            
            if not 'base_query' in locals():
                # 기본 쿼리 (가격 테이블 조인 없음)
                order_col = 'total_assets' if 'total_assets' in available_columns else symbol_col
                base_query = f"""
                SELECT {', '.join(select_parts)}
                FROM {self.etf_table}
                ORDER BY COALESCE({order_col}, 0) DESC
                """
            
            print(f"🔍 디버그: 실행할 쿼리: {base_query[:200]}...")
            
            df = pd.read_sql_query(base_query, self.conn)
            
            if df.empty:
                print("📭 ETF 데이터가 없습니다.")
                return None
            
            print("\n" + "="*100)
            print("📋 ETF 목록")
            print("="*100)
            
            if show_details:
                # 수치 포맷팅
                for col in df.columns:
                    if 'aum' in col and df[col].notna().any():
                        df[col] = df[col].apply(lambda x: f"{x/1e9:.1f}조" if pd.notna(x) and x > 1e9 else f"{x/1e8:.0f}억" if pd.notna(x) and x > 0 else "N/A")
                    elif 'expense_ratio' in col and df[col].notna().any():
                        df[col] = df[col].apply(lambda x: f"{x:.3f}%" if pd.notna(x) else "N/A")
                    elif 'current_price' in col and df[col].notna().any():
                        df[col] = df[col].apply(lambda x: f"₩{x:,.0f}" if pd.notna(x) else "N/A")
                    elif 'price_change_pct' in col and df[col].notna().any():
                        df[col] = df[col].apply(lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A")
                    elif 'volume' in col and df[col].notna().any():
                        df[col] = df[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
                
                print(df.to_string(index=False))
            else:
                # 간단한 목록 표시
                print(f"{'순번':<4} {'심볼':<10} {'ETF명':<30} {'카테고리':<15} {'현재가':<12}")
                print("-" * 75)
                
                for i, row in df.iterrows():
                    symbol = str(row['symbol']) if pd.notna(row['symbol']) else "N/A"
                    name = str(row['name'][:28]) if pd.notna(row['name']) else "N/A"
                    category = str(row['category'][:13]) if pd.notna(row['category']) else "N/A"
                    
                    if 'current_price' in row and pd.notna(row['current_price']):
                        if isinstance(row['current_price'], str):
                            price = row['current_price']
                        else:
                            price = f"₩{row['current_price']:,.0f}"
                    else:
                        price = "N/A"
                    
                    print(f"{i+1:<4} {symbol:<10} {name:<30} {category:<15} {price:<12}")
            
            print(f"\n📊 총 {len(df)}개 ETF")
            return df
            
        except Exception as e:
            print(f"❌ ETF 목록 조회 실패: {e}")
            print(f"   ETF 테이블: {self.etf_table}")
            print(f"   가격 테이블: {self.price_table}")
            if hasattr(self, 'etf_columns'):
                print(f"   사용 가능한 컬럼: {self.etf_columns}")
            
            # 추가 디버그 정보
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT * FROM {self.etf_table} LIMIT 1")
                sample_row = cursor.fetchone()
                if sample_row:
                    print(f"   샘플 데이터: {sample_row}")
            except:
                pass
            
            return None
    
    def get_etf_price_data(self, symbol, days=30, show_chart=False):
        """특정 ETF의 가격 데이터 조회"""
        try:
            if not self.price_table:
                print(f"📭 가격 데이터 테이블이 없습니다.")
                return None
            
            query = f"""
            SELECT date, 
                   {self.open_col} as open_price, 
                   {self.high_col} as high_price, 
                   {self.low_col} as low_price, 
                   {self.close_col} as close_price, 
                   volume
            FROM {self.price_table} 
            WHERE {self.price_symbol_col} = ? 
            ORDER BY date DESC 
            LIMIT ?
            """
            
            df = pd.read_sql_query(query, self.conn, params=(symbol, days))
            
            if df.empty:
                print(f"📭 {symbol}의 가격 데이터가 없습니다.")
                
                # 디버그 정보 출력
                cursor = self.conn.cursor()
                cursor.execute(f"SELECT DISTINCT {self.price_symbol_col} FROM {self.price_table} LIMIT 10")
                available_symbols = [row[0] for row in cursor.fetchall()]
                print(f"사용 가능한 심볼 (처음 10개): {available_symbols}")
                
                return None
            
            # 날짜 컬럼을 datetime으로 변환
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 수익률 계산 (returns 컬럼이 없으므로 직접 계산)
            df['returns'] = df['close_price'].pct_change()
            
            print(f"\n{'='*60}")
            print(f"📈 {symbol} 가격 데이터 (최근 {len(df)}일)")
            print(f"{'='*60}")
            
            # 기본 통계
            latest = df.iloc[-1]
            oldest = df.iloc[0]
            
            print(f"📅 기간: {oldest['date'].strftime('%Y-%m-%d')} ~ {latest['date'].strftime('%Y-%m-%d')}")
            print(f"💰 현재가: ₩{latest['close_price']:,.0f}")
            
            if len(df) > 1:
                period_return = ((latest['close_price'] / oldest['close_price']) - 1) * 100
                print(f"📊 기간 수익률: {period_return:+.2f}%")
            
            print(f"📉 최고가: ₩{df['high_price'].max():,.0f}")
            print(f"📈 최저가: ₩{df['low_price'].min():,.0f}")
            
            if df['volume'].notna().any():
                print(f"💱 평균 거래량: {df['volume'].mean():,.0f}")
            
            # 최근 10일 데이터 표시
            print(f"\n📋 최근 10일 데이터:")
            recent_df = df.tail(10).copy()
            recent_df['date_str'] = recent_df['date'].dt.strftime('%m/%d')
            recent_df['returns_pct'] = recent_df['returns'] * 100
            
            print(f"{'날짜':<8} {'시가':<10} {'고가':<10} {'저가':<10} {'종가':<10} {'등락률':<8} {'거래량':<12}")
            print("-" * 80)
            
            for _, row in recent_df.iterrows():
                open_price = f"₩{row['open_price']:>7,.0f}" if pd.notna(row['open_price']) else "N/A    "
                high_price = f"₩{row['high_price']:>7,.0f}" if pd.notna(row['high_price']) else "N/A    "
                low_price = f"₩{row['low_price']:>7,.0f}" if pd.notna(row['low_price']) else "N/A    "
                close_price = f"₩{row['close_price']:>7,.0f}" if pd.notna(row['close_price']) else "N/A    "
                returns_str = f"{row['returns_pct']:+.2f}%" if pd.notna(row['returns_pct']) else "N/A   "
                volume_str = f"{row['volume']:,.0f}" if pd.notna(row['volume']) else "N/A"
                
                print(f"{row['date_str']:<8} {open_price:<10} {high_price:<10} {low_price:<10} {close_price:<10} {returns_str:>7} {volume_str:>11}")
            
            # 차트 그리기 (선택사항)
            if show_chart and PLOTTING_AVAILABLE:
                self._plot_price_chart(df, symbol)
            
            return df
            
        except Exception as e:
            print(f"❌ {symbol} 가격 데이터 조회 실패: {e}")
            print(f"   가격 테이블: {self.price_table}")
            print(f"   심볼 컬럼: {getattr(self, 'price_symbol_col', 'N/A')}")
            print(f"   사용 가능한 컬럼: {getattr(self, 'price_columns', 'N/A')}")
            return None
    
    def get_market_summary(self):
        """시장 전체 요약 정보"""
        try:
            print("\n" + "="*60)
            print("🌐 시장 전체 요약")
            print("="*60)
            
            if self.etf_table:
                # ETF 기본 통계
                etf_query = f"""
                SELECT 
                    COUNT(*) as total_etfs,
                    AVG(expense_ratio) as avg_expense_ratio,
                    SUM(total_assets) as total_aum,
                    COUNT(CASE WHEN expense_ratio IS NOT NULL THEN 1 END) as etfs_with_expense_ratio
                FROM {self.etf_table}
                """
                
                etf_stats = pd.read_sql_query(etf_query, self.conn).iloc[0]
                
                print(f"📊 총 ETF 수: {etf_stats['total_etfs']}개")
                
                if pd.notna(etf_stats['total_aum']) and etf_stats['total_aum'] > 0:
                    if etf_stats['total_aum'] > 1e12:
                        print(f"💰 총 자산규모: ₩{etf_stats['total_aum']/1e12:.1f}조원")
                    else:
                        print(f"💰 총 자산규모: ₩{etf_stats['total_aum']/1e8:.0f}억원")
                
                if pd.notna(etf_stats['avg_expense_ratio']) and etf_stats['etfs_with_expense_ratio'] > 0:
                    print(f"💸 평균 운용보수: {etf_stats['avg_expense_ratio']:.3f}%")
                
            # 가격 데이터 통계
            if self.price_table:
                price_query = f"""
                SELECT 
                    COUNT(DISTINCT {self.price_symbol_col}) as etfs_with_price_data,
                    COUNT(*) as total_price_records,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    AVG({self.close_col}) as avg_price
                FROM {self.price_table}
                WHERE {self.close_col} IS NOT NULL
                """
                
                price_stats = pd.read_sql_query(price_query, self.conn).iloc[0]
                
                print(f"\n📅 데이터 기간:")
                print(f"   - 가격 데이터 ETF: {price_stats['etfs_with_price_data']}개")
                print(f"   - 총 가격 레코드: {price_stats['total_price_records']:,}개")
                print(f"   - 데이터 기간: {price_stats['earliest_date']} ~ {price_stats['latest_date']}")
                
                if pd.notna(price_stats['avg_price']):
                    print(f"   - 평균 가격: ₩{price_stats['avg_price']:,.0f}")
                
                # 최신 거래일 데이터 통계
                latest_data_query = f"""
                SELECT 
                    AVG({self.close_col}) as avg_latest_price,
                    COUNT(*) as etfs_with_latest_data
                FROM {self.price_table}
                WHERE date = (SELECT MAX(date) FROM {self.price_table})
                AND {self.close_col} IS NOT NULL
                """
                
                latest_stats = pd.read_sql_query(latest_data_query, self.conn).iloc[0]
                print(f"   - 최신일 데이터: {latest_stats['etfs_with_latest_data']}개 ETF")
                if pd.notna(latest_stats['avg_latest_price']):
                    print(f"   - 최신일 평균가: ₩{latest_stats['avg_latest_price']:,.0f}")
            
            # 카테고리별 분포 (ETF 테이블이 있는 경우)
            if self.etf_table:
                category_query = f"""
                SELECT category, 
                       COUNT(*) as count, 
                       AVG(total_assets) as avg_assets,
                       SUM(total_assets) as total_assets
                FROM {self.etf_table} 
                WHERE category IS NOT NULL AND category != ''
                GROUP BY category 
                ORDER BY count DESC
                LIMIT 10
                """
                
                try:
                    category_df = pd.read_sql_query(category_query, self.conn)
                    
                    if not category_df.empty:
                        print(f"\n📋 카테고리별 분포 (상위 10개):")
                        for _, row in category_df.iterrows():
                            category = row['category']
                            count = row['count']
                            
                            if pd.notna(row['total_assets']) and row['total_assets'] > 0:
                                if row['total_assets'] > 1e12:
                                    assets_str = f"₩{row['total_assets']/1e12:.1f}조"
                                else:
                                    assets_str = f"₩{row['total_assets']/1e8:.0f}억"
                                print(f"   - {category}: {count}개 (총자산: {assets_str})")
                            else:
                                print(f"   - {category}: {count}개")
                except:
                    print("\n⚠️ 카테고리별 분포 조회 실패")
            
        except Exception as e:
            print(f"❌ 시장 요약 조회 실패: {e}")
            print(f"   ETF 테이블: {self.etf_table}")
            print(f"   가격 테이블: {self.price_table}")
    
    def get_top_performers(self, n=10, period_days=30):
        """최고 성과 ETF 조회"""
        try:
            if not self.price_table:
                print("📭 가격 데이터 테이블이 없습니다.")
                return None
            
            # 기간별 수익률 계산 (returns 컬럼이 없으므로 직접 계산)
            query = f"""
            WITH period_data AS (
                SELECT 
                    {self.price_symbol_col} as symbol,
                    date,
                    {self.close_col} as close_price,
                    volume,
                    ROW_NUMBER() OVER (PARTITION BY {self.price_symbol_col} ORDER BY date DESC) as rn_latest,
                    ROW_NUMBER() OVER (PARTITION BY {self.price_symbol_col} ORDER BY date) as rn_earliest
                FROM {self.price_table}
                WHERE date >= date('now', '-{period_days} days')
                AND {self.close_col} IS NOT NULL
            ),
            latest_prices AS (
                SELECT symbol, close_price as latest_price, volume as latest_volume
                FROM period_data 
                WHERE rn_latest = 1
            ),
            earliest_prices AS (
                SELECT symbol, close_price as earliest_price
                FROM period_data 
                WHERE rn_earliest = 1
            )
            SELECT 
                l.symbol,
                l.latest_price as current_price,
                l.latest_volume as volume,
                e.earliest_price,
                ((l.latest_price / e.earliest_price) - 1) * 100 as returns
            FROM latest_prices l
            JOIN earliest_prices e ON l.symbol = e.symbol
            WHERE l.latest_price IS NOT NULL AND e.earliest_price IS NOT NULL
            AND e.earliest_price > 0
            ORDER BY returns DESC
            LIMIT {n}
            """
            
            df = pd.read_sql_query(query, self.conn)
            
            if df.empty:
                print(f"📭 {period_days}일 기간의 성과 데이터가 없습니다.")
                return None
            
            # ETF 이름 추가 (가능한 경우)
            if self.etf_table:
                for i, row in df.iterrows():
                    try:
                        cursor = self.conn.cursor()
                        cursor.execute(f"SELECT {self.name_col} FROM {self.etf_table} WHERE {self.symbol_col} = ?", (row['symbol'],))
                        name_result = cursor.fetchone()
                        df.at[i, 'name'] = name_result[0] if name_result else row['symbol']
                    except:
                        df.at[i, 'name'] = row['symbol']
            else:
                df['name'] = df['symbol']
            
            print(f"\n{'='*70}")
            print(f"🏆 최고 성과 ETF TOP {n} (최근 {period_days}일)")
            print(f"{'='*70}")
            
            print(f"{'순위':<4} {'심볼':<10} {'ETF명':<25} {'수익률':<10} {'현재가':<12}")
            print("-" * 65)
            
            for i, row in df.iterrows():
                name = str(row['name'][:23]) if pd.notna(row['name']) else row['symbol']
                price = f"₩{row['current_price']:,.0f}" if pd.notna(row['current_price']) else "N/A"
                returns = f"{row['returns']:+.2f}%" if pd.notna(row['returns']) else "N/A"
                print(f"{i+1:<4} {row['symbol']:<10} {name:<25} {returns:<10} {price:<12}")
            
            return df
            
        except Exception as e:
            print(f"❌ 최고 성과 ETF 조회 실패: {e}")
            print(f"   가격 테이블: {self.price_table}")
            print(f"   심볼 컬럼: {getattr(self, 'price_symbol_col', 'N/A')}")
            return None
    
    def search_etf(self, keyword):
        """ETF 검색"""
        try:
            if not self.etf_table:
                print("📭 ETF 정보 테이블이 없습니다.")
                return None
            
            # 검색 쿼리 구성
            base_query = f"""
            SELECT {self.symbol_col} as symbol, 
                   {self.name_col} as name, 
                   category
            FROM {self.etf_table} 
            WHERE {self.name_col} LIKE ? OR {self.symbol_col} LIKE ? OR category LIKE ?
            """
            
            # 현재가 정보 추가 (있는 경우)
            if self.current_price_col:
                query = f"""
                SELECT {self.symbol_col} as symbol, 
                       {self.name_col} as name, 
                       category, 
                       {self.current_price_col} as current_price, 
                       price_change_pct, 
                       volume
                FROM {self.etf_table} 
                WHERE {self.name_col} LIKE ? OR {self.symbol_col} LIKE ? OR category LIKE ?
                ORDER BY total_assets DESC
                """
            elif self.price_table:
                # 최신 가격 정보를 별도 테이블에서 가져오기
                query = f"""
                SELECT e.{self.symbol_col} as symbol, 
                       e.{self.name_col} as name, 
                       e.category, 
                       p.{self.close_col} as current_price, 
                       p.volume
                FROM {self.etf_table} e
                LEFT JOIN (
                    SELECT {self.price_symbol_col}, {self.close_col}, volume,
                           ROW_NUMBER() OVER (PARTITION BY {self.price_symbol_col} ORDER BY date DESC) as rn
                    FROM {self.price_table}
                ) p ON e.{self.symbol_col} = p.{self.price_symbol_col} AND p.rn = 1
                WHERE e.{self.name_col} LIKE ? OR e.{self.symbol_col} LIKE ? OR e.category LIKE ?
                ORDER BY COALESCE(e.total_assets, 0) DESC
                """
            else:
                query = base_query + " ORDER BY name"
            
            search_term = f"%{keyword}%"
            df = pd.read_sql_query(query, self.conn, params=(search_term, search_term, search_term))
            
            if df.empty:
                print(f"🔍 '{keyword}' 검색 결과가 없습니다.")
                return None
            
            print(f"\n{'='*80}")
            print(f"🔍 '{keyword}' 검색 결과 ({len(df)}개)")
            print(f"{'='*80}")
            
            print(f"{'심볼':<10} {'ETF명':<35} {'카테고리':<20} {'현재가':<12}")
            print("-" * 80)
            
            for _, row in df.iterrows():
                symbol = str(row['symbol']) if pd.notna(row['symbol']) else "N/A"
                name = str(row['name'][:33]) if pd.notna(row['name']) else "N/A"
                category = str(row['category'][:18]) if pd.notna(row['category']) else "N/A"
                
                if 'current_price' in row and pd.notna(row['current_price']):
                    price = f"₩{row['current_price']:,.0f}"
                else:
                    price = "N/A"
                
                print(f"{symbol:<10} {name:<35} {category:<20} {price:<12}")
            
            return df
            
        except Exception as e:
            print(f"❌ ETF 검색 실패: {e}")
            print(f"   ETF 테이블: {self.etf_table}")
            print(f"   검색 키워드: {keyword}")
            return None
    
    def export_data(self, symbol=None, output_file=None):
        """데이터 CSV로 내보내기"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if symbol:
                # 특정 ETF 데이터 내보내기
                if self.price_table and self.etf_table:
                    query = f"""
                    SELECT ep.*, e.{self.name_col} as name, e.category
                    FROM {self.price_table} ep
                    LEFT JOIN {self.etf_table} e ON ep.{self.price_symbol_col} = e.{self.symbol_col}
                    WHERE ep.{self.price_symbol_col} = ?
                    ORDER BY ep.date
                    """
                    params = (symbol,)
                elif self.price_table:
                    query = f"SELECT * FROM {self.price_table} WHERE {self.price_symbol_col} = ? ORDER BY date"
                    params = (symbol,)
                else:
                    print("📭 가격 데이터 테이블이 없습니다.")
                    return
                
                df = pd.read_sql_query(query, self.conn, params=params)
                
                if output_file is None:
                    output_file = f"etf_{symbol}_{timestamp}.csv"
                    
                print(f"💾 {symbol} 데이터를 {output_file}로 내보내는 중...")
                
            else:
                # 전체 ETF 목록 내보내기
                if self.etf_table:
                    query = f"SELECT * FROM {self.etf_table} ORDER BY COALESCE(total_assets, 0) DESC"
                else:
                    print("📭 ETF 정보 테이블이 없습니다.")
                    return
                
                df = pd.read_sql_query(query, self.conn)
                
                if output_file is None:
                    output_file = f"etf_list_{timestamp}.csv"
                    
                print(f"💾 전체 ETF 목록을 {output_file}로 내보내는 중...")
            
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"✅ 내보내기 완료: {output_file} ({len(df)}개 레코드)")
            
        except Exception as e:
            print(f"❌ 데이터 내보내기 실패: {e}")
    
    def _plot_price_chart(self, df, symbol):
        """가격 차트 그리기"""
        if not PLOTTING_AVAILABLE:
            print("⚠️ matplotlib 라이브러리가 없어 차트를 그릴 수 없습니다.")
            return
        
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))
            
            # 컬럼명 매핑
            close_col = 'close_price' if 'close_price' in df.columns else 'close'
            high_col = 'high_price' if 'high_price' in df.columns else 'high'
            low_col = 'low_price' if 'low_price' in df.columns else 'low'
            volume_col = 'volume'
            
            # 가격 차트
            ax1.plot(df['date'], df[close_col], linewidth=2, color='blue', label='종가')
            if high_col in df.columns and low_col in df.columns:
                ax1.fill_between(df['date'], df[low_col], df[high_col], alpha=0.3, color='lightblue', label='일중 범위')
            ax1.set_title(f'{symbol} 가격 추이', fontsize=14, fontweight='bold')
            ax1.set_ylabel('가격 (원)')
            ax1.legend()
            ax1.grid(True, alpha=0.3)
            
            # 거래량 차트
            if volume_col in df.columns and df[volume_col].notna().any():
                ax2.bar(df['date'], df[volume_col], alpha=0.7, color='green', label='거래량')
                ax2.set_title(f'{symbol} 거래량', fontsize=12)
                ax2.set_ylabel('거래량')
                ax2.legend()
                ax2.grid(True, alpha=0.3)
            else:
                ax2.text(0.5, 0.5, '거래량 데이터 없음', transform=ax2.transAxes, 
                        ha='center', va='center', fontsize=12)
            
            ax2.set_xlabel('날짜')
            
            plt.tight_layout()
            plt.xticks(rotation=45)
            
            # 파일로 저장
            filename = f"{symbol}_chart_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"📊 차트 저장: {filename}")
            
            plt.show()
            
        except Exception as e:
            print(f"❌ 차트 생성 실패: {e}")
    
    def close(self):
        """데이터베이스 연결 종료"""
        if self.conn:
            self.conn.close()
            print("📝 데이터베이스 연결을 종료했습니다.")
        else:
            print("📝 활성 연결이 없습니다.")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="ETF 데이터베이스 탐색기")
    parser.add_argument("--db", default="etf_universe.db", help="데이터베이스 파일 경로")
    parser.add_argument("--action", choices=["info", "list", "price", "summary", "top", "search", "export"], 
                       default="info", help="실행할 작업")
    parser.add_argument("--symbol", help="ETF 심볼 (price, export 작업용)")
    parser.add_argument("--keyword", help="검색 키워드 (search 작업용)")
    parser.add_argument("--days", type=int, default=30, help="조회 기간 (일)")
    parser.add_argument("--limit", type=int, default=10, help="조회 개수 제한")
    parser.add_argument("--chart", action="store_true", help="차트 표시")
    parser.add_argument("--output", help="출력 파일명")
    parser.add_argument("--interactive", action="store_true", help="대화형 모드")
    
    args = parser.parse_args()
    
    # 데이터베이스 탐색기 초기화
    explorer = ETFDatabaseExplorer(args.db)
    
    if not explorer.conn:
        print("데이터베이스 연결에 실패했습니다.")
        return
    
    try:
        if args.interactive:
            # 대화형 모드
            interactive_mode(explorer)
        else:
            # 단일 명령 모드
            if args.action == "info":
                explorer.get_table_info()
            elif args.action == "list":
                explorer.get_etf_list()
            elif args.action == "price":
                if not args.symbol:
                    print("❌ --symbol 옵션이 필요합니다.")
                    return
                explorer.get_etf_price_data(args.symbol, args.days, args.chart)
            elif args.action == "summary":
                explorer.get_market_summary()
            elif args.action == "top":
                explorer.get_top_performers(args.limit, args.days)
            elif args.action == "search":
                if not args.keyword:
                    print("❌ --keyword 옵션이 필요합니다.")
                    return
                explorer.search_etf(args.keyword)
            elif args.action == "export":
                explorer.export_data(args.symbol, args.output)
                
    finally:
        explorer.close()

def interactive_mode(explorer):
    """대화형 모드"""
    print("\n" + "="*60)
    print("🎯 ETF 데이터베이스 탐색기 - 대화형 모드")
    print("="*60)
    print("사용 가능한 명령어:")
    print("  info     - 데이터베이스 정보")
    print("  list     - ETF 목록")
    print("  price    - 가격 데이터 (예: price 069500)")
    print("  summary  - 시장 요약")
    print("  top      - 최고 성과 ETF")
    print("  search   - ETF 검색 (예: search 미국)")
    print("  export   - 데이터 내보내기")
    print("  debug    - 디버그 정보")
    print("  quit     - 종료")
    print("-" * 60)
    
    # 연결 상태 확인
    if not explorer.conn:
        print("❌ 데이터베이스에 연결되지 않았습니다.")
        print("사용 가능한 데이터베이스 파일을 확인하세요.")
        return
    
    while True:
        try:
            command = input("\n🔍 명령어 입력: ").strip().lower()
            
            if command == "quit" or command == "q":
                break
            elif command == "info":
                explorer.get_table_info()
            elif command == "list":
                result = explorer.get_etf_list()
                if result is None:
                    print("💡 ETF 목록을 가져올 수 없습니다. 'debug' 명령으로 상세 정보를 확인하세요.")
            elif command.startswith("price"):
                parts = command.split()
                if len(parts) < 2:
                    print("❌ 사용법: price <ETF심볼>")
                    continue
                symbol = parts[1].upper()
                explorer.get_etf_price_data(symbol, 30, False)
            elif command == "summary":
                explorer.get_market_summary()
            elif command == "top":
                explorer.get_top_performers()
            elif command.startswith("search"):
                parts = command.split(maxsplit=1)
                if len(parts) < 2:
                    print("❌ 사용법: search <검색어>")
                    continue
                keyword = parts[1]
                explorer.search_etf(keyword)
            elif command == "export":
                explorer.export_data()
            elif command == "debug":
                print(f"\n🔍 디버그 정보:")
                print(f"   - 연결된 DB: {explorer.db_path}")
                print(f"   - ETF 테이블: {explorer.etf_table}")
                print(f"   - 가격 테이블: {explorer.price_table}")
                print(f"   - 사용 가능한 DB들: {explorer.available_databases}")
                
                if explorer.etf_table:
                    try:
                        cursor = explorer.conn.cursor()
                        cursor.execute(f"PRAGMA table_info({explorer.etf_table});")
                        columns = cursor.fetchall()
                        col_names = [col[1] for col in columns]
                        print(f"   - ETF 테이블 컬럼들: {col_names}")
                        
                        cursor.execute(f"SELECT COUNT(*) FROM {explorer.etf_table}")
                        count = cursor.fetchone()[0]
                        print(f"   - ETF 테이블 레코드 수: {count}")
                        
                        if count > 0:
                            cursor.execute(f"SELECT * FROM {explorer.etf_table} LIMIT 1")
                            sample = cursor.fetchone()
                            print(f"   - 샘플 데이터: {sample}")
                    except Exception as e:
                        print(f"   - ETF 테이블 디버그 실패: {e}")
                
                if explorer.price_table:
                    try:
                        cursor = explorer.conn.cursor()
                        cursor.execute(f"PRAGMA table_info({explorer.price_table});")
                        columns = cursor.fetchall()
                        col_names = [col[1] for col in columns]
                        print(f"   - 가격 테이블 컬럼들: {col_names}")
                        
                        cursor.execute(f"SELECT COUNT(*) FROM {explorer.price_table}")
                        count = cursor.fetchone()[0]
                        print(f"   - 가격 테이블 레코드 수: {count}")
                    except Exception as e:
                        print(f"   - 가격 테이블 디버그 실패: {e}")
            elif command == "help" or command == "h":
                print("사용 가능한 명령어: info, list, price, summary, top, search, export, debug, quit")
            else:
                print("❌ 알 수 없는 명령어입니다. 'help'를 입력하세요.")
                
        except KeyboardInterrupt:
            print("\n\n👋 프로그램을 종료합니다.")
            break
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            print("💡 'debug' 명령으로 상세 정보를 확인하세요.")

if __name__ == "__main__":
    main()