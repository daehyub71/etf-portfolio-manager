"""
데이터베이스 관리 모듈
포트폴리오와 ETF 데이터를 SQLite로 관리하는 핵심 시스템 (실제 데이터 필드 추가)
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import json
import os

logger = logging.getLogger(__name__)

class DatabaseManager:
    """데이터베이스 관리 핵심 클래스 (실제 데이터 수집 지원)"""
    
    def __init__(self, db_path: str = "data"):
        """
        데이터베이스 관리자 초기화
        
        Args:
            db_path: 데이터베이스 파일들이 저장될 경로
        """
        self.db_path = Path(db_path)
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        self.portfolio_db = self.db_path / "portfolio_data.db"
        self.etf_db = self.db_path / "etf_data.db"
        
        # 단일 ETF 데이터베이스 지원 (update_manager.py 호환성)
        self.unified_db = self.db_path / "etf_universe.db"
        
        self._init_databases()
        self._migrate_existing_databases()  # 🆕 기존 DB 마이그레이션
    
    def _init_databases(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            self._create_portfolio_tables()
            self._create_etf_tables()
            self._create_unified_etf_tables()  # 🆕 통합 ETF 테이블
            logger.info("데이터베이스 초기화 완료 (실제 데이터 필드 포함)")
        except Exception as e:
            logger.error(f"데이터베이스 초기화 실패: {e}")
            raise
    
    def _migrate_existing_databases(self):
        """기존 데이터베이스 마이그레이션 (새 컬럼 추가)"""
        try:
            # 포트폴리오 DB 마이그레이션
            self._migrate_portfolio_db()
            
            # ETF DB 마이그레이션  
            self._migrate_etf_db()
            
            # 통합 ETF DB 마이그레이션
            self._migrate_unified_etf_db()
            
            logger.info("데이터베이스 마이그레이션 완료")
            
        except Exception as e:
            logger.warning(f"데이터베이스 마이그레이션 중 오류 (무시 가능): {e}")
    
    def _migrate_portfolio_db(self):
        """포트폴리오 DB 마이그레이션"""
        if not self.portfolio_db.exists():
            return
            
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                cursor = conn.cursor()
                
                # 새 컬럼들 추가 시도
                new_columns = [
                    "ALTER TABLE portfolio_holdings ADD COLUMN data_source TEXT DEFAULT 'unknown'",
                    "ALTER TABLE portfolio_holdings ADD COLUMN data_quality TEXT DEFAULT 'unknown'",
                    "ALTER TABLE transactions ADD COLUMN data_source TEXT DEFAULT 'manual'"
                ]
                
                for sql in new_columns:
                    try:
                        cursor.execute(sql)
                    except sqlite3.OperationalError:
                        # 컬럼이 이미 존재하면 무시
                        pass
                        
                conn.commit()
                
        except Exception as e:
            logger.debug(f"포트폴리오 DB 마이그레이션 오류: {e}")
    
    def _migrate_etf_db(self):
        """ETF DB 마이그레이션"""
        if not self.etf_db.exists():
            return
            
        try:
            with sqlite3.connect(self.etf_db) as conn:
                cursor = conn.cursor()
                
                # 새 컬럼들 추가 시도
                new_columns = [
                    "ALTER TABLE etf_info ADD COLUMN data_quality TEXT DEFAULT 'unknown'",
                    "ALTER TABLE etf_info ADD COLUMN data_source TEXT DEFAULT 'unknown'", 
                    "ALTER TABLE etf_info ADD COLUMN quality_score INTEGER DEFAULT 0",
                    "ALTER TABLE etf_info ADD COLUMN last_real_update TEXT",
                    "ALTER TABLE etf_info ADD COLUMN dividend_yield REAL DEFAULT 0",
                    "ALTER TABLE etf_prices ADD COLUMN data_source TEXT DEFAULT 'unknown'"
                ]
                
                for sql in new_columns:
                    try:
                        cursor.execute(sql)
                    except sqlite3.OperationalError:
                        # 컬럼이 이미 존재하면 무시
                        pass
                        
                conn.commit()
                
        except Exception as e:
            logger.debug(f"ETF DB 마이그레이션 오류: {e}")
    
    def _migrate_unified_etf_db(self):
        """통합 ETF DB 마이그레이션"""
        if not self.unified_db.exists():
            return
            
        try:
            with sqlite3.connect(self.unified_db) as conn:
                cursor = conn.cursor()
                
                # 기존 etf_info 테이블에 새 컬럼들 추가 시도
                new_columns = [
                    "ALTER TABLE etf_info ADD COLUMN data_quality TEXT DEFAULT 'unknown'",
                    "ALTER TABLE etf_info ADD COLUMN data_source TEXT DEFAULT 'unknown'",
                    "ALTER TABLE etf_info ADD COLUMN quality_score INTEGER DEFAULT 0", 
                    "ALTER TABLE etf_info ADD COLUMN last_real_update TEXT",
                    "ALTER TABLE etf_info ADD COLUMN dividend_yield REAL DEFAULT 0",
                    "ALTER TABLE etf_info ADD COLUMN fund_manager TEXT",
                    "ALTER TABLE etf_info ADD COLUMN benchmark TEXT"
                ]
                
                for sql in new_columns:
                    try:
                        cursor.execute(sql)
                    except sqlite3.OperationalError:
                        # 컬럼이 이미 존재하면 무시
                        pass
                
                # etf_price_history 테이블 마이그레이션
                try:
                    cursor.execute("ALTER TABLE etf_price_history ADD COLUMN data_source TEXT DEFAULT 'unknown'")
                except sqlite3.OperationalError:
                    pass
                        
                conn.commit()
                
        except Exception as e:
            logger.debug(f"통합 ETF DB 마이그레이션 오류: {e}")
    
    def _create_portfolio_tables(self):
        """포트폴리오 관련 테이블 생성 (실제 데이터 필드 추가)"""
        with sqlite3.connect(self.portfolio_db) as conn:
            # 포트폴리오 기본 정보
            conn.execute('''
                CREATE TABLE IF NOT EXISTS portfolios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    strategy_type TEXT NOT NULL,
                    target_allocation TEXT NOT NULL,  -- JSON 형태
                    risk_level TEXT NOT NULL,
                    created_date DATE NOT NULL,
                    last_rebalance_date DATE,
                    total_investment REAL DEFAULT 0,
                    current_value REAL DEFAULT 0,
                    active BOOLEAN DEFAULT 1,
                    use_real_data BOOLEAN DEFAULT 1,
                    last_real_data_update TEXT
                )
            ''')
            
            # 포트폴리오 구성 종목 (실제 데이터 필드 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS portfolio_holdings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id INTEGER NOT NULL,
                    etf_code TEXT NOT NULL,
                    target_weight REAL NOT NULL,
                    current_weight REAL DEFAULT 0,
                    shares INTEGER DEFAULT 0,
                    avg_price REAL DEFAULT 0,
                    current_price REAL DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_source TEXT DEFAULT 'unknown',
                    data_quality TEXT DEFAULT 'unknown',
                    price_data_source TEXT DEFAULT 'unknown',
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
                )
            ''')
            
            # 거래 내역 (데이터 소스 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id INTEGER NOT NULL,
                    etf_code TEXT NOT NULL,
                    transaction_type TEXT NOT NULL,  -- 'BUY', 'SELL', 'DIVIDEND'
                    shares INTEGER NOT NULL,
                    price REAL NOT NULL,
                    transaction_date DATE NOT NULL,
                    fee REAL DEFAULT 0,
                    total_amount REAL NOT NULL,
                    note TEXT,
                    data_source TEXT DEFAULT 'manual',
                    real_time_price BOOLEAN DEFAULT 0,
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
                )
            ''')
            
            # 리밸런싱 기록
            conn.execute('''
                CREATE TABLE IF NOT EXISTS rebalancing_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id INTEGER NOT NULL,
                    rebalance_date DATE NOT NULL,
                    trigger_type TEXT NOT NULL,  -- 'SCHEDULED', 'THRESHOLD', 'MANUAL'
                    before_allocation TEXT NOT NULL,  -- JSON
                    after_allocation TEXT NOT NULL,   -- JSON
                    trades_executed TEXT NOT NULL,    -- JSON
                    total_cost REAL DEFAULT 0,
                    data_quality_score REAL DEFAULT 0,
                    price_data_sources TEXT,  -- JSON
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
                )
            ''')
            
            # 성과 추적 (데이터 품질 정보 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS performance_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    portfolio_id INTEGER NOT NULL,
                    date DATE NOT NULL,
                    total_value REAL NOT NULL,
                    total_investment REAL NOT NULL,
                    daily_return REAL DEFAULT 0,
                    cumulative_return REAL DEFAULT 0,
                    benchmark_return REAL DEFAULT 0,
                    data_quality TEXT DEFAULT 'unknown',
                    price_data_sources TEXT,  -- JSON
                    calculation_method TEXT DEFAULT 'standard',
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id),
                    UNIQUE(portfolio_id, date)
                )
            ''')
    
    def _create_etf_tables(self):
        """ETF 관련 테이블 생성 (실제 데이터 필드 추가)"""
        with sqlite3.connect(self.etf_db) as conn:
            # ETF 기본 정보 (실제 데이터 필드 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_info (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    subcategory TEXT,
                    asset_class TEXT NOT NULL,
                    region TEXT NOT NULL,
                    currency TEXT DEFAULT 'KRW',
                    expense_ratio REAL,
                    inception_date DATE,
                    total_assets REAL,
                    avg_volume INTEGER,
                    tracking_index TEXT,
                    fund_company TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    dividend_yield REAL DEFAULT 0,
                    data_quality TEXT DEFAULT 'unknown',
                    data_source TEXT DEFAULT 'unknown',
                    quality_score INTEGER DEFAULT 0,
                    last_real_update TEXT,
                    sources_used TEXT,  -- JSON
                    collection_errors TEXT  -- JSON
                )
            ''')
            
            # ETF 가격 데이터 (데이터 소스 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    etf_code TEXT NOT NULL,
                    date DATE NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL NOT NULL,
                    volume INTEGER DEFAULT 0,
                    nav REAL,
                    premium_discount REAL,
                    data_source TEXT DEFAULT 'unknown',
                    data_quality TEXT DEFAULT 'unknown',
                    collection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (etf_code) REFERENCES etf_info (code),
                    UNIQUE(etf_code, date)
                )
            ''')
            
            # ETF 배당 정보
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_dividends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    etf_code TEXT NOT NULL,
                    ex_date DATE NOT NULL,
                    pay_date DATE,
                    dividend_amount REAL NOT NULL,
                    dividend_type TEXT DEFAULT 'REGULAR',
                    data_source TEXT DEFAULT 'unknown',
                    FOREIGN KEY (etf_code) REFERENCES etf_info (code)
                )
            ''')
            
            # ETF 성과 지표 (데이터 품질 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_performance (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    etf_code TEXT NOT NULL,
                    date DATE NOT NULL,
                    return_1d REAL,
                    return_1w REAL,
                    return_1m REAL,
                    return_3m REAL,
                    return_6m REAL,
                    return_1y REAL,
                    return_3y REAL,
                    volatility_1m REAL,
                    volatility_3m REAL,
                    volatility_1y REAL,
                    sharpe_ratio REAL,
                    max_drawdown REAL,
                    tracking_error REAL,
                    data_source TEXT DEFAULT 'calculated',
                    calculation_quality TEXT DEFAULT 'unknown',
                    FOREIGN KEY (etf_code) REFERENCES etf_info (code),
                    UNIQUE(etf_code, date)
                )
            ''')
    
    def _create_unified_etf_tables(self):
        """통합 ETF 테이블 생성 (update_manager.py 호환)"""
        with sqlite3.connect(self.unified_db) as conn:
            # update_manager.py와 호환되는 etf_info 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_info (
                    code TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    category TEXT,
                    subcategory TEXT,
                    asset_class TEXT,
                    region TEXT,
                    currency TEXT DEFAULT 'KRW',
                    expense_ratio REAL,
                    aum REAL DEFAULT 0,
                    market_price REAL DEFAULT 0,
                    nav REAL DEFAULT 0,
                    premium_discount REAL DEFAULT 0,
                    dividend_yield REAL DEFAULT 0,
                    tracking_error REAL DEFAULT 0,
                    benchmark TEXT,
                    fund_manager TEXT,
                    inception_date TEXT,
                    last_updated TEXT,
                    avg_volume INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1,
                    data_quality TEXT DEFAULT 'unknown',
                    data_source TEXT DEFAULT 'unknown',
                    quality_score INTEGER DEFAULT 0,
                    last_real_update TEXT
                )
            ''')
            
            # ETF 가격 히스토리 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS etf_price_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT,
                    date TEXT,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    close_price REAL,
                    volume INTEGER,
                    returns REAL,
                    data_source TEXT DEFAULT 'unknown',
                    FOREIGN KEY (code) REFERENCES etf_info(code),
                    UNIQUE(code, date)
                )
            ''')
            
            # 업데이트 히스토리 테이블 (실제 데이터 통계 추가)
            conn.execute('''
                CREATE TABLE IF NOT EXISTS update_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time TEXT,
                    end_time TEXT,
                    total_etfs INTEGER,
                    successful_updates INTEGER,
                    failed_updates INTEGER,
                    success_rate REAL,
                    total_duration REAL,
                    update_source TEXT,
                    market_status TEXT,
                    summary_json TEXT,
                    real_data_count INTEGER DEFAULT 0,
                    dummy_data_count INTEGER DEFAULT 0,
                    excellent_quality_count INTEGER DEFAULT 0,
                    data_quality_distribution TEXT
                )
            ''')
            
            # 데이터 품질 추적 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS data_quality_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT,
                    collection_time TEXT,
                    data_quality TEXT,
                    quality_score INTEGER,
                    data_source TEXT,
                    expense_ratio REAL,
                    dividend_yield REAL,
                    current_price REAL,
                    volume INTEGER,
                    sources_used TEXT,
                    FOREIGN KEY (code) REFERENCES etf_info(code)
                )
            ''')
            
            # 시장 상태 로그 테이블
            conn.execute('''
                CREATE TABLE IF NOT EXISTS market_status_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    status_json TEXT,
                    real_data_collector_available BOOLEAN,
                    pykrx_available BOOLEAN
                )
            ''')
    
    # ==========================================
    # 🆕 실제 데이터 관련 메서드들
    # ==========================================
    
    def update_etf_with_real_data(self, etf_code: str, real_data: Dict) -> bool:
        """실제 수집된 ETF 데이터로 업데이트"""
        try:
            # 통합 DB 업데이트
            with sqlite3.connect(self.unified_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO etf_info 
                    (code, name, market_price, nav, avg_volume, 
                     expense_ratio, dividend_yield, fund_manager, benchmark,
                     last_updated, data_quality, data_source, quality_score, last_real_update)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    etf_code,
                    real_data.get('name', etf_code),
                    real_data.get('current_price', 0),
                    real_data.get('nav', 0),
                    real_data.get('volume', 0),
                    real_data.get('expense_ratio', 0),
                    real_data.get('dividend_yield', 0),
                    real_data.get('fund_manager', ''),
                    real_data.get('benchmark', ''),
                    datetime.now().isoformat(),
                    real_data.get('data_quality', 'unknown'),
                    real_data.get('data_source', 'unknown'),
                    real_data.get('quality_score', 0),
                    datetime.now().isoformat() if real_data.get('data_quality') in ['excellent', 'good'] else None
                ))
            
            # ETF DB도 동기화
            with sqlite3.connect(self.etf_db) as conn:
                # 기본 정보만 업데이트 (호환성)
                conn.execute('''
                    INSERT OR REPLACE INTO etf_info 
                    (code, name, category, asset_class, region, expense_ratio, 
                     total_assets, avg_volume, fund_company, last_updated,
                     dividend_yield, data_quality, data_source, quality_score, last_real_update)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    etf_code,
                    real_data.get('name', etf_code),
                    real_data.get('category', 'ETF'),
                    real_data.get('asset_class', 'equity'),
                    real_data.get('region', 'domestic'),
                    real_data.get('expense_ratio', 0),
                    real_data.get('aum', 0),
                    real_data.get('volume', 0),
                    real_data.get('fund_manager', ''),
                    datetime.now().isoformat(),
                    real_data.get('dividend_yield', 0),
                    real_data.get('data_quality', 'unknown'),
                    real_data.get('data_source', 'unknown'),
                    real_data.get('quality_score', 0),
                    datetime.now().isoformat() if real_data.get('data_quality') in ['excellent', 'good'] else None
                ))
            
            logger.info(f"ETF {etf_code} 실제 데이터 업데이트 완료 - 품질: {real_data.get('data_quality')}")
            return True
            
        except Exception as e:
            logger.error(f"ETF {etf_code} 실제 데이터 업데이트 실패: {e}")
            return False
    
    def get_real_data_statistics(self) -> Dict:
        """실제 데이터 수집 통계 조회"""
        try:
            with sqlite3.connect(self.unified_db) as conn:
                stats = pd.read_sql_query('''
                    SELECT 
                        COUNT(*) as total_etfs,
                        COUNT(CASE WHEN data_source = 'real' OR data_source = 'naver_real' THEN 1 END) as real_data_etfs,
                        COUNT(CASE WHEN data_quality = 'excellent' THEN 1 END) as excellent_quality,
                        COUNT(CASE WHEN data_quality = 'good' THEN 1 END) as good_quality,
                        COUNT(CASE WHEN data_quality = 'fair' THEN 1 END) as fair_quality,
                        COUNT(CASE WHEN data_quality = 'poor' THEN 1 END) as poor_quality,
                        COUNT(CASE WHEN last_real_update IS NOT NULL THEN 1 END) as recent_real_updates,
                        AVG(quality_score) as avg_quality_score
                    FROM etf_info
                ''', conn).iloc[0]
                
                return {
                    'total_etfs': int(stats['total_etfs']),
                    'real_data_etfs': int(stats['real_data_etfs']),
                    'real_data_percentage': (stats['real_data_etfs'] / stats['total_etfs'] * 100) if stats['total_etfs'] > 0 else 0,
                    'quality_distribution': {
                        'excellent': int(stats['excellent_quality']),
                        'good': int(stats['good_quality']),
                        'fair': int(stats['fair_quality']),
                        'poor': int(stats['poor_quality'])
                    },
                    'recent_real_updates': int(stats['recent_real_updates']),
                    'avg_quality_score': float(stats['avg_quality_score']) if stats['avg_quality_score'] else 0
                }
                
        except Exception as e:
            logger.error(f"실제 데이터 통계 조회 실패: {e}")
            return {}
    
    def cleanup_poor_quality_data(self, days_old: int = 7):
        """품질이 낮은 오래된 데이터 정리"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_old)).isoformat()
            
            with sqlite3.connect(self.unified_db) as conn:
                # 품질이 나쁘고 오래된 데이터 삭제
                result = conn.execute('''
                    DELETE FROM data_quality_log 
                    WHERE data_quality = 'poor' AND collection_time < ?
                ''', (cutoff_date,))
                
                deleted_count = result.rowcount
                conn.commit()
                
                logger.info(f"품질이 낮은 오래된 데이터 {deleted_count}개 정리 완료")
                return deleted_count
                
        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")
            return 0
    
    # ==========================================
    # 기존 메서드들 (호환성 유지, 실제 데이터 지원 강화)
    # ==========================================
    
    # 포트폴리오 관련 메서드들
    def create_portfolio(self, name: str, strategy_type: str, 
                        target_allocation: Dict[str, float], 
                        risk_level: str, use_real_data: bool = True) -> int:
        """새 포트폴리오 생성 (실제 데이터 사용 옵션 추가)"""
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                cursor = conn.execute('''
                    INSERT INTO portfolios 
                    (name, strategy_type, target_allocation, risk_level, created_date, use_real_data)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (name, strategy_type, json.dumps(target_allocation), 
                     risk_level, datetime.now().date(), use_real_data))
                
                portfolio_id = cursor.lastrowid
                
                # 포트폴리오 구성 종목 추가
                for etf_code, weight in target_allocation.items():
                    conn.execute('''
                        INSERT INTO portfolio_holdings 
                        (portfolio_id, etf_code, target_weight, data_source)
                        VALUES (?, ?, ?, ?)
                    ''', (portfolio_id, etf_code, weight, 'real' if use_real_data else 'dummy'))
                
                logger.info(f"포트폴리오 '{name}' 생성 완료 (ID: {portfolio_id}, 실제데이터: {use_real_data})")
                return portfolio_id
                
        except sqlite3.IntegrityError:
            logger.error(f"포트폴리오 이름 '{name}'이 이미 존재합니다")
            raise ValueError(f"포트폴리오 이름 '{name}'이 이미 존재합니다")
        except Exception as e:
            logger.error(f"포트폴리오 생성 실패: {e}")
            raise
    
    def get_portfolio_info(self, portfolio_id: int) -> Optional[Dict]:
        """포트폴리오 정보 조회 (실제 데이터 사용 여부 포함)"""
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute('''
                    SELECT * FROM portfolios WHERE id = ? AND active = 1
                ''', (portfolio_id,))
                
                row = cursor.fetchone()
                if row:
                    portfolio = dict(row)
                    portfolio['target_allocation'] = json.loads(portfolio['target_allocation'])
                    return portfolio
                return None
                
        except Exception as e:
            logger.error(f"포트폴리오 정보 조회 실패: {e}")
            return None
    
    def get_portfolio_holdings(self, portfolio_id: int) -> pd.DataFrame:
        """포트폴리오 보유 종목 조회 (실제 데이터 품질 정보 포함)"""
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                # 통합 DB에서 ETF 정보 가져오기
                df = pd.read_sql_query('''
                    SELECT 
                        h.*,
                        COALESCE(u.name, e.name) as etf_name,
                        COALESCE(u.category, e.category) as category,
                        COALESCE(u.expense_ratio, e.expense_ratio) as expense_ratio,
                        COALESCE(u.dividend_yield, e.dividend_yield, 0) as dividend_yield,
                        COALESCE(u.data_quality, e.data_quality, 'unknown') as etf_data_quality,
                        COALESCE(u.data_source, e.data_source, 'unknown') as etf_data_source,
                        COALESCE(u.market_price, 0) as current_market_price
                    FROM portfolio_holdings h
                    LEFT JOIN etf_info e ON h.etf_code = e.code
                    LEFT JOIN (SELECT * FROM etf_info) u ON h.etf_code = u.code
                    WHERE h.portfolio_id = ?
                    ORDER BY h.target_weight DESC
                ''', conn, params=(portfolio_id,))
                
                return df
                
        except Exception as e:
            logger.error(f"포트폴리오 보유종목 조회 실패: {e}")
            return pd.DataFrame()
    
    def add_transaction(self, portfolio_id: int, etf_code: str, 
                       transaction_type: str, shares: int, price: float,
                       fee: float = 0, note: str = None, 
                       real_time_price: bool = False) -> int:
        """거래 내역 추가 (실시간 가격 여부 추가)"""
        try:
            total_amount = shares * price + fee
            
            with sqlite3.connect(self.portfolio_db) as conn:
                cursor = conn.execute('''
                    INSERT INTO transactions 
                    (portfolio_id, etf_code, transaction_type, shares, price, 
                     transaction_date, fee, total_amount, note, data_source, real_time_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (portfolio_id, etf_code, transaction_type, shares, price,
                     datetime.now().date(), fee, total_amount, note, 
                     'real' if real_time_price else 'manual', real_time_price))
                
                transaction_id = cursor.lastrowid
                
                # 포트폴리오 보유량 업데이트
                self._update_portfolio_holdings(portfolio_id, etf_code, 
                                               shares, price, transaction_type)
                
                logger.info(f"거래 내역 추가 완료 (ID: {transaction_id}, 실시간: {real_time_price})")
                return transaction_id
                
        except Exception as e:
            logger.error(f"거래 내역 추가 실패: {e}")
            raise
    
    def _update_portfolio_holdings(self, portfolio_id: int, etf_code: str,
                                  shares: int, price: float, transaction_type: str):
        """포트폴리오 보유량 업데이트 (데이터 품질 정보 포함)"""
        with sqlite3.connect(self.portfolio_db) as conn:
            # 현재 보유량 조회
            cursor = conn.execute('''
                SELECT shares, avg_price FROM portfolio_holdings
                WHERE portfolio_id = ? AND etf_code = ?
            ''', (portfolio_id, etf_code))
            
            result = cursor.fetchone()
            
            if result:
                current_shares, current_avg_price = result
                
                if transaction_type == 'BUY':
                    new_shares = current_shares + shares
                    new_avg_price = ((current_shares * current_avg_price) + 
                                   (shares * price)) / new_shares if new_shares > 0 else 0
                elif transaction_type == 'SELL':
                    new_shares = current_shares - shares
                    new_avg_price = current_avg_price  # 평균 단가는 그대로
                else:  # DIVIDEND
                    new_shares = current_shares
                    new_avg_price = current_avg_price
                
                # 업데이트 (현재가와 데이터 품질 정보 포함)
                conn.execute('''
                    UPDATE portfolio_holdings 
                    SET shares = ?, avg_price = ?, current_price = ?, 
                        last_updated = CURRENT_TIMESTAMP, price_data_source = ?
                    WHERE portfolio_id = ? AND etf_code = ?
                ''', (new_shares, new_avg_price, price, 'real', portfolio_id, etf_code))
    
    # ETF 관련 메서드들 (실제 데이터 지원)
    def add_etf_info(self, etf_info: Dict) -> bool:
        """ETF 기본 정보 추가/업데이트 (실제 데이터 지원)"""
        try:
            # ETF DB 업데이트
            with sqlite3.connect(self.etf_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO etf_info 
                    (code, name, category, subcategory, asset_class, region, 
                     currency, expense_ratio, inception_date, total_assets, 
                     avg_volume, tracking_index, fund_company, dividend_yield,
                     data_quality, data_source, quality_score, last_real_update)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    etf_info['code'], etf_info['name'], etf_info['category'],
                    etf_info.get('subcategory'), etf_info['asset_class'],
                    etf_info['region'], etf_info.get('currency', 'KRW'),
                    etf_info.get('expense_ratio'), etf_info.get('inception_date'),
                    etf_info.get('total_assets'), etf_info.get('avg_volume'),
                    etf_info.get('tracking_index'), etf_info.get('fund_company'),
                    etf_info.get('dividend_yield', 0),
                    etf_info.get('data_quality', 'unknown'),
                    etf_info.get('data_source', 'unknown'),
                    etf_info.get('quality_score', 0),
                    etf_info.get('last_real_update')
                ))
            
            # 통합 DB도 동기화
            if self.unified_db.exists():
                self.update_etf_with_real_data(etf_info['code'], etf_info)
                
            logger.info(f"ETF 정보 업데이트: {etf_info['code']} - 품질: {etf_info.get('data_quality', 'unknown')}")
            return True
                
        except Exception as e:
            logger.error(f"ETF 정보 추가 실패: {e}")
            return False
    
    def add_etf_price_data(self, etf_code: str, price_data: List[Dict], data_source: str = 'unknown') -> bool:
        """ETF 가격 데이터 추가 (데이터 소스 정보 포함)"""
        try:
            with sqlite3.connect(self.etf_db) as conn:
                for data in price_data:
                    conn.execute('''
                        INSERT OR REPLACE INTO etf_prices 
                        (etf_code, date, open_price, high_price, low_price, 
                         close_price, volume, nav, premium_discount, data_source, data_quality)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        etf_code, data['date'], data.get('open_price'),
                        data.get('high_price'), data.get('low_price'),
                        data['close_price'], data.get('volume', 0),
                        data.get('nav'), data.get('premium_discount'),
                        data_source, data.get('data_quality', 'unknown')
                    ))
                
                logger.info(f"ETF 가격 데이터 업데이트: {etf_code} ({len(price_data)}건, 소스: {data_source})")
                return True
                
        except Exception as e:
            logger.error(f"ETF 가격 데이터 추가 실패: {e}")
            return False
    
    def get_etf_price_data(self, etf_code: str, 
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """ETF 가격 데이터 조회 (데이터 소스 정보 포함)"""
        try:
            with sqlite3.connect(self.etf_db) as conn:
                query = '''
                    SELECT * FROM etf_prices 
                    WHERE etf_code = ?
                '''
                params = [etf_code]
                
                if start_date:
                    query += ' AND date >= ?'
                    params.append(start_date)
                
                if end_date:
                    query += ' AND date <= ?'
                    params.append(end_date)
                
                query += ' ORDER BY date'
                
                df = pd.read_sql_query(query, conn, params=params)
                df['date'] = pd.to_datetime(df['date'])
                
                return df
                
        except Exception as e:
            logger.error(f"ETF 가격 데이터 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_etf_list(self, category: Optional[str] = None, 
                    asset_class: Optional[str] = None,
                    min_data_quality: Optional[str] = None) -> pd.DataFrame:
        """ETF 목록 조회 (데이터 품질 필터 추가)"""
        try:
            with sqlite3.connect(self.etf_db) as conn:
                query = 'SELECT * FROM etf_info WHERE 1=1'
                params = []
                
                if category:
                    query += ' AND category = ?'
                    params.append(category)
                
                if asset_class:
                    query += ' AND asset_class = ?'
                    params.append(asset_class)
                
                if min_data_quality:
                    quality_order = {'poor': 1, 'fair': 2, 'good': 3, 'excellent': 4}
                    if min_data_quality in quality_order:
                        query += ' AND quality_score >= ?'
                        params.append(quality_order[min_data_quality] * 20)  # 점수 변환
                
                query += ' ORDER BY quality_score DESC, total_assets DESC'
                
                df = pd.read_sql_query(query, conn, params=params)
                return df
                
        except Exception as e:
            logger.error(f"ETF 목록 조회 실패: {e}")
            return pd.DataFrame()
    
    def update_portfolio_performance(self, portfolio_id: int, date: str,
                                   total_value: float, total_investment: float,
                                   daily_return: float = 0, benchmark_return: float = 0,
                                   data_quality: str = 'unknown'):
        """포트폴리오 성과 업데이트 (데이터 품질 정보 포함)"""
        try:
            cumulative_return = (total_value - total_investment) / total_investment * 100
            
            with sqlite3.connect(self.portfolio_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO performance_history
                    (portfolio_id, date, total_value, total_investment, 
                     daily_return, cumulative_return, benchmark_return, data_quality)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (portfolio_id, date, total_value, total_investment,
                     daily_return, cumulative_return, benchmark_return, data_quality))
                
        except Exception as e:
            logger.error(f"포트폴리오 성과 업데이트 실패: {e}")
    
    def get_portfolio_performance(self, portfolio_id: int, 
                                 start_date: Optional[str] = None) -> pd.DataFrame:
        """포트폴리오 성과 이력 조회 (데이터 품질 정보 포함)"""
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                query = '''
                    SELECT * FROM performance_history 
                    WHERE portfolio_id = ?
                '''
                params = [portfolio_id]
                
                if start_date:
                    query += ' AND date >= ?'
                    params.append(start_date)
                
                query += ' ORDER BY date'
                
                df = pd.read_sql_query(query, conn, params=params)
                df['date'] = pd.to_datetime(df['date'])
                
                return df
                
        except Exception as e:
            logger.error(f"포트폴리오 성과 조회 실패: {e}")
            return pd.DataFrame()
    
    def backup_database(self, backup_path: str) -> bool:
        """데이터베이스 백업 (모든 DB 파일 포함)"""
        try:
            import shutil
            
            backup_dir = Path(backup_path)
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 포트폴리오 DB 백업
            if self.portfolio_db.exists():
                portfolio_backup = backup_dir / f"portfolio_data_{timestamp}.db"
                shutil.copy2(self.portfolio_db, portfolio_backup)
                logger.info(f"포트폴리오 DB 백업: {portfolio_backup}")
            
            # ETF DB 백업
            if self.etf_db.exists():
                etf_backup = backup_dir / f"etf_data_{timestamp}.db"
                shutil.copy2(self.etf_db, etf_backup)
                logger.info(f"ETF DB 백업: {etf_backup}")
            
            # 통합 ETF DB 백업
            if self.unified_db.exists():
                unified_backup = backup_dir / f"etf_universe_{timestamp}.db"
                shutil.copy2(self.unified_db, unified_backup)
                logger.info(f"통합 ETF DB 백업: {unified_backup}")
            
            logger.info(f"데이터베이스 백업 완료: {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"데이터베이스 백업 실패: {e}")
            return False
    
    def cleanup_old_data(self, days_to_keep: int = 365):
        """오래된 데이터 정리 (데이터 품질 고려)"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).date()
            
            # ETF DB 정리
            with sqlite3.connect(self.etf_db) as conn:
                # 오래된 가격 데이터 삭제 (품질 낮은 것 우선)
                conn.execute('''
                    DELETE FROM etf_prices 
                    WHERE date < ? AND (data_quality = 'poor' OR data_quality = 'unknown')
                ''', (cutoff_date,))
                
                # 오래된 성과 데이터 삭제
                conn.execute('''
                    DELETE FROM etf_performance WHERE date < ?
                ''', (cutoff_date,))
            
            # 포트폴리오 DB 정리
            with sqlite3.connect(self.portfolio_db) as conn:
                # 오래된 성과 이력 삭제 (품질 낮은 것 우선)
                conn.execute('''
                    DELETE FROM performance_history 
                    WHERE date < ? AND (data_quality = 'poor' OR data_quality = 'unknown')
                ''', (cutoff_date,))
            
            # 통합 DB 정리
            if self.unified_db.exists():
                self.cleanup_poor_quality_data(days_to_keep)
            
            logger.info(f"오래된 데이터 정리 완료 ({cutoff_date} 이전)")
            
        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")

# ==========================================
# 사용 예제
# ==========================================

if __name__ == "__main__":
    print("🗄️ 실제 데이터 지원 DatabaseManager 테스트")
    print("=" * 50)
    
    # 데이터베이스 관리자 초기화
    db_manager = DatabaseManager()
    
    # 실제 데이터 통계 확인
    stats = db_manager.get_real_data_statistics()
    if stats:
        print(f"📊 실제 데이터 통계:")
        print(f"- 총 ETF: {stats['total_etfs']}개")
        print(f"- 실제 데이터: {stats['real_data_etfs']}개 ({stats['real_data_percentage']:.1f}%)")
        print(f"- 품질 분포: {stats['quality_distribution']}")
        print(f"- 평균 품질 점수: {stats['avg_quality_score']:.1f}")
    
    # 테스트 포트폴리오 생성
    try:
        portfolio_id = db_manager.create_portfolio(
            name="테스트 실제데이터 포트폴리오",
            strategy_type="balanced",
            target_allocation={
                "069500": 0.4,  # KODEX 200
                "360750": 0.3,  # TIGER 미국S&P500
                "114260": 0.3   # KODEX 국고채10년
            },
            risk_level="medium",
            use_real_data=True
        )
        print(f"✅ 테스트 포트폴리오 생성 완료 (ID: {portfolio_id})")
        
        # 포트폴리오 보유종목 조회 (실제 데이터 품질 정보 포함)
        holdings = db_manager.get_portfolio_holdings(portfolio_id)
        if not holdings.empty:
            print(f"📊 포트폴리오 보유종목:")
            for _, holding in holdings.iterrows():
                print(f"- {holding['etf_code']}: {holding['target_weight']*100:.1f}% "
                      f"(품질: {holding.get('etf_data_quality', 'unknown')}, "
                      f"소스: {holding.get('etf_data_source', 'unknown')})")
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
    
    print(f"\n✅ DatabaseManager 실제 데이터 지원 테스트 완료!")