"""
데이터베이스 관리 모듈
포트폴리오와 ETF 데이터를 SQLite로 관리하는 핵심 시스템
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import json

logger = logging.getLogger(__name__)

class DatabaseManager:
    """데이터베이스 관리 핵심 클래스"""
    
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
        
        self._init_databases()
    
    def _init_databases(self):
        """데이터베이스 초기화 및 테이블 생성"""
        try:
            self._create_portfolio_tables()
            self._create_etf_tables()
            logger.info("데이터베이스 초기화 완료")
        except Exception as e:
            logger.error(f"데이터베이스 초기화 실패: {e}")
            raise
    
    def _create_portfolio_tables(self):
        """포트폴리오 관련 테이블 생성"""
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
                    active BOOLEAN DEFAULT 1
                )
            ''')
            
            # 포트폴리오 구성 종목
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
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
                )
            ''')
            
            # 거래 내역
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
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id)
                )
            ''')
            
            # 성과 추적
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
                    FOREIGN KEY (portfolio_id) REFERENCES portfolios (id),
                    UNIQUE(portfolio_id, date)
                )
            ''')
    
    def _create_etf_tables(self):
        """ETF 관련 테이블 생성"""
        with sqlite3.connect(self.etf_db) as conn:
            # ETF 기본 정보
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
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # ETF 가격 데이터
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
                    FOREIGN KEY (etf_code) REFERENCES etf_info (code)
                )
            ''')
            
            # ETF 성과 지표
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
                    FOREIGN KEY (etf_code) REFERENCES etf_info (code),
                    UNIQUE(etf_code, date)
                )
            ''')
    
    # 포트폴리오 관련 메서드들
    def create_portfolio(self, name: str, strategy_type: str, 
                        target_allocation: Dict[str, float], 
                        risk_level: str) -> int:
        """새 포트폴리오 생성"""
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                cursor = conn.execute('''
                    INSERT INTO portfolios 
                    (name, strategy_type, target_allocation, risk_level, created_date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (name, strategy_type, json.dumps(target_allocation), 
                     risk_level, datetime.now().date()))
                
                portfolio_id = cursor.lastrowid
                
                # 포트폴리오 구성 종목 추가
                for etf_code, weight in target_allocation.items():
                    conn.execute('''
                        INSERT INTO portfolio_holdings 
                        (portfolio_id, etf_code, target_weight)
                        VALUES (?, ?, ?)
                    ''', (portfolio_id, etf_code, weight))
                
                logger.info(f"포트폴리오 '{name}' 생성 완료 (ID: {portfolio_id})")
                return portfolio_id
                
        except sqlite3.IntegrityError:
            logger.error(f"포트폴리오 이름 '{name}'이 이미 존재합니다")
            raise ValueError(f"포트폴리오 이름 '{name}'이 이미 존재합니다")
        except Exception as e:
            logger.error(f"포트폴리오 생성 실패: {e}")
            raise
    
    def get_portfolio_info(self, portfolio_id: int) -> Optional[Dict]:
        """포트폴리오 정보 조회"""
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
        """포트폴리오 보유 종목 조회"""
        try:
            with sqlite3.connect(self.portfolio_db) as conn:
                df = pd.read_sql_query('''
                    SELECT h.*, e.name as etf_name, e.category, e.expense_ratio
                    FROM portfolio_holdings h
                    LEFT JOIN etf_info e ON h.etf_code = e.code
                    WHERE h.portfolio_id = ?
                    ORDER BY h.target_weight DESC
                ''', conn, params=(portfolio_id,))
                
                return df
                
        except Exception as e:
            logger.error(f"포트폴리오 보유종목 조회 실패: {e}")
            return pd.DataFrame()
    
    def add_transaction(self, portfolio_id: int, etf_code: str, 
                       transaction_type: str, shares: int, price: float,
                       fee: float = 0, note: str = None) -> int:
        """거래 내역 추가"""
        try:
            total_amount = shares * price + fee
            
            with sqlite3.connect(self.portfolio_db) as conn:
                cursor = conn.execute('''
                    INSERT INTO transactions 
                    (portfolio_id, etf_code, transaction_type, shares, price, 
                     transaction_date, fee, total_amount, note)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (portfolio_id, etf_code, transaction_type, shares, price,
                     datetime.now().date(), fee, total_amount, note))
                
                transaction_id = cursor.lastrowid
                
                # 포트폴리오 보유량 업데이트
                self._update_portfolio_holdings(portfolio_id, etf_code, 
                                               shares, price, transaction_type)
                
                logger.info(f"거래 내역 추가 완료 (ID: {transaction_id})")
                return transaction_id
                
        except Exception as e:
            logger.error(f"거래 내역 추가 실패: {e}")
            raise
    
    def _update_portfolio_holdings(self, portfolio_id: int, etf_code: str,
                                  shares: int, price: float, transaction_type: str):
        """포트폴리오 보유량 업데이트"""
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
                
                # 업데이트
                conn.execute('''
                    UPDATE portfolio_holdings 
                    SET shares = ?, avg_price = ?, last_updated = CURRENT_TIMESTAMP
                    WHERE portfolio_id = ? AND etf_code = ?
                ''', (new_shares, new_avg_price, portfolio_id, etf_code))
    
    # ETF 관련 메서드들
    def add_etf_info(self, etf_info: Dict) -> bool:
        """ETF 기본 정보 추가/업데이트"""
        try:
            with sqlite3.connect(self.etf_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO etf_info 
                    (code, name, category, subcategory, asset_class, region, 
                     currency, expense_ratio, inception_date, total_assets, 
                     avg_volume, tracking_index, fund_company)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    etf_info['code'], etf_info['name'], etf_info['category'],
                    etf_info.get('subcategory'), etf_info['asset_class'],
                    etf_info['region'], etf_info.get('currency', 'KRW'),
                    etf_info.get('expense_ratio'), etf_info.get('inception_date'),
                    etf_info.get('total_assets'), etf_info.get('avg_volume'),
                    etf_info.get('tracking_index'), etf_info.get('fund_company')
                ))
                
                logger.info(f"ETF 정보 업데이트: {etf_info['code']}")
                return True
                
        except Exception as e:
            logger.error(f"ETF 정보 추가 실패: {e}")
            return False
    
    def add_etf_price_data(self, etf_code: str, price_data: List[Dict]) -> bool:
        """ETF 가격 데이터 추가"""
        try:
            with sqlite3.connect(self.etf_db) as conn:
                for data in price_data:
                    conn.execute('''
                        INSERT OR REPLACE INTO etf_prices 
                        (etf_code, date, open_price, high_price, low_price, 
                         close_price, volume, nav, premium_discount)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        etf_code, data['date'], data.get('open_price'),
                        data.get('high_price'), data.get('low_price'),
                        data['close_price'], data.get('volume', 0),
                        data.get('nav'), data.get('premium_discount')
                    ))
                
                logger.info(f"ETF 가격 데이터 업데이트: {etf_code} ({len(price_data)}건)")
                return True
                
        except Exception as e:
            logger.error(f"ETF 가격 데이터 추가 실패: {e}")
            return False
    
    def get_etf_price_data(self, etf_code: str, 
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> pd.DataFrame:
        """ETF 가격 데이터 조회"""
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
                    asset_class: Optional[str] = None) -> pd.DataFrame:
        """ETF 목록 조회"""
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
                
                query += ' ORDER BY total_assets DESC'
                
                df = pd.read_sql_query(query, conn, params=params)
                return df
                
        except Exception as e:
            logger.error(f"ETF 목록 조회 실패: {e}")
            return pd.DataFrame()
    
    def update_portfolio_performance(self, portfolio_id: int, date: str,
                                   total_value: float, total_investment: float,
                                   daily_return: float = 0, benchmark_return: float = 0):
        """포트폴리오 성과 업데이트"""
        try:
            cumulative_return = (total_value - total_investment) / total_investment * 100
            
            with sqlite3.connect(self.portfolio_db) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO performance_history
                    (portfolio_id, date, total_value, total_investment, 
                     daily_return, cumulative_return, benchmark_return)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (portfolio_id, date, total_value, total_investment,
                     daily_return, cumulative_return, benchmark_return))
                
        except Exception as e:
            logger.error(f"포트폴리오 성과 업데이트 실패: {e}")
    
    def get_portfolio_performance(self, portfolio_id: int, 
                                 start_date: Optional[str] = None) -> pd.DataFrame:
        """포트폴리오 성과 이력 조회"""
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
        """데이터베이스 백업"""
        try:
            import shutil
            
            backup_dir = Path(backup_path)
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 포트폴리오 DB 백업
            portfolio_backup = backup_dir / f"portfolio_data_{timestamp}.db"
            shutil.copy2(self.portfolio_db, portfolio_backup)
            
            # ETF DB 백업
            etf_backup = backup_dir / f"etf_data_{timestamp}.db"
            shutil.copy2(self.etf_db, etf_backup)
            
            logger.info(f"데이터베이스 백업 완료: {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"데이터베이스 백업 실패: {e}")
            return False
    
    def cleanup_old_data(self, days_to_keep: int = 365):
        """오래된 데이터 정리"""
        try:
            cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).date()
            
            with sqlite3.connect(self.etf_db) as conn:
                # 오래된 가격 데이터 삭제
                conn.execute('''
                    DELETE FROM etf_prices WHERE date < ?
                ''', (cutoff_date,))
                
                # 오래된 성과 데이터 삭제
                conn.execute('''
                    DELETE FROM etf_performance WHERE date < ?
                ''', (cutoff_date,))
            
            with sqlite3.connect(self.portfolio_db) as conn:
                # 오래된 성과 이력 삭제
                conn.execute('''
                    DELETE FROM performance_history WHERE date < ?
                ''', (cutoff_date,))
            
            logger.info(f"오래된 데이터 정리 완료 ({cutoff_date} 이전)")
            
        except Exception as e:
            logger.error(f"데이터 정리 실패: {e}")