"""
Data 모듈 - ETF 포트폴리오 관리 시스템의 데이터 관리

이 모듈은 ETF 유니버스, 시장 데이터 수집, 데이터베이스 관리,
포트폴리오 추적 등의 데이터 관련 기능을 제공합니다.
"""

from .database_manager import DatabaseManager
from .etf_universe import ETFUniverse
from .market_data_collector import MarketDataCollector
from .portfolio_tracker import PortfolioTracker

__version__ = "1.0.0"
__author__ = "ETF Portfolio Manager Team"

# 데이터 관련 클래스들을 모듈 레벨에서 직접 접근 가능하도록 export
__all__ = [
    'DatabaseManager',
    'ETFUniverse', 
    'MarketDataCollector',
    'PortfolioTracker'
]

# 모듈 메타데이터
MODULE_INFO = {
    'name': 'ETF Portfolio Manager Data',
    'description': '데이터 관리 및 처리 모듈',
    'version': __version__,
    'components': {
        'database_manager': 'SQLite 기반 데이터베이스 관리',
        'etf_universe': '한국 ETF 유니버스 관리',
        'market_data_collector': '실시간 시장 데이터 수집',
        'portfolio_tracker': '포트폴리오 실시간 추적'
    }
}

# 데이터 소스 설정
DATA_SOURCES = {
    'krx': {
        'name': '한국거래소',
        'base_url': 'http://data.krx.co.kr',
        'supported_data': ['etf_list', 'price_data', 'trading_volume']
    },
    'naver_finance': {
        'name': '네이버 금융',
        'base_url': 'https://finance.naver.com',
        'supported_data': ['price_data', 'chart_data']
    },
    'investing_com': {
        'name': 'Investing.com',
        'base_url': 'https://kr.investing.com',
        'supported_data': ['foreign_etf_data', 'economic_indicators']
    }
}

def get_supported_data_sources():
    """지원되는 데이터 소스 목록 반환"""
    return DATA_SOURCES.copy()

# ETF 카테고리 정의
ETF_CATEGORIES = {
    'domestic_equity': '국내 주식',
    'foreign_equity': '해외 주식', 
    'bonds': '채권',
    'alternatives': '대안투자',
    'thematic': '테마/섹터'
}

def get_etf_categories():
    """ETF 카테고리 목록 반환"""
    return ETF_CATEGORIES.copy()

# 데이터베이스 스키마 버전
DATABASE_SCHEMA_VERSION = "1.0.0"

def get_database_schema_version():
    """데이터베이스 스키마 버전 반환"""
    return DATABASE_SCHEMA_VERSION

# 데이터 수집 주기 설정
DATA_UPDATE_FREQUENCIES = {
    'real_time': '실시간 (거래시간 중)',
    'hourly': '1시간마다',
    'daily': '일일 (장 마감 후)',
    'weekly': '주간 (주말)',
    'monthly': '월간 (월말)'
}

def get_update_frequencies():
    """데이터 업데이트 주기 옵션 반환"""
    return DATA_UPDATE_FREQUENCIES.copy()

# 기본 설정
DEFAULT_DATA_CONFIG = {
    'database_path': 'data/',
    'backup_enabled': True,
    'backup_frequency': 'daily',
    'data_retention_days': 365,
    'cache_enabled': True,
    'cache_ttl_seconds': 3600,
    'max_concurrent_requests': 10,
    'request_timeout_seconds': 30
}

def get_default_data_config():
    """기본 데이터 설정 반환"""
    return DEFAULT_DATA_CONFIG.copy()

# 데이터 품질 검증 설정
DATA_QUALITY_CHECKS = {
    'price_data': {
        'min_trading_days': 30,
        'max_price_change': 0.5,  # 50% 초과 변동시 이상 데이터로 판단
        'required_fields': ['date', 'close_price', 'volume']
    },
    'etf_info': {
        'required_fields': ['code', 'name', 'category', 'expense_ratio'],
        'max_expense_ratio': 3.0  # 3% 초과시 검토 필요
    }
}

def get_data_quality_checks():
    """데이터 품질 검증 기준 반환"""
    return DATA_QUALITY_CHECKS.copy()

# 로깅 설정
import logging

def setup_data_logging(level=logging.INFO):
    """데이터 모듈 로깅 설정"""
    logger = logging.getLogger('data')
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(level)
    return logger

# 모듈 초기화
logger = setup_data_logging()

def initialize_data_module(config=None):
    """데이터 모듈 초기화"""
    if config is None:
        config = get_default_data_config()
    
    logger.info("데이터 모듈 초기화 시작")
    
    try:
        # 데이터 디렉토리 생성
        import os
        os.makedirs(config['database_path'], exist_ok=True)
        
        # ETF 유니버스 초기화
        etf_universe = ETFUniverse()
        logger.info(f"ETF 유니버스 로드 완료: {len(etf_universe.etf_data)}개 ETF")
        
        logger.info("데이터 모듈 초기화 완료")
        return True
        
    except Exception as e:
        logger.error(f"데이터 모듈 초기화 실패: {e}")
        return False

def data_module_health_check():
    """데이터 모듈 상태 확인"""
    health_status = {
        'module_loaded': True,
        'database_accessible': None,
        'etf_universe_loaded': None,
        'data_sources_available': None
    }
    
    try:
        # 기본 클래스 로드 확인
        db_manager = DatabaseManager()
        health_status['database_accessible'] = True
        
        etf_universe = ETFUniverse()
        health_status['etf_universe_loaded'] = len(etf_universe.etf_data) > 0
        
        # 데이터 소스 연결 확인 (간단한 체크)
        import requests
        try:
            response = requests.get('https://finance.naver.com', timeout=5)
            health_status['data_sources_available'] = response.status_code == 200
        except:
            health_status['data_sources_available'] = False
            
    except Exception as e:
        health_status['module_loaded'] = False
        health_status['error'] = str(e)
    
    return health_status

# 자동 초기화 (선택적)
def auto_initialize():
    """모듈 임포트시 자동 초기화"""
    try:
        return initialize_data_module()
    except:
        return False

# 유틸리티 함수들
def validate_etf_code(etf_code: str) -> bool:
    """ETF 코드 유효성 검증"""
    if not etf_code or not isinstance(etf_code, str):
        return False
    
    # 한국 ETF 코드는 6자리 숫자
    return etf_code.isdigit() and len(etf_code) == 6

def format_etf_name(etf_code: str, etf_name: str) -> str:
    """ETF 이름 표준 형식으로 포맷팅"""
    return f"{etf_code} - {etf_name}"

def parse_date_string(date_str: str):
    """날짜 문자열 파싱"""
    from datetime import datetime
    
    formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    raise ValueError(f"지원하지 않는 날짜 형식: {date_str}")