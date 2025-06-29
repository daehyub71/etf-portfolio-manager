"""
Utils 모듈 - ETF 포트폴리오 관리 시스템의 유틸리티

이 모듈은 비용 계산, 성과 지표, 데이터 검증, 이메일 발송 등
시스템 전반에서 사용되는 유틸리티 함수와 클래스들을 제공합니다.
"""

from .cost_calculator import CostCalculator
from .performance_metrics import PerformanceMetrics
from .data_validator import DataValidator
from .email_sender import EmailSender

__version__ = "1.0.0"
__author__ = "ETF Portfolio Manager Team"

# 유틸리티 클래스들을 모듈 레벨에서 직접 접근 가능하도록 export
__all__ = [
    'CostCalculator',
    'PerformanceMetrics', 
    'DataValidator',
    'EmailSender',
    # 헬퍼 함수들
    'format_currency',
    'format_percentage',
    'calculate_compound_return',
    'annualize_return',
    'validate_allocation',
    'safe_divide'
]

# 모듈 메타데이터
MODULE_INFO = {
    'name': 'ETF Portfolio Manager Utils',
    'description': '공통 유틸리티 및 헬퍼 함수 모듈',
    'version': __version__,
    'components': {
        'cost_calculator': 'ETF 투자 비용 계산기',
        'performance_metrics': '포트폴리오 성과 지표 계산',
        'data_validator': '데이터 검증 및 정리',
        'email_sender': '이메일 알림 발송'
    }
}

# 상수 정의
CONSTANTS = {
    'TRADING_DAYS_PER_YEAR': 252,
    'WEEKS_PER_YEAR': 52,
    'MONTHS_PER_YEAR': 12,
    'BUSINESS_DAYS_PER_WEEK': 5,
    'DEFAULT_RISK_FREE_RATE': 0.02,
    'KRW_CURRENCY_CODE': 'KRW',
    'USD_CURRENCY_CODE': 'USD'
}

def get_constants():
    """시스템 상수 반환"""
    return CONSTANTS.copy()

# 공통 헬퍼 함수들
def format_currency(amount: float, currency: str = 'KRW') -> str:
    """통화 형식으로 포맷팅"""
    if currency == 'KRW':
        if amount >= 100000000:  # 1억 이상
            return f"{amount/100000000:.1f}억원"
        elif amount >= 10000:  # 1만 이상
            return f"{amount/10000:.0f}만원"
        else:
            return f"{amount:,.0f}원"
    elif currency == 'USD':
        return f"${amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency}"

def format_percentage(value: float, decimal_places: int = 2) -> str:
    """퍼센트 형식으로 포맷팅"""
    return f"{value:.{decimal_places}f}%"

def calculate_compound_return(returns: list) -> float:
    """복리 수익률 계산"""
    if not returns:
        return 0.0
    
    compound = 1.0
    for ret in returns:
        compound *= (1 + ret)
    
    return compound - 1.0

def annualize_return(total_return: float, days: int) -> float:
    """연환산 수익률 계산"""
    if days <= 0:
        return 0.0
    
    years = days / 365.25
    if years <= 0:
        return 0.0
    
    return (1 + total_return) ** (1/years) - 1

def validate_allocation(allocation: dict) -> tuple:
    """자산배분 유효성 검증
    
    Returns:
        (is_valid: bool, error_message: str)
    """
    if not allocation:
        return False, "자산배분이 비어있습니다"
    
    # 비중 합계 확인
    total_weight = sum(allocation.values())
    if abs(total_weight - 100) > 0.1:
        return False, f"비중 합계가 100%가 아닙니다 (현재: {total_weight:.1f}%)"
    
    # 개별 비중 확인
    for etf_code, weight in allocation.items():
        if weight < 0:
            return False, f"{etf_code}의 비중이 음수입니다"
        if weight > 100:
            return False, f"{etf_code}의 비중이 100%를 초과합니다"
    
    return True, ""

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """안전한 나눗셈 (0으로 나누기 방지)"""
    if denominator == 0:
        return default
    return numerator / denominator

def calculate_correlation(series1: list, series2: list) -> float:
    """두 시계열의 상관계수 계산"""
    try:
        import numpy as np
        return np.corrcoef(series1, series2)[0, 1]
    except:
        return 0.0

def moving_average(data: list, window: int) -> list:
    """이동평균 계산"""
    if len(data) < window:
        return data
    
    result = []
    for i in range(window - 1, len(data)):
        avg = sum(data[i - window + 1:i + 1]) / window
        result.append(avg)
    
    return result

def calculate_drawdown(prices: list) -> list:
    """드로우다운 계산"""
    if not prices:
        return []
    
    drawdowns = []
    peak = prices[0]
    
    for price in prices:
        if price > peak:
            peak = price
        
        drawdown = (price - peak) / peak if peak > 0 else 0
        drawdowns.append(drawdown)
    
    return drawdowns

def standardize_etf_code(etf_code: str) -> str:
    """ETF 코드 표준화"""
    if not etf_code:
        return ""
    
    # 공백 제거 및 대문자 변환
    code = etf_code.strip().upper()
    
    # 한국 ETF 코드는 6자리 숫자
    if code.isdigit() and len(code) == 6:
        return code
    
    return etf_code  # 원본 반환

def parse_korean_number(text: str) -> float:
    """한국어 숫자 표현 파싱 (예: "1.5억", "300만")"""
    if not text:
        return 0.0
    
    text = text.strip().replace(',', '').replace(' ', '')
    
    try:
        # 억 단위
        if '억' in text:
            value = float(text.replace('억', '').replace('원', ''))
            return value * 100000000
        
        # 만 단위
        elif '만' in text:
            value = float(text.replace('만', '').replace('원', ''))
            return value * 10000
        
        # 천 단위
        elif '천' in text:
            value = float(text.replace('천', '').replace('원', ''))
            return value * 1000
        
        # 일반 숫자
        else:
            return float(text.replace('원', ''))
            
    except ValueError:
        return 0.0

def format_duration(days: int) -> str:
    """기간을 읽기 쉬운 형태로 포맷팅"""
    if days < 0:
        return "잘못된 기간"
    elif days == 0:
        return "오늘"
    elif days == 1:
        return "1일"
    elif days < 7:
        return f"{days}일"
    elif days < 30:
        weeks = days // 7
        remaining_days = days % 7
        if remaining_days == 0:
            return f"{weeks}주"
        else:
            return f"{weeks}주 {remaining_days}일"
    elif days < 365:
        months = days // 30
        remaining_days = days % 30
        if remaining_days == 0:
            return f"{months}개월"
        else:
            return f"{months}개월 {remaining_days}일"
    else:
        years = days // 365
        remaining_days = days % 365
        if remaining_days == 0:
            return f"{years}년"
        else:
            return f"{years}년 {remaining_days}일"

def calculate_tax_efficiency_score(gross_return: float, net_return: float) -> float:
    """세금 효율성 점수 계산 (0-100)"""
    if gross_return <= 0:
        return 100.0  # 손실시 세금 없으므로 효율성 최대
    
    tax_efficiency = net_return / gross_return
    return min(100.0, max(0.0, tax_efficiency * 100))

# 로깅 유틸리티
import logging

def setup_utils_logging(level=logging.INFO):
    """유틸리티 모듈 로깅 설정"""
    logger = logging.getLogger('utils')
    
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
logger = setup_utils_logging()

def utils_health_check():
    """유틸리티 모듈 상태 확인"""
    health_status = {
        'numpy_available': False,
        'pandas_available': False,
        'scipy_available': False,
        'email_dependencies': False
    }
    
    try:
        import numpy
        health_status['numpy_available'] = True
    except ImportError:
        pass
    
    try:
        import pandas
        health_status['pandas_available'] = True
    except ImportError:
        pass
    
    try:
        import scipy
        health_status['scipy_available'] = True
    except ImportError:
        pass
    
    try:
        import smtplib
        import email
        health_status['email_dependencies'] = True
    except ImportError:
        pass
    
    return health_status

# 버전 정보
def get_version():
    """현재 버전 반환"""
    return __version__

def get_module_info():
    """모듈 정보 반환"""
    return MODULE_INFO.copy()

# 초기화 로그
logger.info(f"Utils 모듈 로드 완료 (버전: {__version__})")