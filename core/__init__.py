"""
Core 모듈 - ETF 포트폴리오 관리 시스템의 핵심 엔진 (수정 버전)

이 모듈은 포트폴리오 관리, 업데이트 관리, 데이터 백업 등
시스템의 핵심 기능들을 제공합니다.
"""

import logging
from typing import Dict, Any

__version__ = "1.0.0"
__author__ = "ETF Portfolio Manager Team"

# 실제 존재하는 모듈들만 import (오류 처리 포함)
available_modules = {}

# PortfolioManager import 시도
try:
    from .portfolio_manager import PortfolioManager
    available_modules['PortfolioManager'] = PortfolioManager
    print("✅ PortfolioManager 로드 성공")
except ImportError as e:
    print(f"⚠️ PortfolioManager 로드 실패: {e}")

# ETFUpdateManager import 시도
try:
    from .update_manager import ETFUpdateManager
    available_modules['ETFUpdateManager'] = ETFUpdateManager
    print("✅ ETFUpdateManager 로드 성공")
except ImportError as e:
    print(f"⚠️ ETFUpdateManager 로드 실패: {e}")

# DataBackupManager import 시도 (올바른 클래스 이름 사용)
try:
    from .data_backup import DataBackupManager as DataBackup  # 별칭 사용
    available_modules['DataBackup'] = DataBackup
    print("✅ DataBackupManager 로드 성공")
except ImportError as e:
    print(f"⚠️ DataBackupManager 로드 실패: {e}")

# 기타 모듈들 (존재하는 경우에만 import)
optional_modules = [
    ('risk_manager', 'RiskManager'),
    ('backtesting_engine', 'BacktestingEngine'),
    ('report_generator', 'ReportGenerator'),
    ('notification_system', 'NotificationSystem'),
    ('scheduler', 'Scheduler'),
    ('tax_optimizer', 'TaxOptimizer'),
]

for module_name, class_name in optional_modules:
    try:
        module = __import__(f'core.{module_name}', fromlist=[class_name])
        cls = getattr(module, class_name)
        available_modules[class_name] = cls
        print(f"✅ {class_name} 로드 성공")
    except ImportError:
        print(f"⚠️ {class_name} 모듈 없음 (선택사항)")
    except AttributeError:
        print(f"⚠️ {class_name} 클래스 없음 (선택사항)")

# 사용 가능한 클래스들만 export
__all__ = list(available_modules.keys())

# 모듈 메타데이터
MODULE_INFO = {
    'name': 'ETF Portfolio Manager Core',
    'description': '직장인을 위한 장기 ETF 자산배분 관리 시스템 - 핵심 엔진',
    'version': __version__,
    'available_modules': list(available_modules.keys()),
    'components': {
        'portfolio_manager': '포트폴리오 관리 및 추적',
        'update_manager': '데이터 업데이트 관리',
        'data_backup': '데이터 백업 및 복구',
        'risk_manager': '리스크 분석 및 관리 (선택사항)',
        'backtesting_engine': '전략 백테스팅 및 성과 검증 (선택사항)',
        'report_generator': '자동 리포트 생성 (선택사항)',
        'notification_system': '실시간 알림 시스템 (선택사항)',
        'scheduler': '자동 작업 스케줄링 (선택사항)',
        'tax_optimizer': '세금 최적화 (선택사항)'
    }
}

def get_version():
    """현재 버전 반환"""
    return __version__

def get_module_info():
    """모듈 정보 반환"""
    return MODULE_INFO

def get_available_modules():
    """사용 가능한 모듈 목록 반환"""
    return available_modules

# 기본 설정값
DEFAULT_CONFIG = {
    'risk_free_rate': 0.02,          # 무위험 이자율 2%
    'trading_days_per_year': 252,    # 연간 거래일
    'rebalancing_threshold': 5.0,    # 리밸런싱 임계치 5%
    'max_position_size': 40.0,       # 최대 종목 비중 40%
    'min_position_size': 2.0,        # 최소 종목 비중 2%
    'backup_retention_days': 30,     # 백업 보관 기간 30일
    'notification_cooldown': 3600,   # 알림 쿨다운 1시간
}

def get_default_config():
    """기본 설정값 반환"""
    return DEFAULT_CONFIG.copy()

# 로깅 설정
def setup_logging(level=logging.INFO):
    """핵심 모듈 로깅 설정"""
    logger = logging.getLogger('core')
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(level)
    return logger

# 모듈 임포트시 기본 로깅 설정
setup_logging()

# 시스템 상태 확인
def system_health_check():
    """시스템 상태 확인"""
    health_status = {
        'core_modules_loaded': len(available_modules),
        'total_attempted': len(optional_modules) + 3,  # 필수 3개 + 선택사항들
        'available_modules': list(available_modules.keys()),
        'missing_modules': [],
        'status': 'healthy' if len(available_modules) >= 2 else 'warning'
    }
    
    # 필수 모듈 확인
    required_modules = ['PortfolioManager', 'ETFUpdateManager']
    for module in required_modules:
        if module not in available_modules:
            health_status['missing_modules'].append(module)
            health_status['status'] = 'critical'
    
    return health_status

# 동적 클래스 접근을 위한 helper 함수
def get_class(class_name: str):
    """클래스 이름으로 클래스 객체 반환"""
    return available_modules.get(class_name)

def is_available(class_name: str) -> bool:
    """특정 클래스가 사용 가능한지 확인"""
    return class_name in available_modules

# 모듈 정보 출력
if __name__ == "__main__":
    print("🔧 ETF Portfolio Manager Core 모듈")
    print("=" * 50)
    print(f"버전: {__version__}")
    print(f"로드된 모듈: {len(available_modules)}개")
    
    for module_name in available_modules:
        print(f"  ✅ {module_name}")
    
    health = system_health_check()
    print(f"\n시스템 상태: {health['status']}")
    
    if health['missing_modules']:
        print("누락된 필수 모듈:")
        for missing in health['missing_modules']:
            print(f"  ❌ {missing}")