"""
Strategies 모듈 - ETF 포트폴리오 투자 전략

이 모듈은 다양한 자산배분 투자 전략을 제공합니다:
- 코어-새틀라이트 전략
- 글로벌 분산 전략  
- 생애주기 전략
- 리스크 패리티 전략
- 커스텀 전략
"""

from .core_satellite import CoreSatelliteStrategy
from .global_diversified import GlobalDiversifiedStrategy
from .lifecycle_strategy import LifecycleStrategy
from .risk_parity import RiskParityStrategy
from .custom_strategy import CustomStrategy

__version__ = "1.0.0"
__author__ = "ETF Portfolio Manager Team"

# 전략 클래스들을 모듈 레벨에서 직접 접근 가능하도록 export
__all__ = [
    'CoreSatelliteStrategy',
    'GlobalDiversifiedStrategy',
    'LifecycleStrategy', 
    'RiskParityStrategy',
    'CustomStrategy',
    # 헬퍼 함수들
    'get_available_strategies',
    'get_strategy_by_name',
    'recommend_strategy',
    'compare_strategies'
]

# 모듈 메타데이터
MODULE_INFO = {
    'name': 'ETF Portfolio Investment Strategies',
    'description': '다양한 ETF 자산배분 투자 전략 모음',
    'version': __version__,
    'strategies': {
        'core_satellite': {
            'name': '코어-새틀라이트 전략',
            'description': '안정적 코어(70-80%) + 성장 새틀라이트(20-30%)',
            'risk_level': 'moderate',
            'complexity': 'medium',
            'suitable_for': '균형잡힌 장기투자자'
        },
        'global_diversified': {
            'name': '글로벌 분산 전략',
            'description': '전 세계 주요 시장 분산투자',
            'risk_level': 'moderate',
            'complexity': 'low',
            'suitable_for': '글로벌 투자 선호자'
        },
        'lifecycle': {
            'name': '생애주기 전략',
            'description': '연령대별 맞춤형 자산배분',
            'risk_level': 'variable',
            'complexity': 'medium',
            'suitable_for': '체계적 장기투자자'
        },
        'risk_parity': {
            'name': '리스크 패리티 전략',
            'description': '위험 기여도 균등화',
            'risk_level': 'conservative',
            'complexity': 'high',
            'suitable_for': '고급 투자자'
        },
        'custom': {
            'name': '커스텀 전략',
            'description': '사용자 정의 자산배분',
            'risk_level': 'variable',
            'complexity': 'variable',
            'suitable_for': '모든 투자자'
        }
    }
}

# 전략 팩토리 클래스
class StrategyFactory:
    """투자 전략 팩토리"""
    
    _strategies = {
        'core_satellite': CoreSatelliteStrategy,
        'global_diversified': GlobalDiversifiedStrategy,
        'lifecycle': LifecycleStrategy,
        'risk_parity': RiskParityStrategy,
        'custom': CustomStrategy
    }
    
    @classmethod
    def create_strategy(cls, strategy_name: str, **kwargs):
        """전략 인스턴스 생성"""
        if strategy_name not in cls._strategies:
            raise ValueError(f"지원하지 않는 전략: {strategy_name}")
        
        strategy_class = cls._strategies[strategy_name]
        return strategy_class(**kwargs)
    
    @classmethod
    def get_available_strategies(cls):
        """사용 가능한 전략 목록 반환"""
        return list(cls._strategies.keys())

def get_available_strategies():
    """사용 가능한 전략 목록 반환"""
    return StrategyFactory.get_available_strategies()

def get_strategy_by_name(strategy_name: str, **kwargs):
    """이름으로 전략 인스턴스 생성"""
    return StrategyFactory.create_strategy(strategy_name, **kwargs)

def get_strategy_info(strategy_name: str = None):
    """전략 정보 반환"""
    if strategy_name:
        return MODULE_INFO['strategies'].get(strategy_name)
    return MODULE_INFO['strategies']

def recommend_strategy(user_profile: dict):
    """사용자 프로필에 따른 전략 추천"""
    age = user_profile.get('age', 35)
    risk_tolerance = user_profile.get('risk_tolerance', 'moderate')
    investment_experience = user_profile.get('experience', 'beginner')
    investment_goal = user_profile.get('goal', 'growth')
    
    recommendations = []
    
    # 나이 기반 추천
    if age < 30:
        recommendations.append({
            'strategy': 'lifecycle',
            'reason': '젊은 나이로 공격적 성장 가능',
            'priority': 1
        })
        recommendations.append({
            'strategy': 'core_satellite', 
            'reason': '안정성과 성장성의 균형',
            'priority': 2
        })
    elif age < 50:
        recommendations.append({
            'strategy': 'core_satellite',
            'reason': '중년기 최적 전략',
            'priority': 1
        })
        recommendations.append({
            'strategy': 'global_diversified',
            'reason': '글로벌 분산효과',
            'priority': 2
        })
    else:
        recommendations.append({
            'strategy': 'lifecycle',
            'reason': '연령에 맞는 안정적 투자',
            'priority': 1
        })
        recommendations.append({
            'strategy': 'risk_parity',
            'reason': '위험 관리 중심',
            'priority': 2
        })
    
    # 위험성향 기반 조정
    if risk_tolerance == 'conservative':
        # 보수적 투자자는 리스크 패리티 우선
        for rec in recommendations:
            if rec['strategy'] == 'risk_parity':
                rec['priority'] = 1
                break
    elif risk_tolerance == 'aggressive':
        # 공격적 투자자는 코어-새틀라이트 우선
        for rec in recommendations:
            if rec['strategy'] == 'core_satellite':
                rec['priority'] = 1
                break
    
    # 경험 수준 기반 조정
    if investment_experience == 'beginner':
        # 초보자는 간단한 전략 우선
        simple_strategies = ['global_diversified', 'lifecycle']
        for rec in recommendations:
            if rec['strategy'] in simple_strategies:
                rec['priority'] = max(1, rec['priority'] - 1)
    elif investment_experience == 'expert':
        # 전문가는 고급 전략 추가
        recommendations.append({
            'strategy': 'risk_parity',
            'reason': '고급 위험 관리 기법',
            'priority': 2
        })
    
    # 우선순위 정렬
    recommendations.sort(key=lambda x: x['priority'])
    
    return recommendations[:3]  # 상위 3개 추천

def compare_strategies(strategies: list, criteria: list = None):
    """여러 전략 비교 분석"""
    if criteria is None:
        criteria = ['complexity', 'risk_level', 'expected_return', 'suitable_for']
    
    comparison = {}
    
    for strategy_name in strategies:
        strategy_info = get_strategy_info(strategy_name)
        if strategy_info:
            comparison[strategy_name] = {
                criterion: strategy_info.get(criterion, 'N/A')
                for criterion in criteria
            }
    
    return comparison

# 전략별 기본 설정
DEFAULT_STRATEGY_CONFIGS = {
    'core_satellite': {
        'core_ratio': 0.8,
        'risk_level': 'moderate'
    },
    'global_diversified': {
        'strategy_variant': 'balanced'
    },
    'lifecycle': {
        'retirement_age': 65,
        'risk_tolerance': 'moderate'
    },
    'risk_parity': {
        'lookback_period': 252,
        'risk_budget_method': 'equal'
    },
    'custom': {
        'strategy_name': 'My Portfolio'
    }
}

def get_default_config(strategy_name: str):
    """전략별 기본 설정 반환"""
    return DEFAULT_STRATEGY_CONFIGS.get(strategy_name, {})

# 전략 성과 벤치마크
STRATEGY_BENCHMARKS = {
    'core_satellite': {
        'primary': '069500',  # KODEX 200
        'secondary': 'mixed_benchmark'
    },
    'global_diversified': {
        'primary': 'msci_world',
        'secondary': 'mixed_global'
    },
    'lifecycle': {
        'primary': 'age_appropriate_benchmark',
        'secondary': 'target_date_fund'
    },
    'risk_parity': {
        'primary': 'equal_weight_benchmark',
        'secondary': 'minimum_variance'
    }
}

def get_strategy_benchmark(strategy_name: str):
    """전략별 벤치마크 반환"""
    return STRATEGY_BENCHMARKS.get(strategy_name, {})

# 전략 유효성 검증
def validate_strategy_parameters(strategy_name: str, parameters: dict):
    """전략 파라미터 유효성 검증"""
    validation_rules = {
        'core_satellite': {
            'core_ratio': (0.5, 0.9),
            'risk_level': ['conservative', 'moderate', 'aggressive']
        },
        'global_diversified': {
            'strategy_variant': ['simple', 'balanced', 'growth_focused']
        },
        'lifecycle': {
            'age': (18, 100),
            'retirement_age': (50, 80),
            'risk_tolerance': ['conservative', 'moderate', 'aggressive']
        },
        'risk_parity': {
            'lookback_period': (30, 1000),
            'risk_budget_method': ['equal', 'strategic', 'conservative']
        }
    }
    
    rules = validation_rules.get(strategy_name, {})
    errors = []
    
    for param, value in parameters.items():
        if param in rules:
            rule = rules[param]
            
            if isinstance(rule, tuple):  # 범위 검증
                min_val, max_val = rule
                if not (min_val <= value <= max_val):
                    errors.append(f"{param}: {min_val}-{max_val} 범위를 벗어남")
            
            elif isinstance(rule, list):  # 선택지 검증
                if value not in rule:
                    errors.append(f"{param}: {rule} 중에서 선택해야 함")
    
    return len(errors) == 0, errors

# 로깅 설정
import logging

def setup_strategies_logging(level=logging.INFO):
    """전략 모듈 로깅 설정"""
    logger = logging.getLogger('strategies')
    
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
logger = setup_strategies_logging()

def strategies_health_check():
    """전략 모듈 상태 확인"""
    health_status = {
        'all_strategies_loaded': True,
        'strategy_count': 0,
        'factory_operational': False
    }
    
    try:
        # 모든 전략 클래스 로드 확인
        strategies = get_available_strategies()
        health_status['strategy_count'] = len(strategies)
        
        # 팩토리 기능 테스트
        test_strategy = StrategyFactory.create_strategy('custom')
        health_status['factory_operational'] = test_strategy is not None
        
    except Exception as e:
        health_status['all_strategies_loaded'] = False
        health_status['error'] = str(e)
    
    return health_status

# 버전 정보
def get_version():
    """현재 버전 반환"""
    return __version__

def get_module_info():
    """모듈 정보 반환"""
    return MODULE_INFO.copy()

# 초기화 로그
logger.info(f"Strategies 모듈 로드 완료 (전략 수: {len(get_available_strategies())})")