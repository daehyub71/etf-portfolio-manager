"""
커스텀 투자전략 모듈
사용자 정의 자산배분 전략을 생성하고 관리하는 유연한 시스템
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging
import json

logger = logging.getLogger(__name__)

class CustomStrategy:
    """커스텀 투자전략 클래스"""
    
    def __init__(self, strategy_name: str = "My Custom Strategy"):
        """
        커스텀 전략 초기화
        
        Args:
            strategy_name: 전략 이름
        """
        self.strategy_name = strategy_name
        self.created_date = datetime.now()
        self.last_modified = datetime.now()
        
        # 전략 구성 요소
        self.asset_allocation = {}
        self.constraints = {}
        self.rules = {}
        self.preferences = {}
        
        # 기본 설정
        self._setup_default_configuration()
        
        # 사용 가능한 ETF 유니버스
        self._load_etf_universe()
    
    def _setup_default_configuration(self):
        """기본 설정값 초기화"""
        
        self.default_config = {
            'rebalancing_threshold': 5.0,      # 리밸런싱 임계치 (%)
            'max_position_size': 30.0,         # 최대 종목 비중 (%)
            'min_position_size': 2.0,          # 최소 종목 비중 (%)
            'max_etf_count': 15,               # 최대 ETF 개수
            'min_etf_count': 3,                # 최소 ETF 개수
            'expense_ratio_limit': 1.0,        # 비용 한도 (%)
            'min_liquidity_requirement': 1000000,  # 최소 유동성 (일평균 거래대금)
            'currency_hedge_preference': 'mixed',  # 환헤지 선호도
            'esg_preference': False,           # ESG 선호 여부
            'dividend_preference': False       # 배당 선호 여부
        }
        
        # 초기 제약조건 설정
        self.constraints = self.default_config.copy()
    
    def _load_etf_universe(self):
        """사용 가능한 ETF 유니버스 로드"""
        
        self.etf_universe = {
            # 국내 주식
            'domestic_equity': {
                '069500': {'name': 'KODEX 200', 'category': 'large_cap', 'expense_ratio': 0.15},
                '152100': {'name': 'TIGER 코스피', 'category': 'large_cap', 'expense_ratio': 0.06},
                '229200': {'name': 'KODEX 코스닥150', 'category': 'growth', 'expense_ratio': 0.15},
                '069660': {'name': 'KODEX 200고배당', 'category': 'dividend', 'expense_ratio': 0.25},
                '148020': {'name': 'KODEX ESG Korea', 'category': 'esg', 'expense_ratio': 0.25}
            },
            # 해외 주식
            'foreign_equity': {
                '139660': {'name': 'TIGER 미국S&P500', 'category': 'us_equity', 'expense_ratio': 0.045},
                '117460': {'name': 'KODEX 나스닥100', 'category': 'us_tech', 'expense_ratio': 0.045},
                '195930': {'name': 'TIGER 선진국MSCI World', 'category': 'developed', 'expense_ratio': 0.08},
                '192090': {'name': 'TIGER 신흥국MSCI', 'category': 'emerging', 'expense_ratio': 0.49},
                '160570': {'name': 'TIGER 중국CSI300', 'category': 'china', 'expense_ratio': 0.19}
            },
            # 채권
            'bonds': {
                '114260': {'name': 'KODEX 국고채10년', 'category': 'government', 'expense_ratio': 0.15},
                '136340': {'name': 'TIGER 미국채10년', 'category': 'foreign_government', 'expense_ratio': 0.14},
                '139230': {'name': 'TIGER 회사채AA-', 'category': 'corporate', 'expense_ratio': 0.15},
                '130730': {'name': 'KODEX 글로벌하이일드', 'category': 'high_yield', 'expense_ratio': 0.45}
            },
            # 대안투자
            'alternatives': {
                '157490': {'name': 'KODEX 리츠', 'category': 'reit', 'expense_ratio': 0.5},
                '351590': {'name': 'TIGER 미국리츠', 'category': 'us_reit', 'expense_ratio': 0.49},
                '132030': {'name': 'KODEX 골드선물', 'category': 'gold', 'expense_ratio': 0.49},
                '130680': {'name': 'TIGER 원유선물', 'category': 'oil', 'expense_ratio': 0.65}
            },
            # 테마/섹터
            'thematic': {
                '305540': {'name': 'KODEX 2차전지산업', 'category': 'battery', 'expense_ratio': 0.45},
                '091160': {'name': 'KODEX 반도체', 'category': 'semiconductor', 'expense_ratio': 0.49},
                '227560': {'name': 'KODEX 바이오', 'category': 'biotech', 'expense_ratio': 0.49}
            }
        }
    
    def create_strategy_from_allocation(self, allocation: Dict[str, float], 
                                      strategy_config: Optional[Dict] = None) -> bool:
        """
        자산배분을 기반으로 전략 생성
        
        Args:
            allocation: ETF 코드별 목표 비중 (%)
            strategy_config: 전략 설정
            
        Returns:
            전략 생성 성공 여부
        """
        try:
            # 자산배분 검증
            if not self._validate_allocation(allocation):
                return False
            
            self.asset_allocation = allocation.copy()
            
            # 설정 업데이트
            if strategy_config:
                self.constraints.update(strategy_config)
            
            # 전략 규칙 자동 생성
            self._generate_strategy_rules()
            
            self.last_modified = datetime.now()
            
            logger.info(f"커스텀 전략 '{self.strategy_name}' 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"커스텀 전략 생성 실패: {e}")
            return False
    
    def create_strategy_from_template(self, template_name: str, 
                                    customizations: Optional[Dict] = None) -> bool:
        """
        템플릿을 기반으로 전략 생성
        
        Args:
            template_name: 템플릿 이름
            customizations: 커스터마이징 옵션
            
        Returns:
            전략 생성 성공 여부
        """
        try:
            # 템플릿 가져오기
            template = self._get_strategy_template(template_name)
            if not template:
                logger.error(f"템플릿 '{template_name}'을 찾을 수 없습니다")
                return False
            
            # 기본 자산배분 설정
            self.asset_allocation = template['allocation'].copy()
            self.constraints.update(template['constraints'])
            
            # 커스터마이징 적용
            if customizations:
                self._apply_customizations(customizations)
            
            # 전략 규칙 생성
            self._generate_strategy_rules()
            
            self.last_modified = datetime.now()
            
            logger.info(f"템플릿 '{template_name}' 기반 커스텀 전략 생성 완료")
            return True
            
        except Exception as e:
            logger.error(f"템플릿 기반 전략 생성 실패: {e}")
            return False
    
    def _get_strategy_template(self, template_name: str) -> Optional[Dict]:
        """전략 템플릿 가져오기"""
        
        templates = {
            'balanced_growth': {
                'name': '균형 성장형',
                'allocation': {
                    '069500': 25,    # KODEX 200
                    '139660': 25,    # TIGER 미국S&P500
                    '195930': 20,    # TIGER 선진국MSCI
                    '114260': 20,    # KODEX 국고채10년
                    '157490': 10     # KODEX 리츠
                },
                'constraints': {
                    'rebalancing_threshold': 5.0,
                    'max_position_size': 30.0,
                    'expense_ratio_limit': 0.5
                }
            },
            'aggressive_growth': {
                'name': '공격적 성장형',
                'allocation': {
                    '139660': 30,    # TIGER 미국S&P500
                    '117460': 25,    # KODEX 나스닥100
                    '069500': 20,    # KODEX 200
                    '192090': 15,    # TIGER 신흥국MSCI
                    '114260': 10     # KODEX 국고채10년
                },
                'constraints': {
                    'rebalancing_threshold': 7.0,
                    'max_position_size': 35.0,
                    'expense_ratio_limit': 0.8
                }
            },
            'conservative_income': {
                'name': '보수적 수익형',
                'allocation': {
                    '114260': 30,    # KODEX 국고채10년
                    '069500': 25,    # KODEX 200
                    '069660': 20,    # KODEX 200고배당
                    '136340': 15,    # TIGER 미국채10년
                    '157490': 10     # KODEX 리츠
                },
                'constraints': {
                    'rebalancing_threshold': 3.0,
                    'max_position_size': 35.0,
                    'dividend_preference': True
                }
            },
            'global_diversified': {
                'name': '글로벌 분산형',
                'allocation': {
                    '069500': 20,    # KODEX 200
                    '139660': 25,    # TIGER 미국S&P500
                    '195930': 20,    # TIGER 선진국MSCI
                    '192090': 10,    # TIGER 신흥국MSCI
                    '114260': 15,    # KODEX 국고채10년
                    '132030': 10     # KODEX 골드선물
                },
                'constraints': {
                    'rebalancing_threshold': 5.0,
                    'max_position_size': 25.0,
                    'currency_hedge_preference': 'mixed'
                }
            },
            'thematic_tech': {
                'name': '테마형(기술)',
                'allocation': {
                    '117460': 25,    # KODEX 나스닥100
                    '091160': 20,    # KODEX 반도체
                    '305540': 20,    # KODEX 2차전지산업
                    '139660': 20,    # TIGER 미국S&P500
                    '114260': 15     # KODEX 국고채10년
                },
                'constraints': {
                    'rebalancing_threshold': 8.0,
                    'max_position_size': 30.0,
                    'expense_ratio_limit': 1.0
                }
            }
        }
        
        return templates.get(template_name)
    
    def _validate_allocation(self, allocation: Dict[str, float]) -> bool:
        """자산배분 유효성 검증"""
        
        # 비중 합계 검증
        total_weight = sum(allocation.values())
        if abs(total_weight - 100) > 0.1:
            logger.error(f"비중 합계 오류: {total_weight}% (100%가 되어야 함)")
            return False
        
        # ETF 개수 검증
        etf_count = len(allocation)
        if etf_count < self.constraints['min_etf_count']:
            logger.error(f"ETF 개수 부족: {etf_count}개 (최소 {self.constraints['min_etf_count']}개)")
            return False
        
        if etf_count > self.constraints['max_etf_count']:
            logger.error(f"ETF 개수 초과: {etf_count}개 (최대 {self.constraints['max_etf_count']}개)")
            return False
        
        # 개별 종목 비중 검증
        for etf_code, weight in allocation.items():
            if weight < self.constraints['min_position_size']:
                logger.error(f"{etf_code} 비중 부족: {weight}% (최소 {self.constraints['min_position_size']}%)")
                return False
            
            if weight > self.constraints['max_position_size']:
                logger.error(f"{etf_code} 비중 초과: {weight}% (최대 {self.constraints['max_position_size']}%)")
                return False
        
        # ETF 존재 여부 검증
        all_etfs = set()
        for category_etfs in self.etf_universe.values():
            all_etfs.update(category_etfs.keys())
        
        for etf_code in allocation.keys():
            if etf_code not in all_etfs:
                logger.warning(f"알 수 없는 ETF 코드: {etf_code}")
        
        return True
    
    def _apply_customizations(self, customizations: Dict):
        """커스터마이징 옵션 적용"""
        
        # 자산배분 조정
        if 'allocation_adjustments' in customizations:
            adjustments = customizations['allocation_adjustments']
            
            for etf_code, adjustment in adjustments.items():
                if etf_code in self.asset_allocation:
                    self.asset_allocation[etf_code] += adjustment
                else:
                    self.asset_allocation[etf_code] = adjustment
            
            # 비중 재정규화
            total_weight = sum(self.asset_allocation.values())
            if total_weight != 100:
                self.asset_allocation = {
                    etf_code: weight / total_weight * 100
                    for etf_code, weight in self.asset_allocation.items()
                }
        
        # 제약조건 조정
        if 'constraints' in customizations:
            self.constraints.update(customizations['constraints'])
        
        # 선호도 설정
        if 'preferences' in customizations:
            self.preferences.update(customizations['preferences'])
    
    def _generate_strategy_rules(self):
        """전략 규칙 자동 생성"""
        
        self.rules = {
            'rebalancing': {
                'frequency': 'monthly',
                'threshold': self.constraints['rebalancing_threshold'],
                'method': 'threshold_based'
            },
            'risk_management': {
                'max_drawdown_limit': 20.0,
                'volatility_target': None,
                'stop_loss_level': None
            },
            'asset_selection': {
                'expense_ratio_limit': self.constraints['expense_ratio_limit'],
                'liquidity_requirement': self.constraints['min_liquidity_requirement'],
                'esg_filter': self.constraints.get('esg_preference', False)
            },
            'portfolio_construction': {
                'concentration_limit': self.constraints['max_position_size'],
                'minimum_weight': self.constraints['min_position_size'],
                'diversification_requirement': True
            }
        }
    
    def add_etf(self, etf_code: str, target_weight: float, 
               adjustment_method: str = 'proportional') -> bool:
        """
        ETF 추가
        
        Args:
            etf_code: 추가할 ETF 코드
            target_weight: 목표 비중 (%)
            adjustment_method: 기존 비중 조정 방법
            
        Returns:
            추가 성공 여부
        """
        try:
            if etf_code in self.asset_allocation:
                logger.warning(f"{etf_code}는 이미 포트폴리오에 있습니다")
                return False
            
            # 비중 조정
            if adjustment_method == 'proportional':
                # 기존 비중을 비례적으로 축소
                reduction_factor = (100 - target_weight) / 100
                for code in self.asset_allocation:
                    self.asset_allocation[code] *= reduction_factor
            
            elif adjustment_method == 'equal_reduction':
                # 기존 ETF에서 균등하게 차감
                reduction_per_etf = target_weight / len(self.asset_allocation)
                for code in self.asset_allocation:
                    self.asset_allocation[code] -= reduction_per_etf
            
            # 새 ETF 추가
            self.asset_allocation[etf_code] = target_weight
            
            # 유효성 검증
            if not self._validate_allocation(self.asset_allocation):
                # 실패시 롤백
                del self.asset_allocation[etf_code]
                return False
            
            self.last_modified = datetime.now()
            logger.info(f"ETF {etf_code} 추가 완료 (비중: {target_weight}%)")
            return True
            
        except Exception as e:
            logger.error(f"ETF 추가 실패: {e}")
            return False
    
    def remove_etf(self, etf_code: str, 
                  redistribution_method: str = 'proportional') -> bool:
        """
        ETF 제거
        
        Args:
            etf_code: 제거할 ETF 코드
            redistribution_method: 비중 재분배 방법
            
        Returns:
            제거 성공 여부
        """
        try:
            if etf_code not in self.asset_allocation:
                logger.warning(f"{etf_code}는 포트폴리오에 없습니다")
                return False
            
            removed_weight = self.asset_allocation[etf_code]
            del self.asset_allocation[etf_code]
            
            if len(self.asset_allocation) == 0:
                logger.error("마지막 ETF는 제거할 수 없습니다")
                self.asset_allocation[etf_code] = removed_weight
                return False
            
            # 비중 재분배
            if redistribution_method == 'proportional':
                # 기존 비중에 비례하여 분배
                total_remaining = sum(self.asset_allocation.values())
                for code in self.asset_allocation:
                    self.asset_allocation[code] = (
                        self.asset_allocation[code] / total_remaining * 100
                    )
            
            elif redistribution_method == 'equal':
                # 균등 분배
                redistribution_per_etf = removed_weight / len(self.asset_allocation)
                for code in self.asset_allocation:
                    self.asset_allocation[code] += redistribution_per_etf
            
            self.last_modified = datetime.now()
            logger.info(f"ETF {etf_code} 제거 완료")
            return True
            
        except Exception as e:
            logger.error(f"ETF 제거 실패: {e}")
            return False
    
    def adjust_weights(self, weight_adjustments: Dict[str, float]) -> bool:
        """
        비중 조정
        
        Args:
            weight_adjustments: ETF별 비중 조정값
            
        Returns:
            조정 성공 여부
        """
        try:
            # 백업 생성
            original_allocation = self.asset_allocation.copy()
            
            # 비중 조정 적용
            for etf_code, adjustment in weight_adjustments.items():
                if etf_code in self.asset_allocation:
                    self.asset_allocation[etf_code] += adjustment
                else:
                    logger.warning(f"{etf_code}는 포트폴리오에 없습니다")
            
            # 비중 재정규화
            total_weight = sum(self.asset_allocation.values())
            if total_weight != 100:
                self.asset_allocation = {
                    etf_code: weight / total_weight * 100
                    for etf_code, weight in self.asset_allocation.items()
                }
            
            # 유효성 검증
            if not self._validate_allocation(self.asset_allocation):
                # 실패시 롤백
                self.asset_allocation = original_allocation
                return False
            
            self.last_modified = datetime.now()
            logger.info("비중 조정 완료")
            return True
            
        except Exception as e:
            logger.error(f"비중 조정 실패: {e}")
            # 롤백
            self.asset_allocation = original_allocation
            return False
    
    def optimize_for_cost(self) -> Dict[str, float]:
        """비용 최적화된 대안 포트폴리오 제안"""
        
        optimized_allocation = {}
        
        try:
            # 각 자산군별 최저비용 ETF 찾기
            current_asset_classes = self._get_current_asset_classes()
            
            for asset_class, current_weight in current_asset_classes.items():
                if current_weight > 0:
                    # 해당 자산군에서 최저비용 ETF 찾기
                    best_etf = self._find_lowest_cost_etf(asset_class)
                    if best_etf:
                        optimized_allocation[best_etf] = current_weight
            
            # 비중 재정규화
            total_weight = sum(optimized_allocation.values())
            if total_weight > 0:
                optimized_allocation = {
                    etf_code: weight / total_weight * 100
                    for etf_code, weight in optimized_allocation.items()
                }
            
            return optimized_allocation
            
        except Exception as e:
            logger.error(f"비용 최적화 실패: {e}")
            return self.asset_allocation.copy()
    
    def _get_current_asset_classes(self) -> Dict[str, float]:
        """현재 자산군별 비중 계산"""
        
        asset_classes = {}
        
        for etf_code, weight in self.asset_allocation.items():
            asset_class = self._get_etf_asset_class(etf_code)
            
            if asset_class in asset_classes:
                asset_classes[asset_class] += weight
            else:
                asset_classes[asset_class] = weight
        
        return asset_classes
    
    def _get_etf_asset_class(self, etf_code: str) -> str:
        """ETF의 자산군 찾기"""
        
        for asset_class, etfs in self.etf_universe.items():
            if etf_code in etfs:
                return asset_class
        
        return 'unknown'
    
    def _find_lowest_cost_etf(self, asset_class: str) -> Optional[str]:
        """자산군에서 최저비용 ETF 찾기"""
        
        etfs = self.etf_universe.get(asset_class, {})
        
        if not etfs:
            return None
        
        # 비용이 가장 낮은 ETF 찾기
        lowest_cost_etf = min(etfs.items(), 
                             key=lambda x: x[1]['expense_ratio'])
        
        return lowest_cost_etf[0]
    
    def generate_rebalancing_plan(self, current_weights: Dict[str, float]) -> Dict[str, Dict]:
        """리밸런싱 계획 생성"""
        
        rebalancing_plan = {}
        threshold = self.constraints['rebalancing_threshold']
        
        all_etfs = set(list(current_weights.keys()) + list(self.asset_allocation.keys()))
        
        for etf_code in all_etfs:
            current_weight = current_weights.get(etf_code, 0)
            target_weight = self.asset_allocation.get(etf_code, 0)
            
            deviation = target_weight - current_weight
            
            if abs(deviation) >= threshold:
                action = 'BUY' if deviation > 0 else 'SELL'
                urgency = 'High' if abs(deviation) >= threshold * 2 else 'Medium'
                
                rebalancing_plan[etf_code] = {
                    'action': action,
                    'current_weight': current_weight,
                    'target_weight': target_weight,
                    'deviation': deviation,
                    'urgency': urgency,
                    'etf_name': self._get_etf_name(etf_code)
                }
        
        return rebalancing_plan
    
    def _get_etf_name(self, etf_code: str) -> str:
        """ETF 이름 조회"""
        
        for asset_class, etfs in self.etf_universe.items():
            if etf_code in etfs:
                return etfs[etf_code]['name']
        
        return etf_code
    
    def export_strategy(self) -> Dict[str, Any]:
        """전략 설정 내보내기"""
        
        strategy_export = {
            'strategy_name': self.strategy_name,
            'created_date': self.created_date.isoformat(),
            'last_modified': self.last_modified.isoformat(),
            'asset_allocation': self.asset_allocation,
            'constraints': self.constraints,
            'rules': self.rules,
            'preferences': self.preferences,
            'version': '1.0'
        }
        
        return strategy_export
    
    def import_strategy(self, strategy_data: Dict[str, Any]) -> bool:
        """전략 설정 가져오기"""
        
        try:
            # 필수 필드 검증
            required_fields = ['strategy_name', 'asset_allocation']
            for field in required_fields:
                if field not in strategy_data:
                    logger.error(f"필수 필드 누락: {field}")
                    return False
            
            # 자산배분 유효성 검증
            if not self._validate_allocation(strategy_data['asset_allocation']):
                return False
            
            # 전략 데이터 적용
            self.strategy_name = strategy_data['strategy_name']
            self.asset_allocation = strategy_data['asset_allocation']
            
            if 'constraints' in strategy_data:
                self.constraints.update(strategy_data['constraints'])
            
            if 'rules' in strategy_data:
                self.rules.update(strategy_data['rules'])
            
            if 'preferences' in strategy_data:
                self.preferences.update(strategy_data['preferences'])
            
            self.last_modified = datetime.now()
            
            logger.info(f"전략 '{self.strategy_name}' 가져오기 완료")
            return True
            
        except Exception as e:
            logger.error(f"전략 가져오기 실패: {e}")
            return False
    
    def get_strategy_analysis(self) -> Dict[str, Any]:
        """전략 분석 정보 제공"""
        
        analysis = {
            'basic_info': {
                'strategy_name': self.strategy_name,
                'etf_count': len(self.asset_allocation),
                'last_modified': self.last_modified.strftime('%Y-%m-%d'),
                'is_valid': self._validate_allocation(self.asset_allocation)
            },
            'asset_distribution': self._analyze_asset_distribution(),
            'risk_characteristics': self._analyze_risk_characteristics(),
            'cost_analysis': self._analyze_costs(),
            'diversification_metrics': self._analyze_diversification(),
            'compliance_check': self._check_compliance()
        }
        
        return analysis
    
    def _analyze_asset_distribution(self) -> Dict[str, float]:
        """자산 분포 분석"""
        
        distribution = {}
        asset_class_weights = self._get_current_asset_classes()
        
        total_weight = sum(asset_class_weights.values())
        
        for asset_class, weight in asset_class_weights.items():
            distribution[asset_class] = weight / total_weight * 100 if total_weight > 0 else 0
        
        return distribution
    
    def _analyze_risk_characteristics(self) -> Dict[str, Any]:
        """위험 특성 분석"""
        
        # 간단화된 위험 분석 (실제로는 더 정교한 계산 필요)
        equity_weight = sum(
            weight for etf_code, weight in self.asset_allocation.items()
            if self._get_etf_asset_class(etf_code) in ['domestic_equity', 'foreign_equity', 'thematic']
        )
        
        risk_level = 'Conservative' if equity_weight < 40 else \
                    'Moderate' if equity_weight < 70 else 'Aggressive'
        
        return {
            'equity_ratio': equity_weight,
            'risk_level': risk_level,
            'estimated_volatility': min(25, max(5, equity_weight * 0.25)),  # 간단한 추정
            'max_position_concentration': max(self.asset_allocation.values())
        }
    
    def _analyze_costs(self) -> Dict[str, float]:
        """비용 분석"""
        
        total_expense_ratio = 0
        weighted_costs = {}
        
        for etf_code, weight in self.asset_allocation.items():
            expense_ratio = self._get_etf_expense_ratio(etf_code)
            weighted_cost = expense_ratio * weight / 100
            total_expense_ratio += weighted_cost
            weighted_costs[etf_code] = weighted_cost
        
        return {
            'total_expense_ratio': total_expense_ratio,
            'annual_cost_per_million': total_expense_ratio * 10000,  # 100만원당
            'cost_efficiency_score': max(0, 100 - total_expense_ratio * 100)
        }
    
    def _get_etf_expense_ratio(self, etf_code: str) -> float:
        """ETF 운용보수 조회"""
        
        for asset_class, etfs in self.etf_universe.items():
            if etf_code in etfs:
                return etfs[etf_code]['expense_ratio']
        
        return 0.5  # 기본값
    
    def _analyze_diversification(self) -> Dict[str, float]:
        """분산화 지표 분석"""
        
        # 허핀달 지수 계산
        weights = list(self.asset_allocation.values())
        hhi = sum((w/100)**2 for w in weights)
        
        # 등가 ETF 개수
        effective_etf_count = 1 / hhi if hhi > 0 else 0
        
        return {
            'herfindahl_index': hhi,
            'effective_etf_count': effective_etf_count,
            'diversification_score': min(100, (1 - hhi) * 125),  # 0-100 점수
            'concentration_risk': max(weights) if weights else 0
        }
    
    def _check_compliance(self) -> Dict[str, bool]:
        """제약 조건 준수 여부 확인"""
        
        compliance = {}
        
        # 기본 제약조건 확인
        max_weight = max(self.asset_allocation.values()) if self.asset_allocation else 0
        min_weight = min(self.asset_allocation.values()) if self.asset_allocation else 0
        etf_count = len(self.asset_allocation)
        
        compliance['max_position_limit'] = max_weight <= self.constraints['max_position_size']
        compliance['min_position_limit'] = min_weight >= self.constraints['min_position_size']
        compliance['etf_count_range'] = (self.constraints['min_etf_count'] <= 
                                       etf_count <= 
                                       self.constraints['max_etf_count'])
        
        # 비용 제한 확인
        cost_analysis = self._analyze_costs()
        compliance['expense_ratio_limit'] = (cost_analysis['total_expense_ratio'] <= 
                                           self.constraints['expense_ratio_limit'])
        
        return compliance
    
    def get_strategy_description(self) -> Dict[str, Any]:
        """전략 설명 정보"""
        
        analysis = self.get_strategy_analysis()
        
        return {
            'strategy_name': self.strategy_name,
            'strategy_type': 'Custom Portfolio',
            'description': f"사용자 정의 {len(self.asset_allocation)}개 ETF 포트폴리오",
            'risk_level': analysis['risk_characteristics']['risk_level'],
            'main_characteristics': [
                f"총 {analysis['basic_info']['etf_count']}개 ETF 구성",
                f"주식 비중 {analysis['risk_characteristics']['equity_ratio']:.1f}%",
                f"예상 연간 비용 {analysis['cost_analysis']['total_expense_ratio']:.2f}%",
                f"분산화 점수 {analysis['diversification_metrics']['diversification_score']:.0f}점"
            ],
            'rebalancing_threshold': self.constraints['rebalancing_threshold'],
            'last_updated': self.last_modified.strftime('%Y-%m-%d %H:%M'),
            'asset_allocation': self.asset_allocation,
            'top_holdings': sorted(self.asset_allocation.items(), 
                                 key=lambda x: x[1], reverse=True)[:5]
        }