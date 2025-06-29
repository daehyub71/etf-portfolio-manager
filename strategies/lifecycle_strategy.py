"""
생애주기 투자전략 모듈
연령대별 맞춤형 자산배분으로 인생 단계에 따른 최적 포트폴리오 구성
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

class LifecycleStrategy:
    """생애주기 투자전략 클래스"""
    
    def __init__(self, age: int, retirement_age: int = 65, risk_tolerance: str = 'moderate'):
        """
        생애주기 전략 초기화
        
        Args:
            age: 현재 나이
            retirement_age: 은퇴 예정 나이
            risk_tolerance: 위험 성향 ('conservative', 'moderate', 'aggressive')
        """
        self.age = age
        self.retirement_age = retirement_age
        self.risk_tolerance = risk_tolerance
        self.years_to_retirement = max(0, retirement_age - age)
        
        # 생애주기 단계 구분
        self.life_stage = self._determine_life_stage()
        
        # 단계별 기본 설정
        self._setup_lifecycle_configs()
        
        # 추천 ETF 설정
        self._setup_recommended_etfs()
    
    def _determine_life_stage(self) -> str:
        """생애주기 단계 결정"""
        if self.age < 30:
            return 'accumulation_early'      # 초기 자산축적기
        elif self.age < 40:
            return 'accumulation_growth'     # 성장 자산축적기
        elif self.age < 50:
            return 'accumulation_mature'     # 성숙 자산축적기
        elif self.age < self.retirement_age:
            return 'pre_retirement'          # 은퇴 준비기
        else:
            return 'retirement'              # 은퇴기
    
    def _setup_lifecycle_configs(self):
        """생애주기별 기본 설정"""
        
        self.lifecycle_configs = {
            'accumulation_early': {
                'name': '초기 자산축적기 (20대)',
                'description': '공격적 성장 중심의 자산축적',
                'equity_ratio_base': 90,
                'bond_ratio_base': 10,
                'alternative_ratio_base': 0,
                'domestic_ratio': 40,
                'foreign_ratio': 60,
                'growth_premium': 10,  # 성장주 추가 비중
                'rebalancing_threshold': 10.0,
                'savings_rate_target': 20  # 소득 대비 저축률 목표
            },
            'accumulation_growth': {
                'name': '성장 자산축적기 (30대)',
                'description': '균형잡힌 성장과 안정성 추구',
                'equity_ratio_base': 80,
                'bond_ratio_base': 15,
                'alternative_ratio_base': 5,
                'domestic_ratio': 45,
                'foreign_ratio': 55,
                'growth_premium': 5,
                'rebalancing_threshold': 8.0,
                'savings_rate_target': 25
            },
            'accumulation_mature': {
                'name': '성숙 자산축적기 (40대)',
                'description': '안정성과 성장의 균형',
                'equity_ratio_base': 70,
                'bond_ratio_base': 25,
                'alternative_ratio_base': 5,
                'domestic_ratio': 50,
                'foreign_ratio': 50,
                'growth_premium': 0,
                'rebalancing_threshold': 6.0,
                'savings_rate_target': 30
            },
            'pre_retirement': {
                'name': '은퇴 준비기 (50대)',
                'description': '자본 보전과 안정적 수익 추구',
                'equity_ratio_base': 60,
                'bond_ratio_base': 35,
                'alternative_ratio_base': 5,
                'domestic_ratio': 55,
                'foreign_ratio': 45,
                'growth_premium': -5,
                'rebalancing_threshold': 5.0,
                'savings_rate_target': 35
            },
            'retirement': {
                'name': '은퇴기 (60대+)',
                'description': '소득 창출과 자본 보전',
                'equity_ratio_base': 40,
                'bond_ratio_base': 50,
                'alternative_ratio_base': 10,
                'domestic_ratio': 60,
                'foreign_ratio': 40,
                'growth_premium': -10,
                'rebalancing_threshold': 3.0,
                'savings_rate_target': 10  # 인출 단계
            }
        }
        
        self.current_config = self.lifecycle_configs[self.life_stage]
    
    def _setup_recommended_etfs(self):
        """생애주기별 추천 ETF 설정"""
        
        self.etf_pools = {
            'domestic_equity': {
                'core': {
                    '069500': {'name': 'KODEX 200', 'type': 'large_cap', 'risk': 'low'},
                    '152100': {'name': 'TIGER 코스피', 'type': 'large_cap', 'risk': 'low'}
                },
                'growth': {
                    '229200': {'name': 'KODEX 코스닥150', 'type': 'growth', 'risk': 'high'},
                    '233740': {'name': 'KODEX 코스닥150 레버리지', 'type': 'growth', 'risk': 'very_high'}
                },
                'value': {
                    '069660': {'name': 'KODEX 200 고배당', 'type': 'dividend', 'risk': 'low'},
                    '161510': {'name': 'TIGER 배당성장', 'type': 'dividend', 'risk': 'low'}
                }
            },
            'foreign_equity': {
                'us_core': {
                    '139660': {'name': 'TIGER 미국S&P500', 'type': 'large_cap', 'risk': 'medium'},
                    '360200': {'name': 'TIGER 미국S&P500선물', 'type': 'large_cap', 'risk': 'medium'}
                },
                'us_growth': {
                    '117460': {'name': 'KODEX 나스닥100', 'type': 'tech_growth', 'risk': 'high'},
                    '381170': {'name': 'TIGER 나스닥100', 'type': 'tech_growth', 'risk': 'high'}
                },
                'global': {
                    '195930': {'name': 'TIGER 선진국MSCI World', 'type': 'developed', 'risk': 'medium'},
                    '192090': {'name': 'TIGER 신흥국MSCI', 'type': 'emerging', 'risk': 'high'}
                }
            },
            'bonds': {
                'domestic': {
                    '114260': {'name': 'KODEX 국고채10년', 'type': 'government', 'risk': 'very_low'},
                    '139230': {'name': 'TIGER 회사채AA-', 'type': 'corporate', 'risk': 'low'}
                },
                'foreign': {
                    '136340': {'name': 'TIGER 미국채10년', 'type': 'government', 'risk': 'low'},
                    '130730': {'name': 'KODEX 글로벌하이일드', 'type': 'high_yield', 'risk': 'medium'}
                }
            },
            'alternatives': {
                'reits': {
                    '157490': {'name': 'KODEX 리츠', 'type': 'domestic_reit', 'risk': 'medium'},
                    '351590': {'name': 'TIGER 미국리츠', 'type': 'global_reit', 'risk': 'medium'}
                },
                'commodities': {
                    '132030': {'name': 'KODEX 골드선물', 'type': 'gold', 'risk': 'medium'},
                    '130680': {'name': 'TIGER 원유선물', 'type': 'oil', 'risk': 'high'}
                }
            }
        }
    
    def generate_portfolio(self, investment_amount: float, 
                          custom_preferences: Optional[Dict] = None) -> Dict[str, float]:
        """
        생애주기에 맞는 포트폴리오 생성
        
        Args:
            investment_amount: 총 투자금액
            custom_preferences: 사용자 커스텀 설정
            
        Returns:
            ETF 코드별 투자비중 딕셔너리
        """
        try:
            # 위험 성향 반영한 자산배분 계산
            asset_allocation = self._calculate_risk_adjusted_allocation()
            
            # ETF 선택 및 배분
            portfolio = self._select_etfs_for_allocation(asset_allocation, custom_preferences)
            
            logger.info(f"생애주기 포트폴리오 생성 완료 - {self.life_stage} ({len(portfolio)}개 ETF)")
            return portfolio
            
        except Exception as e:
            logger.error(f"포트폴리오 생성 실패: {e}")
            return {}
    
    def _calculate_risk_adjusted_allocation(self) -> Dict[str, float]:
        """위험 성향을 반영한 자산배분 계산"""
        
        base_config = self.current_config
        
        # 위험 성향별 조정
        risk_adjustments = {
            'conservative': {'equity': -10, 'bond': +8, 'alternative': +2},
            'moderate': {'equity': 0, 'bond': 0, 'alternative': 0},
            'aggressive': {'equity': +10, 'bond': -8, 'alternative': -2}
        }
        
        adjustment = risk_adjustments.get(self.risk_tolerance, risk_adjustments['moderate'])
        
        # 조정된 자산배분 계산
        equity_ratio = max(20, min(95, base_config['equity_ratio_base'] + adjustment['equity']))
        bond_ratio = max(5, min(70, base_config['bond_ratio_base'] + adjustment['bond']))
        alternative_ratio = max(0, min(20, base_config['alternative_ratio_base'] + adjustment['alternative']))
        
        # 100% 맞추기 위한 정규화
        total = equity_ratio + bond_ratio + alternative_ratio
        
        allocation = {
            'equity_ratio': equity_ratio / total * 100,
            'bond_ratio': bond_ratio / total * 100,
            'alternative_ratio': alternative_ratio / total * 100,
            'domestic_ratio': base_config['domestic_ratio'],
            'foreign_ratio': base_config['foreign_ratio']
        }
        
        return allocation
    
    def _select_etfs_for_allocation(self, allocation: Dict[str, float], 
                                   preferences: Optional[Dict] = None) -> Dict[str, float]:
        """자산배분에 맞는 ETF 선택"""
        
        portfolio = {}
        
        # 주식 ETF 선택
        equity_weight = allocation['equity_ratio']
        if equity_weight > 0:
            equity_etfs = self._select_equity_etfs(equity_weight, allocation, preferences)
            portfolio.update(equity_etfs)
        
        # 채권 ETF 선택
        bond_weight = allocation['bond_ratio']
        if bond_weight > 0:
            bond_etfs = self._select_bond_etfs(bond_weight, preferences)
            portfolio.update(bond_etfs)
        
        # 대안투자 ETF 선택
        alt_weight = allocation['alternative_ratio']
        if alt_weight > 0:
            alt_etfs = self._select_alternative_etfs(alt_weight, preferences)
            portfolio.update(alt_etfs)
        
        return portfolio
    
    def _select_equity_etfs(self, total_equity_weight: float, 
                           allocation: Dict[str, float], 
                           preferences: Optional[Dict] = None) -> Dict[str, float]:
        """주식 ETF 선택"""
        
        equity_etfs = {}
        
        # 국내/해외 비중 계산
        domestic_weight = total_equity_weight * allocation['domestic_ratio'] / 100
        foreign_weight = total_equity_weight * allocation['foreign_ratio'] / 100
        
        # 국내 주식 ETF
        if domestic_weight > 0:
            if self.life_stage in ['accumulation_early', 'accumulation_growth']:
                # 성장기: 코어 + 성장주
                equity_etfs['069500'] = domestic_weight * 0.7  # 코어
                equity_etfs['229200'] = domestic_weight * 0.3  # 성장
            else:
                # 안정기: 코어 + 배당
                equity_etfs['069500'] = domestic_weight * 0.8  # 코어
                equity_etfs['069660'] = domestic_weight * 0.2  # 배당
        
        # 해외 주식 ETF
        if foreign_weight > 0:
            if self.life_stage in ['accumulation_early', 'accumulation_growth']:
                # 성장기: 미국 중심 + 글로벌
                equity_etfs['139660'] = foreign_weight * 0.5   # 미국 S&P500
                equity_etfs['117460'] = foreign_weight * 0.3   # 나스닥100
                equity_etfs['195930'] = foreign_weight * 0.2   # 선진국
            elif self.life_stage == 'accumulation_mature':
                # 성숙기: 균형 분산
                equity_etfs['139660'] = foreign_weight * 0.4   # 미국 S&P500
                equity_etfs['195930'] = foreign_weight * 0.4   # 선진국
                equity_etfs['192090'] = foreign_weight * 0.2   # 신흥국
            else:
                # 안정기: 선진국 중심
                equity_etfs['139660'] = foreign_weight * 0.5   # 미국 S&P500
                equity_etfs['195930'] = foreign_weight * 0.5   # 선진국
        
        return equity_etfs
    
    def _select_bond_etfs(self, total_bond_weight: float, 
                         preferences: Optional[Dict] = None) -> Dict[str, float]:
        """채권 ETF 선택"""
        
        bond_etfs = {}
        
        if self.life_stage in ['accumulation_early', 'accumulation_growth']:
            # 성장기: 국내 중심
            bond_etfs['114260'] = total_bond_weight * 0.8  # 국고채
            bond_etfs['136340'] = total_bond_weight * 0.2  # 미국채
        elif self.life_stage == 'accumulation_mature':
            # 성숙기: 균형
            bond_etfs['114260'] = total_bond_weight * 0.6  # 국고채
            bond_etfs['136340'] = total_bond_weight * 0.4  # 미국채
        else:
            # 안정기: 다양화
            bond_etfs['114260'] = total_bond_weight * 0.5  # 국고채
            bond_etfs['136340'] = total_bond_weight * 0.3  # 미국채
            bond_etfs['139230'] = total_bond_weight * 0.2  # 회사채
        
        return bond_etfs
    
    def _select_alternative_etfs(self, total_alt_weight: float, 
                               preferences: Optional[Dict] = None) -> Dict[str, float]:
        """대안투자 ETF 선택"""
        
        alt_etfs = {}
        
        if total_alt_weight > 0:
            if self.life_stage == 'retirement':
                # 은퇴기: 리츠 중심 (소득 창출)
                alt_etfs['157490'] = total_alt_weight * 0.6  # 국내 리츠
                alt_etfs['351590'] = total_alt_weight * 0.4  # 미국 리츠
            else:
                # 기타: 리츠 + 금
                alt_etfs['157490'] = total_alt_weight * 0.5  # 국내 리츠
                alt_etfs['132030'] = total_alt_weight * 0.5  # 금
        
        return alt_etfs
    
    def get_lifecycle_projection(self, current_portfolio_value: float, 
                               monthly_contribution: float) -> Dict[str, any]:
        """생애주기 투자 전망"""
        
        projection = {}
        
        # 미래 자산배분 변화 예측
        future_allocations = self._project_future_allocations()
        projection['future_allocations'] = future_allocations
        
        # 예상 포트폴리오 가치 계산
        projected_values = self._calculate_projected_values(
            current_portfolio_value, monthly_contribution
        )
        projection['projected_values'] = projected_values
        
        # 은퇴 준비 상황 평가
        retirement_readiness = self._assess_retirement_readiness(
            current_portfolio_value, monthly_contribution
        )
        projection['retirement_readiness'] = retirement_readiness
        
        # 단계별 전략 조정 제안
        strategy_adjustments = self._suggest_strategy_adjustments()
        projection['strategy_adjustments'] = strategy_adjustments
        
        return projection
    
    def _project_future_allocations(self) -> Dict[int, Dict[str, float]]:
        """미래 자산배분 변화 예측"""
        
        future_allocations = {}
        
        # 5년 간격으로 예측
        for future_age in range(self.age + 5, min(self.age + 30, 85), 5):
            temp_strategy = LifecycleStrategy(future_age, self.retirement_age, self.risk_tolerance)
            allocation = temp_strategy._calculate_risk_adjusted_allocation()
            
            future_allocations[future_age] = {
                'life_stage': temp_strategy.life_stage,
                'equity_ratio': allocation['equity_ratio'],
                'bond_ratio': allocation['bond_ratio'],
                'alternative_ratio': allocation['alternative_ratio']
            }
        
        return future_allocations
    
    def _calculate_projected_values(self, current_value: float, 
                                   monthly_contribution: float) -> Dict[str, float]:
        """예상 포트폴리오 가치 계산"""
        
        # 생애주기별 예상 수익률 (연간)
        expected_returns = {
            'accumulation_early': 0.08,      # 8%
            'accumulation_growth': 0.075,    # 7.5%
            'accumulation_mature': 0.07,     # 7%
            'pre_retirement': 0.06,          # 6%
            'retirement': 0.05               # 5%
        }
        
        current_return = expected_returns.get(self.life_stage, 0.07)
        
        projected_values = {}
        portfolio_value = current_value
        
        for year in range(1, min(self.years_to_retirement + 10, 30)):
            future_age = self.age + year
            
            # 생애주기 단계 변화 반영
            temp_strategy = LifecycleStrategy(future_age, self.retirement_age, self.risk_tolerance)
            stage_return = expected_returns.get(temp_strategy.life_stage, current_return)
            
            # 연간 투자수익률 적용
            portfolio_value *= (1 + stage_return)
            
            # 월 적립금 추가 (은퇴 전까지)
            if future_age < self.retirement_age:
                annual_contribution = monthly_contribution * 12
                portfolio_value += annual_contribution * (1 + stage_return / 2)  # 연중 평균
            
            projected_values[future_age] = portfolio_value
        
        return projected_values
    
    def _assess_retirement_readiness(self, current_value: float, 
                                   monthly_contribution: float) -> Dict[str, any]:
        """은퇴 준비 상황 평가"""
        
        # 은퇴시 필요 자산 추정 (현재 연소득의 10-15배)
        estimated_annual_income = monthly_contribution * 12 / (self.current_config['savings_rate_target'] / 100)
        target_retirement_asset = estimated_annual_income * 12  # 12배 기준
        
        # 현재 저축률로 은퇴시 예상 자산
        projected_values = self._calculate_projected_values(current_value, monthly_contribution)
        retirement_asset = projected_values.get(self.retirement_age, current_value)
        
        # 준비 정도 평가
        readiness_ratio = retirement_asset / target_retirement_asset if target_retirement_asset > 0 else 0
        
        assessment = {
            'target_retirement_asset': target_retirement_asset,
            'projected_retirement_asset': retirement_asset,
            'readiness_ratio': readiness_ratio,
            'status': 'Excellent' if readiness_ratio >= 1.2 else
                     'Good' if readiness_ratio >= 1.0 else
                     'Fair' if readiness_ratio >= 0.8 else 'Poor',
            'shortfall': max(0, target_retirement_asset - retirement_asset),
            'required_monthly_increase': 0
        }
        
        # 부족분을 메우기 위한 추가 적립 필요금액
        if readiness_ratio < 1.0:
            shortfall = assessment['shortfall']
            years_remaining = max(1, self.years_to_retirement)
            
            # 복리 고려한 필요 월 적립액 계산
            monthly_rate = 0.07 / 12  # 연 7% 가정
            months_remaining = years_remaining * 12
            
            if months_remaining > 0:
                required_monthly_increase = shortfall / (
                    ((1 + monthly_rate) ** months_remaining - 1) / monthly_rate
                )
                assessment['required_monthly_increase'] = required_monthly_increase
        
        return assessment
    
    def _suggest_strategy_adjustments(self) -> List[Dict[str, str]]:
        """단계별 전략 조정 제안"""
        
        adjustments = []
        
        if self.life_stage == 'accumulation_early':
            adjustments.extend([
                {
                    'timeframe': '현재',
                    'adjustment': '공격적 성장 투자',
                    'reason': '장기 투자기간으로 높은 변동성 감내 가능',
                    'action': '성장주 ETF 비중 확대'
                },
                {
                    'timeframe': '5년 후',
                    'adjustment': '글로벌 분산 강화',
                    'reason': '자산 규모 증가에 따른 리스크 분산 필요',
                    'action': '해외 ETF 비중 점진적 확대'
                }
            ])
        
        elif self.life_stage == 'accumulation_growth':
            adjustments.extend([
                {
                    'timeframe': '현재',
                    'adjustment': '균형 잡힌 성장 전략',
                    'reason': '안정성과 성장성의 균형 추구',
                    'action': '채권 비중 점진적 확대'
                },
                {
                    'timeframe': '10년 후',
                    'adjustment': '안정성 강화',
                    'reason': '은퇴 준비를 위한 리스크 감소',
                    'action': '주식 비중 단계적 축소'
                }
            ])
        
        elif self.life_stage == 'pre_retirement':
            adjustments.extend([
                {
                    'timeframe': '현재',
                    'adjustment': '자본 보전 중심',
                    'reason': '은퇴 임박으로 안정성 우선',
                    'action': '채권 및 배당주 비중 확대'
                },
                {
                    'timeframe': '은퇴 후',
                    'adjustment': '소득 창출 전략',
                    'reason': '정기적 현금흐름 필요',
                    'action': '배당ETF 및 리츠 비중 확대'
                }
            ])
        
        return adjustments
    
    def evaluate_current_strategy(self, current_portfolio: Dict[str, float]) -> Dict[str, any]:
        """현재 전략 평가"""
        
        evaluation = {
            'lifecycle_alignment': 0,
            'age_appropriateness': 0,
            'risk_suitability': 0,
            'recommendations': []
        }
        
        # 생애주기 적합성 평가
        target_portfolio = self.generate_portfolio(100)  # 100% 기준
        
        # 자산배분 유사도 계산
        alignment_score = self._calculate_allocation_similarity(current_portfolio, target_portfolio)
        evaluation['lifecycle_alignment'] = alignment_score
        
        # 연령 적절성 평가
        age_score = self._evaluate_age_appropriateness(current_portfolio)
        evaluation['age_appropriateness'] = age_score
        
        # 위험 적합성 평가
        risk_score = self._evaluate_risk_suitability(current_portfolio)
        evaluation['risk_suitability'] = risk_score
        
        # 개선 제안
        recommendations = self._generate_improvement_recommendations(
            current_portfolio, target_portfolio
        )
        evaluation['recommendations'] = recommendations
        
        return evaluation
    
    def _calculate_allocation_similarity(self, current: Dict[str, float], 
                                       target: Dict[str, float]) -> float:
        """자산배분 유사도 계산"""
        
        # 자산군별 집계
        current_allocation = self._aggregate_by_asset_class(current)
        target_allocation = self._aggregate_by_asset_class(target)
        
        total_deviation = 0
        for asset_class in ['equity', 'bond', 'alternative']:
            current_weight = current_allocation.get(asset_class, 0)
            target_weight = target_allocation.get(asset_class, 0)
            total_deviation += abs(current_weight - target_weight)
        
        # 유사도 점수 (편차가 적을수록 높은 점수)
        similarity_score = max(0, 100 - total_deviation / 2)
        return similarity_score
    
    def _aggregate_by_asset_class(self, portfolio: Dict[str, float]) -> Dict[str, float]:
        """ETF를 자산군별로 집계"""
        
        # ETF별 자산군 매핑
        asset_mapping = {
            '069500': 'equity', '229200': 'equity', '139660': 'equity',
            '117460': 'equity', '195930': 'equity', '192090': 'equity',
            '114260': 'bond', '136340': 'bond', '139230': 'bond', '130730': 'bond',
            '157490': 'alternative', '351590': 'alternative', '132030': 'alternative'
        }
        
        aggregated = {'equity': 0, 'bond': 0, 'alternative': 0}
        
        for etf_code, weight in portfolio.items():
            asset_class = asset_mapping.get(etf_code, 'other')
            if asset_class in aggregated:
                aggregated[asset_class] += weight
        
        return aggregated
    
    def _evaluate_age_appropriateness(self, portfolio: Dict[str, float]) -> float:
        """연령 적절성 평가"""
        
        allocation = self._aggregate_by_asset_class(portfolio)
        equity_ratio = allocation['equity']
        
        # 연령별 적정 주식 비중 (100 - 나이 규칙 변형)
        if self.age < 30:
            target_equity_range = (80, 95)
        elif self.age < 40:
            target_equity_range = (70, 85)
        elif self.age < 50:
            target_equity_range = (60, 75)
        elif self.age < 65:
            target_equity_range = (50, 65)
        else:
            target_equity_range = (30, 50)
        
        # 적정 범위 내에 있는지 평가
        if target_equity_range[0] <= equity_ratio <= target_equity_range[1]:
            return 100
        elif equity_ratio < target_equity_range[0]:
            deviation = target_equity_range[0] - equity_ratio
        else:
            deviation = equity_ratio - target_equity_range[1]
        
        return max(0, 100 - deviation * 2)
    
    def _evaluate_risk_suitability(self, portfolio: Dict[str, float]) -> float:
        """위험 적합성 평가"""
        
        # 포트폴리오 위험도 계산 (간단화된 버전)
        high_risk_etfs = ['229200', '117460', '192090', '130730']
        medium_risk_etfs = ['139660', '195930', '136340', '157490']
        low_risk_etfs = ['069500', '114260', '139230']
        
        risk_score = 0
        for etf_code, weight in portfolio.items():
            if etf_code in high_risk_etfs:
                risk_score += weight * 3
            elif etf_code in medium_risk_etfs:
                risk_score += weight * 2
            elif etf_code in low_risk_etfs:
                risk_score += weight * 1
        
        # 위험 성향별 적정 범위
        target_ranges = {
            'conservative': (100, 180),
            'moderate': (150, 250),
            'aggressive': (200, 300)
        }
        
        target_range = target_ranges.get(self.risk_tolerance, target_ranges['moderate'])
        
        if target_range[0] <= risk_score <= target_range[1]:
            return 100
        elif risk_score < target_range[0]:
            return max(0, 100 - (target_range[0] - risk_score) * 2)
        else:
            return max(0, 100 - (risk_score - target_range[1]) * 2)
    
    def _generate_improvement_recommendations(self, current: Dict[str, float], 
                                            target: Dict[str, float]) -> List[str]:
        """개선 제안 생성"""
        
        recommendations = []
        
        current_allocation = self._aggregate_by_asset_class(current)
        target_allocation = self._aggregate_by_asset_class(target)
        
        # 주식 비중 검토
        equity_diff = target_allocation['equity'] - current_allocation['equity']
        if abs(equity_diff) > 10:
            if equity_diff > 0:
                recommendations.append(f"주식 비중을 {equity_diff:.1f}%p 늘려 {target_allocation['equity']:.1f}%로 조정하세요")
            else:
                recommendations.append(f"주식 비중을 {abs(equity_diff):.1f}%p 줄여 {target_allocation['equity']:.1f}%로 조정하세요")
        
        # 채권 비중 검토
        bond_diff = target_allocation['bond'] - current_allocation['bond']
        if abs(bond_diff) > 5:
            if bond_diff > 0:
                recommendations.append(f"채권 비중을 {bond_diff:.1f}%p 늘려 안정성을 높이세요")
            else:
                recommendations.append(f"채권 비중을 {abs(bond_diff):.1f}%p 줄여도 됩니다")
        
        # 생애주기별 특별 제안
        if self.life_stage == 'accumulation_early':
            recommendations.append("젊은 나이의 장점을 활용해 성장주 투자를 늘려보세요")
        elif self.life_stage == 'pre_retirement':
            recommendations.append("은퇴가 가까워지니 안정적인 자산의 비중을 늘리세요")
        elif self.life_stage == 'retirement':
            recommendations.append("정기적인 소득 창출을 위해 배당 ETF나 리츠 비중을 고려하세요")
        
        return recommendations
    
    def get_strategy_description(self) -> Dict[str, any]:
        """전략 설명 반환"""
        
        return {
            'strategy_name': f"생애주기 투자전략 ({self.current_config['name']})",
            'current_life_stage': self.life_stage,
            'years_to_retirement': self.years_to_retirement,
            'description': self.current_config['description'],
            'key_principles': [
                "나이에 따른 위험 감내 능력 반영",
                "시간에 따른 자산배분 자동 조정",
                "목표 지향적 투자 계획",
                "생애주기별 최적화"
            ],
            'current_allocation_guide': {
                '주식': f"{self.current_config['equity_ratio_base']}% (±{10 if self.risk_tolerance == 'aggressive' else 5}%)",
                '채권': f"{self.current_config['bond_ratio_base']}%",
                '대안투자': f"{self.current_config['alternative_ratio_base']}%"
            },
            'target_savings_rate': f"소득의 {self.current_config['savings_rate_target']}%",
            'rebalancing_frequency': "연 2회 (연령 변화 반영)",
            'next_stage_transition': f"{self.age + 5}세 경 다음 단계로 전환",
            'advantages': [
                "자동적인 위험 관리",
                "명확한 투자 가이드라인",
                "장기 목표 달성 최적화",
                "감정적 투자 실수 방지"
            ],
            'suitable_for': "체계적인 장기 투자를 원하는 모든 연령층"
        }