"""
글로벌 분산 투자전략 모듈
전 세계 주요 시장에 분산투자하는 4분할 전략 (간편형)
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class GlobalDiversifiedStrategy:
    """글로벌 분산 투자전략 클래스"""
    
    def __init__(self, strategy_variant: str = 'balanced'):
        """
        글로벌 분산전략 초기화
        
        Args:
            strategy_variant: 전략 변형 ('simple', 'balanced', 'growth_focused')
        """
        self.strategy_variant = strategy_variant
        
        # 전략별 기본 설정
        self.strategy_configs = {
            'simple': {
                'name': '글로벌 4분할 (간편형)',
                'description': '국내/미국/선진국/채권으로 단순 분할',
                'domestic_equity': 25,
                'us_equity': 30,
                'developed_equity': 25,
                'bonds': 20,
                'emerging_equity': 0,
                'alternatives': 0,
                'rebalancing_threshold': 5.0,
                'target_etfs': 4
            },
            'balanced': {
                'name': '글로벌 균형 분산',
                'description': '6개 자산군에 균형 분산투자',
                'domestic_equity': 20,
                'us_equity': 25,
                'developed_equity': 20,
                'emerging_equity': 10,
                'bonds': 20,
                'alternatives': 5,
                'rebalancing_threshold': 5.0,
                'target_etfs': 6
            },
            'growth_focused': {
                'name': '글로벌 성장 중심',
                'description': '성장 시장 위주의 공격적 분산',
                'domestic_equity': 15,
                'us_equity': 35,
                'developed_equity': 15,
                'emerging_equity': 20,
                'bonds': 10,
                'alternatives': 5,
                'rebalancing_threshold': 7.0,
                'target_etfs': 6
            }
        }
        
        self.config = self.strategy_configs.get(strategy_variant, self.strategy_configs['balanced'])
        
        # 자산군별 추천 ETF 설정
        self._setup_recommended_etfs()
    
    def _setup_recommended_etfs(self):
        """자산군별 추천 ETF 설정"""
        
        self.recommended_etfs = {
            'domestic_equity': {
                'primary': {
                    '069500': {'name': 'KODEX 200', 'expense_ratio': 0.15, 'priority': 1},
                    '229200': {'name': 'KODEX 코스닥150', 'expense_ratio': 0.15, 'priority': 2}
                },
                'alternative': {
                    '152100': {'name': 'TIGER 코스피', 'expense_ratio': 0.06, 'priority': 1}
                }
            },
            'us_equity': {
                'primary': {
                    '139660': {'name': 'TIGER 미국S&P500', 'expense_ratio': 0.045, 'priority': 1},
                    '117460': {'name': 'KODEX 나스닥100', 'expense_ratio': 0.045, 'priority': 2}
                },
                'alternative': {
                    '360200': {'name': 'TIGER 미국S&P500선물', 'expense_ratio': 0.05, 'priority': 2}
                }
            },
            'developed_equity': {
                'primary': {
                    '195930': {'name': 'TIGER 선진국MSCI World', 'expense_ratio': 0.08, 'priority': 1},
                    '238720': {'name': 'KODEX 선진국MSCI World', 'expense_ratio': 0.08, 'priority': 2}
                },
                'alternative': {
                    '143850': {'name': 'TIGER 유럽STOXX600', 'expense_ratio': 0.19, 'priority': 1},
                    '243890': {'name': 'TIGER 일본니케이225', 'expense_ratio': 0.19, 'priority': 2}
                }
            },
            'emerging_equity': {
                'primary': {
                    '192090': {'name': 'TIGER 신흥국MSCI', 'expense_ratio': 0.49, 'priority': 1},
                    '160570': {'name': 'TIGER 중국CSI300', 'expense_ratio': 0.19, 'priority': 2}
                },
                'alternative': {
                    '238490': {'name': 'KODEX 신흥국MSCI', 'expense_ratio': 0.49, 'priority': 1}
                }
            },
            'bonds': {
                'primary': {
                    '114260': {'name': 'KODEX 국고채10년', 'expense_ratio': 0.15, 'priority': 1},
                    '136340': {'name': 'TIGER 미국채10년', 'expense_ratio': 0.14, 'priority': 2}
                },
                'alternative': {
                    '130730': {'name': 'KODEX 글로벌하이일드', 'expense_ratio': 0.45, 'priority': 1},
                    '139230': {'name': 'TIGER 회사채AA-', 'expense_ratio': 0.15, 'priority': 2}
                }
            },
            'alternatives': {
                'primary': {
                    '157490': {'name': 'KODEX 리츠', 'expense_ratio': 0.5, 'priority': 1},
                    '132030': {'name': 'KODEX 골드선물', 'expense_ratio': 0.49, 'priority': 2}
                },
                'alternative': {
                    '351590': {'name': 'TIGER 미국리츠', 'expense_ratio': 0.49, 'priority': 1}
                }
            }
        }
    
    def generate_portfolio(self, investment_amount: float, 
                          etf_preferences: Optional[Dict] = None) -> Dict[str, float]:
        """
        글로벌 분산 포트폴리오 생성
        
        Args:
            investment_amount: 총 투자금액
            etf_preferences: ETF 선호도 설정
            
        Returns:
            ETF 코드별 투자비중 딕셔너리
        """
        try:
            portfolio = {}
            
            # 자산군별 비중에 따라 ETF 선택
            asset_allocations = {
                'domestic_equity': self.config['domestic_equity'],
                'us_equity': self.config['us_equity'],
                'developed_equity': self.config['developed_equity'],
                'emerging_equity': self.config['emerging_equity'],
                'bonds': self.config['bonds'],
                'alternatives': self.config['alternatives']
            }
            
            for asset_class, allocation in asset_allocations.items():
                if allocation > 0:
                    selected_etf = self._select_etf_for_asset_class(
                        asset_class, etf_preferences
                    )
                    
                    if selected_etf:
                        portfolio[selected_etf] = allocation
            
            # 비중 정규화 (100%가 되도록)
            total_weight = sum(portfolio.values())
            if total_weight > 0:
                portfolio = {etf_code: weight / total_weight * 100 
                           for etf_code, weight in portfolio.items()}
            
            logger.info(f"글로벌 분산 포트폴리오 생성 완료 ({len(portfolio)}개 ETF)")
            return portfolio
            
        except Exception as e:
            logger.error(f"포트폴리오 생성 실패: {e}")
            return {}
    
    def _select_etf_for_asset_class(self, asset_class: str, 
                                   preferences: Optional[Dict] = None) -> Optional[str]:
        """자산군별 최적 ETF 선택"""
        
        available_etfs = self.recommended_etfs.get(asset_class, {})
        
        # 사용자 선호도가 있는 경우
        if preferences and asset_class in preferences:
            preferred_etf = preferences[asset_class]
            if preferred_etf in available_etfs.get('primary', {}):
                return preferred_etf
            if preferred_etf in available_etfs.get('alternative', {}):
                return preferred_etf
        
        # 기본 선택: 우선순위 1번 ETF
        primary_etfs = available_etfs.get('primary', {})
        if primary_etfs:
            # 우선순위와 비용을 고려한 선택
            best_etf = min(primary_etfs.items(), 
                          key=lambda x: (x[1]['priority'], x[1]['expense_ratio']))
            return best_etf[0]
        
        return None
    
    def get_portfolio_variants(self) -> Dict[str, Dict]:
        """포트폴리오 변형들 반환"""
        variants = {}
        
        for variant_name, config in self.strategy_configs.items():
            temp_strategy = GlobalDiversifiedStrategy(variant_name)
            portfolio = temp_strategy.generate_portfolio(100)  # 100% 기준
            
            variants[variant_name] = {
                'name': config['name'],
                'description': config['description'],
                'allocation': portfolio,
                'expected_etfs': config['target_etfs'],
                'risk_level': self._assess_risk_level(config),
                'rebalancing_threshold': config['rebalancing_threshold']
            }
        
        return variants
    
    def _assess_risk_level(self, config: Dict) -> str:
        """포트폴리오 위험 수준 평가"""
        equity_ratio = (config['domestic_equity'] + config['us_equity'] + 
                       config['developed_equity'] + config['emerging_equity'])
        
        if equity_ratio <= 60:
            return 'Conservative'
        elif equity_ratio <= 80:
            return 'Moderate'
        else:
            return 'Aggressive'
    
    def analyze_geographic_allocation(self, portfolio: Dict[str, float]) -> Dict[str, float]:
        """지역별 자산배분 분석"""
        
        # ETF별 지역 매핑
        geographic_mapping = {
            '069500': 'Korea',
            '229200': 'Korea',
            '152100': 'Korea',
            '139660': 'US',
            '117460': 'US',
            '360200': 'US',
            '195930': 'Developed',
            '238720': 'Developed',
            '143850': 'Europe',
            '243890': 'Japan',
            '192090': 'Emerging',
            '160570': 'China',
            '238490': 'Emerging',
            '114260': 'Korea',
            '136340': 'US',
            '130730': 'Global',
            '139230': 'Korea',
            '157490': 'Korea',
            '132030': 'Global',
            '351590': 'US'
        }
        
        geographic_allocation = {}
        
        for etf_code, weight in portfolio.items():
            region = geographic_mapping.get(etf_code, 'Other')
            
            if region in geographic_allocation:
                geographic_allocation[region] += weight
            else:
                geographic_allocation[region] = weight
        
        return geographic_allocation
    
    def analyze_currency_exposure(self, portfolio: Dict[str, float]) -> Dict[str, float]:
        """통화별 노출도 분석"""
        
        # ETF별 통화 매핑
        currency_mapping = {
            '069500': 'KRW',
            '229200': 'KRW',
            '152100': 'KRW',
            '139660': 'USD',
            '117460': 'USD',
            '360200': 'USD',
            '195930': 'USD',
            '238720': 'USD',
            '143850': 'EUR',
            '243890': 'JPY',
            '192090': 'USD',
            '160570': 'CNY',
            '238490': 'USD',
            '114260': 'KRW',
            '136340': 'USD',
            '130730': 'USD',
            '139230': 'KRW',
            '157490': 'KRW',
            '132030': 'USD',
            '351590': 'USD'
        }
        
        currency_exposure = {}
        
        for etf_code, weight in portfolio.items():
            currency = currency_mapping.get(etf_code, 'Other')
            
            if currency in currency_exposure:
                currency_exposure[currency] += weight
            else:
                currency_exposure[currency] = weight
        
        return currency_exposure
    
    def calculate_portfolio_cost(self, portfolio: Dict[str, float]) -> Dict[str, float]:
        """포트폴리오 총 비용 계산"""
        
        total_expense_ratio = 0
        weighted_costs = {}
        
        for etf_code, weight in portfolio.items():
            # ETF별 비용 찾기
            expense_ratio = self._get_etf_expense_ratio(etf_code)
            
            weighted_cost = expense_ratio * weight / 100
            total_expense_ratio += weighted_cost
            
            weighted_costs[etf_code] = {
                'weight': weight,
                'expense_ratio': expense_ratio,
                'weighted_cost': weighted_cost
            }
        
        return {
            'total_expense_ratio': total_expense_ratio,
            'annual_cost_per_100k': total_expense_ratio * 1000,  # 100만원당 연간 비용
            'etf_costs': weighted_costs
        }
    
    def _get_etf_expense_ratio(self, etf_code: str) -> float:
        """ETF 운용보수 조회"""
        
        # 모든 자산군에서 ETF 찾기
        for asset_class, etf_groups in self.recommended_etfs.items():
            for group_name, etfs in etf_groups.items():
                if etf_code in etfs:
                    return etfs[etf_code]['expense_ratio']
        
        # 기본값 (정보가 없는 경우)
        return 0.5
    
    def get_rebalancing_calendar(self) -> Dict[str, str]:
        """리밸런싱 캘린더 제안"""
        
        calendar = {
            'frequency': 'Quarterly',
            'suggested_dates': [
                '3월 마지막 주 (1분기 정산)',
                '6월 마지막 주 (2분기 정산)',
                '9월 마지막 주 (3분기 정산)',
                '12월 마지막 주 (연말 정산)'
            ],
            'threshold_check': 'Monthly',
            'emergency_rebalancing': '20% 이상 이탈시 즉시',
            'considerations': [
                '분기 배당금 지급일 이후 실시',
                '환율 급변동시 추가 고려',
                '세금 효율성을 위한 연말 조정',
                '신규 투자금 활용한 자연스러운 조정'
            ]
        }
        
        return calendar
    
    def evaluate_portfolio_efficiency(self, portfolio: Dict[str, float]) -> Dict[str, any]:
        """포트폴리오 효율성 평가"""
        
        evaluation = {
            'diversification_score': 0,
            'cost_efficiency': 0,
            'simplicity_score': 0,
            'global_coverage': 0,
            'overall_score': 0,
            'strengths': [],
            'weaknesses': [],
            'recommendations': []
        }
        
        # 분산투자 점수 (ETF 개수와 자산군 분산도)
        num_etfs = len(portfolio)
        geographic_allocation = self.analyze_geographic_allocation(portfolio)
        num_regions = len([region for region, weight in geographic_allocation.items() if weight >= 5])
        
        diversification_score = min(100, (num_regions / 4) * 50 + (min(num_etfs, 6) / 6) * 50)
        evaluation['diversification_score'] = diversification_score
        
        # 비용 효율성 (낮은 비용일수록 높은 점수)
        cost_info = self.calculate_portfolio_cost(portfolio)
        avg_expense_ratio = cost_info['total_expense_ratio']
        cost_efficiency = max(0, 100 - avg_expense_ratio * 200)  # 0.5% 기준
        evaluation['cost_efficiency'] = cost_efficiency
        
        # 단순성 점수 (ETF 개수가 적을수록 높은 점수)
        simplicity_score = max(0, 100 - (num_etfs - 4) * 15)
        evaluation['simplicity_score'] = simplicity_score
        
        # 글로벌 커버리지 점수
        global_coverage = (num_regions / 5) * 100  # 최대 5개 지역
        evaluation['global_coverage'] = min(100, global_coverage)
        
        # 종합 점수
        overall_score = (diversification_score * 0.3 + cost_efficiency * 0.25 + 
                        simplicity_score * 0.2 + global_coverage * 0.25)
        evaluation['overall_score'] = overall_score
        
        # 강점과 약점 분석
        if diversification_score >= 80:
            evaluation['strengths'].append("우수한 분산투자")
        else:
            evaluation['weaknesses'].append("분산투자 부족")
            evaluation['recommendations'].append("더 많은 지역과 자산군에 분산투자하세요")
        
        if cost_efficiency >= 70:
            evaluation['strengths'].append("합리적인 비용 구조")
        else:
            evaluation['weaknesses'].append("높은 운용 비용")
            evaluation['recommendations'].append("더 저비용 ETF로 교체를 고려하세요")
        
        if simplicity_score >= 70:
            evaluation['strengths'].append("관리하기 쉬운 구조")
        else:
            evaluation['weaknesses'].append("복잡한 포트폴리오 구조")
            evaluation['recommendations'].append("핵심 ETF로 단순화하세요")
        
        if global_coverage >= 70:
            evaluation['strengths'].append("광범위한 글로벌 노출")
        else:
            evaluation['weaknesses'].append("제한적인 글로벌 노출")
            evaluation['recommendations'].append("해외 시장 비중을 늘리세요")
        
        return evaluation
    
    def get_strategy_description(self) -> Dict[str, str]:
        """전략 설명 반환"""
        
        return {
            'strategy_name': f"글로벌 분산 투자전략 ({self.strategy_variant})",
            'overview': self.config['description'],
            'philosophy': "전 세계 주요 시장에 분산투자하여 지역별 리스크를 최소화하고 안정적인 성장 추구",
            'target_allocation': {
                '국내 주식': f"{self.config['domestic_equity']}%",
                '미국 주식': f"{self.config['us_equity']}%",
                '선진국 주식': f"{self.config['developed_equity']}%",
                '신흥국 주식': f"{self.config['emerging_equity']}%",
                '채권': f"{self.config['bonds']}%",
                '대안투자': f"{self.config['alternatives']}%"
            },
            'advantages': [
                "지역별 리스크 분산",
                "환율 다변화 효과",
                "글로벌 경제 성장 참여",
                "관리 용이성",
                "투명한 자산배분"
            ],
            'considerations': [
                "환율 변동 위험",
                "해외 세금 이슈",
                "정보 접근성 한계",
                "시차에 따른 거래 제약"
            ],
            'suitable_for': "글로벌 투자를 원하는 중장기 투자자",
            'minimum_investment': "월 30만원 이상 권장",
            'rebalancing_frequency': "분기별",
            'expected_return': "연 6-9% (변형에 따라 상이)",
            'risk_level': self._assess_risk_level(self.config)
        }