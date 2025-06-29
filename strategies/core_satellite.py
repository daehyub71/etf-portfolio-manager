"""
코어-새틀라이트 투자전략 모듈
안정적인 코어 자산(70-80%)과 성장성 있는 새틀라이트 자산(20-30%)으로 구성
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class CoreSatelliteStrategy:
    """코어-새틀라이트 전략 클래스"""
    
    def __init__(self, core_ratio: float = 0.8, risk_level: str = 'moderate'):
        """
        코어-새틀라이트 전략 초기화
        
        Args:
            core_ratio: 코어 자산 비중 (0.7-0.8 권장)
            risk_level: 위험 수준 ('conservative', 'moderate', 'aggressive')
        """
        self.core_ratio = core_ratio
        self.satellite_ratio = 1.0 - core_ratio
        self.risk_level = risk_level
        
        # 위험 수준별 설정
        self.risk_configs = {
            'conservative': {
                'core_equity_ratio': 0.6,
                'core_bond_ratio': 0.4,
                'satellite_growth_ratio': 0.3,
                'satellite_theme_ratio': 0.2,
                'satellite_alternative_ratio': 0.5,
                'rebalancing_threshold': 3.0
            },
            'moderate': {
                'core_equity_ratio': 0.7,
                'core_bond_ratio': 0.3,
                'satellite_growth_ratio': 0.4,
                'satellite_theme_ratio': 0.3,
                'satellite_alternative_ratio': 0.3,
                'rebalancing_threshold': 5.0
            },
            'aggressive': {
                'core_equity_ratio': 0.8,
                'core_bond_ratio': 0.2,
                'satellite_growth_ratio': 0.5,
                'satellite_theme_ratio': 0.4,
                'satellite_alternative_ratio': 0.1,
                'rebalancing_threshold': 7.0
            }
        }
        
        self.config = self.risk_configs.get(risk_level, self.risk_configs['moderate'])
        
        # 추천 ETF 포트폴리오 설정
        self._setup_recommended_etfs()
    
    def _setup_recommended_etfs(self):
        """위험 수준별 추천 ETF 설정"""
        
        # 코어 ETF (안정적이고 광범위한 분산투자)
        self.core_etfs = {
            'conservative': {
                '069500': {'weight': 0.3, 'name': 'KODEX 200', 'type': 'domestic_equity'},
                '139660': {'weight': 0.3, 'name': 'TIGER 미국S&P500', 'type': 'foreign_equity'},
                '114260': {'weight': 0.25, 'name': 'KODEX 국고채10년', 'type': 'domestic_bond'},
                '136340': {'weight': 0.15, 'name': 'TIGER 미국채10년', 'type': 'foreign_bond'}
            },
            'moderate': {
                '069500': {'weight': 0.35, 'name': 'KODEX 200', 'type': 'domestic_equity'},
                '139660': {'weight': 0.35, 'name': 'TIGER 미국S&P500', 'type': 'foreign_equity'},
                '114260': {'weight': 0.2, 'name': 'KODEX 국고채10년', 'type': 'domestic_bond'},
                '136340': {'weight': 0.1, 'name': 'TIGER 미국채10년', 'type': 'foreign_bond'}
            },
            'aggressive': {
                '069500': {'weight': 0.25, 'name': 'KODEX 200', 'type': 'domestic_equity'},
                '139660': {'weight': 0.4, 'name': 'TIGER 미국S&P500', 'type': 'foreign_equity'},
                '195930': {'weight': 0.15, 'name': 'TIGER 선진국MSCI World', 'type': 'foreign_equity'},
                '114260': {'weight': 0.15, 'name': 'KODEX 국고채10년', 'type': 'domestic_bond'},
                '136340': {'weight': 0.05, 'name': 'TIGER 미국채10년', 'type': 'foreign_bond'}
            }
        }
        
        # 새틀라이트 ETF (성장성, 테마, 대안투자)
        self.satellite_etfs = {
            'growth': {
                '229200': {'weight': 0.4, 'name': 'KODEX 코스닥150', 'type': 'domestic_growth'},
                '117460': {'weight': 0.3, 'name': 'KODEX 나스닥100', 'type': 'us_tech'},
                '192090': {'weight': 0.3, 'name': 'TIGER 신흥국MSCI', 'type': 'emerging_market'}
            },
            'thematic': {
                '305540': {'weight': 0.3, 'name': 'KODEX 2차전지산업', 'type': 'battery_theme'},
                '091160': {'weight': 0.25, 'name': 'KODEX 반도체', 'type': 'semiconductor'},
                '148020': {'weight': 0.25, 'name': 'KODEX ESG Korea', 'type': 'esg_theme'},
                '160570': {'weight': 0.2, 'name': 'TIGER 중국CSI300', 'type': 'china_equity'}
            },
            'alternative': {
                '157490': {'weight': 0.4, 'name': 'KODEX 리츠', 'type': 'domestic_reit'},
                '351590': {'weight': 0.3, 'name': 'TIGER 미국리츠', 'type': 'us_reit'},
                '132030': {'weight': 0.3, 'name': 'KODEX 골드선물', 'type': 'gold_commodity'}
            }
        }
    
    def generate_portfolio(self, investment_amount: float, 
                          custom_preferences: Optional[Dict] = None) -> Dict[str, float]:
        """
        코어-새틀라이트 포트폴리오 생성
        
        Args:
            investment_amount: 총 투자금액
            custom_preferences: 사용자 커스텀 선호도
            
        Returns:
            ETF 코드별 투자금액 딕셔너리
        """
        try:
            portfolio = {}
            
            # 코어 포트폴리오 구성
            core_amount = investment_amount * self.core_ratio
            core_etfs = self.core_etfs[self.risk_level]
            
            for etf_code, etf_info in core_etfs.items():
                portfolio[etf_code] = core_amount * etf_info['weight']
            
            # 새틀라이트 포트폴리오 구성
            satellite_amount = investment_amount * self.satellite_ratio
            
            growth_amount = satellite_amount * self.config['satellite_growth_ratio']
            theme_amount = satellite_amount * self.config['satellite_theme_ratio']
            alternative_amount = satellite_amount * self.config['satellite_alternative_ratio']
            
            # 성장주 ETF
            growth_etfs = self._select_satellite_etfs('growth', custom_preferences)
            for etf_code, weight in growth_etfs.items():
                if etf_code in portfolio:
                    portfolio[etf_code] += growth_amount * weight
                else:
                    portfolio[etf_code] = growth_amount * weight
            
            # 테마 ETF
            theme_etfs = self._select_satellite_etfs('thematic', custom_preferences)
            for etf_code, weight in theme_etfs.items():
                if etf_code in portfolio:
                    portfolio[etf_code] += theme_amount * weight
                else:
                    portfolio[etf_code] = theme_amount * weight
            
            # 대안투자 ETF
            alternative_etfs = self._select_satellite_etfs('alternative', custom_preferences)
            for etf_code, weight in alternative_etfs.items():
                if etf_code in portfolio:
                    portfolio[etf_code] += alternative_amount * weight
                else:
                    portfolio[etf_code] = alternative_amount * weight
            
            # 투자금액을 비중으로 변환
            total_amount = sum(portfolio.values())
            portfolio_weights = {etf_code: amount / total_amount * 100 
                               for etf_code, amount in portfolio.items()}
            
            logger.info(f"코어-새틀라이트 포트폴리오 생성 완료 ({len(portfolio_weights)}개 ETF)")
            return portfolio_weights
            
        except Exception as e:
            logger.error(f"포트폴리오 생성 실패: {e}")
            return {}
    
    def _select_satellite_etfs(self, category: str, 
                              custom_preferences: Optional[Dict] = None) -> Dict[str, float]:
        """새틀라이트 ETF 선택"""
        available_etfs = self.satellite_etfs.get(category, {})
        
        if custom_preferences and category in custom_preferences:
            # 사용자 선호도 반영
            preferred_etfs = custom_preferences[category]
            selected_etfs = {}
            
            total_weight = 0
            for etf_code in preferred_etfs:
                if etf_code in available_etfs:
                    selected_etfs[etf_code] = available_etfs[etf_code]['weight']
                    total_weight += available_etfs[etf_code]['weight']
            
            # 가중치 정규화
            if total_weight > 0:
                for etf_code in selected_etfs:
                    selected_etfs[etf_code] /= total_weight
            
            return selected_etfs
        
        # 기본 선택
        return {etf_code: etf_info['weight'] for etf_code, etf_info in available_etfs.items()}
    
    def evaluate_current_portfolio(self, current_holdings: Dict[str, float]) -> Dict[str, any]:
        """현재 포트폴리오 평가"""
        try:
            evaluation = {
                'core_ratio': 0,
                'satellite_ratio': 0,
                'asset_allocation': {},
                'strategy_alignment': 0,
                'recommendations': []
            }
            
            # 코어/새틀라이트 비율 계산
            core_etfs = set(self.core_etfs[self.risk_level].keys())
            
            total_weight = sum(current_holdings.values())
            core_weight = sum(weight for etf_code, weight in current_holdings.items() 
                            if etf_code in core_etfs)
            
            evaluation['core_ratio'] = core_weight / total_weight * 100 if total_weight > 0 else 0
            evaluation['satellite_ratio'] = 100 - evaluation['core_ratio']
            
            # 자산배분 분석
            asset_allocation = self._analyze_asset_allocation(current_holdings)
            evaluation['asset_allocation'] = asset_allocation
            
            # 전략 일치도 평가
            alignment_score = self._calculate_strategy_alignment(current_holdings)
            evaluation['strategy_alignment'] = alignment_score
            
            # 추천사항 생성
            recommendations = self._generate_recommendations(current_holdings, evaluation)
            evaluation['recommendations'] = recommendations
            
            return evaluation
            
        except Exception as e:
            logger.error(f"포트폴리오 평가 실패: {e}")
            return {}
    
    def _analyze_asset_allocation(self, holdings: Dict[str, float]) -> Dict[str, float]:
        """자산배분 분석"""
        asset_allocation = {
            'domestic_equity': 0,
            'foreign_equity': 0,
            'domestic_bond': 0,
            'foreign_bond': 0,
            'alternative': 0,
            'thematic': 0
        }
        
        # ETF별 자산군 매핑 (실제로는 ETF 유니버스에서 가져와야 함)
        asset_mapping = {
            '069500': 'domestic_equity',
            '139660': 'foreign_equity',
            '117460': 'foreign_equity',
            '229200': 'domestic_equity',
            '195930': 'foreign_equity',
            '192090': 'foreign_equity',
            '114260': 'domestic_bond',
            '136340': 'foreign_bond',
            '157490': 'alternative',
            '351590': 'alternative',
            '132030': 'alternative',
            '305540': 'thematic',
            '091160': 'thematic',
            '148020': 'thematic',
            '160570': 'foreign_equity'
        }
        
        total_weight = sum(holdings.values())
        
        for etf_code, weight in holdings.items():
            asset_class = asset_mapping.get(etf_code, 'other')
            if asset_class in asset_allocation:
                asset_allocation[asset_class] += weight / total_weight * 100
        
        return asset_allocation
    
    def _calculate_strategy_alignment(self, holdings: Dict[str, float]) -> float:
        """전략 일치도 계산"""
        try:
            target_portfolio = self.generate_portfolio(100)  # 100% 기준
            
            alignment_score = 0
            total_etfs = set(list(holdings.keys()) + list(target_portfolio.keys()))
            
            for etf_code in total_etfs:
                current_weight = holdings.get(etf_code, 0)
                target_weight = target_portfolio.get(etf_code, 0)
                
                # 절대 편차 계산 (낮을수록 좋음)
                deviation = abs(current_weight - target_weight)
                alignment_score += deviation
            
            # 100점 만점으로 변환 (편차가 작을수록 높은 점수)
            max_possible_deviation = 200  # 최대 편차 (모든 비중이 다른 경우)
            alignment_score = max(0, 100 - (alignment_score / max_possible_deviation * 100))
            
            return alignment_score
            
        except Exception as e:
            logger.warning(f"전략 일치도 계산 실패: {e}")
            return 0
    
    def _generate_recommendations(self, holdings: Dict[str, float], 
                                evaluation: Dict[str, any]) -> List[str]:
        """추천사항 생성"""
        recommendations = []
        
        # 코어/새틀라이트 비율 점검
        target_core_ratio = self.core_ratio * 100
        current_core_ratio = evaluation['core_ratio']
        
        if abs(current_core_ratio - target_core_ratio) > 5:
            if current_core_ratio < target_core_ratio:
                recommendations.append(f"코어 자산 비중을 {target_core_ratio:.1f}%로 늘리세요 (현재 {current_core_ratio:.1f}%)")
            else:
                recommendations.append(f"새틀라이트 자산 비중을 늘리고 코어 자산을 {target_core_ratio:.1f}%로 줄이세요")
        
        # 자산배분 점검
        asset_allocation = evaluation['asset_allocation']
        
        # 주식 비중 점검
        equity_ratio = asset_allocation['domestic_equity'] + asset_allocation['foreign_equity']
        target_equity_ratio = (self.config['core_equity_ratio'] * self.core_ratio + 
                             self.config['satellite_growth_ratio'] * self.satellite_ratio) * 100
        
        if abs(equity_ratio - target_equity_ratio) > 10:
            if equity_ratio < target_equity_ratio:
                recommendations.append(f"주식 비중을 {target_equity_ratio:.1f}%로 늘리세요 (현재 {equity_ratio:.1f}%)")
            else:
                recommendations.append(f"주식 비중이 과도합니다. {target_equity_ratio:.1f}%로 줄이세요")
        
        # 채권 비중 점검
        bond_ratio = asset_allocation['domestic_bond'] + asset_allocation['foreign_bond']
        target_bond_ratio = self.config['core_bond_ratio'] * self.core_ratio * 100
        
        if bond_ratio < target_bond_ratio - 5:
            recommendations.append(f"채권 비중을 {target_bond_ratio:.1f}%로 늘려 안정성을 높이세요")
        
        # 분산투자 점검
        if len(holdings) < 4:
            recommendations.append("더 많은 ETF로 분산투자를 고려하세요")
        elif len(holdings) > 12:
            recommendations.append("너무 많은 ETF로 구성되어 있습니다. 핵심 ETF로 단순화하세요")
        
        # 전략 일치도 점검
        if evaluation['strategy_alignment'] < 70:
            recommendations.append("현재 포트폴리오가 코어-새틀라이트 전략과 많이 다릅니다. 리밸런싱을 고려하세요")
        
        return recommendations
    
    def get_rebalancing_plan(self, current_holdings: Dict[str, float], 
                           target_amount: float) -> Dict[str, Dict]:
        """리밸런싱 계획 수립"""
        try:
            target_portfolio = self.generate_portfolio(target_amount)
            
            rebalancing_plan = {}
            all_etfs = set(list(current_holdings.keys()) + list(target_portfolio.keys()))
            
            for etf_code in all_etfs:
                current_weight = current_holdings.get(etf_code, 0)
                target_weight = target_portfolio.get(etf_code, 0)
                
                # 현재 금액과 목표 금액
                current_amount = target_amount * current_weight / 100
                target_amount_etf = target_amount * target_weight / 100
                
                difference = target_amount_etf - current_amount
                
                if abs(difference) > target_amount * 0.01:  # 1% 이상 차이가 있는 경우
                    action = 'BUY' if difference > 0 else 'SELL'
                    
                    rebalancing_plan[etf_code] = {
                        'action': action,
                        'current_weight': current_weight,
                        'target_weight': target_weight,
                        'current_amount': current_amount,
                        'target_amount': target_amount_etf,
                        'difference_amount': abs(difference),
                        'priority': abs(difference) / target_amount * 100
                    }
            
            # 우선순위별 정렬
            sorted_plan = dict(sorted(rebalancing_plan.items(), 
                                    key=lambda x: x[1]['priority'], reverse=True))
            
            return sorted_plan
            
        except Exception as e:
            logger.error(f"리밸런싱 계획 수립 실패: {e}")
            return {}
    
    def calculate_expected_performance(self) -> Dict[str, float]:
        """예상 성과 계산 (백테스팅 기반)"""
        # 실제로는 과거 데이터를 바탕으로 계산해야 함
        # 여기서는 일반적인 예상치를 제공
        
        performance_estimates = {
            'conservative': {
                'expected_return': 6.5,      # 연간 기대수익률 (%)
                'volatility': 12.0,          # 연간 변동성 (%)
                'sharpe_ratio': 0.45,        # 샤프 비율
                'max_drawdown': -15.0        # 최대 낙폭 (%)
            },
            'moderate': {
                'expected_return': 8.0,
                'volatility': 15.0,
                'sharpe_ratio': 0.50,
                'max_drawdown': -20.0
            },
            'aggressive': {
                'expected_return': 9.5,
                'volatility': 18.0,
                'sharpe_ratio': 0.48,
                'max_drawdown': -25.0
            }
        }
        
        return performance_estimates.get(self.risk_level, performance_estimates['moderate'])
    
    def get_strategy_description(self) -> Dict[str, str]:
        """전략 설명 반환"""
        descriptions = {
            'strategy_name': '코어-새틀라이트 전략',
            'overview': f"안정적인 코어 자산({self.core_ratio*100:.0f}%)과 성장성 있는 새틀라이트 자산({self.satellite_ratio*100:.0f}%)으로 구성된 전략",
            'core_description': "광범위한 시장을 추종하는 안정적이고 저비용의 ETF로 구성",
            'satellite_description': "특정 테마나 고성장 잠재력을 가진 ETF로 구성하여 추가 수익 추구",
            'advantages': "안정성과 성장성의 균형, 체계적인 리스크 관리, 명확한 자산배분 기준",
            'suitable_for': f"{self.risk_level.title()} 투자성향의 장기 투자자",
            'rebalancing_frequency': "분기별 또는 임계치 초과시",
            'minimum_investment': "월 50만원 이상 권장"
        }
        
        return descriptions