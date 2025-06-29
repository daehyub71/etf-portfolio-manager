"""
비용 계산 유틸리티 모듈
ETF 투자에 관련된 모든 비용을 계산하고 최적화하는 도구
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class CostCalculator:
    """ETF 투자 비용 계산기"""
    
    def __init__(self):
        """비용 계산기 초기화"""
        # 한국 주요 증권사 수수료 정보
        self._setup_brokerage_fees()
        
        # ETF 운용보수 정보
        self._setup_etf_expense_ratios()
        
        # 세금 정보
        self._setup_tax_rates()
    
    def _setup_brokerage_fees(self):
        """증권사별 수수료 정보 설정"""
        
        self.brokerage_fees = {
            'mirae_asset': {
                'name': '미래에셋증권',
                'domestic_etf': {
                    'rate': 0.015,      # 0.015%
                    'min_fee': 1500,    # 최소 1,500원
                    'max_fee': 99000    # 최대 99,000원
                },
                'foreign_etf': {
                    'rate': 0.25,       # 0.25%
                    'min_fee': 9900,    # 최소 9,900원
                    'max_fee': 99000    # 최대 99,000원
                }
            },
            'samsung_securities': {
                'name': '삼성증권',
                'domestic_etf': {
                    'rate': 0.015,
                    'min_fee': 1500,
                    'max_fee': 99000
                },
                'foreign_etf': {
                    'rate': 0.25,
                    'min_fee': 9900,
                    'max_fee': 99000
                }
            },
            'kb_securities': {
                'name': 'KB증권',
                'domestic_etf': {
                    'rate': 0.015,
                    'min_fee': 1500,
                    'max_fee': 99000
                },
                'foreign_etf': {
                    'rate': 0.25,
                    'min_fee': 9900,
                    'max_fee': 99000
                }
            },
            'kiwoom': {
                'name': '키움증권',
                'domestic_etf': {
                    'rate': 0.015,
                    'min_fee': 1500,
                    'max_fee': 99000
                },
                'foreign_etf': {
                    'rate': 0.25,
                    'min_fee': 9900,
                    'max_fee': 99000
                }
            },
            'toss': {
                'name': '토스증권',
                'domestic_etf': {
                    'rate': 0.0,        # 무료
                    'min_fee': 0,
                    'max_fee': 0
                },
                'foreign_etf': {
                    'rate': 0.25,
                    'min_fee': 9900,
                    'max_fee': 99000
                }
            }
        }
    
    def _setup_etf_expense_ratios(self):
        """ETF 운용보수 정보 설정"""
        
        self.etf_expense_ratios = {
            # 국내 주식 ETF
            '069500': 0.15,    # KODEX 200
            '152100': 0.06,    # TIGER 코스피
            '229200': 0.15,    # KODEX 코스닥150
            '069660': 0.25,    # KODEX 200고배당
            
            # 해외 주식 ETF
            '139660': 0.045,   # TIGER 미국S&P500
            '117460': 0.045,   # KODEX 나스닥100
            '195930': 0.08,    # TIGER 선진국MSCI World
            '192090': 0.49,    # TIGER 신흥국MSCI
            '160570': 0.19,    # TIGER 중국CSI300
            
            # 채권 ETF
            '114260': 0.15,    # KODEX 국고채10년
            '136340': 0.14,    # TIGER 미국채10년
            '139230': 0.15,    # TIGER 회사채AA-
            '130730': 0.45,    # KODEX 글로벌하이일드
            
            # 대안투자 ETF
            '157490': 0.5,     # KODEX 리츠
            '351590': 0.49,    # TIGER 미국리츠
            '132030': 0.49,    # KODEX 골드선물
            
            # 테마 ETF
            '305540': 0.45,    # KODEX 2차전지산업
            '091160': 0.49,    # KODEX 반도체
            '148020': 0.25     # KODEX ESG Korea
        }
    
    def _setup_tax_rates(self):
        """세금 정보 설정"""
        
        self.tax_rates = {
            'domestic_etf': {
                'capital_gains': 0.0,      # 양도소득세 없음
                'dividend': 0.154,         # 배당소득세 15.4%
                'local_tax': 0.014         # 지방소득세 1.4%
            },
            'foreign_etf': {
                'capital_gains': 0.22,     # 양도소득세 22%
                'dividend': 0.154,         # 배당소득세 15.4%
                'local_tax': 0.014,        # 지방소득세 1.4%
                'withholding_tax': 0.15    # 원천징수세 15% (미국 기준)
            }
        }
    
    def calculate_trading_cost(self, etf_code: str, trade_amount: float, 
                             brokerage: str = 'mirae_asset', 
                             trade_type: str = 'buy') -> Dict[str, float]:
        """
        거래 비용 계산
        
        Args:
            etf_code: ETF 코드
            trade_amount: 거래 금액
            brokerage: 증권사
            trade_type: 거래 유형 ('buy', 'sell')
            
        Returns:
            거래 비용 상세 정보
        """
        try:
            # ETF 유형 판단 (국내/해외)
            is_domestic = self._is_domestic_etf(etf_code)
            
            # 증권사 수수료 정보
            brokerage_info = self.brokerage_fees.get(brokerage, self.brokerage_fees['mirae_asset'])
            fee_info = brokerage_info['domestic_etf'] if is_domestic else brokerage_info['foreign_etf']
            
            # 수수료 계산
            commission_rate = fee_info['rate'] / 100
            commission = trade_amount * commission_rate
            commission = max(fee_info['min_fee'], min(commission, fee_info['max_fee']))
            
            # 기타 비용
            costs = {
                'trade_amount': trade_amount,
                'commission': commission,
                'commission_rate': fee_info['rate'],
                'transaction_tax': 0,  # ETF는 거래세 없음
                'total_cost': commission
            }
            
            # 매도시 추가 비용 (증권거래세 등)
            if trade_type == 'sell':
                if not is_domestic:
                    # 해외 ETF 매도시 양도소득세 고려 필요 (실제 계산은 복잡)
                    costs['notes'] = '해외 ETF 매도시 양도소득세 별도 계산 필요'
            
            return costs
            
        except Exception as e:
            logger.error(f"거래 비용 계산 실패: {e}")
            return {}
    
    def calculate_annual_expense_ratio(self, portfolio: Dict[str, float]) -> Dict[str, float]:
        """
        포트폴리오 연간 운용보수 계산
        
        Args:
            portfolio: ETF별 비중 (%)
            
        Returns:
            연간 운용보수 정보
        """
        try:
            total_expense_ratio = 0
            etf_costs = {}
            
            total_weight = sum(portfolio.values())
            
            for etf_code, weight in portfolio.items():
                expense_ratio = self.etf_expense_ratios.get(etf_code, 0.5)  # 기본값 0.5%
                
                # 가중 평균 비용
                weighted_cost = expense_ratio * (weight / total_weight) if total_weight > 0 else 0
                total_expense_ratio += weighted_cost
                
                etf_costs[etf_code] = {
                    'weight': weight,
                    'expense_ratio': expense_ratio,
                    'weighted_cost': weighted_cost
                }
            
            return {
                'total_expense_ratio': total_expense_ratio,
                'annual_cost_per_100k': total_expense_ratio * 1000,  # 100만원당 연간 비용
                'etf_breakdown': etf_costs
            }
            
        except Exception as e:
            logger.error(f"연간 운용보수 계산 실패: {e}")
            return {}
    
    def calculate_rebalancing_cost(self, current_portfolio: Dict[str, float],
                                 target_portfolio: Dict[str, float],
                                 portfolio_value: float,
                                 brokerage: str = 'mirae_asset') -> Dict[str, Any]:
        """
        리밸런싱 비용 계산
        
        Args:
            current_portfolio: 현재 포트폴리오 비중
            target_portfolio: 목표 포트폴리오 비중
            portfolio_value: 총 포트폴리오 가치
            brokerage: 증권사
            
        Returns:
            리밸런싱 비용 상세 정보
        """
        try:
            trades = []
            total_cost = 0
            
            all_etfs = set(list(current_portfolio.keys()) + list(target_portfolio.keys()))
            
            for etf_code in all_etfs:
                current_weight = current_portfolio.get(etf_code, 0)
                target_weight = target_portfolio.get(etf_code, 0)
                
                weight_diff = target_weight - current_weight
                
                if abs(weight_diff) > 0.1:  # 0.1% 이상 차이가 있을 때만
                    trade_amount = abs(weight_diff / 100 * portfolio_value)
                    trade_type = 'buy' if weight_diff > 0 else 'sell'
                    
                    trade_cost = self.calculate_trading_cost(
                        etf_code, trade_amount, brokerage, trade_type
                    )
                    
                    total_cost += trade_cost.get('total_cost', 0)
                    
                    trades.append({
                        'etf_code': etf_code,
                        'trade_type': trade_type,
                        'trade_amount': trade_amount,
                        'weight_change': weight_diff,
                        'cost': trade_cost.get('total_cost', 0)
                    })
            
            return {
                'total_rebalancing_cost': total_cost,
                'cost_as_percentage': total_cost / portfolio_value * 100 if portfolio_value > 0 else 0,
                'number_of_trades': len(trades),
                'trade_details': trades,
                'cost_efficiency_score': self._calculate_cost_efficiency_score(total_cost, portfolio_value)
            }
            
        except Exception as e:
            logger.error(f"리밸런싱 비용 계산 실패: {e}")
            return {}
    
    def calculate_tax_impact(self, etf_code: str, 
                           investment_amount: float,
                           holding_period_years: float,
                           expected_return: float = 0.07,
                           dividend_yield: float = 0.02) -> Dict[str, float]:
        """
        세금 영향 계산
        
        Args:
            etf_code: ETF 코드
            investment_amount: 투자 금액
            holding_period_years: 보유 기간 (년)
            expected_return: 예상 수익률
            dividend_yield: 배당 수익률
            
        Returns:
            세금 영향 분석
        """
        try:
            is_domestic = self._is_domestic_etf(etf_code)
            tax_info = self.tax_rates['domestic_etf'] if is_domestic else self.tax_rates['foreign_etf']
            
            # 예상 투자 수익
            total_return = investment_amount * ((1 + expected_return) ** holding_period_years - 1)
            capital_gains = total_return - (investment_amount * dividend_yield * holding_period_years)
            dividend_income = investment_amount * dividend_yield * holding_period_years
            
            # 세금 계산
            capital_gains_tax = 0
            if not is_domestic and capital_gains > 0:
                # 해외 ETF 양도소득세 (250만원 기본공제 후)
                taxable_gains = max(0, capital_gains - 2500000)
                capital_gains_tax = taxable_gains * tax_info['capital_gains']
            
            # 배당소득세
            dividend_tax = dividend_income * tax_info['dividend']
            
            # 지방소득세
            local_tax = dividend_income * tax_info['local_tax']
            
            # 해외 원천징수세 (해외 ETF만)
            withholding_tax = 0
            if not is_domestic:
                withholding_tax = dividend_income * tax_info.get('withholding_tax', 0)
            
            total_tax = capital_gains_tax + dividend_tax + local_tax + withholding_tax
            
            return {
                'investment_amount': investment_amount,
                'expected_total_return': total_return,
                'capital_gains': capital_gains,
                'dividend_income': dividend_income,
                'capital_gains_tax': capital_gains_tax,
                'dividend_tax': dividend_tax,
                'local_tax': local_tax,
                'withholding_tax': withholding_tax,
                'total_tax': total_tax,
                'after_tax_return': total_return - total_tax,
                'effective_tax_rate': total_tax / total_return * 100 if total_return > 0 else 0
            }
            
        except Exception as e:
            logger.error(f"세금 영향 계산 실패: {e}")
            return {}
    
    def compare_brokerage_costs(self, trades: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        증권사별 거래 비용 비교
        
        Args:
            trades: 거래 목록 [{'etf_code': '', 'amount': 0, 'type': 'buy/sell'}]
            
        Returns:
            증권사별 비용 비교 DataFrame
        """
        try:
            comparison_data = []
            
            for brokerage_code, brokerage_info in self.brokerage_fees.items():
                total_cost = 0
                
                for trade in trades:
                    cost_info = self.calculate_trading_cost(
                        trade['etf_code'], 
                        trade['amount'], 
                        brokerage_code, 
                        trade.get('type', 'buy')
                    )
                    total_cost += cost_info.get('total_cost', 0)
                
                comparison_data.append({
                    '증권사': brokerage_info['name'],
                    '총_거래비용': total_cost,
                    '거래_건수': len(trades),
                    '평균_건당_비용': total_cost / len(trades) if trades else 0
                })
            
            df = pd.DataFrame(comparison_data)
            df = df.sort_values('총_거래비용')
            
            return df
            
        except Exception as e:
            logger.error(f"증권사 비용 비교 실패: {e}")
            return pd.DataFrame()
    
    def calculate_cost_efficiency_portfolio(self, 
                                          target_allocation: Dict[str, float],
                                          min_expense_ratio: bool = True) -> Dict[str, float]:
        """
        비용 효율적인 대안 포트폴리오 제안
        
        Args:
            target_allocation: 목표 자산배분
            min_expense_ratio: 최소 비용 ETF 우선 선택 여부
            
        Returns:
            비용 최적화된 포트폴리오
        """
        try:
            # 자산군별 그룹화
            asset_groups = self._group_etfs_by_asset_class(target_allocation)
            
            optimized_portfolio = {}
            
            for asset_class, etfs in asset_groups.items():
                if min_expense_ratio:
                    # 해당 자산군에서 가장 저렴한 ETF 선택
                    best_etf = min(etfs.items(), key=lambda x: self.etf_expense_ratios.get(x[0], 1.0))
                    total_weight = sum(etfs.values())
                    optimized_portfolio[best_etf[0]] = total_weight
                else:
                    # 기존 ETF 유지
                    optimized_portfolio.update(etfs)
            
            return optimized_portfolio
            
        except Exception as e:
            logger.error(f"비용 효율 포트폴리오 계산 실패: {e}")
            return target_allocation
    
    def calculate_dollar_cost_averaging_impact(self,
                                             monthly_investment: float,
                                             investment_months: int,
                                             etf_code: str,
                                             brokerage: str = 'mirae_asset') -> Dict[str, Any]:
        """
        달러 코스트 평균법 비용 영향 계산
        
        Args:
            monthly_investment: 월 투자 금액
            investment_months: 투자 개월 수
            etf_code: ETF 코드
            brokerage: 증권사
            
        Returns:
            적립식 투자 비용 분석
        """
        try:
            monthly_cost = self.calculate_trading_cost(
                etf_code, monthly_investment, brokerage, 'buy'
            )
            
            total_investment = monthly_investment * investment_months
            total_trading_cost = monthly_cost['total_cost'] * investment_months
            
            # 일시 투자와 비교
            lump_sum_cost = self.calculate_trading_cost(
                etf_code, total_investment, brokerage, 'buy'
            )
            
            return {
                'monthly_investment': monthly_investment,
                'investment_months': investment_months,
                'total_investment': total_investment,
                'monthly_trading_cost': monthly_cost['total_cost'],
                'total_trading_cost': total_trading_cost,
                'lump_sum_cost': lump_sum_cost['total_cost'],
                'additional_cost': total_trading_cost - lump_sum_cost['total_cost'],
                'cost_as_percentage': total_trading_cost / total_investment * 100,
                'break_even_months': self._calculate_break_even_months(monthly_cost['total_cost'], lump_sum_cost['total_cost'])
            }
            
        except Exception as e:
            logger.error(f"적립식 투자 비용 계산 실패: {e}")
            return {}
    
    def get_cost_optimization_recommendations(self, 
                                            portfolio: Dict[str, float],
                                            investment_amount: float) -> List[Dict[str, str]]:
        """
        비용 최적화 권장사항 제공
        
        Args:
            portfolio: 현재 포트폴리오
            investment_amount: 투자 금액
            
        Returns:
            최적화 권장사항 목록
        """
        recommendations = []
        
        try:
            # 운용보수 분석
            expense_analysis = self.calculate_annual_expense_ratio(portfolio)
            
            if expense_analysis['total_expense_ratio'] > 0.5:
                recommendations.append({
                    'type': '운용보수 최적화',
                    'description': f"포트폴리오 평균 운용보수가 {expense_analysis['total_expense_ratio']:.2f}%로 높습니다",
                    'suggestion': '저비용 ETF로 교체를 고려하세요',
                    'potential_savings': f"연간 {(expense_analysis['total_expense_ratio'] - 0.2) * investment_amount / 100:,.0f}원 절약 가능"
                })
            
            # ETF 개수 분석
            if len(portfolio) > 10:
                recommendations.append({
                    'type': '거래비용 최적화',
                    'description': f"ETF 개수가 {len(portfolio)}개로 많습니다",
                    'suggestion': '핵심 ETF로 통합하여 리밸런싱 비용을 줄이세요',
                    'potential_savings': '리밸런싱시 거래비용 50% 이상 절약 가능'
                })
            
            # 소액 투자 분석
            for etf_code, weight in portfolio.items():
                etf_amount = investment_amount * weight / 100
                if etf_amount < 100000:  # 10만원 미만
                    recommendations.append({
                        'type': '최소투자금액',
                        'description': f"{etf_code}의 투자금액이 {etf_amount:,.0f}원으로 적습니다",
                        'suggestion': '비중을 늘리거나 다른 ETF와 통합하세요',
                        'potential_savings': '거래비용 대비 투자효율성 개선'
                    })
            
            # 해외 ETF 비중 분석
            foreign_weight = self._calculate_foreign_etf_weight(portfolio)
            if foreign_weight > 70:
                recommendations.append({
                    'type': '세금 최적화',
                    'description': f"해외 ETF 비중이 {foreign_weight:.1f}%로 높습니다",
                    'suggestion': '양도소득세 부담을 고려하여 국내 ETF 비중을 늘리는 것을 고려하세요',
                    'potential_savings': '세금 효율성 개선'
                })
            
        except Exception as e:
            logger.error(f"비용 최적화 권장사항 생성 실패: {e}")
        
        return recommendations
    
    # 헬퍼 메서드들
    
    def _is_domestic_etf(self, etf_code: str) -> bool:
        """국내 ETF 여부 판단"""
        # 간단한 판단 로직 (실제로는 더 정교한 분류 필요)
        domestic_etfs = [
            '069500', '152100', '229200', '069660', '114260', 
            '139230', '157490', '091160', '148020'
        ]
        return etf_code in domestic_etfs
    
    def _calculate_cost_efficiency_score(self, cost: float, portfolio_value: float) -> float:
        """비용 효율성 점수 계산"""
        if portfolio_value <= 0:
            return 0
        
        cost_ratio = cost / portfolio_value * 100
        
        # 비용 비율에 따른 점수 (낮을수록 높은 점수)
        if cost_ratio <= 0.1:
            return 100
        elif cost_ratio <= 0.5:
            return 80
        elif cost_ratio <= 1.0:
            return 60
        elif cost_ratio <= 2.0:
            return 40
        else:
            return 20
    
    def _group_etfs_by_asset_class(self, portfolio: Dict[str, float]) -> Dict[str, Dict[str, float]]:
        """ETF를 자산군별로 그룹화"""
        
        # 간단한 자산군 매핑 (실제로는 더 정교한 분류 필요)
        asset_mapping = {
            '069500': 'domestic_equity', '152100': 'domestic_equity', '229200': 'domestic_equity',
            '139660': 'us_equity', '117460': 'us_equity', '195930': 'developed_equity',
            '114260': 'domestic_bond', '136340': 'foreign_bond', '139230': 'domestic_bond',
            '157490': 'reit', '132030': 'commodity'
        }
        
        groups = {}
        for etf_code, weight in portfolio.items():
            asset_class = asset_mapping.get(etf_code, 'other')
            
            if asset_class not in groups:
                groups[asset_class] = {}
            
            groups[asset_class][etf_code] = weight
        
        return groups
    
    def _calculate_break_even_months(self, monthly_cost: float, lump_sum_cost: float) -> int:
        """손익분기점 개월 수 계산"""
        if monthly_cost <= 0:
            return 0
        
        return int(lump_sum_cost / monthly_cost) + 1
    
    def _calculate_foreign_etf_weight(self, portfolio: Dict[str, float]) -> float:
        """해외 ETF 비중 계산"""
        
        foreign_etfs = ['139660', '117460', '195930', '192090', '160570', '136340', '130730', '351590', '132030']
        
        foreign_weight = sum(
            weight for etf_code, weight in portfolio.items()
            if etf_code in foreign_etfs
        )
        
        return foreign_weight
    
    def calculate_total_cost_of_ownership(self, 
                                        portfolio: Dict[str, float],
                                        investment_amount: float,
                                        holding_period_years: int = 5,
                                        rebalancing_frequency: int = 4) -> Dict[str, Any]:
        """
        총 소유 비용 계산 (TCO)
        
        Args:
            portfolio: 포트폴리오 구성
            investment_amount: 투자 금액
            holding_period_years: 보유 기간 (년)
            rebalancing_frequency: 연간 리밸런싱 횟수
            
        Returns:
            총 소유 비용 분석
        """
        try:
            # 연간 운용보수
            expense_analysis = self.calculate_annual_expense_ratio(portfolio)
            annual_expense = investment_amount * expense_analysis['total_expense_ratio'] / 100
            total_expense = annual_expense * holding_period_years
            
            # 초기 매수 비용
            initial_purchase_cost = 0
            for etf_code, weight in portfolio.items():
                etf_amount = investment_amount * weight / 100
                cost_info = self.calculate_trading_cost(etf_code, etf_amount)
                initial_purchase_cost += cost_info.get('total_cost', 0)
            
            # 리밸런싱 비용 (추정)
            estimated_rebalancing_cost = initial_purchase_cost * 0.3  # 초기 비용의 30% 추정
            total_rebalancing_cost = estimated_rebalancing_cost * rebalancing_frequency * holding_period_years
            
            # 최종 매도 비용 (추정)
            final_sale_cost = initial_purchase_cost  # 매수 비용과 동일하다고 가정
            
            # 총 비용
            total_cost = total_expense + initial_purchase_cost + total_rebalancing_cost + final_sale_cost
            
            return {
                'investment_amount': investment_amount,
                'holding_period_years': holding_period_years,
                'annual_expense_ratio': expense_analysis['total_expense_ratio'],
                'total_expense_cost': total_expense,
                'initial_purchase_cost': initial_purchase_cost,
                'total_rebalancing_cost': total_rebalancing_cost,
                'final_sale_cost': final_sale_cost,
                'total_cost': total_cost,
                'cost_as_percentage': total_cost / investment_amount * 100,
                'annual_cost_percentage': (total_cost / holding_period_years) / investment_amount * 100,
                'cost_breakdown': {
                    '운용보수': total_expense / total_cost * 100,
                    '초기매수': initial_purchase_cost / total_cost * 100,
                    '리밸런싱': total_rebalancing_cost / total_cost * 100,
                    '최종매도': final_sale_cost / total_cost * 100
                }
            }
            
        except Exception as e:
            logger.error(f"총 소유 비용 계산 실패: {e}")
            return {}