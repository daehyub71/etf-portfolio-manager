"""
세금 최적화 도구
- 손익 실현 최적화
- 세액공제 최대화
- 연말정산 대비 전략
- 퇴직연금 최적화
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import json
import yaml

class TaxOptimizer:
    """세금 최적화 전문 클래스"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """초기화"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # 2025년 기준 세율표
        self.tax_rates = {
            'dividend_tax': 0.154,  # 배당소득세 15.4%
            'capital_gains_tax': 0.22,  # 대주주 양도소득세 22%
            'general_capital_gains': 0.0,  # 일반 주식 양도소득세 없음
            'pension_deduction': 7000000,  # 연금저축 세액공제 한도
            'isa_limit': 20000000,  # ISA 한도
            'pension_limit': 4000000  # 퇴직연금 한도
        }
        
    def analyze_tax_implications(self, portfolio_data: pd.DataFrame) -> Dict:
        """포트폴리오의 세금 영향 분석"""
        
        analysis = {
            'current_year_gains': {},
            'unrealized_gains': {},
            'tax_loss_harvesting': {},
            'dividend_tax': {},
            'optimization_suggestions': []
        }
        
        # 1. 올해 실현손익 분석
        current_year = datetime.now().year
        ytd_trades = portfolio_data[
            pd.to_datetime(portfolio_data['date']).dt.year == current_year
        ]
        
        for etf in ytd_trades['etf_code'].unique():
            etf_trades = ytd_trades[ytd_trades['etf_code'] == etf]
            realized_gain = etf_trades['realized_gain'].sum()
            analysis['current_year_gains'][etf] = realized_gain
            
        # 2. 미실현손익 분석
        for etf in portfolio_data['etf_code'].unique():
            latest_data = portfolio_data[portfolio_data['etf_code'] == etf].iloc[-1]
            unrealized = latest_data['current_value'] - latest_data['total_cost']
            analysis['unrealized_gains'][etf] = unrealized
            
        # 3. 세금 손실 수확 기회 분석
        analysis['tax_loss_harvesting'] = self._find_tax_loss_opportunities(
            portfolio_data
        )
        
        # 4. 배당세 분석
        analysis['dividend_tax'] = self._calculate_dividend_tax(portfolio_data)
        
        # 5. 최적화 제안
        analysis['optimization_suggestions'] = self._generate_tax_optimization_suggestions(
            analysis
        )
        
        return analysis
    
    def _find_tax_loss_opportunities(self, portfolio_data: pd.DataFrame) -> Dict:
        """세금 손실 수확 기회 찾기"""
        
        opportunities = {
            'loss_harvesting_candidates': [],
            'wash_sale_warnings': [],
            'optimal_timing': {}
        }
        
        current_date = datetime.now()
        year_end = datetime(current_date.year, 12, 31)
        days_to_year_end = (year_end - current_date).days
        
        for etf in portfolio_data['etf_code'].unique():
            latest_data = portfolio_data[portfolio_data['etf_code'] == etf].iloc[-1]
            unrealized_gain = latest_data['current_value'] - latest_data['total_cost']
            
            # 손실이 있는 경우
            if unrealized_gain < 0:
                loss_amount = abs(unrealized_gain)
                
                # 연말 임박시 손실 실현 검토
                if days_to_year_end < 60:  # 2개월 전부터 검토
                    opportunities['loss_harvesting_candidates'].append({
                        'etf_code': etf,
                        'loss_amount': loss_amount,
                        'recommendation': 'year_end_loss_realization',
                        'tax_benefit': loss_amount * 0.22  # 가정 세율
                    })
                
                # Wash Sale 규칙 체크 (한국은 명시적 규정 없지만 유사 ETF 체크)
                similar_etfs = self._find_similar_etfs(etf)
                if similar_etfs:
                    opportunities['wash_sale_warnings'].append({
                        'etf_code': etf,
                        'similar_etfs': similar_etfs,
                        'warning': '유사 ETF 보유로 인한 손실 인정 문제 가능성'
                    })
        
        return opportunities
    
    def _calculate_dividend_tax(self, portfolio_data: pd.DataFrame) -> Dict:
        """배당세 계산"""
        
        dividend_analysis = {
            'total_dividend_received': 0,
            'total_dividend_tax': 0,
            'etf_wise_breakdown': {},
            'monthly_breakdown': {}
        }
        
        current_year = datetime.now().year
        dividend_data = portfolio_data[
            (pd.to_datetime(portfolio_data['date']).dt.year == current_year) &
            (portfolio_data['dividend_amount'] > 0)
        ]
        
        # ETF별 배당 분석
        for etf in dividend_data['etf_code'].unique():
            etf_dividends = dividend_data[dividend_data['etf_code'] == etf]
            total_dividend = etf_dividends['dividend_amount'].sum()
            dividend_tax = total_dividend * self.tax_rates['dividend_tax']
            
            dividend_analysis['etf_wise_breakdown'][etf] = {
                'dividend_amount': total_dividend,
                'tax_amount': dividend_tax,
                'net_dividend': total_dividend - dividend_tax
            }
            
            dividend_analysis['total_dividend_received'] += total_dividend
            dividend_analysis['total_dividend_tax'] += dividend_tax
        
        # 월별 배당 분석
        dividend_data['month'] = pd.to_datetime(dividend_data['date']).dt.month
        monthly_dividends = dividend_data.groupby('month')['dividend_amount'].sum()
        
        for month, amount in monthly_dividends.items():
            dividend_analysis['monthly_breakdown'][month] = {
                'dividend_amount': amount,
                'tax_amount': amount * self.tax_rates['dividend_tax']
            }
        
        return dividend_analysis
    
    def _generate_tax_optimization_suggestions(self, analysis: Dict) -> List[Dict]:
        """세금 최적화 제안 생성"""
        
        suggestions = []
        current_date = datetime.now()
        
        # 1. 손실 실현 제안
        loss_candidates = analysis['tax_loss_harvesting']['loss_harvesting_candidates']
        if loss_candidates:
            total_loss = sum(item['loss_amount'] for item in loss_candidates)
            suggestions.append({
                'type': 'loss_harvesting',
                'priority': 'high',
                'title': '연말 손실 실현을 통한 세금 절약',
                'description': f'총 {total_loss:,.0f}원의 손실 실현으로 세금 절약 가능',
                'action': '손실 ETF 매도 후 유사하지 않은 ETF로 대체',
                'deadline': f'{current_date.year}-12-30',
                'benefit': f'약 {total_loss * 0.22:,.0f}원 세금 절약 예상'
            })
        
        # 2. 퇴직연금 최적화
        suggestions.append({
            'type': 'pension_optimization',
            'priority': 'medium',
            'title': '퇴직연금 세액공제 최대화',
            'description': '퇴직연금 한도 활용으로 세액공제 혜택',
            'action': f'연간 {self.tax_rates["pension_limit"]:,}원까지 추가 납입',
            'benefit': f'최대 {self.tax_rates["pension_limit"] * 0.165:,.0f}원 세액공제'
        })
        
        # 3. ISA 계좌 활용
        suggestions.append({
            'type': 'isa_optimization',
            'priority': 'medium',
            'title': 'ISA 계좌 활용 검토',
            'description': '비과세 혜택을 위한 ISA 계좌 활용',
            'action': f'연간 {self.tax_rates["isa_limit"]:,}원까지 ISA 활용',
            'benefit': '매매차익 및 배당소득 비과세'
        })
        
        # 4. 배당 집중 시기 분산
        dividend_analysis = analysis['dividend_tax']
        if dividend_analysis['total_dividend_received'] > 0:
            suggestions.append({
                'type': 'dividend_timing',
                'priority': 'low',
                'title': '배당 시기 분산 검토',
                'description': '배당 집중 시기 분산으로 세금 관리',
                'action': '배당 시기가 다른 ETF로 분산',
                'benefit': '세금 부담 시기 분산'
            })
        
        return suggestions
    
    def _find_similar_etfs(self, etf_code: str) -> List[str]:
        """유사 ETF 찾기 (Wash Sale 방지용)"""
        
        # 간단한 유사 ETF 매핑 (실제로는 더 정교한 분류 필요)
        similar_etf_groups = {
            'KODEX 200': ['TIGER 코스피200', 'KODEX 코스피'],
            'TIGER 미국S&P500': ['KODEX 미국S&P500', 'ARIRANG 미국S&P500'],
            'KODEX 나스닥100': ['TIGER 나스닥100', 'ARIRANG 나스닥100'],
        }
        
        for group in similar_etf_groups.values():
            if etf_code in group:
                return [etf for etf in group if etf != etf_code]
        
        return []
    
    def generate_year_end_tax_report(self, portfolio_data: pd.DataFrame) -> Dict:
        """연말정산 대비 세금 리포트 생성"""
        
        current_year = datetime.now().year
        report = {
            'summary': {},
            'realized_gains_losses': {},
            'dividend_income': {},
            'tax_deductions': {},
            'recommendations': []
        }
        
        # 실현손익 요약
        ytd_data = portfolio_data[
            pd.to_datetime(portfolio_data['date']).dt.year == current_year
        ]
        
        total_realized_gain = ytd_data['realized_gain'].sum()
        total_dividend = ytd_data['dividend_amount'].sum()
        total_dividend_tax = total_dividend * self.tax_rates['dividend_tax']
        
        report['summary'] = {
            'total_realized_gain': total_realized_gain,
            'total_dividend_income': total_dividend,
            'total_dividend_tax_paid': total_dividend_tax,
            'net_investment_income': total_realized_gain + total_dividend - total_dividend_tax
        }
        
        # 세액공제 활용 현황
        report['tax_deductions'] = {
            'pension_contribution': 0,  # 실제 데이터에서 가져와야 함
            'isa_contribution': 0,
            'available_deduction': self.tax_rates['pension_deduction']
        }
        
        # 추천사항
        if total_realized_gain < 0:  # 손실이 있는 경우
            report['recommendations'].append(
                "실현손실이 발생했습니다. 내년도 이월공제 가능합니다."
            )
        
        if total_dividend > 0:
            report['recommendations'].append(
                f"배당소득세 {total_dividend_tax:,.0f}원이 원천징수되었습니다."
            )
        
        return report
    
    def optimize_rebalancing_for_tax(self, 
                                   current_portfolio: Dict,
                                   target_allocation: Dict,
                                   cash_available: float = 0) -> Dict:
        """세금을 고려한 리밸런싱 최적화"""
        
        optimization = {
            'preferred_transactions': [],
            'tax_cost': 0,
            'alternatives': []
        }
        
        # 현재 포트폴리오와 목표 포트폴리오 비교
        for etf_code in target_allocation.keys():
            current_value = current_portfolio.get(etf_code, {}).get('current_value', 0)
            current_cost = current_portfolio.get(etf_code, {}).get('total_cost', 0)
            target_value = target_allocation[etf_code]['target_value']
            
            rebalance_amount = target_value - current_value
            
            if rebalance_amount > 0:  # 매수 필요
                optimization['preferred_transactions'].append({
                    'etf_code': etf_code,
                    'action': 'buy',
                    'amount': rebalance_amount,
                    'tax_impact': 0  # 매수는 세금 영향 없음
                })
            
            elif rebalance_amount < 0:  # 매도 필요
                sell_amount = abs(rebalance_amount)
                gain_loss = current_value - current_cost
                
                if gain_loss > 0:  # 수익 상태
                    # 대주주 여부에 따른 세금 계산 (간소화)
                    tax_cost = gain_loss * 0.22 if self._is_major_shareholder(etf_code) else 0
                else:  # 손실 상태
                    tax_cost = 0  # 오히려 세금 절약 효과
                
                optimization['preferred_transactions'].append({
                    'etf_code': etf_code,
                    'action': 'sell',
                    'amount': sell_amount,
                    'tax_impact': tax_cost,
                    'gain_loss': gain_loss
                })
                
                optimization['tax_cost'] += tax_cost
        
        # 신규 자금 활용 대안 제시
        if cash_available > 0:
            optimization['alternatives'].append({
                'strategy': 'use_new_cash',
                'description': f'신규 자금 {cash_available:,.0f}원을 활용하여 매도 없이 리밸런싱',
                'tax_benefit': optimization['tax_cost']
            })
        
        return optimization
    
    def _is_major_shareholder(self, etf_code: str) -> bool:
        """대주주 여부 판단 (간소화)"""
        # 실제로는 보유 비중을 확인해야 함
        return False
    
    def save_tax_report(self, report_data: Dict, filename: str = None):
        """세금 리포트 저장"""
        
        if filename is None:
            filename = f"tax_report_{datetime.now().strftime('%Y%m%d')}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"세금 리포트가 {filename}에 저장되었습니다.")

# 사용 예시
if __name__ == "__main__":
    # 세금 최적화 도구 초기화
    tax_optimizer = TaxOptimizer()
    
    # 더미 데이터로 테스트 (실제로는 portfolio_manager에서 데이터 가져옴)
    dummy_portfolio = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=100, freq='D'),
        'etf_code': ['KODEX 200'] * 50 + ['TIGER 미국S&P500'] * 50,
        'current_value': np.random.uniform(950000, 1050000, 100),
        'total_cost': [1000000] * 100,
        'realized_gain': np.random.uniform(-50000, 50000, 100),
        'dividend_amount': np.random.uniform(0, 10000, 100)
    })
    
    # 세금 영향 분석
    tax_analysis = tax_optimizer.analyze_tax_implications(dummy_portfolio)
    print("=== 세금 영향 분석 ===")
    for suggestion in tax_analysis['optimization_suggestions']:
        print(f"• {suggestion['title']}: {suggestion['description']}")
    
    # 연말정산 리포트
    year_end_report = tax_optimizer.generate_year_end_tax_report(dummy_portfolio)
    print(f"\n=== 연말정산 리포트 ===")
    print(f"총 실현손익: {year_end_report['summary']['total_realized_gain']:,.0f}원")
    print(f"총 배당소득: {year_end_report['summary']['total_dividend_income']:,.0f}원")
    
    print("\n세금 최적화 도구가 성공적으로 실행되었습니다.")