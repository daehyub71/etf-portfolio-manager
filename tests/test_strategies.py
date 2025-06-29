"""
투자 전략 테스트 모듈
모든 투자 전략의 단위 테스트 및 통합 테스트
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from strategies.core_satellite import CoreSatelliteStrategy
from strategies.global_diversified import GlobalDiversifiedStrategy
from strategies.lifecycle_strategy import LifecycleStrategy
from strategies.risk_parity import RiskParityStrategy
from strategies.custom_strategy import CustomStrategy

class TestCoreSatelliteStrategy(unittest.TestCase):
    """코어-새틀라이트 전략 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.strategy = CoreSatelliteStrategy(core_ratio=0.8, risk_level='moderate')
    
    def test_portfolio_generation(self):
        """포트폴리오 생성 테스트"""
        portfolio = self.strategy.generate_portfolio(investment_amount=10000000)
        
        # 기본 검증
        self.assertIsInstance(portfolio, dict)
        self.assertGreater(len(portfolio), 0)
        
        # 비중 합계 검증
        total_weight = sum(portfolio.values())
        self.assertAlmostEqual(total_weight, 100, places=1)
        
        # 개별 비중 검증
        for etf_code, weight in portfolio.items():
            self.assertGreater(weight, 0)
            self.assertLessEqual(weight, 50)  # 최대 50% 제한
    
    def test_core_satellite_ratio(self):
        """코어-새틀라이트 비율 테스트"""
        portfolio = self.strategy.generate_portfolio(investment_amount=5000000)
        
        # 코어 ETF 식별 (실제 구현에서는 더 정교한 분류 필요)
        core_etfs = {'069500', '139660', '195930', '114260', '136340'}
        
        core_weight = sum(weight for etf_code, weight in portfolio.items() 
                         if etf_code in core_etfs)
        satellite_weight = sum(weight for etf_code, weight in portfolio.items() 
                              if etf_code not in core_etfs)
        
        # 비율 검증 (±5% 오차 허용)
        expected_core_ratio = self.strategy.core_ratio * 100
        self.assertAlmostEqual(core_weight, expected_core_ratio, delta=5)
    
    def test_risk_level_adjustment(self):
        """위험 수준 조정 테스트"""
        conservative = CoreSatelliteStrategy(core_ratio=0.8, risk_level='conservative')
        aggressive = CoreSatelliteStrategy(core_ratio=0.8, risk_level='aggressive')
        
        conservative_portfolio = conservative.generate_portfolio(1000000)
        aggressive_portfolio = aggressive.generate_portfolio(1000000)
        
        # 보수적 전략이 더 안전한 자산에 투자하는지 확인
        # (실제로는 ETF별 위험도 데이터 필요)
        self.assertIsInstance(conservative_portfolio, dict)
        self.assertIsInstance(aggressive_portfolio, dict)
    
    def test_portfolio_evaluation(self):
        """포트폴리오 평가 테스트"""
        target_portfolio = self.strategy.generate_portfolio(1000000)
        
        # 약간 다른 현재 포트폴리오 생성
        current_portfolio = target_portfolio.copy()
        if len(current_portfolio) > 1:
            etf_codes = list(current_portfolio.keys())
            current_portfolio[etf_codes[0]] += 5
            current_portfolio[etf_codes[1]] -= 5
        
        evaluation = self.strategy.evaluate_current_portfolio(current_portfolio)
        
        # 평가 결과 검증
        self.assertIn('strategy_alignment', evaluation)
        self.assertIn('recommendations', evaluation)
        self.assertIsInstance(evaluation['strategy_alignment'], (int, float))
        self.assertIsInstance(evaluation['recommendations'], list)
    
    def test_rebalancing_plan(self):
        """리밸런싱 계획 테스트"""
        target_portfolio = self.strategy.generate_portfolio(1000000)
        
        # 이탈된 현재 포트폴리오
        current_portfolio = {}
        for etf_code, weight in target_portfolio.items():
            # 10% 이탈 시뮬레이션
            deviation = np.random.uniform(-10, 10)
            current_portfolio[etf_code] = max(0, weight + deviation)
        
        # 비중 재정규화
        total = sum(current_portfolio.values())
        current_portfolio = {k: v/total*100 for k, v in current_portfolio.items()}
        
        rebalancing_plan = self.strategy.get_rebalancing_plan(
            current_portfolio, target_amount=1000000
        )
        
        # 계획 검증
        self.assertIsInstance(rebalancing_plan, dict)
        for etf_code, plan in rebalancing_plan.items():
            self.assertIn('action', plan)
            self.assertIn('target_weight', plan)
            self.assertIn('current_weight', plan)
            self.assertIn(plan['action'], ['BUY', 'SELL'])

class TestGlobalDiversifiedStrategy(unittest.TestCase):
    """글로벌 분산 전략 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.strategy = GlobalDiversifiedStrategy(strategy_variant='balanced')
    
    def test_portfolio_variants(self):
        """포트폴리오 변형 테스트"""
        variants = self.strategy.get_portfolio_variants()
        
        self.assertIsInstance(variants, dict)
        self.assertGreater(len(variants), 0)
        
        for variant_name, variant_info in variants.items():
            self.assertIn('name', variant_info)
            self.assertIn('allocation', variant_info)
            self.assertIn('risk_level', variant_info)
            
            # 자산배분 검증
            allocation = variant_info['allocation']
            total_weight = sum(allocation.values())
            self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_geographic_allocation(self):
        """지역별 자산배분 테스트"""
        portfolio = self.strategy.generate_portfolio(1000000)
        geographic_allocation = self.strategy.analyze_geographic_allocation(portfolio)
        
        self.assertIsInstance(geographic_allocation, dict)
        
        # 주요 지역이 포함되어 있는지 확인
        expected_regions = ['Korea', 'US', 'Developed']
        for region in expected_regions:
            if region in geographic_allocation:
                self.assertGreater(geographic_allocation[region], 0)
    
    def test_currency_exposure(self):
        """통화 노출도 테스트"""
        portfolio = self.strategy.generate_portfolio(1000000)
        currency_exposure = self.strategy.analyze_currency_exposure(portfolio)
        
        self.assertIsInstance(currency_exposure, dict)
        
        # KRW와 USD 노출이 있는지 확인
        expected_currencies = ['KRW', 'USD']
        for currency in expected_currencies:
            if currency in currency_exposure:
                self.assertGreater(currency_exposure[currency], 0)
    
    def test_cost_analysis(self):
        """비용 분석 테스트"""
        portfolio = self.strategy.generate_portfolio(1000000)
        cost_analysis = self.strategy.calculate_portfolio_cost(portfolio)
        
        self.assertIn('total_expense_ratio', cost_analysis)
        self.assertIn('annual_cost_per_100k', cost_analysis)
        
        # 비용이 합리적 범위인지 확인
        total_expense_ratio = cost_analysis['total_expense_ratio']
        self.assertGreater(total_expense_ratio, 0)
        self.assertLess(total_expense_ratio, 2.0)  # 2% 미만

class TestLifecycleStrategy(unittest.TestCase):
    """생애주기 전략 테스트"""
    
    def test_age_based_allocation(self):
        """연령별 자산배분 테스트"""
        ages = [25, 35, 45, 55, 65]
        
        for age in ages:
            strategy = LifecycleStrategy(age=age, risk_tolerance='moderate')
            portfolio = strategy.generate_portfolio(1000000)
            
            self.assertIsInstance(portfolio, dict)
            self.assertGreater(len(portfolio), 0)
            
            # 비중 합계 검증
            total_weight = sum(portfolio.values())
            self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_lifecycle_projection(self):
        """생애주기 전망 테스트"""
        strategy = LifecycleStrategy(age=35, retirement_age=65)
        
        projection = strategy.get_lifecycle_projection(
            current_portfolio_value=10000000,
            monthly_contribution=1000000
        )
        
        self.assertIn('future_allocations', projection)
        self.assertIn('projected_values', projection)
        self.assertIn('retirement_readiness', projection)
        
        # 미래 자산배분 검증
        future_allocations = projection['future_allocations']
        self.assertIsInstance(future_allocations, dict)
        
        # 예상 가치 검증
        projected_values = projection['projected_values']
        self.assertIsInstance(projected_values, dict)
    
    def test_retirement_readiness(self):
        """은퇴 준비도 평가 테스트"""
        strategy = LifecycleStrategy(age=45, retirement_age=65)
        
        assessment = strategy._assess_retirement_readiness(
            current_value=50000000,
            monthly_contribution=2000000
        )
        
        self.assertIn('target_retirement_asset', assessment)
        self.assertIn('projected_retirement_asset', assessment)
        self.assertIn('readiness_ratio', assessment)
        self.assertIn('status', assessment)
        
        # 준비도 비율 검증
        readiness_ratio = assessment['readiness_ratio']
        self.assertGreaterEqual(readiness_ratio, 0)

class TestRiskParityStrategy(unittest.TestCase):
    """리스크 패리티 전략 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.strategy = RiskParityStrategy(lookback_period=252)
        
        # 테스트용 가격 데이터 생성
        self.price_data = self._generate_test_price_data()
    
    def _generate_test_price_data(self):
        """테스트용 가격 데이터 생성"""
        np.random.seed(42)
        dates = pd.date_range(start='2023-01-01', end='2024-12-31', freq='D')
        
        etf_codes = ['069500', '139660', '114260']
        price_data = {}
        
        for etf_code in etf_codes:
            returns = np.random.normal(0.0005, 0.02, len(dates))
            prices = 100 * np.cumprod(1 + returns)
            
            price_data[etf_code] = pd.DataFrame({
                'date': dates,
                'close_price': prices,
                'volume': np.random.randint(1000000, 10000000, len(dates))
            })
        
        return price_data
    
    def test_risk_parity_weights(self):
        """리스크 패리티 가중치 테스트"""
        weights = self.strategy.calculate_risk_parity_weights(self.price_data)
        
        self.assertIsInstance(weights, dict)
        self.assertGreater(len(weights), 0)
        
        # 비중 합계 검증
        total_weight = sum(weights.values())
        self.assertAlmostEqual(total_weight, 100, places=1)
        
        # 모든 비중이 양수인지 확인
        for weight in weights.values():
            self.assertGreater(weight, 0)
    
    def test_risk_contributions(self):
        """위험 기여도 계산 테스트"""
        weights = self.strategy.calculate_risk_parity_weights(self.price_data)
        risk_contributions = self.strategy.calculate_risk_contributions(
            weights, self.price_data
        )
        
        self.assertIsInstance(risk_contributions, dict)
        self.assertEqual(len(risk_contributions), len(weights))
        
        # 위험 기여도 합계가 100%인지 확인
        total_risk_contrib = sum(risk_contributions.values())
        self.assertAlmostEqual(total_risk_contrib, 100, places=0)
    
    def test_portfolio_risk_analysis(self):
        """포트폴리오 위험 분석 테스트"""
        weights = self.strategy.calculate_risk_parity_weights(self.price_data)
        risk_analysis = self.strategy.analyze_portfolio_risk(weights, self.price_data)
        
        if 'error' not in risk_analysis:
            self.assertIn('portfolio_volatility', risk_analysis)
            self.assertIn('diversification_ratio', risk_analysis)
            self.assertIn('concentration_measure', risk_analysis)
            
            # 변동성이 양수인지 확인
            portfolio_vol = risk_analysis['portfolio_volatility']
            self.assertGreater(portfolio_vol, 0)

class TestCustomStrategy(unittest.TestCase):
    """커스텀 전략 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.strategy = CustomStrategy(strategy_name="테스트 전략")
    
    def test_strategy_creation_from_allocation(self):
        """자산배분 기반 전략 생성 테스트"""
        allocation = {
            '069500': 40.0,
            '139660': 35.0,
            '114260': 25.0
        }
        
        success = self.strategy.create_strategy_from_allocation(allocation)
        self.assertTrue(success)
        self.assertEqual(self.strategy.asset_allocation, allocation)
    
    def test_strategy_creation_from_template(self):
        """템플릿 기반 전략 생성 테스트"""
        success = self.strategy.create_strategy_from_template('balanced_growth')
        self.assertTrue(success)
        self.assertGreater(len(self.strategy.asset_allocation), 0)
    
    def test_etf_addition(self):
        """ETF 추가 테스트"""
        # 초기 포트폴리오 설정
        initial_allocation = {'069500': 50.0, '139660': 50.0}
        self.strategy.create_strategy_from_allocation(initial_allocation)
        
        # ETF 추가
        success = self.strategy.add_etf('114260', 20.0, 'proportional')
        self.assertTrue(success)
        self.assertIn('114260', self.strategy.asset_allocation)
        
        # 비중 합계 검증
        total_weight = sum(self.strategy.asset_allocation.values())
        self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_etf_removal(self):
        """ETF 제거 테스트"""
        # 초기 포트폴리오 설정
        initial_allocation = {'069500': 40.0, '139660': 35.0, '114260': 25.0}
        self.strategy.create_strategy_from_allocation(initial_allocation)
        
        # ETF 제거
        success = self.strategy.remove_etf('114260', 'proportional')
        self.assertTrue(success)
        self.assertNotIn('114260', self.strategy.asset_allocation)
        
        # 비중 합계 검증
        total_weight = sum(self.strategy.asset_allocation.values())
        self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_weight_adjustment(self):
        """비중 조정 테스트"""
        # 초기 포트폴리오 설정
        initial_allocation = {'069500': 40.0, '139660': 35.0, '114260': 25.0}
        self.strategy.create_strategy_from_allocation(initial_allocation)
        
        # 비중 조정
        adjustments = {'069500': 5.0, '139660': -3.0, '114260': -2.0}
        success = self.strategy.adjust_weights(adjustments)
        
        self.assertTrue(success)
        
        # 비중 합계 검증
        total_weight = sum(self.strategy.asset_allocation.values())
        self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_cost_optimization(self):
        """비용 최적화 테스트"""
        # 초기 포트폴리오 설정
        initial_allocation = {'069500': 50.0, '139660': 50.0}
        self.strategy.create_strategy_from_allocation(initial_allocation)
        
        # 비용 최적화 실행
        optimized_allocation = self.strategy.optimize_for_cost()
        
        self.assertIsInstance(optimized_allocation, dict)
        self.assertGreater(len(optimized_allocation), 0)
        
        # 비중 합계 검증
        total_weight = sum(optimized_allocation.values())
        self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_strategy_export_import(self):
        """전략 내보내기/가져오기 테스트"""
        # 전략 설정
        allocation = {'069500': 60.0, '139660': 40.0}
        self.strategy.create_strategy_from_allocation(allocation)
        
        # 내보내기
        exported_data = self.strategy.export_strategy()
        self.assertIn('strategy_name', exported_data)
        self.assertIn('asset_allocation', exported_data)
        
        # 새 전략 인스턴스에 가져오기
        new_strategy = CustomStrategy()
        success = new_strategy.import_strategy(exported_data)
        
        self.assertTrue(success)
        self.assertEqual(new_strategy.asset_allocation, allocation)
        self.assertEqual(new_strategy.strategy_name, self.strategy.strategy_name)

class TestStrategyIntegration(unittest.TestCase):
    """전략 통합 테스트"""
    
    def test_strategy_comparison(self):
        """여러 전략 비교 테스트"""
        strategies = {
            'core_satellite': CoreSatelliteStrategy(risk_level='moderate'),
            'global_diversified': GlobalDiversifiedStrategy(strategy_variant='balanced'),
            'lifecycle': LifecycleStrategy(age=35, risk_tolerance='moderate')
        }
        
        portfolios = {}
        for name, strategy in strategies.items():
            portfolio = strategy.generate_portfolio(10000000)
            portfolios[name] = portfolio
            
            # 기본 검증
            self.assertIsInstance(portfolio, dict)
            self.assertGreater(len(portfolio), 0)
            
            total_weight = sum(portfolio.values())
            self.assertAlmostEqual(total_weight, 100, places=1)
    
    def test_strategy_performance_consistency(self):
        """전략 성과 일관성 테스트"""
        strategy = CoreSatelliteStrategy(risk_level='moderate')
        
        # 동일한 입력으로 여러 번 실행
        portfolios = []
        for _ in range(5):
            portfolio = strategy.generate_portfolio(5000000)
            portfolios.append(portfolio)
        
        # 결과 일관성 확인
        first_portfolio = portfolios[0]
        for portfolio in portfolios[1:]:
            self.assertEqual(set(portfolio.keys()), set(first_portfolio.keys()))
            for etf_code in first_portfolio:
                self.assertAlmostEqual(
                    portfolio[etf_code], 
                    first_portfolio[etf_code], 
                    places=1
                )

if __name__ == '__main__':
    # 테스트 실행
    unittest.main(verbosity=2)