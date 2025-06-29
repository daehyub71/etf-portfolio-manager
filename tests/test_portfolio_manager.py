"""
포트폴리오 매니저 테스트 모듈
핵심 포트폴리오 관리 기능의 단위 테스트 및 통합 테스트
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
import sys

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.portfolio_manager import PortfolioManager
from data.database_manager import DatabaseManager
from strategies.core_satellite import CoreSatelliteStrategy

class TestPortfolioManager(unittest.TestCase):
    """포트폴리오 매니저 테스트 클래스"""
    
    def setUp(self):
        """테스트 설정"""
        # 임시 데이터베이스 생성
        self.temp_dir = tempfile.mkdtemp()
        self.db_manager = DatabaseManager(self.temp_dir)
        
        # 포트폴리오 매니저 초기화
        self.portfolio_manager = PortfolioManager(database_manager=self.db_manager)
        
        # 테스트용 ETF 데이터
        self.test_etfs = {
            '069500': {'name': 'KODEX 200', 'expense_ratio': 0.15},
            '139660': {'name': 'TIGER 미국S&P500', 'expense_ratio': 0.045},
            '114260': {'name': 'KODEX 국고채10년', 'expense_ratio': 0.15}
        }
        
        # 테스트용 가격 데이터 생성
        self.test_price_data = self._generate_test_price_data()
    
    def tearDown(self):
        """테스트 정리"""
        # 임시 파일 정리
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _generate_test_price_data(self):
        """테스트용 가격 데이터 생성"""
        dates = pd.date_range(start='2023-01-01', end='2024-12-31', freq='D')
        
        price_data = {}
        for etf_code in self.test_etfs.keys():
            # 랜덤 워크 가격 생성
            np.random.seed(42)  # 재현 가능한 결과를 위한 시드
            returns = np.random.normal(0.0005, 0.02, len(dates))  # 일일 수익률
            prices = 100 * np.cumprod(1 + returns)  # 누적 가격
            
            price_data[etf_code] = pd.DataFrame({
                'date': dates,
                'close_price': prices,
                'volume': np.random.randint(1000000, 10000000, len(dates))
            })
        
        return price_data
    
    def test_portfolio_creation(self):
        """포트폴리오 생성 테스트"""
        # 포트폴리오 생성
        allocation = {
            '069500': 40.0,
            '139660': 35.0,
            '114260': 25.0
        }
        
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="테스트 포트폴리오",
            strategy_type="core_satellite",
            target_allocation=allocation,
            risk_level="moderate"
        )
        
        # 생성 확인
        self.assertIsInstance(portfolio_id, int)
        self.assertGreater(portfolio_id, 0)
        
        # 포트폴리오 정보 확인
        portfolio_info = self.db_manager.get_portfolio_info(portfolio_id)
        self.assertIsNotNone(portfolio_info)
        self.assertEqual(portfolio_info['name'], "테스트 포트폴리오")
        self.assertEqual(portfolio_info['strategy_type'], "core_satellite")
        self.assertEqual(portfolio_info['target_allocation'], allocation)
    
    def test_transaction_recording(self):
        """거래 기록 테스트"""
        # 포트폴리오 생성
        allocation = {'069500': 50.0, '139660': 50.0}
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="거래 테스트",
            strategy_type="custom",
            target_allocation=allocation,
            risk_level="moderate"
        )
        
        # 매수 거래 기록
        transaction_id = self.portfolio_manager.add_transaction(
            portfolio_id=portfolio_id,
            etf_code='069500',
            transaction_type='BUY',
            shares=100,
            price=10000,
            fee=1500
        )
        
        # 거래 기록 확인
        self.assertIsInstance(transaction_id, int)
        
        # 포트폴리오 보유 종목 확인
        holdings = self.db_manager.get_portfolio_holdings(portfolio_id)
        self.assertFalse(holdings.empty)
        
        kodex_holding = holdings[holdings['etf_code'] == '069500'].iloc[0]
        self.assertEqual(kodex_holding['shares'], 100)
        self.assertEqual(kodex_holding['avg_price'], 10000)
    
    def test_portfolio_valuation(self):
        """포트폴리오 평가 테스트"""
        # 포트폴리오 생성 및 거래 추가
        allocation = {'069500': 100.0}
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="평가 테스트",
            strategy_type="custom",
            target_allocation=allocation,
            risk_level="moderate"
        )
        
        # 매수 거래
        self.portfolio_manager.add_transaction(
            portfolio_id=portfolio_id,
            etf_code='069500',
            transaction_type='BUY',
            shares=100,
            price=10000,
            fee=1500
        )
        
        # 현재 가격으로 평가
        current_price = 11000
        valuation = self.portfolio_manager.calculate_portfolio_value(
            portfolio_id, 
            current_prices={'069500': current_price}
        )
        
        # 평가 결과 확인
        expected_value = 100 * current_price  # 100주 * 11,000원
        expected_pnl = expected_value - (100 * 10000 + 1500)  # 손익
        
        self.assertEqual(valuation['current_value'], expected_value)
        self.assertAlmostEqual(valuation['unrealized_pnl'], expected_pnl, places=0)
    
    def test_rebalancing_calculation(self):
        """리밸런싱 계산 테스트"""
        # 현재 포트폴리오와 목표 배분
        current_weights = {'069500': 45.0, '139660': 30.0, '114260': 25.0}
        target_weights = {'069500': 40.0, '139660': 35.0, '114260': 25.0}
        
        # 리밸런싱 계획 생성
        rebalancing_plan = self.portfolio_manager.calculate_rebalancing_needs(
            current_weights=current_weights,
            target_weights=target_weights,
            threshold=3.0
        )
        
        # 결과 확인
        self.assertIn('069500', rebalancing_plan)  # 5% 차이로 조정 필요
        self.assertIn('139660', rebalancing_plan)  # 5% 차이로 조정 필요
        self.assertNotIn('114260', rebalancing_plan)  # 차이 없음
        
        # 조정 방향 확인
        self.assertEqual(rebalancing_plan['069500']['action'], 'SELL')  # 45% -> 40%
        self.assertEqual(rebalancing_plan['139660']['action'], 'BUY')   # 30% -> 35%
    
    def test_performance_calculation(self):
        """성과 계산 테스트"""
        # 포트폴리오 생성
        allocation = {'069500': 100.0}
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="성과 테스트",
            strategy_type="custom",
            target_allocation=allocation,
            risk_level="moderate"
        )
        
        # 거래 추가
        self.portfolio_manager.add_transaction(
            portfolio_id=portfolio_id,
            etf_code='069500',
            transaction_type='BUY',
            shares=100,
            price=10000,
            fee=1500
        )
        
        # 성과 데이터 추가 (시뮬레이션)
        dates = pd.date_range(start='2024-01-01', end='2024-01-10', freq='D')
        for i, date in enumerate(dates):
            total_value = 1000000 + i * 1000  # 매일 1,000원씩 증가
            daily_return = 0.1 if i > 0 else 0.0  # 일일 수익률 0.1%
            
            self.db_manager.update_portfolio_performance(
                portfolio_id=portfolio_id,
                date=date.strftime('%Y-%m-%d'),
                total_value=total_value,
                total_investment=1001500,  # 초기 투자금 + 수수료
                daily_return=daily_return
            )
        
        # 성과 조회
        performance_data = self.db_manager.get_portfolio_performance(
            portfolio_id, start_date='2024-01-01'
        )
        
        # 결과 확인
        self.assertFalse(performance_data.empty)
        self.assertEqual(len(performance_data), len(dates))
        
        # 마지막 날 수익률 확인
        last_row = performance_data.iloc[-1]
        expected_return = (last_row['total_value'] - last_row['total_investment']) / last_row['total_investment'] * 100
        self.assertAlmostEqual(last_row['cumulative_return'], expected_return, places=2)
    
    def test_risk_metrics_calculation(self):
        """리스크 지표 계산 테스트"""
        # 테스트용 수익률 데이터 생성
        np.random.seed(42)
        returns = pd.Series(np.random.normal(0.001, 0.02, 252))  # 1년치 일일 수익률
        returns.index = pd.date_range(start='2024-01-01', periods=252, freq='D')
        
        # 리스크 지표 계산
        risk_metrics = self.portfolio_manager.calculate_risk_metrics(returns)
        
        # 결과 확인
        self.assertIn('volatility', risk_metrics)
        self.assertIn('var_95', risk_metrics)
        self.assertIn('max_drawdown', risk_metrics)
        self.assertIn('sharpe_ratio', risk_metrics)
        
        # 변동성이 양수인지 확인
        self.assertGreater(risk_metrics['volatility'], 0)
        
        # VaR이 음수인지 확인 (손실이므로)
        self.assertLess(risk_metrics['var_95'], 0)
    
    def test_strategy_integration(self):
        """전략 통합 테스트"""
        # 코어-새틀라이트 전략 생성
        strategy = CoreSatelliteStrategy(core_ratio=0.8, risk_level='moderate')
        
        # 포트폴리오 생성
        target_allocation = strategy.generate_portfolio(investment_amount=10000000)
        
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="전략 통합 테스트",
            strategy_type="core_satellite",
            target_allocation=target_allocation,
            risk_level="moderate"
        )
        
        # 포트폴리오 평가
        evaluation = strategy.evaluate_current_portfolio(target_allocation)
        
        # 결과 확인
        self.assertIsInstance(portfolio_id, int)
        self.assertIn('strategy_alignment', evaluation)
        self.assertIn('recommendations', evaluation)
        
        # 전략 일치도가 높아야 함 (목표와 현재가 동일하므로)
        self.assertGreater(evaluation['strategy_alignment'], 95)
    
    def test_data_validation(self):
        """데이터 검증 테스트"""
        # 잘못된 자산배분 테스트
        invalid_allocation = {
            '069500': 60.0,
            '139660': 50.0,  # 합계가 110%
        }
        
        # 포트폴리오 생성이 실패해야 함
        with self.assertRaises(ValueError):
            self.portfolio_manager.create_portfolio(
                name="잘못된 포트폴리오",
                strategy_type="custom",
                target_allocation=invalid_allocation,
                risk_level="moderate"
            )
    
    def test_portfolio_backup_restore(self):
        """포트폴리오 백업 및 복원 테스트"""
        # 포트폴리오 생성
        allocation = {'069500': 60.0, '139660': 40.0}
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="백업 테스트",
            strategy_type="custom",
            target_allocation=allocation,
            risk_level="moderate"
        )
        
        # 거래 추가
        self.portfolio_manager.add_transaction(
            portfolio_id=portfolio_id,
            etf_code='069500',
            transaction_type='BUY',
            shares=50,
            price=10000,
            fee=1000
        )
        
        # 백업 실행
        backup_path = os.path.join(self.temp_dir, 'backup')
        backup_success = self.db_manager.backup_database(backup_path)
        
        self.assertTrue(backup_success)
        self.assertTrue(os.path.exists(backup_path))
        
        # 백업 파일 존재 확인
        backup_files = os.listdir(backup_path)
        self.assertGreater(len(backup_files), 0)
        
        # 포트폴리오 DB 파일이 백업되었는지 확인
        portfolio_backup_files = [f for f in backup_files if 'portfolio_data' in f]
        self.assertGreater(len(portfolio_backup_files), 0)

class TestPortfolioManagerPerformance(unittest.TestCase):
    """포트폴리오 매니저 성능 테스트"""
    
    def setUp(self):
        """성능 테스트 설정"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_manager = DatabaseManager(self.temp_dir)
        self.portfolio_manager = PortfolioManager(database_manager=self.db_manager)
    
    def tearDown(self):
        """테스트 정리"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_large_portfolio_performance(self):
        """대형 포트폴리오 성능 테스트"""
        import time
        
        # 10개 ETF로 구성된 큰 포트폴리오
        large_allocation = {
            f'ETF{i:03d}': 10.0 for i in range(10)
        }
        
        start_time = time.time()
        
        portfolio_id = self.portfolio_manager.create_portfolio(
            name="대형 포트폴리오",
            strategy_type="custom",
            target_allocation=large_allocation,
            risk_level="moderate"
        )
        
        # 100개 거래 추가
        for i in range(100):
            etf_code = f'ETF{i % 10:03d}'
            self.portfolio_manager.add_transaction(
                portfolio_id=portfolio_id,
                etf_code=etf_code,
                transaction_type='BUY',
                shares=10,
                price=10000 + i,
                fee=1000
            )
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # 실행 시간이 합리적인 범위 내인지 확인 (10초 이내)
        self.assertLess(execution_time, 10.0)
        
        # 데이터 정확성 확인
        holdings = self.db_manager.get_portfolio_holdings(portfolio_id)
        self.assertEqual(len(holdings), 10)  # 10개 ETF
        
        # 총 거래량 확인
        total_shares = holdings['shares'].sum()
        self.assertEqual(total_shares, 1000)  # 100거래 * 10주씩

if __name__ == '__main__':
    # 테스트 실행
    unittest.main(verbosity=2)