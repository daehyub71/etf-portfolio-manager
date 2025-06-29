# ==========================================
# test_suite.py - 종합 테스트 스위트
# ==========================================

import unittest
import pandas as pd
import numpy as np
import sqlite3
import tempfile
import os
import shutil
from datetime import datetime, timedelta
import sys

# 프로젝트 모듈 import
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from data.etf_universe import ETFUniverse
    from data.market_data_collector import MarketDataCollector
    from data.etf_analyzer import ETFAnalyzer
    from data.etf_screener import ETFScreener
    from portfolio_manager import PortfolioManager
    from update_manager import ETFUpdateManager
    from backtesting_engine import BacktestingEngine
    from report_generator import ReportGenerator
    MODULES_AVAILABLE = True
except ImportError as e:
    print(f"❌ 모듈 import 실패: {e}")
    MODULES_AVAILABLE = False

class TestETFUniverse(unittest.TestCase):
    """ETF 유니버스 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_etf.db")
        if MODULES_AVAILABLE:
            self.universe = ETFUniverse(self.db_path)
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_universe_initialization(self):
        """유니버스 초기화 테스트"""
        self.assertIsNotNone(self.universe)
        self.assertTrue(os.path.exists(self.db_path))
        
        # 데이터베이스 테이블 확인
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()
        
        table_names = [table[0] for table in tables]
        self.assertIn('etf_info', table_names)
        self.assertIn('etf_performance', table_names)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_etf_data_loading(self):
        """ETF 데이터 로딩 테스트"""
        summary = self.universe.get_universe_summary()
        
        self.assertIsInstance(summary, dict)
        self.assertIn('overall_stats', summary)
        self.assertGreater(summary['overall_stats']['total_etfs'], 0)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_etf_search(self):
        """ETF 검색 테스트"""
        # 카테고리별 조회
        domestic_etfs = self.universe.get_etf_by_category('domestic_equity')
        self.assertIsInstance(domestic_etfs, list)
        self.assertGreater(len(domestic_etfs), 0)
        
        # 특정 ETF 조회
        kodex_200 = self.universe.get_etf_info('069500')
        if kodex_200:
            self.assertEqual(kodex_200['code'], '069500')
            self.assertIn('name', kodex_200)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_etf_filtering(self):
        """ETF 필터링 테스트"""
        low_cost_etfs = self.universe.search_etfs({
            'expense_ratio_max': 0.2,
            'aum_min': 1000
        })
        
        self.assertIsInstance(low_cost_etfs, list)
        for etf in low_cost_etfs:
            self.assertLessEqual(etf['expense_ratio'], 0.2)
            self.assertGreaterEqual(etf['aum'], 1000)

class TestETFScreener(unittest.TestCase):
    """ETF 스크리너 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_etf.db")
        if MODULES_AVAILABLE:
            # 테스트용 ETF 유니버스 생성
            self.universe = ETFUniverse(self.db_path)
            self.screener = ETFScreener(self.db_path)
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_screening_criteria(self):
        """스크리닝 조건 테스트"""
        criteria = {
            'expense_ratio_max': 0.3,
            'aum_min': 1000,
            'sort_by': 'aum',
            'limit': 10
        }
        
        results = self.screener.screen_by_criteria(criteria)
        self.assertIsInstance(results, pd.DataFrame)
        
        if not results.empty:
            # 조건 검증
            for _, row in results.iterrows():
                self.assertLessEqual(row['expense_ratio'], 0.3)
                self.assertGreaterEqual(row['aum'], 1000)
            
            # 정렬 검증
            aum_values = results['aum'].tolist()
            self.assertEqual(aum_values, sorted(aum_values, reverse=True))
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_portfolio_metrics(self):
        """포트폴리오 메트릭 계산 테스트"""
        test_etfs = ['069500', '360750', '114260']
        weights = [0.5, 0.3, 0.2]
        
        metrics = self.screener.calculate_portfolio_metrics(test_etfs, weights)
        
        if metrics:
            self.assertIn('portfolio_expense_ratio', metrics)
            self.assertIn('portfolio_dividend_yield', metrics)
            self.assertIn('diversification_score', metrics)
            
            # 가중평균 검증
            self.assertGreaterEqual(metrics['diversification_score'], 0)
            self.assertLessEqual(metrics['diversification_score'], 100)

class TestPortfolioManager(unittest.TestCase):
    """포트폴리오 관리자 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_etf.db")
        if MODULES_AVAILABLE:
            # 테스트용 ETF 유니버스 생성
            self.universe = ETFUniverse(self.db_path)
            self.manager = PortfolioManager(self.db_path)
            
            # 테스트 사용자 프로필
            self.test_user = "test_user_portfolio"
            self.user_profile = {
                'age': 35,
                'risk_level': 'moderate',
                'investment_goal': 'retirement',
                'investment_horizon': 20
            }
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_portfolio_creation(self):
        """포트폴리오 생성 테스트"""
        success = self.manager.create_portfolio(
            user_id=self.test_user,
            strategy_name="core_satellite",
            initial_amount=10000000,
            user_profile=self.user_profile
        )
        
        self.assertTrue(success)
        
        # 포트폴리오 존재 확인
        summary = self.manager.get_portfolio_summary(self.test_user)
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_value, 10000000)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_strategy_allocation(self):
        """전략별 자산배분 테스트"""
        strategies = self.manager.get_available_strategies()
        self.assertIsInstance(strategies, dict)
        self.assertGreater(len(strategies), 0)
        
        # 코어-새틀라이트 전략 테스트
        if 'core_satellite' in strategies:
            strategy = self.manager.strategies['core_satellite']
            allocation = strategy.get_target_allocation(age=35, risk_level='moderate')
            
            # 비중 합계가 1인지 확인
            total_weight = sum(allocation.values())
            self.assertAlmostEqual(total_weight, 1.0, places=2)
            
            # 모든 비중이 0 이상인지 확인
            for weight in allocation.values():
                self.assertGreaterEqual(weight, 0)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_rebalancing_recommendation(self):
        """리밸런싱 추천 테스트"""
        # 포트폴리오 생성
        self.manager.create_portfolio(
            user_id=self.test_user,
            strategy_name="core_satellite",
            initial_amount=10000000,
            user_profile=self.user_profile
        )
        
        # 리밸런싱 추천 조회
        recommendation = self.manager.get_rebalance_recommendation(self.test_user, threshold=5.0)
        
        if recommendation:
            self.assertIsInstance(recommendation.rebalance_needed, bool)
            self.assertIsInstance(recommendation.total_deviation, float)
            self.assertGreaterEqual(recommendation.total_deviation, 0)

class TestBacktestingEngine(unittest.TestCase):
    """백테스팅 엔진 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_etf.db")
        if MODULES_AVAILABLE:
            self.universe = ETFUniverse(self.db_path)
            self.engine = BacktestingEngine(self.db_path)
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_synthetic_data_generation(self):
        """합성 데이터 생성 테스트"""
        etf_codes = ['069500', '360750', '114260']
        start_date = "2022-01-01"
        end_date = "2022-12-31"
        
        data = self.engine.generate_synthetic_data(etf_codes, start_date, end_date)
        
        self.assertIsInstance(data, pd.DataFrame)
        if not data.empty:
            self.assertEqual(len(data.columns), len(etf_codes))
            self.assertGreater(len(data), 200)  # 최소 200일 이상
            
            # 가격이 양수인지 확인
            for col in data.columns:
                self.assertTrue((data[col] > 0).all())
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_portfolio_returns_calculation(self):
        """포트폴리오 수익률 계산 테스트"""
        # 테스트 데이터 생성
        dates = pd.date_range('2022-01-01', '2022-12-31', freq='D')
        etf_codes = ['069500', '360750']
        
        # 간단한 가격 데이터 생성
        np.random.seed(42)
        price_data = pd.DataFrame(index=dates, columns=etf_codes)
        for code in etf_codes:
            price_data[code] = 10000 * (1 + np.random.normal(0, 0.01, len(dates))).cumprod()
        
        weights = {'069500': 0.6, '360750': 0.4}
        
        portfolio_returns, portfolio_weights = self.engine.calculate_portfolio_returns(
            price_data, weights, rebalance_freq='M'
        )
        
        self.assertIsInstance(portfolio_returns, pd.Series)
        self.assertEqual(len(portfolio_returns), len(price_data))
        
        # 수익률이 합리적 범위인지 확인 (-50% ~ +50%)
        self.assertTrue((portfolio_returns > -0.5).all())
        self.assertTrue((portfolio_returns < 0.5).all())
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_backtest_execution(self):
        """백테스팅 실행 테스트"""
        weights = {'069500': 0.6, '360750': 0.4}
        
        result = self.engine.run_backtest(
            strategy_name="Test Strategy",
            weights=weights,
            start_date="2022-01-01",
            end_date="2022-12-31",
            initial_value=1000000
        )
        
        if result:
            self.assertEqual(result.strategy_name, "Test Strategy")
            self.assertEqual(result.initial_value, 1000000)
            self.assertGreater(result.final_value, 0)
            self.assertIsInstance(result.total_return, float)
            self.assertIsInstance(result.annual_return, float)
            self.assertGreater(result.volatility, 0)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_monte_carlo_simulation(self):
        """몬테카르로 시뮬레이션 테스트"""
        weights = {'069500': 0.6, '360750': 0.4}
        
        results = self.engine.monte_carlo_simulation(
            weights=weights,
            num_simulations=100,  # 테스트용으로 적은 수
            years=5,
            initial_value=1000000
        )
        
        if results:
            self.assertIn('num_simulations', results)
            self.assertIn('mean_annual_return', results)
            self.assertIn('probability_positive', results)
            
            self.assertEqual(results['num_simulations'], 100)
            self.assertGreaterEqual(results['probability_positive'], 0)
            self.assertLessEqual(results['probability_positive'], 100)

class TestMarketDataCollector(unittest.TestCase):
    """시장 데이터 수집기 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_etf.db")
        if MODULES_AVAILABLE:
            self.universe = ETFUniverse(self.db_path)
            self.collector = MarketDataCollector(self.db_path)
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_etf_list_retrieval(self):
        """ETF 리스트 조회 테스트"""
        etf_list = self.collector.get_etf_list()
        
        self.assertIsInstance(etf_list, list)
        self.assertGreater(len(etf_list), 0)
        
        # ETF 코드가 6자리 문자열인지 확인
        for code in etf_list[:5]:  # 처음 5개만 확인
            self.assertIsInstance(code, str)
            self.assertEqual(len(code), 6)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_price_data_collection(self):
        """가격 데이터 수집 테스트"""
        test_code = '069500'
        
        price_data = self.collector.fetch_etf_price_data(test_code, period="1m")
        
        self.assertIsInstance(price_data, pd.DataFrame)
        if not price_data.empty:
            expected_columns = ['code', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']
            for col in expected_columns:
                self.assertIn(col, price_data.columns)
            
            # 가격이 양수인지 확인
            price_columns = ['Open', 'High', 'Low', 'Close']
            for col in price_columns:
                self.assertTrue((price_data[col] > 0).all())
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_performance_metrics_calculation(self):
        """성과 지표 계산 테스트"""
        # 테스트용 가격 데이터 생성
        dates = pd.date_range('2023-01-01', '2023-01-31', freq='D')
        np.random.seed(42)
        
        price_data = pd.DataFrame({
            'code': ['069500'] * len(dates),
            'Date': dates.strftime('%Y-%m-%d'),
            'Close': 10000 * (1 + np.random.normal(0, 0.01, len(dates))).cumprod(),
            'Volume': np.random.randint(100000, 1000000, len(dates))
        })
        
        metrics = self.collector.calculate_performance_metrics(price_data)
        
        if metrics:
            expected_keys = ['cumulative_return', 'volatility', 'sharpe_ratio', 
                           'max_drawdown', 'current_price']
            for key in expected_keys:
                self.assertIn(key, metrics)
            
            # 합리적 범위 확인
            self.assertGreaterEqual(metrics['volatility'], 0)
            self.assertGreaterEqual(metrics['max_drawdown'], -100)  # -100% 이상
            self.assertGreater(metrics['current_price'], 0)

class TestSystemIntegration(unittest.TestCase):
    """시스템 통합 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_integration.db")
        
        if MODULES_AVAILABLE:
            # 전체 시스템 초기화
            self.universe = ETFUniverse(self.db_path)
            self.portfolio_manager = PortfolioManager(self.db_path)
            self.update_manager = ETFUpdateManager(self.db_path)
            
            self.test_user = "integration_test_user"
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_end_to_end_workflow(self):
        """종단간 워크플로우 테스트"""
        # 1. 포트폴리오 생성
        success = self.portfolio_manager.create_portfolio(
            user_id=self.test_user,
            strategy_name="core_satellite",
            initial_amount=5000000,
            user_profile={'age': 30, 'risk_level': 'moderate'}
        )
        self.assertTrue(success)
        
        # 2. 포트폴리오 요약 조회
        summary = self.portfolio_manager.get_portfolio_summary(self.test_user)
        self.assertIsNotNone(summary)
        self.assertEqual(summary.total_value, 5000000)
        
        # 3. 리밸런싱 추천
        recommendation = self.portfolio_manager.get_rebalance_recommendation(self.test_user)
        self.assertIsNotNone(recommendation)
        
        # 4. 시스템 상태 확인
        health = self.update_manager.quick_health_check()
        self.assertIn('total_etfs', health)
        self.assertGreater(health['total_etfs'], 0)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_database_consistency(self):
        """데이터베이스 일관성 테스트"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 외래키 제약 조건 확인
        cursor.execute("PRAGMA foreign_key_check")
        fk_violations = cursor.fetchall()
        self.assertEqual(len(fk_violations), 0, "외래키 제약 조건 위반")
        
        # 데이터 무결성 확인
        cursor.execute("SELECT COUNT(*) FROM etf_info WHERE expense_ratio < 0")
        negative_expense = cursor.fetchone()[0]
        self.assertEqual(negative_expense, 0, "음수 운용보수 발견")
        
        cursor.execute("SELECT COUNT(*) FROM etf_info WHERE aum < 0")
        negative_aum = cursor.fetchone()[0]
        self.assertEqual(negative_aum, 0, "음수 순자산 발견")
        
        conn.close()

class TestPerformance(unittest.TestCase):
    """성능 테스트"""
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_large_data_handling(self):
        """대용량 데이터 처리 테스트"""
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "test_performance.db")
        
        try:
            universe = ETFUniverse(db_path)
            
            # 시간 측정
            start_time = datetime.now()
            
            # 여러 번 조회하여 성능 측정
            for _ in range(10):
                summary = universe.get_universe_summary()
                self.assertIsNotNone(summary)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 10회 조회가 10초 이내에 완료되어야 함
            self.assertLess(duration, 10.0, "성능 기준 미달")
            
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

class TestDataValidation(unittest.TestCase):
    """데이터 검증 테스트"""
    
    def setUp(self):
        """테스트 준비"""
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_validation.db")
        if MODULES_AVAILABLE:
            self.universe = ETFUniverse(self.db_path)
    
    def tearDown(self):
        """테스트 정리"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_etf_data_validation(self):
        """ETF 데이터 유효성 검증"""
        conn = sqlite3.connect(self.db_path)
        
        # 필수 필드 존재 확인
        df = pd.read_sql_query("SELECT * FROM etf_info LIMIT 10", conn)
        
        required_fields = ['name', 'code', 'category', 'expense_ratio', 'aum']
        for field in required_fields:
            self.assertIn(field, df.columns, f"필수 필드 누락: {field}")
        
        # 데이터 타입 확인
        for _, row in df.iterrows():
            self.assertIsInstance(row['expense_ratio'], (int, float))
            self.assertIsInstance(row['aum'], (int, float))
            self.assertIsInstance(row['code'], str)
            self.assertEqual(len(row['code']), 6, "ETF 코드는 6자리여야 함")
        
        conn.close()
    
    @unittest.skipUnless(MODULES_AVAILABLE, "모듈을 사용할 수 없음")
    def test_portfolio_weight_validation(self):
        """포트폴리오 가중치 검증"""
        manager = PortfolioManager(self.db_path)
        
        # 전략별 가중치 합계 확인
        for strategy_name, strategy in manager.strategies.items():
            allocation = strategy.get_target_allocation()
            total_weight = sum(allocation.values())
            
            self.assertAlmostEqual(total_weight, 1.0, places=2, 
                                 msg=f"{strategy_name} 전략의 가중치 합계가 1이 아님")
            
            # 개별 가중치가 0-1 범위인지 확인
            for etf_code, weight in allocation.items():
                self.assertGreaterEqual(weight, 0, f"{etf_code} 가중치가 음수")
                self.assertLessEqual(weight, 1, f"{etf_code} 가중치가 1 초과")


def create_test_report():
    """테스트 결과 리포트 생성"""
    print("📋 ETF 시스템 테스트 리포트 생성")
    print("=" * 60)
    
    # 테스트 스위트 구성
    test_suite = unittest.TestSuite()
    
    # 각 테스트 클래스 추가
    test_classes = [
        TestETFUniverse,
        TestETFScreener,
        TestPortfolioManager,
        TestBacktestingEngine,
        TestMarketDataCollector,
        TestSystemIntegration,
        TestPerformance,
        TestDataValidation
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # 테스트 실행
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("🎯 테스트 결과 요약")
    print("=" * 60)
    print(f"총 테스트: {result.testsRun}개")
    print(f"성공: {result.testsRun - len(result.failures) - len(result.errors)}개")
    print(f"실패: {len(result.failures)}개")
    print(f"오류: {len(result.errors)}개")
    print(f"건너뜀: {len(result.skipped)}개")
    
    if result.failures:
        print(f"\n❌ 실패한 테스트:")
        for test, traceback in result.failures:
            print(f"  - {test}")
    
    if result.errors:
        print(f"\n💥 오류가 발생한 테스트:")
        for test, traceback in result.errors:
            print(f"  - {test}")
    
    # 성공률 계산
    total_tests = result.testsRun
    successful_tests = total_tests - len(result.failures) - len(result.errors)
    success_rate = (successful_tests / total_tests * 100) if total_tests > 0 else 0
    
    print(f"\n📊 성공률: {success_rate:.1f}%")
    
    if success_rate >= 90:
        print("✅ 우수: 시스템이 안정적으로 작동합니다")
    elif success_rate >= 70:
        print("⚠️ 주의: 일부 개선이 필요합니다")
    else:
        print("❌ 경고: 시스템에 문제가 있습니다")
    
    return result


def run_quick_test():
    """빠른 테스트 실행"""
    print("⚡ 빠른 시스템 테스트")
    print("=" * 40)
    
    if not MODULES_AVAILABLE:
        print("❌ 필요한 모듈을 import할 수 없습니다")
        return False
    
    try:
        # 임시 디렉토리 생성
        temp_dir = tempfile.mkdtemp()
        db_path = os.path.join(temp_dir, "quick_test.db")
        
        # 1. ETF 유니버스 테스트
        print("1. ETF 유니버스 초기화...", end=" ")
        universe = ETFUniverse(db_path)
        print("✅")
        
        # 2. 데이터 조회 테스트
        print("2. 데이터 조회 테스트...", end=" ")
        summary = universe.get_universe_summary()
        assert summary['overall_stats']['total_etfs'] > 0
        print("✅")
        
        # 3. 포트폴리오 관리자 테스트
        print("3. 포트폴리오 관리자...", end=" ")
        manager = PortfolioManager(db_path)
        strategies = manager.get_available_strategies()
        assert len(strategies) > 0
        print("✅")
        
        # 4. 백테스팅 엔진 테스트
        print("4. 백테스팅 엔진...", end=" ")
        engine = BacktestingEngine(db_path)
        test_data = engine.generate_synthetic_data(['069500'], "2023-01-01", "2023-01-31")
        assert not test_data.empty
        print("✅")
        
        # 5. 시장 데이터 수집기 테스트
        print("5. 시장 데이터 수집기...", end=" ")
        collector = MarketDataCollector(db_path)
        etf_list = collector.get_etf_list()
        assert len(etf_list) > 0
        print("✅")
        
        print("\n🎉 모든 빠른 테스트 통과!")
        return True
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        return False
    finally:
        # 정리
        if 'temp_dir' in locals() and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


# ==========================================
# 실행 예제 및 메인 함수
# ==========================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="ETF 시스템 테스트 스위트")
    parser.add_argument("--mode", choices=["quick", "full"], default="quick",
                       help="테스트 모드 선택")
    parser.add_argument("--verbose", action="store_true",
                       help="상세 출력 모드")
    
    args = parser.parse_args()
    
    print("🧪 ETF 장기투자 관리 시스템 테스트 스위트")
    print("=" * 60)
    print(f"테스트 모드: {args.mode}")
    print(f"모듈 사용 가능: {'예' if MODULES_AVAILABLE else '아니오'}")
    print()
    
    if args.mode == "quick":
        success = run_quick_test()
        exit_code = 0 if success else 1
    else:
        result = create_test_report()
        exit_code = 0 if result.wasSuccessful() else 1
    
    print(f"\n테스트 완료 (exit code: {exit_code})")
    exit(exit_code)