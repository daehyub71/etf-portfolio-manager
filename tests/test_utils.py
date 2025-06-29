"""
유틸리티 모듈 테스트
모든 유틸리티 함수와 클래스의 정확성을 검증하는 테스트
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os
import tempfile
from unittest.mock import patch, MagicMock

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.cost_calculator import CostCalculator
from utils.performance_metrics import PerformanceMetrics
from utils.data_validator import DataValidator
from utils.email_sender import EmailSender
from utils import (
    format_currency, format_percentage, calculate_compound_return,
    annualize_return, validate_allocation, safe_divide
)

class TestCostCalculator(unittest.TestCase):
    """비용 계산기 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.calculator = CostCalculator()
    
    def test_domestic_etf_trading_cost(self):
        """국내 ETF 거래 비용 계산 테스트"""
        # 100만원 거래 비용
        costs = self.calculator.calculate_trading_cost(
            trade_amount=1000000,
            etf_type='domestic_etf',
            platform='online'
        )
        
        self.assertIn('brokerage_fee', costs)
        self.assertIn('transaction_tax', costs)
        self.assertIn('total_cost', costs)
        
        # 수수료 검증
        expected_brokerage = max(1000000 * 0.015 / 100, 1000)  # 0.015%, 최소 1000원
        self.assertAlmostEqual(costs['brokerage_fee'], expected_brokerage, places=0)
        
        # 총 비용이 양수인지 확인
        self.assertGreater(costs['total_cost'], 0)
        
        # 비용 비율이 합리적인지 확인
        self.assertLess(costs['cost_percentage'], 1.0)  # 1% 미만
    
    def test_foreign_etf_trading_cost(self):
        """해외 ETF 거래 비용 계산 테스트"""
        costs = self.calculator.calculate_trading_cost(
            trade_amount=1300000,  # 100만원 상당 (USD 1000 * 환율 1300)
            etf_type='foreign_etf',
            platform='online'
        )
        
        self.assertIn('brokerage_fee_usd', costs)
        self.assertIn('conversion_cost', costs)
        self.assertIn('total_cost', costs)
        
        # USD 거래 금액 검증
        expected_usd_amount = 1300000 / self.calculator.usd_krw_rate
        self.assertAlmostEqual(costs['trade_amount_usd'], expected_usd_amount, places=2)
        
        # 환전 비용 검증
        expected_conversion = 1300000 * self.calculator.other_costs['conversion_spread']
        self.assertAlmostEqual(costs['conversion_cost'], expected_conversion, places=0)
    
    def test_annual_expense_ratio(self):
        """연간 운용보수 계산 테스트"""
        portfolio = {
            '069500': 40.0,  # KODEX 200
            '139660': 35.0,  # TIGER 미국S&P500
            '114260': 25.0   # KODEX 국고채10년
        }
        
        etf_expense_ratios = {
            '069500': 0.15,
            '139660': 0.045,
            '114260': 0.15
        }
        
        analysis = self.calculator.calculate_annual_expense_ratio(
            portfolio, etf_expense_ratios
        )
        
        self.assertIn('total_expense_ratio', analysis)
        self.assertIn('annual_cost_1M', analysis)
        self.assertIn('cost_efficiency_grade', analysis)
        
        # 가중 평균 운용보수 검증
        expected_expense_ratio = (0.15 * 0.4 + 0.045 * 0.35 + 0.15 * 0.25)
        self.assertAlmostEqual(
            analysis['total_expense_ratio'], 
            expected_expense_ratio, 
            places=4
        )
        
        # 100만원당 연간 비용
        expected_annual_cost = expected_expense_ratio * 10000
        self.assertAlmostEqual(
            analysis['annual_cost_1M'], 
            expected_annual_cost, 
            places=0
        )
    
    def test_tax_impact_calculation(self):
        """세금 영향 계산 테스트"""
        transactions = [
            {
                'etf_type': 'domestic_etf',
                'type': 'sell',
                'purchase_price': 10000,
                'sale_price': 12000,
                'capital_gain': 2000,
                'amount': 12000
            },
            {
                'etf_type': 'foreign_etf',
                'type': 'sell',
                'purchase_price': 8000,
                'sale_price': 10000,
                'capital_gain': 2000,
                'amount': 10000
            },
            {
                'etf_type': 'domestic_etf',
                'type': 'dividend',
                'amount': 500
            }
        ]
        
        tax_summary = self.calculator.calculate_tax_impact(transactions, 365)
        
        self.assertIn('capital_gains_tax', tax_summary)
        self.assertIn('dividend_tax', tax_summary)
        self.assertIn('total_tax', tax_summary)
        
        # 세금이 음수가 아닌지 확인
        self.assertGreaterEqual(tax_summary['total_tax'], 0)

class TestPerformanceMetrics(unittest.TestCase):
    """성과 지표 계산기 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.metrics = PerformanceMetrics(risk_free_rate=0.02)
        
        # 테스트용 수익률 데이터 생성
        np.random.seed(42)
        dates = pd.date_range(start='2023-01-01', periods=252, freq='D')
        self.returns = pd.Series(
            np.random.normal(0.0008, 0.015, 252),  # 일일 평균 0.08% 수익률
            index=dates
        )
        
        # 벤치마크 수익률
        self.benchmark_returns = pd.Series(
            np.random.normal(0.0006, 0.012, 252),
            index=dates
        )
    
    def test_basic_return_calculations(self):
        """기본 수익률 계산 테스트"""
        # 누적 수익률
        cumulative_returns = self.metrics.calculate_cumulative_returns(self.returns)
        self.assertEqual(len(cumulative_returns), len(self.returns))
        self.assertGreater(cumulative_returns.iloc[-1], cumulative_returns.iloc[0])
        
        # 총 수익률
        total_return = self.metrics.calculate_total_return(self.returns)
        expected_total = (1 + self.returns).prod() - 1
        self.assertAlmostEqual(total_return, expected_total, places=6)
        
        # 연환산 수익률
        annualized_return = self.metrics.calculate_annualized_return(self.returns)
        self.assertIsInstance(annualized_return, (int, float))
        self.assertGreater(annualized_return, -1.0)  # -100% 이상
        self.assertLess(annualized_return, 3.0)      # 300% 미만
    
    def test_volatility_calculation(self):
        """변동성 계산 테스트"""
        # 일별 변동성
        daily_vol = self.metrics.calculate_volatility(self.returns, annualized=False)
        self.assertGreater(daily_vol, 0)
        
        # 연환산 변동성
        annual_vol = self.metrics.calculate_volatility(self.returns, annualized=True)
        self.assertGreater(annual_vol, daily_vol)
        
        # 연환산 변동성이 일별 변동성의 √252배인지 확인
        expected_annual_vol = daily_vol * np.sqrt(252)
        self.assertAlmostEqual(annual_vol, expected_annual_vol, places=6)
    
    def test_sharpe_ratio(self):
        """샤프 비율 테스트"""
        sharpe = self.metrics.calculate_sharpe_ratio(self.returns)
        
        self.assertIsInstance(sharpe, (int, float))
        self.assertGreater(sharpe, -5.0)  # 극단적으로 낮지 않음
        self.assertLess(sharpe, 10.0)     # 극단적으로 높지 않음
        
        # 수동 계산과 비교
        excess_returns = self.returns - self.metrics.risk_free_rate / 252
        expected_sharpe = excess_returns.mean() / excess_returns.std() * np.sqrt(252)
        self.assertAlmostEqual(sharpe, expected_sharpe, places=6)
    
    def test_sortino_ratio(self):
        """소르티노 비율 테스트"""
        sortino = self.metrics.calculate_sortino_ratio(self.returns)
        
        self.assertIsInstance(sortino, (int, float))
        # 소르티노 비율은 샤프 비율보다 높거나 같아야 함 (하방 리스크만 고려)
        sharpe = self.metrics.calculate_sharpe_ratio(self.returns)
        self.assertGreaterEqual(sortino, sharpe)
    
    def test_max_drawdown(self):
        """최대 낙폭 테스트"""
        mdd_info = self.metrics.calculate_max_drawdown(self.returns)
        
        self.assertIn('max_drawdown', mdd_info)
        self.assertIn('max_drawdown_duration', mdd_info)
        
        # 최대 낙폭은 0 이하여야 함
        self.assertLessEqual(mdd_info['max_drawdown'], 0)
        
        # 지속 기간은 0 이상이어야 함
        self.assertGreaterEqual(mdd_info['max_drawdown_duration'], 0)
    
    def test_var_and_cvar(self):
        """VaR과 CVaR 테스트"""
        var_95 = self.metrics.calculate_var(self.returns, 0.05)
        cvar_95 = self.metrics.calculate_cvar(self.returns, 0.05)
        
        # VaR과 CVaR 모두 음수여야 함 (손실)
        self.assertLess(var_95, 0)
        self.assertLess(cvar_95, 0)
        
        # CVaR이 VaR보다 더 극단적(절댓값이 큰)이어야 함
        self.assertLess(cvar_95, var_95)
    
    def test_beta_and_alpha(self):
        """베타와 알파 테스트"""
        beta = self.metrics.calculate_beta(self.returns, self.benchmark_returns)
        alpha = self.metrics.calculate_alpha(self.returns, self.benchmark_returns)
        
        self.assertIsInstance(beta, (int, float))
        self.assertIsInstance(alpha, (int, float))
        
        # 베타가 합리적 범위인지 확인
        self.assertGreater(beta, -2.0)
        self.assertLess(beta, 5.0)
    
    def test_comprehensive_metrics(self):
        """종합 성과 지표 테스트"""
        metrics = self.metrics.calculate_comprehensive_metrics(
            self.returns, self.benchmark_returns
        )
        
        required_metrics = [
            'total_return', 'annualized_return', 'volatility',
            'sharpe_ratio', 'max_drawdown', 'beta', 'alpha'
        ]
        
        for metric in required_metrics:
            self.assertIn(metric, metrics)
            self.assertIsInstance(metrics[metric], (int, float))
    
    def test_performance_report(self):
        """성과 리포트 생성 테스트"""
        report = self.metrics.create_performance_report(
            self.returns, self.benchmark_returns, "테스트 포트폴리오"
        )
        
        self.assertIsInstance(report, pd.DataFrame)
        self.assertGreater(len(report), 0)
        
        required_columns = ['지표', '값', '설명']
        for col in required_columns:
            self.assertIn(col, report.columns)

class TestDataValidator(unittest.TestCase):
    """데이터 검증기 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.validator = DataValidator()
    
    def test_price_data_validation(self):
        """가격 데이터 검증 테스트"""
        # 정상 데이터
        valid_data = pd.DataFrame({
            'date': pd.date_range('2023-01-01', periods=10),
            'close_price': [100, 102, 98, 105, 110, 108, 112, 115, 113, 118],
            'volume': [1000000] * 10
        })
        
        validation_result = self.validator.validate_price_data(valid_data)
        self.assertTrue(validation_result['is_valid'])
        
        # 비정상 데이터 - 음수 가격
        invalid_data = valid_data.copy()
        invalid_data.loc[5, 'close_price'] = -50
        
        validation_result = self.validator.validate_price_data(invalid_data)
        self.assertFalse(validation_result['is_valid'])
        self.assertIn('음수 가격', str(validation_result['errors']))
    
    def test_etf_code_validation(self):
        """ETF 코드 검증 테스트"""
        # 유효한 코드들
        valid_codes = ['069500', '139660', '114260']
        for code in valid_codes:
            self.assertTrue(self.validator.validate_etf_code(code))
        
        # 무효한 코드들
        invalid_codes = ['12345', '0695001', 'KODEX', '', None]
        for code in invalid_codes:
            self.assertFalse(self.validator.validate_etf_code(code))
    
    def test_allocation_validation(self):
        """자산배분 검증 테스트"""
        # 유효한 자산배분
        valid_allocation = {'069500': 40.0, '139660': 35.0, '114260': 25.0}
        is_valid, errors = self.validator.validate_allocation(valid_allocation)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
        
        # 비중 합계 오류
        invalid_allocation = {'069500': 50.0, '139660': 35.0, '114260': 25.0}  # 110%
        is_valid, errors = self.validator.validate_allocation(invalid_allocation)
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
    
    def test_outlier_detection(self):
        """이상치 탐지 테스트"""
        # 정상 데이터
        normal_data = pd.Series([100, 102, 98, 105, 110, 108, 112, 115, 113, 118])
        outliers = self.validator.detect_outliers(normal_data)
        self.assertEqual(len(outliers), 0)
        
        # 이상치 포함 데이터
        data_with_outliers = pd.Series([100, 102, 98, 500, 110, 108, 112, 115, 113, 118])
        outliers = self.validator.detect_outliers(data_with_outliers)
        self.assertGreater(len(outliers), 0)
        self.assertIn(3, outliers)  # 500이 있는 인덱스

class TestEmailSender(unittest.TestCase):
    """이메일 발송기 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        # 실제 이메일 발송은 하지 않고 모킹
        self.email_sender = EmailSender(
            smtp_server='smtp.test.com',
            smtp_port=587,
            username='test@test.com',
            password='test_password'
        )
    
    @patch('smtplib.SMTP')
    def test_send_notification_email(self, mock_smtp):
        """알림 이메일 발송 테스트"""
        # SMTP 모킹
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        
        success = self.email_sender.send_notification(
            to_email='user@test.com',
            subject='테스트 알림',
            message='테스트 메시지입니다.'
        )
        
        self.assertTrue(success)
        mock_smtp.assert_called_once()
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once()
        mock_server.send_message.assert_called_once()
    
    @patch('smtplib.SMTP')
    def test_send_portfolio_report(self, mock_smtp):
        """포트폴리오 리포트 이메일 테스트"""
        mock_server = MagicMock()
        mock_smtp.return_value = mock_server
        
        portfolio_data = {
            'name': '테스트 포트폴리오',
            'total_value': 10000000,
            'total_return': 150000,
            'holdings': [
                {'etf_code': '069500', 'name': 'KODEX 200', 'weight': 40.0},
                {'etf_code': '139660', 'name': 'TIGER 미국S&P500', 'weight': 60.0}
            ]
        }
        
        success = self.email_sender.send_portfolio_report(
            to_email='user@test.com',
            portfolio_data=portfolio_data
        )
        
        self.assertTrue(success)
        mock_server.send_message.assert_called_once()
    
    def test_email_template_generation(self):
        """이메일 템플릿 생성 테스트"""
        template = self.email_sender.generate_rebalancing_template(
            portfolio_name='테스트 포트폴리오',
            rebalancing_actions=[
                {'etf_code': '069500', 'action': 'BUY', 'amount': 500000},
                {'etf_code': '139660', 'action': 'SELL', 'amount': 300000}
            ]
        )
        
        self.assertIsInstance(template, str)
        self.assertIn('테스트 포트폴리오', template)
        self.assertIn('069500', template)
        self.assertIn('BUY', template)

class TestUtilityFunctions(unittest.TestCase):
    """유틸리티 함수 테스트"""
    
    def test_format_currency(self):
        """통화 포맷팅 테스트"""
        # 원화 포맷팅
        self.assertEqual(format_currency(1500000), "150만원")
        self.assertEqual(format_currency(250000000), "2.5억원")
        self.assertEqual(format_currency(5000), "5,000원")
        
        # 달러 포맷팅
        self.assertEqual(format_currency(1500.50, 'USD'), "$1,500.50")
    
    def test_format_percentage(self):
        """퍼센트 포맷팅 테스트"""
        self.assertEqual(format_percentage(15.3456, 2), "15.35%")
        self.assertEqual(format_percentage(8.1, 1), "8.1%")
        self.assertEqual(format_percentage(-5.25, 2), "-5.25%")
    
    def test_calculate_compound_return(self):
        """복리 수익률 계산 테스트"""
        returns = [0.10, 0.05, -0.03, 0.08]
        compound = calculate_compound_return(returns)
        
        expected = (1.10 * 1.05 * 0.97 * 1.08) - 1
        self.assertAlmostEqual(compound, expected, places=6)
        
        # 빈 리스트 테스트
        self.assertEqual(calculate_compound_return([]), 0.0)
    
    def test_annualize_return(self):
        """연환산 수익률 테스트"""
        # 6개월 (약 180일) 10% 수익률
        annual_return = annualize_return(0.10, 180)
        
        # 연환산하면 약 20% 정도
        self.assertGreater(annual_return, 0.15)
        self.assertLess(annual_return, 0.25)
        
        # 경계 조건 테스트
        self.assertEqual(annualize_return(0.10, 0), 0.0)
        self.assertEqual(annualize_return(0.10, -10), 0.0)
    
    def test_validate_allocation(self):
        """자산배분 검증 함수 테스트"""
        # 유효한 배분
        valid_allocation = {'A': 30.0, 'B': 40.0, 'C': 30.0}
        is_valid, error = validate_allocation(valid_allocation)
        self.assertTrue(is_valid)
        self.assertEqual(error, "")
        
        # 무효한 배분 (합계 != 100%)
        invalid_allocation = {'A': 30.0, 'B': 40.0, 'C': 40.0}  # 110%
        is_valid, error = validate_allocation(invalid_allocation)
        self.assertFalse(is_valid)
        self.assertIn("100%", error)
        
        # 빈 배분
        is_valid, error = validate_allocation({})
        self.assertFalse(is_valid)
        self.assertIn("비어있습니다", error)
    
    def test_safe_divide(self):
        """안전한 나눗셈 테스트"""
        # 정상 나눗셈
        self.assertEqual(safe_divide(10, 2), 5.0)
        
        # 0으로 나누기
        self.assertEqual(safe_divide(10, 0), 0.0)
        self.assertEqual(safe_divide(10, 0, default=999), 999)
        
        # 부동소수점 계산
        result = safe_divide(1, 3)
        self.assertAlmostEqual(result, 0.3333333333333333, places=10)

class TestUtilityIntegration(unittest.TestCase):
    """유틸리티 통합 테스트"""
    
    def test_cost_and_performance_integration(self):
        """비용과 성과 계산 통합 테스트"""
        # 포트폴리오 설정
        portfolio = {'069500': 50.0, '139660': 50.0}
        expense_ratios = {'069500': 0.15, '139660': 0.045}
        
        # 비용 계산
        calculator = CostCalculator()
        cost_analysis = calculator.calculate_annual_expense_ratio(portfolio, expense_ratios)
        
        # 성과 데이터 생성
        np.random.seed(42)
        returns = pd.Series(np.random.normal(0.0008, 0.015, 252))
        
        # 성과 계산
        metrics = PerformanceMetrics()
        performance = metrics.calculate_comprehensive_metrics(returns)
        
        # 통합 분석
        net_return = performance['annualized_return'] - cost_analysis['total_expense_ratio']
        
        self.assertIsInstance(net_return, (int, float))
        self.assertLess(net_return, performance['annualized_return'])  # 비용 차감 후 수익률이 낮아야 함
    
    def test_validation_and_formatting_integration(self):
        """검증과 포맷팅 통합 테스트"""
        # 자산배분 검증
        allocation = {'069500': 40.0, '139660': 35.5, '114260': 24.5}
        is_valid, error = validate_allocation(allocation)
        
        if is_valid:
            # 포맷팅된 출력 생성
            formatted_allocation = {}
            for etf_code, weight in allocation.items():
                formatted_allocation[etf_code] = format_percentage(weight, 1)
            
            self.assertEqual(formatted_allocation['069500'], "40.0%")
            self.assertEqual(formatted_allocation['139660'], "35.5%")
            self.assertEqual(formatted_allocation['114260'], "24.5%")
        else:
            self.fail(f"유효한 자산배분이 검증에 실패: {error}")

if __name__ == '__main__':
    # 테스트 실행
    unittest.main(verbosity=2)