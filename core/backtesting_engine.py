# ==========================================
# backtesting_engine.py - 백테스팅 및 전략 검증 엔진
# ==========================================

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import logging

# 최적화 라이브러리 (선택적)
try:
    from scipy import stats
    from scipy.optimize import minimize
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️ scipy 없음 - 기본 통계만 사용")

@dataclass
class BacktestResult:
    """백테스팅 결과"""
    strategy_name: str
    start_date: str
    end_date: str
    initial_value: float
    final_value: float
    total_return: float
    annual_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    calmar_ratio: float
    win_rate: float
    avg_gain: float
    avg_loss: float
    num_trades: int
    transaction_costs: float
    
@dataclass
class PerformanceMetrics:
    """성과 지표"""
    returns: np.ndarray
    cumulative_returns: np.ndarray
    portfolio_values: np.ndarray
    drawdowns: np.ndarray
    rolling_sharpe: np.ndarray
    rolling_volatility: np.ndarray

class BacktestingEngine:
    """백테스팅 엔진"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        self.risk_free_rate = 0.025  # 2.5% 무위험 수익률
        
        # 거래 비용 설정
        self.transaction_cost_rate = 0.001  # 0.1% 거래 비용
        self.min_trade_amount = 50000       # 최소 거래 금액
        
        self.logger.info("📈 백테스팅 엔진 초기화 완료")
    
    def generate_synthetic_data(self, etf_codes: List[str], 
                               start_date: str, end_date: str,
                               freq: str = 'D') -> pd.DataFrame:
        """합성 가격 데이터 생성 (실제 데이터 없을 때)"""
        try:
            start = pd.to_datetime(start_date)
            end = pd.to_datetime(end_date)
            dates = pd.date_range(start=start, end=end, freq=freq)
            
            # 거래일만 필터링 (평일)
            business_dates = dates[dates.weekday < 5]
            
            # ETF별 기본 설정
            etf_settings = {
                '069500': {'initial_price': 28400, 'annual_return': 0.08, 'volatility': 0.18},
                '360750': {'initial_price': 15800, 'annual_return': 0.12, 'volatility': 0.22},
                '114260': {'initial_price': 108500, 'annual_return': 0.03, 'volatility': 0.05},
                '133690': {'initial_price': 24500, 'annual_return': 0.15, 'volatility': 0.28},
                '195930': {'initial_price': 13200, 'annual_return': 0.10, 'volatility': 0.20},
                '195980': {'initial_price': 8400, 'annual_return': 0.09, 'volatility': 0.25},
                '229200': {'initial_price': 9800, 'annual_return': 0.11, 'volatility': 0.24},
            }
            
            data = pd.DataFrame(index=business_dates)
            
            for etf_code in etf_codes:
                settings = etf_settings.get(etf_code, {
                    'initial_price': 10000, 
                    'annual_return': 0.08, 
                    'volatility': 0.18
                })
                
                # 기하 브라운 운동으로 가격 생성
                dt = 1/252  # 일일 단위
                num_days = len(business_dates)
                
                # 일일 수익률 생성
                drift = (settings['annual_return'] - 0.5 * settings['volatility']**2) * dt
                diffusion = settings['volatility'] * np.sqrt(dt) * np.random.normal(0, 1, num_days)
                
                log_returns = drift + diffusion
                
                # 가격 계산
                prices = [settings['initial_price']]
                for i in range(1, num_days):
                    prices.append(prices[-1] * np.exp(log_returns[i]))
                
                data[etf_code] = prices
            
            self.logger.info(f"📊 합성 데이터 생성: {len(etf_codes)}개 ETF, {len(business_dates)}일")
            return data
            
        except Exception as e:
            self.logger.error(f"❌ 합성 데이터 생성 실패: {e}")
            return pd.DataFrame()
    
    def load_historical_data(self, etf_codes: List[str], 
                           start_date: str, end_date: str) -> pd.DataFrame:
        """과거 데이터 로드 (실제 DB에서)"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 성과 데이터 테이블에서 가격 조회
            all_data = []
            
            for etf_code in etf_codes:
                query = '''
                    SELECT date, price 
                    FROM etf_performance 
                    WHERE code = ? AND date BETWEEN ? AND ?
                    ORDER BY date
                '''
                df = pd.read_sql_query(query, conn, params=(etf_code, start_date, end_date))
                
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    df.columns = [etf_code]
                    all_data.append(df)
            
            conn.close()
            
            if all_data:
                # 모든 ETF 데이터 병합
                combined_data = pd.concat(all_data, axis=1, sort=True)
                combined_data = combined_data.fillna(method='ffill')  # 결측값 전진 채우기
                
                self.logger.info(f"📊 실제 데이터 로드: {len(etf_codes)}개 ETF, {len(combined_data)}일")
                return combined_data
            else:
                # 실제 데이터가 없으면 합성 데이터 생성
                self.logger.warning("⚠️ 실제 데이터 없음, 합성 데이터 생성")
                return self.generate_synthetic_data(etf_codes, start_date, end_date)
                
        except Exception as e:
            self.logger.error(f"❌ 과거 데이터 로드 실패: {e}")
            return self.generate_synthetic_data(etf_codes, start_date, end_date)
    
    def calculate_portfolio_returns(self, price_data: pd.DataFrame, 
                                  weights: Dict[str, float],
                                  rebalance_freq: str = 'M') -> Tuple[pd.Series, pd.DataFrame]:
        """포트폴리오 수익률 계산"""
        try:
            # 일일 수익률 계산
            returns = price_data.pct_change().fillna(0)
            
            # 리밸런싱 일정 생성
            if rebalance_freq == 'M':  # 월별
                rebalance_dates = pd.date_range(
                    start=price_data.index[0], 
                    end=price_data.index[-1], 
                    freq='M'
                )
            elif rebalance_freq == 'Q':  # 분기별
                rebalance_dates = pd.date_range(
                    start=price_data.index[0], 
                    end=price_data.index[-1], 
                    freq='Q'
                )
            elif rebalance_freq == 'Y':  # 연별
                rebalance_dates = pd.date_range(
                    start=price_data.index[0], 
                    end=price_data.index[-1], 
                    freq='Y'
                )
            else:  # 리밸런싱 없음
                rebalance_dates = [price_data.index[0]]
            
            # 포트폴리오 가치 추적
            portfolio_values = []
            portfolio_weights = pd.DataFrame(index=price_data.index, columns=price_data.columns)
            
            initial_value = 1000000  # 초기 100만원
            current_value = initial_value
            current_weights = pd.Series(weights)
            
            for date in price_data.index:
                # 리밸런싱 체크
                if date in rebalance_dates:
                    current_weights = pd.Series(weights)
                
                # 일일 수익률 적용
                daily_return = (returns.loc[date] * current_weights).sum()
                current_value *= (1 + daily_return)
                
                portfolio_values.append(current_value)
                portfolio_weights.loc[date] = current_weights
                
                # 가중치 자연 변화 (리밸런싱 전까지)
                if date not in rebalance_dates:
                    asset_values = current_weights * current_value
                    asset_values *= (1 + returns.loc[date])
                    current_weights = asset_values / asset_values.sum()
                    current_weights = current_weights.fillna(0)
            
            portfolio_series = pd.Series(portfolio_values, index=price_data.index)
            portfolio_returns = portfolio_series.pct_change().fillna(0)
            
            return portfolio_returns, portfolio_weights
            
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 수익률 계산 실패: {e}")
            return pd.Series(), pd.DataFrame()
    
    def calculate_performance_metrics(self, returns: pd.Series, 
                                    portfolio_values: pd.Series) -> PerformanceMetrics:
        """성과 지표 계산"""
        try:
            # 누적 수익률
            cumulative_returns = (1 + returns).cumprod() - 1
            
            # 드로우다운 계산
            peak = portfolio_values.expanding(min_periods=1).max()
            drawdowns = (portfolio_values / peak - 1) * 100
            
            # 롤링 샤프 비율 (60일 윈도우)
            rolling_sharpe = []
            rolling_vol = []
            
            for i in range(60, len(returns)):
                window_returns = returns.iloc[i-60:i]
                excess_returns = window_returns.mean() * 252 - self.risk_free_rate
                volatility = window_returns.std() * np.sqrt(252)
                
                if volatility > 0:
                    sharpe = excess_returns / volatility
                else:
                    sharpe = 0
                
                rolling_sharpe.append(sharpe)
                rolling_vol.append(volatility)
            
            # 앞의 59일은 NaN으로 채움
            rolling_sharpe = [np.nan] * 59 + rolling_sharpe
            rolling_vol = [np.nan] * 59 + rolling_vol
            
            return PerformanceMetrics(
                returns=returns.values,
                cumulative_returns=cumulative_returns.values,
                portfolio_values=portfolio_values.values,
                drawdowns=drawdowns.values,
                rolling_sharpe=np.array(rolling_sharpe),
                rolling_volatility=np.array(rolling_vol)
            )
            
        except Exception as e:
            self.logger.error(f"❌ 성과 지표 계산 실패: {e}")
            return PerformanceMetrics(
                returns=np.array([]),
                cumulative_returns=np.array([]),
                portfolio_values=np.array([]),
                drawdowns=np.array([]),
                rolling_sharpe=np.array([]),
                rolling_volatility=np.array([])
            )
    
    def run_backtest(self, strategy_name: str, weights: Dict[str, float],
                    start_date: str, end_date: str, 
                    initial_value: float = 1000000,
                    rebalance_freq: str = 'M') -> BacktestResult:
        """백테스팅 실행"""
        try:
            self.logger.info(f"📈 백테스팅 시작: {strategy_name} ({start_date} ~ {end_date})")
            
            # 1. 가격 데이터 로드
            etf_codes = list(weights.keys())
            price_data = self.load_historical_data(etf_codes, start_date, end_date)
            
            if price_data.empty:
                raise ValueError("가격 데이터를 로드할 수 없습니다")
            
            # 2. 포트폴리오 수익률 계산
            portfolio_returns, portfolio_weights = self.calculate_portfolio_returns(
                price_data, weights, rebalance_freq
            )
            
            # 3. 포트폴리오 가치 계산
            portfolio_values = initial_value * (1 + portfolio_returns).cumprod()
            
            # 4. 기본 성과 지표
            total_return = (portfolio_values.iloc[-1] / initial_value - 1) * 100
            
            # 연환산 수익률
            years = len(portfolio_returns) / 252
            annual_return = ((portfolio_values.iloc[-1] / initial_value) ** (1/years) - 1) * 100
            
            # 변동성 (연환산)
            volatility = portfolio_returns.std() * np.sqrt(252) * 100
            
            # 샤프 비율
            excess_return = annual_return - self.risk_free_rate * 100
            sharpe_ratio = excess_return / volatility if volatility > 0 else 0
            
            # 최대 낙폭
            peak = portfolio_values.expanding(min_periods=1).max()
            drawdowns = (portfolio_values / peak - 1) * 100
            max_drawdown = drawdowns.min()
            
            # 칼마 비율
            calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0
            
            # 승률 및 평균 손익
            positive_days = portfolio_returns[portfolio_returns > 0]
            negative_days = portfolio_returns[portfolio_returns < 0]
            
            win_rate = len(positive_days) / len(portfolio_returns) * 100
            avg_gain = positive_days.mean() * 100 if len(positive_days) > 0 else 0
            avg_loss = negative_days.mean() * 100 if len(negative_days) > 0 else 0
            
            # 리밸런싱 횟수 추정
            if rebalance_freq == 'M':
                num_trades = int(years * 12) * len(etf_codes)
            elif rebalance_freq == 'Q':
                num_trades = int(years * 4) * len(etf_codes)
            elif rebalance_freq == 'Y':
                num_trades = int(years) * len(etf_codes)
            else:
                num_trades = len(etf_codes)  # 초기 매수만
            
            # 거래 비용
            total_traded_value = portfolio_values.iloc[-1] * 0.1 * num_trades  # 추정
            transaction_costs = total_traded_value * self.transaction_cost_rate
            
            result = BacktestResult(
                strategy_name=strategy_name,
                start_date=start_date,
                end_date=end_date,
                initial_value=initial_value,
                final_value=round(portfolio_values.iloc[-1], 2),
                total_return=round(total_return, 2),
                annual_return=round(annual_return, 2),
                volatility=round(volatility, 2),
                sharpe_ratio=round(sharpe_ratio, 2),
                max_drawdown=round(max_drawdown, 2),
                calmar_ratio=round(calmar_ratio, 2),
                win_rate=round(win_rate, 2),
                avg_gain=round(avg_gain, 3),
                avg_loss=round(avg_loss, 3),
                num_trades=num_trades,
                transaction_costs=round(transaction_costs, 2)
            )
            
            self.logger.info(f"✅ 백테스팅 완료: 연수익률 {annual_return:.2f}%, 샤프비율 {sharpe_ratio:.2f}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ 백테스팅 실행 실패: {e}")
            return None
    
    def compare_strategies(self, strategies: Dict[str, Dict[str, float]],
                          start_date: str, end_date: str,
                          initial_value: float = 1000000) -> pd.DataFrame:
        """여러 전략 비교"""
        results = []
        
        self.logger.info(f"⚖️ 전략 비교 시작: {len(strategies)}개 전략")
        
        for strategy_name, weights in strategies.items():
            result = self.run_backtest(
                strategy_name, weights, start_date, end_date, initial_value
            )
            
            if result:
                results.append(asdict(result))
        
        if results:
            comparison_df = pd.DataFrame(results)
            comparison_df = comparison_df.sort_values('sharpe_ratio', ascending=False)
            
            self.logger.info(f"✅ 전략 비교 완료: {len(results)}개 결과")
            return comparison_df
        else:
            self.logger.error("❌ 전략 비교 결과 없음")
            return pd.DataFrame()
    
    def monte_carlo_simulation(self, weights: Dict[str, float],
                              num_simulations: int = 1000,
                              years: int = 10,
                              initial_value: float = 1000000) -> Dict:
        """몬테카르로 시뮬레이션"""
        try:
            self.logger.info(f"🎲 몬테카르로 시뮬레이션: {num_simulations}회, {years}년")
            
            # ETF별 과거 수익률 통계 (간단화)
            etf_stats = {
                '069500': {'mean': 0.08, 'std': 0.18},
                '360750': {'mean': 0.12, 'std': 0.22},
                '114260': {'mean': 0.03, 'std': 0.05},
                '133690': {'mean': 0.15, 'std': 0.28},
                '195930': {'mean': 0.10, 'std': 0.20},
            }
            
            simulation_results = []
            
            for _ in range(num_simulations):
                portfolio_value = initial_value
                
                for year in range(years):
                    # 포트폴리오 연간 수익률 계산
                    annual_return = 0
                    
                    for etf_code, weight in weights.items():
                        if etf_code in etf_stats:
                            stats = etf_stats[etf_code]
                            etf_return = np.random.normal(stats['mean'], stats['std'])
                            annual_return += weight * etf_return
                    
                    portfolio_value *= (1 + annual_return)
                
                total_return = (portfolio_value / initial_value - 1) * 100
                annual_return = ((portfolio_value / initial_value) ** (1/years) - 1) * 100
                
                simulation_results.append({
                    'final_value': portfolio_value,
                    'total_return': total_return,
                    'annual_return': annual_return
                })
            
            # 통계 계산
            final_values = [r['final_value'] for r in simulation_results]
            annual_returns = [r['annual_return'] for r in simulation_results]
            
            results = {
                'num_simulations': num_simulations,
                'years': years,
                'mean_final_value': np.mean(final_values),
                'median_final_value': np.median(final_values),
                'std_final_value': np.std(final_values),
                'min_final_value': np.min(final_values),
                'max_final_value': np.max(final_values),
                'mean_annual_return': np.mean(annual_returns),
                'median_annual_return': np.median(annual_returns),
                'std_annual_return': np.std(annual_returns),
                'percentile_5': np.percentile(annual_returns, 5),
                'percentile_95': np.percentile(annual_returns, 95),
                'probability_positive': len([r for r in annual_returns if r > 0]) / num_simulations * 100,
                'probability_beat_inflation': len([r for r in annual_returns if r > 3]) / num_simulations * 100,
            }
            
            self.logger.info(f"✅ 몬테카르로 완료: 평균 연수익률 {results['mean_annual_return']:.2f}%")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 몬테카르로 시뮬레이션 실패: {e}")
            return {}
    
    def stress_test(self, weights: Dict[str, float],
                   stress_scenarios: Dict[str, Dict[str, float]]) -> Dict:
        """스트레스 테스트"""
        try:
            self.logger.info(f"🔥 스트레스 테스트: {len(stress_scenarios)}개 시나리오")
            
            results = {}
            
            for scenario_name, etf_shocks in stress_scenarios.items():
                portfolio_shock = 0
                
                for etf_code, weight in weights.items():
                    shock = etf_shocks.get(etf_code, 0)
                    portfolio_shock += weight * shock
                
                results[scenario_name] = {
                    'portfolio_impact': round(portfolio_shock * 100, 2),
                    'value_change': round(1000000 * portfolio_shock, 0)
                }
            
            self.logger.info(f"✅ 스트레스 테스트 완료")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 스트레스 테스트 실패: {e}")
            return {}


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("📈 백테스팅 엔진 테스트")
    print("=" * 60)
    
    # 백테스팅 엔진 초기화
    engine = BacktestingEngine()
    
    # 테스트 전략들
    strategies = {
        "코어-새틀라이트": {
            '069500': 0.4,  # KODEX 200
            '360750': 0.25, # TIGER 미국S&P500
            '114260': 0.15, # KODEX 국고채10년
            '133690': 0.1,  # KODEX 나스닥100
            '195930': 0.1   # KODEX 선진국MSCI
        },
        "공격적 성장": {
            '069500': 0.3,
            '360750': 0.4,
            '133690': 0.2,
            '195930': 0.1
        },
        "보수적 균형": {
            '069500': 0.3,
            '360750': 0.2,
            '114260': 0.4,
            '195930': 0.1
        }
    }
    
    # 백테스팅 기간
    start_date = "2020-01-01"
    end_date = "2023-12-31"
    initial_value = 10000000  # 1천만원
    
    print(f"\n📊 백테스팅 설정:")
    print(f"- 기간: {start_date} ~ {end_date}")
    print(f"- 초기 투자금: {initial_value:,}원")
    print(f"- 전략 개수: {len(strategies)}개")
    
    # 1. 개별 전략 백테스팅
    print(f"\n🎯 개별 전략 백테스팅:")
    
    strategy_name = "코어-새틀라이트"
    weights = strategies[strategy_name]
    
    result = engine.run_backtest(
        strategy_name=strategy_name,
        weights=weights,
        start_date=start_date,
        end_date=end_date,
        initial_value=initial_value,
        rebalance_freq='M'  # 월별 리밸런싱
    )
    
    if result:
        print(f"\n📈 {strategy_name} 결과:")
        print(f"- 최종 가치: {result.final_value:,.0f}원")
        print(f"- 총 수익률: {result.total_return:+.2f}%")
        print(f"- 연평균 수익률: {result.annual_return:+.2f}%")
        print(f"- 변동성: {result.volatility:.2f}%")
        print(f"- 샤프 비율: {result.sharpe_ratio:.2f}")
        print(f"- 최대 낙폭: {result.max_drawdown:.2f}%")
        print(f"- 칼마 비율: {result.calmar_ratio:.2f}")
        print(f"- 승률: {result.win_rate:.1f}%")
        print(f"- 거래 횟수: {result.num_trades}회")
        print(f"- 거래 비용: {result.transaction_costs:,.0f}원")
    
    # 2. 전략 비교
    print(f"\n⚖️ 전략 비교 분석:")
    
    comparison_df = engine.compare_strategies(
        strategies=strategies,
        start_date=start_date,
        end_date=end_date,
        initial_value=initial_value
    )
    
    if not comparison_df.empty:
        print(f"\n📊 전략별 성과 순위 (샤프비율 기준):")
        for i, (_, row) in enumerate(comparison_df.iterrows(), 1):
            print(f"{i}. {row['strategy_name']}")
            print(f"   연수익률: {row['annual_return']:+.2f}%, "
                  f"변동성: {row['volatility']:.2f}%, "
                  f"샤프비율: {row['sharpe_ratio']:.2f}")
        
        # 리스크-수익 분석
        print(f"\n📈 리스크-수익 매트릭스:")
        for _, row in comparison_df.iterrows():
            risk_level = "낮음" if row['volatility'] < 15 else "보통" if row['volatility'] < 20 else "높음"
            return_level = "낮음" if row['annual_return'] < 8 else "보통" if row['annual_return'] < 12 else "높음"
            print(f"- {row['strategy_name']}: 위험도 {risk_level}, 수익성 {return_level}")
    
    # 3. 몬테카르로 시뮬레이션
    print(f"\n🎲 몬테카르로 시뮬레이션:")
    
    mc_results = engine.monte_carlo_simulation(
        weights=strategies["코어-새틀라이트"],
        num_simulations=1000,
        years=10,
        initial_value=initial_value
    )
    
    if mc_results:
        print(f"- 시뮬레이션: {mc_results['num_simulations']}회 × {mc_results['years']}년")
        print(f"- 평균 연수익률: {mc_results['mean_annual_return']:.2f}%")
        print(f"- 연수익률 표준편차: {mc_results['std_annual_return']:.2f}%")
        print(f"- 5% 하위 시나리오: {mc_results['percentile_5']:.2f}%")
        print(f"- 95% 상위 시나리오: {mc_results['percentile_95']:.2f}%")
        print(f"- 수익 확률: {mc_results['probability_positive']:.1f}%")
        print(f"- 인플레이션 초과 확률: {mc_results['probability_beat_inflation']:.1f}%")
        print(f"- 예상 최종 가치: {mc_results['mean_final_value']:,.0f}원")
    
    # 4. 스트레스 테스트
    print(f"\n🔥 스트레스 테스트:")
    
    stress_scenarios = {
        "코로나 급락": {
            '069500': -0.20,  # 국내 주식 -20%
            '360750': -0.15,  # 미국 주식 -15%
            '133690': -0.25,  # 나스닥 -25%
            '195930': -0.18,  # 선진국 -18%
            '114260': 0.05    # 국채 +5%
        },
        "금리 급상승": {
            '069500': -0.10,
            '360750': -0.12,
            '133690': -0.15,
            '195930': -0.10,
            '114260': -0.08   # 채권 -8%
        },
        "경기침체": {
            '069500': -0.25,
            '360750': -0.20,
            '133690': -0.30,
            '195930': -0.22,
            '114260': 0.03
        }
    }
    
    stress_results = engine.stress_test(
        weights=strategies["코어-새틀라이트"],
        stress_scenarios=stress_scenarios
    )
    
    if stress_results:
        for scenario, result in stress_results.items():
            print(f"- {scenario}: {result['portfolio_impact']:+.2f}% "
                  f"({result['value_change']:+,.0f}원)")
    
    # 5. 리밸런싱 빈도 비교
    print(f"\n🔄 리밸런싱 빈도별 성과:")
    
    rebalance_frequencies = {
        "리밸런싱 없음": None,
        "연간": 'Y',
        "분기": 'Q',
        "월간": 'M'
    }
    
    for freq_name, freq_code in rebalance_frequencies.items():
        result = engine.run_backtest(
            strategy_name="코어-새틀라이트",
            weights=strategies["코어-새틀라이트"],
            start_date=start_date,
            end_date=end_date,
            initial_value=initial_value,
            rebalance_freq=freq_code or 'None'
        )
        
        if result:
            net_return = result.annual_return - (result.transaction_costs / initial_value * 100)
            print(f"- {freq_name}: 연수익률 {result.annual_return:.2f}% "
                  f"(비용차감 {net_return:.2f}%), 거래비용 {result.transaction_costs:,.0f}원")
    
    print(f"\n✅ 백테스팅 엔진 테스트 완료!")
    print(f"💡 주요 인사이트:")
    print(f"   - 리밸런싱은 적절한 빈도가 중요 (과도한 리밸런싱은 비용 증가)")
    print(f"   - 분산투자를 통한 리스크 감소 효과 확인")
    print(f"   - 장기투자시 시장 변동성 극복 가능")
    print(f"   - 스트레스 테스트로 최악 시나리오 대비 필요")