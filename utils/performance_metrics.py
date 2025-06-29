"""
성과 지표 계산 모듈
포트폴리오와 ETF의 다양한 성과 지표를 계산하는 핵심 유틸리티
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import logging
from scipy import stats
import warnings

logger = logging.getLogger(__name__)

class PerformanceMetrics:
    """성과 지표 계산 클래스"""
    
    def __init__(self, risk_free_rate: float = 0.02):
        """
        성과 지표 계산기 초기화
        
        Args:
            risk_free_rate: 무위험 이자율 (연간, 소수점 형태)
        """
        self.risk_free_rate = risk_free_rate
        self.trading_days_per_year = 252
    
    def calculate_returns(self, prices: Union[pd.Series, pd.DataFrame], 
                         method: str = 'simple') -> Union[pd.Series, pd.DataFrame]:
        """
        수익률 계산
        
        Args:
            prices: 가격 데이터 (Series 또는 DataFrame)
            method: 계산 방법 ('simple', 'log')
            
        Returns:
            수익률 데이터
        """
        try:
            if method == 'simple':
                returns = prices.pct_change().dropna()
            elif method == 'log':
                returns = np.log(prices / prices.shift(1)).dropna()
            else:
                raise ValueError(f"지원하지 않는 방법: {method}")
            
            return returns
            
        except Exception as e:
            logger.error(f"수익률 계산 실패: {e}")
            return pd.Series() if isinstance(prices, pd.Series) else pd.DataFrame()
    
    def calculate_cumulative_returns(self, returns: Union[pd.Series, pd.DataFrame]) -> Union[pd.Series, pd.DataFrame]:
        """누적 수익률 계산"""
        try:
            return (1 + returns).cumprod() - 1
        except Exception as e:
            logger.error(f"누적 수익률 계산 실패: {e}")
            return pd.Series() if isinstance(returns, pd.Series) else pd.DataFrame()
    
    def calculate_total_return(self, returns: pd.Series) -> float:
        """총 수익률 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            return (1 + returns).prod() - 1
        except Exception as e:
            logger.error(f"총 수익률 계산 실패: {e}")
            return 0.0
    
    def calculate_annualized_return(self, returns: pd.Series, 
                                  periods_per_year: Optional[int] = None) -> float:
        """연환산 수익률 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            if periods_per_year is None:
                periods_per_year = self.trading_days_per_year
            
            total_return = self.calculate_total_return(returns)
            num_periods = len(returns)
            
            if num_periods == 0:
                return 0.0
            
            annualized_return = (1 + total_return) ** (periods_per_year / num_periods) - 1
            return annualized_return
            
        except Exception as e:
            logger.error(f"연환산 수익률 계산 실패: {e}")
            return 0.0
    
    def calculate_volatility(self, returns: pd.Series, 
                           annualized: bool = True) -> float:
        """변동성 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            volatility = returns.std()
            
            if annualized:
                volatility *= np.sqrt(self.trading_days_per_year)
            
            return volatility
            
        except Exception as e:
            logger.error(f"변동성 계산 실패: {e}")
            return 0.0
    
    def calculate_sharpe_ratio(self, returns: pd.Series, 
                             risk_free_rate: Optional[float] = None) -> float:
        """샤프 비율 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            if risk_free_rate is None:
                risk_free_rate = self.risk_free_rate
            
            excess_returns = returns - risk_free_rate / self.trading_days_per_year
            
            if excess_returns.std() == 0:
                return 0.0
            
            sharpe_ratio = excess_returns.mean() / excess_returns.std() * np.sqrt(self.trading_days_per_year)
            return sharpe_ratio
            
        except Exception as e:
            logger.error(f"샤프 비율 계산 실패: {e}")
            return 0.0
    
    def calculate_sortino_ratio(self, returns: pd.Series, 
                              risk_free_rate: Optional[float] = None,
                              target_return: float = 0.0) -> float:
        """소르티노 비율 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            if risk_free_rate is None:
                risk_free_rate = self.risk_free_rate
            
            excess_returns = returns - risk_free_rate / self.trading_days_per_year
            
            # 하방 편차 계산
            downside_returns = excess_returns[excess_returns < target_return]
            
            if len(downside_returns) == 0:
                return np.inf if excess_returns.mean() > 0 else 0.0
            
            downside_deviation = np.sqrt((downside_returns ** 2).mean())
            
            if downside_deviation == 0:
                return 0.0
            
            sortino_ratio = excess_returns.mean() / downside_deviation * np.sqrt(self.trading_days_per_year)
            return sortino_ratio
            
        except Exception as e:
            logger.error(f"소르티노 비율 계산 실패: {e}")
            return 0.0
    
    def calculate_max_drawdown(self, returns: pd.Series) -> Dict[str, float]:
        """최대 낙폭 계산"""
        try:
            if len(returns) == 0:
                return {'max_drawdown': 0.0, 'max_drawdown_duration': 0}
            
            cumulative_returns = self.calculate_cumulative_returns(returns)
            running_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - running_max) / (1 + running_max)
            
            max_drawdown = drawdown.min()
            
            # 최대 낙폭 지속 기간 계산
            max_dd_end = drawdown.idxmin()
            max_dd_start = running_max.loc[:max_dd_end].idxmax()
            
            if isinstance(max_dd_start, str):
                max_dd_start = pd.to_datetime(max_dd_start)
            if isinstance(max_dd_end, str):
                max_dd_end = pd.to_datetime(max_dd_end)
            
            max_dd_duration = (max_dd_end - max_dd_start).days if max_dd_start != max_dd_end else 0
            
            return {
                'max_drawdown': max_drawdown,
                'max_drawdown_duration': max_dd_duration,
                'max_drawdown_start': max_dd_start,
                'max_drawdown_end': max_dd_end
            }
            
        except Exception as e:
            logger.error(f"최대 낙폭 계산 실패: {e}")
            return {'max_drawdown': 0.0, 'max_drawdown_duration': 0}
    
    def calculate_calmar_ratio(self, returns: pd.Series) -> float:
        """칼마 비율 계산"""
        try:
            annualized_return = self.calculate_annualized_return(returns)
            max_drawdown_info = self.calculate_max_drawdown(returns)
            max_drawdown = abs(max_drawdown_info['max_drawdown'])
            
            if max_drawdown == 0:
                return np.inf if annualized_return > 0 else 0.0
            
            calmar_ratio = annualized_return / max_drawdown
            return calmar_ratio
            
        except Exception as e:
            logger.error(f"칼마 비율 계산 실패: {e}")
            return 0.0
    
    def calculate_var(self, returns: pd.Series, confidence_level: float = 0.05) -> float:
        """VaR (Value at Risk) 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            var = np.percentile(returns, confidence_level * 100)
            return var
            
        except Exception as e:
            logger.error(f"VaR 계산 실패: {e}")
            return 0.0
    
    def calculate_cvar(self, returns: pd.Series, confidence_level: float = 0.05) -> float:
        """CVaR (Conditional Value at Risk) 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            var = self.calculate_var(returns, confidence_level)
            cvar = returns[returns <= var].mean()
            return cvar
            
        except Exception as e:
            logger.error(f"CVaR 계산 실패: {e}")
            return 0.0
    
    def calculate_skewness(self, returns: pd.Series) -> float:
        """왜도 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            return stats.skew(returns.dropna())
        except Exception as e:
            logger.error(f"왜도 계산 실패: {e}")
            return 0.0
    
    def calculate_kurtosis(self, returns: pd.Series) -> float:
        """첨도 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            return stats.kurtosis(returns.dropna())
        except Exception as e:
            logger.error(f"첨도 계산 실패: {e}")
            return 0.0
    
    def calculate_beta(self, asset_returns: pd.Series, 
                      benchmark_returns: pd.Series) -> float:
        """베타 계산"""
        try:
            if len(asset_returns) == 0 or len(benchmark_returns) == 0:
                return 1.0
            
            # 공통 인덱스로 정렬
            common_index = asset_returns.index.intersection(benchmark_returns.index)
            asset_aligned = asset_returns.reindex(common_index).dropna()
            benchmark_aligned = benchmark_returns.reindex(common_index).dropna()
            
            if len(asset_aligned) == 0 or len(benchmark_aligned) == 0:
                return 1.0
            
            covariance = np.cov(asset_aligned, benchmark_aligned)[0, 1]
            benchmark_variance = np.var(benchmark_aligned)
            
            if benchmark_variance == 0:
                return 1.0
            
            beta = covariance / benchmark_variance
            return beta
            
        except Exception as e:
            logger.error(f"베타 계산 실패: {e}")
            return 1.0
    
    def calculate_alpha(self, asset_returns: pd.Series, 
                       benchmark_returns: pd.Series,
                       risk_free_rate: Optional[float] = None) -> float:
        """알파 계산"""
        try:
            if risk_free_rate is None:
                risk_free_rate = self.risk_free_rate
            
            beta = self.calculate_beta(asset_returns, benchmark_returns)
            
            asset_annual_return = self.calculate_annualized_return(asset_returns)
            benchmark_annual_return = self.calculate_annualized_return(benchmark_returns)
            
            alpha = asset_annual_return - (risk_free_rate + beta * (benchmark_annual_return - risk_free_rate))
            return alpha
            
        except Exception as e:
            logger.error(f"알파 계산 실패: {e}")
            return 0.0
    
    def calculate_information_ratio(self, asset_returns: pd.Series, 
                                  benchmark_returns: pd.Series) -> float:
        """정보 비율 계산"""
        try:
            # 공통 인덱스로 정렬
            common_index = asset_returns.index.intersection(benchmark_returns.index)
            asset_aligned = asset_returns.reindex(common_index).dropna()
            benchmark_aligned = benchmark_returns.reindex(common_index).dropna()
            
            if len(asset_aligned) == 0 or len(benchmark_aligned) == 0:
                return 0.0
            
            excess_returns = asset_aligned - benchmark_aligned
            
            if excess_returns.std() == 0:
                return 0.0
            
            information_ratio = excess_returns.mean() / excess_returns.std() * np.sqrt(self.trading_days_per_year)
            return information_ratio
            
        except Exception as e:
            logger.error(f"정보 비율 계산 실패: {e}")
            return 0.0
    
    def calculate_treynor_ratio(self, asset_returns: pd.Series, 
                              benchmark_returns: pd.Series,
                              risk_free_rate: Optional[float] = None) -> float:
        """트레이너 비율 계산"""
        try:
            if risk_free_rate is None:
                risk_free_rate = self.risk_free_rate
            
            beta = self.calculate_beta(asset_returns, benchmark_returns)
            
            if beta == 0:
                return 0.0
            
            excess_return = self.calculate_annualized_return(asset_returns) - risk_free_rate
            treynor_ratio = excess_return / beta
            return treynor_ratio
            
        except Exception as e:
            logger.error(f"트레이너 비율 계산 실패: {e}")
            return 0.0
    
    def calculate_tracking_error(self, asset_returns: pd.Series, 
                               benchmark_returns: pd.Series) -> float:
        """추적 오차 계산"""
        try:
            # 공통 인덱스로 정렬
            common_index = asset_returns.index.intersection(benchmark_returns.index)
            asset_aligned = asset_returns.reindex(common_index).dropna()
            benchmark_aligned = benchmark_returns.reindex(common_index).dropna()
            
            if len(asset_aligned) == 0 or len(benchmark_aligned) == 0:
                return 0.0
            
            excess_returns = asset_aligned - benchmark_aligned
            tracking_error = excess_returns.std() * np.sqrt(self.trading_days_per_year)
            return tracking_error
            
        except Exception as e:
            logger.error(f"추적 오차 계산 실패: {e}")
            return 0.0
    
    def calculate_win_rate(self, returns: pd.Series) -> float:
        """승률 계산"""
        try:
            if len(returns) == 0:
                return 0.0
            
            positive_returns = returns[returns > 0]
            win_rate = len(positive_returns) / len(returns)
            return win_rate
            
        except Exception as e:
            logger.error(f"승률 계산 실패: {e}")
            return 0.0
    
    def calculate_hit_ratio(self, asset_returns: pd.Series, 
                          benchmark_returns: pd.Series) -> float:
        """적중률 계산 (벤치마크 대비)"""
        try:
            # 공통 인덱스로 정렬
            common_index = asset_returns.index.intersection(benchmark_returns.index)
            asset_aligned = asset_returns.reindex(common_index).dropna()
            benchmark_aligned = benchmark_returns.reindex(common_index).dropna()
            
            if len(asset_aligned) == 0 or len(benchmark_aligned) == 0:
                return 0.0
            
            outperformance = asset_aligned > benchmark_aligned
            hit_ratio = outperformance.sum() / len(outperformance)
            return hit_ratio
            
        except Exception as e:
            logger.error(f"적중률 계산 실패: {e}")
            return 0.0
    
    def calculate_rolling_metrics(self, returns: pd.Series, 
                                window: int = 252) -> Dict[str, pd.Series]:
        """롤링 성과 지표 계산"""
        try:
            rolling_metrics = {}
            
            # 롤링 수익률
            rolling_metrics['rolling_return'] = returns.rolling(window).apply(
                lambda x: self.calculate_annualized_return(x), raw=False
            )
            
            # 롤링 변동성
            rolling_metrics['rolling_volatility'] = returns.rolling(window).std() * np.sqrt(self.trading_days_per_year)
            
            # 롤링 샤프 비율
            rolling_metrics['rolling_sharpe'] = returns.rolling(window).apply(
                lambda x: self.calculate_sharpe_ratio(x), raw=False
            )
            
            # 롤링 최대 낙폭
            rolling_metrics['rolling_max_drawdown'] = returns.rolling(window).apply(
                lambda x: self.calculate_max_drawdown(x)['max_drawdown'], raw=False
            )
            
            return rolling_metrics
            
        except Exception as e:
            logger.error(f"롤링 성과 지표 계산 실패: {e}")
            return {}
    
    def calculate_comprehensive_metrics(self, returns: pd.Series, 
                                      benchmark_returns: Optional[pd.Series] = None) -> Dict[str, float]:
        """종합 성과 지표 계산"""
        try:
            metrics = {}
            
            # 기본 수익률 지표
            metrics['total_return'] = self.calculate_total_return(returns)
            metrics['annualized_return'] = self.calculate_annualized_return(returns)
            metrics['volatility'] = self.calculate_volatility(returns)
            
            # 위험 조정 수익률
            metrics['sharpe_ratio'] = self.calculate_sharpe_ratio(returns)
            metrics['sortino_ratio'] = self.calculate_sortino_ratio(returns)
            metrics['calmar_ratio'] = self.calculate_calmar_ratio(returns)
            
            # 위험 지표
            max_dd_info = self.calculate_max_drawdown(returns)
            metrics['max_drawdown'] = max_dd_info['max_drawdown']
            metrics['max_drawdown_duration'] = max_dd_info['max_drawdown_duration']
            
            metrics['var_95'] = self.calculate_var(returns, 0.05)
            metrics['cvar_95'] = self.calculate_cvar(returns, 0.05)
            
            # 분포 특성
            metrics['skewness'] = self.calculate_skewness(returns)
            metrics['kurtosis'] = self.calculate_kurtosis(returns)
            
            # 승률
            metrics['win_rate'] = self.calculate_win_rate(returns)
            
            # 벤치마크 비교 지표 (벤치마크가 제공된 경우)
            if benchmark_returns is not None:
                metrics['beta'] = self.calculate_beta(returns, benchmark_returns)
                metrics['alpha'] = self.calculate_alpha(returns, benchmark_returns)
                metrics['information_ratio'] = self.calculate_information_ratio(returns, benchmark_returns)
                metrics['treynor_ratio'] = self.calculate_treynor_ratio(returns, benchmark_returns)
                metrics['tracking_error'] = self.calculate_tracking_error(returns, benchmark_returns)
                metrics['hit_ratio'] = self.calculate_hit_ratio(returns, benchmark_returns)
            
            # 기간 정보
            metrics['observation_period'] = len(returns)
            metrics['start_date'] = returns.index[0] if len(returns) > 0 else None
            metrics['end_date'] = returns.index[-1] if len(returns) > 0 else None
            
            return metrics
            
        except Exception as e:
            logger.error(f"종합 성과 지표 계산 실패: {e}")
            return {}
    
    def create_performance_report(self, returns: pd.Series, 
                                benchmark_returns: Optional[pd.Series] = None,
                                portfolio_name: str = "Portfolio") -> pd.DataFrame:
        """성과 리포트 생성"""
        try:
            metrics = self.calculate_comprehensive_metrics(returns, benchmark_returns)
            
            # 리포트 데이터 정리
            report_data = []
            
            # 수익률 섹션
            report_data.extend([
                ['수익률 지표', '', ''],
                ['총 수익률', f"{metrics.get('total_return', 0):.2%}", '투자기간 전체 수익률'],
                ['연환산 수익률', f"{metrics.get('annualized_return', 0):.2%}", '연간 기준 수익률'],
                ['변동성 (연환산)', f"{metrics.get('volatility', 0):.2%}", '연간 기준 표준편차'],
                ['', '', '']
            ])
            
            # 위험조정 수익률 섹션
            report_data.extend([
                ['위험조정 수익률', '', ''],
                ['샤프 비율', f"{metrics.get('sharpe_ratio', 0):.3f}", '위험 단위당 초과수익률'],
                ['소르티노 비율', f"{metrics.get('sortino_ratio', 0):.3f}", '하방위험 단위당 초과수익률'],
                ['칼마 비율', f"{metrics.get('calmar_ratio', 0):.3f}", '최대낙폭 대비 연수익률'],
                ['', '', '']
            ])
            
            # 위험 지표 섹션
            report_data.extend([
                ['위험 지표', '', ''],
                ['최대 낙폭', f"{metrics.get('max_drawdown', 0):.2%}", '최대 손실 구간'],
                ['최대 낙폭 지속기간', f"{metrics.get('max_drawdown_duration', 0):.0f}일", '최대 손실 지속 기간'],
                ['VaR (95%)', f"{metrics.get('var_95', 0):.2%}", '5% 확률의 최대 손실'],
                ['CVaR (95%)', f"{metrics.get('cvar_95', 0):.2%}", '5% 꼬리 위험의 평균 손실'],
                ['', '', '']
            ])
            
            # 벤치마크 비교 (있는 경우)
            if benchmark_returns is not None:
                report_data.extend([
                    ['벤치마크 비교', '', ''],
                    ['베타', f"{metrics.get('beta', 0):.3f}", '시장 민감도'],
                    ['알파', f"{metrics.get('alpha', 0):.2%}", '벤치마크 대비 초과성과'],
                    ['정보비율', f"{metrics.get('information_ratio', 0):.3f}", '추적오차 대비 초과수익률'],
                    ['추적오차', f"{metrics.get('tracking_error', 0):.2%}", '벤치마크 대비 변동성'],
                    ['적중률', f"{metrics.get('hit_ratio', 0):.1%}", '벤치마크 초과 성과 비율'],
                    ['', '', '']
                ])
            
            # 기타 지표
            report_data.extend([
                ['기타 지표', '', ''],
                ['승률', f"{metrics.get('win_rate', 0):.1%}", '양의 수익률 발생 비율'],
                ['왜도', f"{metrics.get('skewness', 0):.3f}", '수익률 분포의 비대칭성'],
                ['첨도', f"{metrics.get('kurtosis', 0):.3f}", '수익률 분포의 뾰족함'],
                ['관측 기간', f"{metrics.get('observation_period', 0):.0f}일", '분석 대상 기간']
            ])
            
            # DataFrame 생성
            report_df = pd.DataFrame(report_data, columns=['지표', '값', '설명'])
            
            return report_df
            
        except Exception as e:
            logger.error(f"성과 리포트 생성 실패: {e}")
            return pd.DataFrame()
    
    def compare_multiple_assets(self, returns_dict: Dict[str, pd.Series], 
                              benchmark_returns: Optional[pd.Series] = None) -> pd.DataFrame:
        """다중 자산 성과 비교"""
        try:
            comparison_data = []
            
            for asset_name, returns in returns_dict.items():
                metrics = self.calculate_comprehensive_metrics(returns, benchmark_returns)
                
                comparison_data.append({
                    '자산명': asset_name,
                    '총수익률': f"{metrics.get('total_return', 0):.2%}",
                    '연수익률': f"{metrics.get('annualized_return', 0):.2%}",
                    '변동성': f"{metrics.get('volatility', 0):.2%}",
                    '샤프비율': f"{metrics.get('sharpe_ratio', 0):.3f}",
                    '최대낙폭': f"{metrics.get('max_drawdown', 0):.2%}",
                    '승률': f"{metrics.get('win_rate', 0):.1%}",
                    '관측기간': f"{metrics.get('observation_period', 0):.0f}일"
                })
            
            comparison_df = pd.DataFrame(comparison_data)
            return comparison_df
            
        except Exception as e:
            logger.error(f"다중 자산 성과 비교 실패: {e}")
            return pd.DataFrame()