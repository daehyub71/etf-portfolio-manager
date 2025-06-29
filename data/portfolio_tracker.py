"""
포트폴리오 추적 모듈
실시간 포트폴리오 성과 추적 및 모니터링
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
from dataclasses import dataclass
import json

logger = logging.getLogger(__name__)

@dataclass
class PortfolioHolding:
    """포트폴리오 보유 종목 정보"""
    etf_code: str
    etf_name: str
    shares: int
    avg_price: float
    current_price: float
    target_weight: float
    current_weight: float
    market_value: float
    unrealized_pnl: float
    unrealized_pnl_pct: float

@dataclass
class PortfolioSummary:
    """포트폴리오 요약 정보"""
    total_investment: float
    current_value: float
    total_return: float
    total_return_pct: float
    daily_return: float
    daily_return_pct: float
    realized_pnl: float
    unrealized_pnl: float
    cash_balance: float
    num_holdings: int
    last_updated: datetime

class PortfolioTracker:
    """포트폴리오 추적 시스템"""
    
    def __init__(self, portfolio_id: int, database_manager=None, market_data_collector=None):
        """
        포트폴리오 추적기 초기화
        
        Args:
            portfolio_id: 추적할 포트폴리오 ID
            database_manager: 데이터베이스 관리자
            market_data_collector: 시장 데이터 수집기
        """
        self.portfolio_id = portfolio_id
        self.db_manager = database_manager
        self.market_data = market_data_collector
        
        self.holdings: Dict[str, PortfolioHolding] = {}
        self.portfolio_summary: Optional[PortfolioSummary] = None
        self.target_allocation: Dict[str, float] = {}
        
        # 성과 지표 캐시
        self.performance_cache = {}
        self.last_cache_update = None
        
        self._load_portfolio_data()
    
    def _load_portfolio_data(self):
        """포트폴리오 데이터 로드"""
        try:
            if not self.db_manager:
                logger.warning("데이터베이스 관리자가 설정되지 않음")
                return
            
            # 포트폴리오 기본 정보 로드
            portfolio_info = self.db_manager.get_portfolio_info(self.portfolio_id)
            if portfolio_info:
                self.target_allocation = portfolio_info['target_allocation']
            
            # 보유 종목 정보 로드
            holdings_df = self.db_manager.get_portfolio_holdings(self.portfolio_id)
            
            self._update_holdings_from_dataframe(holdings_df)
            
            logger.info(f"포트폴리오 {self.portfolio_id} 데이터 로드 완료")
            
        except Exception as e:
            logger.error(f"포트폴리오 데이터 로드 실패: {e}")
    
    def _update_holdings_from_dataframe(self, holdings_df: pd.DataFrame):
        """DataFrame에서 보유 종목 정보 업데이트"""
        self.holdings.clear()
        
        if holdings_df.empty:
            return
        
        total_value = 0
        
        for _, row in holdings_df.iterrows():
            etf_code = row['etf_code']
            
            # 현재 가격 조회 (시장 데이터에서)
            current_price = self._get_current_price(etf_code, row.get('current_price', row['avg_price']))
            
            market_value = row['shares'] * current_price
            total_value += market_value
            
            # 손익 계산
            unrealized_pnl = (current_price - row['avg_price']) * row['shares']
            unrealized_pnl_pct = (current_price / row['avg_price'] - 1) * 100 if row['avg_price'] > 0 else 0
            
            holding = PortfolioHolding(
                etf_code=etf_code,
                etf_name=row.get('etf_name', etf_code),
                shares=row['shares'],
                avg_price=row['avg_price'],
                current_price=current_price,
                target_weight=row['target_weight'],
                current_weight=0,  # 나중에 계산
                market_value=market_value,
                unrealized_pnl=unrealized_pnl,
                unrealized_pnl_pct=unrealized_pnl_pct
            )
            
            self.holdings[etf_code] = holding
        
        # 현재 비중 계산
        if total_value > 0:
            for holding in self.holdings.values():
                holding.current_weight = holding.market_value / total_value * 100
    
    def _get_current_price(self, etf_code: str, fallback_price: float) -> float:
        """현재 가격 조회"""
        try:
            if self.market_data:
                current_price = self.market_data.get_current_price(etf_code)
                if current_price and current_price > 0:
                    return current_price
            
            # 폴백: 데이터베이스에서 최근 가격 조회
            if self.db_manager:
                recent_data = self.db_manager.get_etf_price_data(
                    etf_code, 
                    start_date=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
                )
                if not recent_data.empty:
                    return recent_data.iloc[-1]['close_price']
            
            return fallback_price
            
        except Exception as e:
            logger.warning(f"가격 조회 실패 ({etf_code}): {e}")
            return fallback_price
    
    def update_portfolio(self) -> bool:
        """포트폴리오 정보 업데이트"""
        try:
            # 보유 종목 정보 다시 로드
            holdings_df = self.db_manager.get_portfolio_holdings(self.portfolio_id)
            self._update_holdings_from_dataframe(holdings_df)
            
            # 포트폴리오 요약 정보 업데이트
            self._update_portfolio_summary()
            
            # 성과 지표 캐시 갱신
            self._update_performance_cache()
            
            logger.info(f"포트폴리오 {self.portfolio_id} 업데이트 완료")
            return True
            
        except Exception as e:
            logger.error(f"포트폴리오 업데이트 실패: {e}")
            return False
    
    def _update_portfolio_summary(self):
        """포트폴리오 요약 정보 업데이트"""
        if not self.holdings:
            return
        
        # 현재 포트폴리오 가치 계산
        current_value = sum(holding.market_value for holding in self.holdings.values())
        
        # 총 투자원금 계산
        total_investment = sum(holding.shares * holding.avg_price for holding in self.holdings.values())
        
        # 손익 계산
        total_return = current_value - total_investment
        total_return_pct = (total_return / total_investment * 100) if total_investment > 0 else 0
        
        # 일일 손익 계산 (전일 대비)
        daily_return, daily_return_pct = self._calculate_daily_return()
        
        # 실현/미실현 손익
        realized_pnl = self._get_realized_pnl()
        unrealized_pnl = sum(holding.unrealized_pnl for holding in self.holdings.values())
        
        self.portfolio_summary = PortfolioSummary(
            total_investment=total_investment,
            current_value=current_value,
            total_return=total_return,
            total_return_pct=total_return_pct,
            daily_return=daily_return,
            daily_return_pct=daily_return_pct,
            realized_pnl=realized_pnl,
            unrealized_pnl=unrealized_pnl,
            cash_balance=0,  # 추후 구현
            num_holdings=len(self.holdings),
            last_updated=datetime.now()
        )
    
    def _calculate_daily_return(self) -> Tuple[float, float]:
        """일일 손익 계산"""
        try:
            # 전일 포트폴리오 가치 조회
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            if self.db_manager:
                performance_data = self.db_manager.get_portfolio_performance(
                    self.portfolio_id, start_date=yesterday
                )
                
                if not performance_data.empty and len(performance_data) >= 2:
                    latest_value = performance_data.iloc[-1]['total_value']
                    previous_value = performance_data.iloc[-2]['total_value']
                    
                    daily_return = latest_value - previous_value
                    daily_return_pct = (daily_return / previous_value * 100) if previous_value > 0 else 0
                    
                    return daily_return, daily_return_pct
            
            return 0.0, 0.0
            
        except Exception as e:
            logger.warning(f"일일 손익 계산 실패: {e}")
            return 0.0, 0.0
    
    def _get_realized_pnl(self) -> float:
        """실현 손익 조회"""
        try:
            if not self.db_manager:
                return 0.0
            
            # 매도 거래 내역에서 실현 손익 계산
            # 이는 별도의 데이터베이스 쿼리나 계산이 필요
            # 여기서는 단순화하여 0으로 반환
            return 0.0
            
        except Exception as e:
            logger.warning(f"실현 손익 조회 실패: {e}")
            return 0.0
    
    def _update_performance_cache(self):
        """성과 지표 캐시 업데이트"""
        try:
            # 캐시 유효성 확인 (1시간)
            if (self.last_cache_update and 
                datetime.now() - self.last_cache_update < timedelta(hours=1)):
                return
            
            # 성과 지표 계산
            performance_metrics = self.calculate_performance_metrics()
            self.performance_cache = performance_metrics
            self.last_cache_update = datetime.now()
            
        except Exception as e:
            logger.warning(f"성과 캐시 업데이트 실패: {e}")
    
    def get_portfolio_summary(self) -> Optional[PortfolioSummary]:
        """포트폴리오 요약 정보 반환"""
        return self.portfolio_summary
    
    def get_holdings(self) -> Dict[str, PortfolioHolding]:
        """보유 종목 정보 반환"""
        return self.holdings
    
    def get_holdings_dataframe(self) -> pd.DataFrame:
        """보유 종목을 DataFrame으로 반환"""
        if not self.holdings:
            return pd.DataFrame()
        
        holdings_data = []
        for holding in self.holdings.values():
            holdings_data.append({
                'ETF코드': holding.etf_code,
                'ETF명': holding.etf_name,
                '보유주수': holding.shares,
                '평균단가': holding.avg_price,
                '현재가': holding.current_price,
                '목표비중(%)': holding.target_weight,
                '현재비중(%)': holding.current_weight,
                '평가금액': holding.market_value,
                '평가손익': holding.unrealized_pnl,
                '수익률(%)': holding.unrealized_pnl_pct
            })
        
        return pd.DataFrame(holdings_data)
    
    def get_allocation_deviation(self) -> Dict[str, float]:
        """목표 자산배분 대비 이탈률 계산"""
        deviations = {}
        
        for etf_code, target_weight in self.target_allocation.items():
            if etf_code in self.holdings:
                current_weight = self.holdings[etf_code].current_weight
                deviation = current_weight - target_weight
                deviations[etf_code] = deviation
            else:
                deviations[etf_code] = -target_weight  # 보유하지 않은 경우
        
        return deviations
    
    def get_rebalancing_needs(self, threshold: float = 5.0) -> List[Dict]:
        """리밸런싱 필요 종목 식별"""
        rebalancing_needs = []
        deviations = self.get_allocation_deviation()
        
        for etf_code, deviation in deviations.items():
            if abs(deviation) >= threshold:
                action = 'BUY' if deviation < 0 else 'SELL'
                
                rebalancing_needs.append({
                    'etf_code': etf_code,
                    'etf_name': self.holdings.get(etf_code, {}).etf_name if etf_code in self.holdings else etf_code,
                    'target_weight': self.target_allocation.get(etf_code, 0),
                    'current_weight': self.holdings.get(etf_code, {}).current_weight if etf_code in self.holdings else 0,
                    'deviation': deviation,
                    'action': action,
                    'priority': abs(deviation)
                })
        
        # 이탈률 순으로 정렬
        rebalancing_needs.sort(key=lambda x: x['priority'], reverse=True)
        
        return rebalancing_needs
    
    def calculate_performance_metrics(self, period_days: int = 252) -> Dict[str, float]:
        """성과 지표 계산"""
        try:
            if not self.db_manager:
                return {}
            
            # 성과 데이터 조회
            start_date = (datetime.now() - timedelta(days=period_days)).strftime('%Y-%m-%d')
            performance_data = self.db_manager.get_portfolio_performance(
                self.portfolio_id, start_date=start_date
            )
            
            if performance_data.empty:
                return {}
            
            # 일별 수익률 계산
            returns = performance_data['daily_return'].dropna()
            
            if len(returns) < 30:  # 최소 30일 데이터 필요
                return {}
            
            # 기본 통계
            total_return = performance_data.iloc[-1]['cumulative_return']
            annualized_return = (1 + total_return / 100) ** (252 / len(returns)) - 1
            volatility = returns.std() * np.sqrt(252)
            
            # 샤프 비율 (무위험 이자율 2% 가정)
            risk_free_rate = 0.02
            sharpe_ratio = (annualized_return - risk_free_rate) / volatility if volatility > 0 else 0
            
            # 최대 낙폭 (MDD)
            cumulative_returns = (1 + returns / 100).cumprod()
            running_max = cumulative_returns.expanding().max()
            drawdown = (cumulative_returns - running_max) / running_max
            max_drawdown = drawdown.min()
            
            # 소르티노 비율
            negative_returns = returns[returns < 0]
            downside_deviation = negative_returns.std() * np.sqrt(252) if len(negative_returns) > 0 else 0
            sortino_ratio = (annualized_return - risk_free_rate) / downside_deviation if downside_deviation > 0 else 0
            
            # 승률
            win_rate = len(returns[returns > 0]) / len(returns) * 100
            
            # 평균 승리/패배 크기
            avg_win = returns[returns > 0].mean() if len(returns[returns > 0]) > 0 else 0
            avg_loss = returns[returns < 0].mean() if len(returns[returns < 0]) > 0 else 0
            
            # 벤치마크 대비 성과 (추가 구현 필요)
            benchmark_returns = performance_data['benchmark_return'].dropna()
            alpha = 0  # 간단화
            beta = 1   # 간단화
            
            metrics = {
                'total_return': total_return,
                'annualized_return': annualized_return * 100,
                'volatility': volatility * 100,
                'sharpe_ratio': sharpe_ratio,
                'sortino_ratio': sortino_ratio,
                'max_drawdown': max_drawdown * 100,
                'win_rate': win_rate,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'alpha': alpha,
                'beta': beta,
                'observation_period': len(returns)
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"성과 지표 계산 실패: {e}")
            return {}
    
    def get_sector_allocation(self) -> Dict[str, float]:
        """섹터별 자산배분 현황"""
        sector_allocation = {}
        
        if not self.holdings:
            return sector_allocation
        
        total_value = sum(holding.market_value for holding in self.holdings.values())
        
        # ETF 유니버스에서 섹터 정보를 가져와야 함 (추후 구현)
        # 현재는 간단하게 ETF 코드 기반으로 분류
        
        for holding in self.holdings.values():
            # 섹터 분류 (간단화된 버전)
            sector = self._classify_etf_sector(holding.etf_code)
            allocation = (holding.market_value / total_value * 100) if total_value > 0 else 0
            
            if sector in sector_allocation:
                sector_allocation[sector] += allocation
            else:
                sector_allocation[sector] = allocation
        
        return sector_allocation
    
    def _classify_etf_sector(self, etf_code: str) -> str:
        """ETF 섹터 분류 (간단화된 버전)"""
        # 실제로는 ETF 유니버스나 데이터베이스에서 가져와야 함
        sector_mapping = {
            '069500': '국내 대형주',
            '139660': '미국 주식',
            '117460': '미국 기술주',
            '229200': '국내 중소형주',
            '195930': '선진국 주식',
            '192090': '신흥국 주식',
            '114260': '국내 채권',
            '136340': '해외 채권',
            '157490': '국내 부동산',
            '132030': '금/원자재'
        }
        
        return sector_mapping.get(etf_code, '기타')
    
    def get_risk_metrics(self) -> Dict[str, float]:
        """위험 지표 계산"""
        try:
            if not self.performance_cache:
                self._update_performance_cache()
            
            risk_metrics = {
                'volatility': self.performance_cache.get('volatility', 0),
                'max_drawdown': self.performance_cache.get('max_drawdown', 0),
                'var_95': 0,  # VaR 95% (추후 구현)
                'var_99': 0,  # VaR 99% (추후 구현)
                'beta': self.performance_cache.get('beta', 1),
                'correlation_kospi': 0  # 코스피 상관계수 (추후 구현)
            }
            
            return risk_metrics
            
        except Exception as e:
            logger.error(f"위험 지표 계산 실패: {e}")
            return {}
    
    def export_portfolio_report(self, file_path: str) -> bool:
        """포트폴리오 리포트 내보내기"""
        try:
            # 포트폴리오 요약 정보
            summary_data = []
            if self.portfolio_summary:
                summary_data.append({
                    '항목': '총 투자금액',
                    '값': f"{self.portfolio_summary.total_investment:,.0f}원"
                })
                summary_data.append({
                    '항목': '현재 평가금액', 
                    '값': f"{self.portfolio_summary.current_value:,.0f}원"
                })
                summary_data.append({
                    '항목': '총 손익',
                    '값': f"{self.portfolio_summary.total_return:,.0f}원 ({self.portfolio_summary.total_return_pct:.2f}%)"
                })
                summary_data.append({
                    '항목': '일일 손익',
                    '값': f"{self.portfolio_summary.daily_return:,.0f}원 ({self.portfolio_summary.daily_return_pct:.2f}%)"
                })
            
            # 여러 시트로 구성된 Excel 파일 생성
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # 요약 정보
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='포트폴리오요약', index=False)
                
                # 보유 종목
                holdings_df = self.get_holdings_dataframe()
                holdings_df.to_excel(writer, sheet_name='보유종목', index=False)
                
                # 자산배분 현황
                allocation_data = []
                for etf_code, target_weight in self.target_allocation.items():
                    current_weight = self.holdings.get(etf_code, PortfolioHolding('', '', 0, 0, 0, 0, 0, 0, 0, 0)).current_weight
                    allocation_data.append({
                        'ETF코드': etf_code,
                        '목표비중(%)': target_weight,
                        '현재비중(%)': current_weight,
                        '이탈률(%)': current_weight - target_weight
                    })
                
                pd.DataFrame(allocation_data).to_excel(writer, sheet_name='자산배분', index=False)
                
                # 성과 지표
                performance_metrics = self.performance_cache
                if performance_metrics:
                    metrics_data = []
                    for metric, value in performance_metrics.items():
                        metrics_data.append({
                            '지표': metric,
                            '값': f"{value:.2f}" if isinstance(value, (int, float)) else str(value)
                        })
                    
                    pd.DataFrame(metrics_data).to_excel(writer, sheet_name='성과지표', index=False)
            
            logger.info(f"포트폴리오 리포트 내보내기 완료: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"포트폴리오 리포트 내보내기 실패: {e}")
            return False