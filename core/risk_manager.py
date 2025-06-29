# ==========================================
# risk_manager.py - 리스크 관리 시스템
# ==========================================

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import logging
import json

# 통계 계산 라이브러리 (선택적)
try:
    from scipy import stats
    from scipy.optimize import minimize
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️ scipy 없음 - 기본 리스크 계산만 사용")

@dataclass
class RiskMetrics:
    """리스크 지표"""
    volatility: float
    var_95: float          # 95% VaR
    var_99: float          # 99% VaR
    cvar_95: float         # 95% CVaR (Expected Shortfall)
    max_drawdown: float
    beta: float
    correlation_risk: float
    concentration_risk: float
    tracking_error: float
    information_ratio: float
    
@dataclass
class RiskAlert:
    """리스크 경고"""
    risk_type: str
    severity: str          # low, medium, high, critical
    message: str
    current_value: float
    threshold: float
    recommendation: str

@dataclass
class RiskLimit:
    """리스크 한계"""
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    enabled: bool = True

class RiskManager:
    """리스크 관리 시스템"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        
        # 기본 리스크 한계 설정
        self.risk_limits = {
            'volatility': RiskLimit('변동성', 20.0, 30.0),           # 20%, 30%
            'max_drawdown': RiskLimit('최대낙폭', -15.0, -25.0),      # -15%, -25%
            'var_95': RiskLimit('95% VaR', -5.0, -10.0),            # -5%, -10%
            'concentration': RiskLimit('집중도', 40.0, 60.0),         # 40%, 60%
            'correlation': RiskLimit('상관관계', 0.8, 0.9),          # 0.8, 0.9
            'beta': RiskLimit('베타', 1.3, 1.5),                    # 1.3, 1.5
            'tracking_error': RiskLimit('추적오차', 3.0, 5.0)        # 3%, 5%
        }
        
        # 벤치마크 설정
        self.benchmarks = {
            'domestic': '069500',    # KODEX 200
            'international': '360750', # TIGER 미국S&P500
            'balanced': ['069500', '360750', '114260']  # 균형형
        }
        
        self.risk_free_rate = 0.025  # 2.5% 무위험 수익률
        
        self.logger.info("⚠️ 리스크 관리 시스템 초기화 완료")
    
    def calculate_portfolio_risk(self, user_id: str, 
                               benchmark_type: str = 'balanced') -> Optional[RiskMetrics]:
        """포트폴리오 리스크 계산"""
        try:
            # 포트폴리오 수익률 데이터 가져오기
            portfolio_returns = self._get_portfolio_returns(user_id)
            
            if portfolio_returns is None or len(portfolio_returns) < 30:
                self.logger.warning(f"⚠️ 충분한 포트폴리오 데이터가 없음: {user_id}")
                return None
            
            # 벤치마크 수익률 가져오기
            benchmark_returns = self._get_benchmark_returns(benchmark_type, len(portfolio_returns))
            
            # 기본 리스크 지표 계산
            volatility = portfolio_returns.std() * np.sqrt(252) * 100  # 연율화
            
            # VaR 계산 (95%, 99%)
            var_95 = np.percentile(portfolio_returns, 5) * 100
            var_99 = np.percentile(portfolio_returns, 1) * 100
            
            # CVaR 계산 (95%)
            var_95_threshold = np.percentile(portfolio_returns, 5)
            tail_returns = portfolio_returns[portfolio_returns <= var_95_threshold]
            cvar_95 = tail_returns.mean() * 100 if len(tail_returns) > 0 else var_95
            
            # 최대 낙폭 계산
            cumulative_returns = (1 + portfolio_returns).cumprod()
            peak = cumulative_returns.expanding(min_periods=1).max()
            drawdown = (cumulative_returns / peak - 1) * 100
            max_drawdown = drawdown.min()
            
            # 베타 계산
            if benchmark_returns is not None and len(benchmark_returns) == len(portfolio_returns):
                covariance = np.cov(portfolio_returns, benchmark_returns)[0, 1]
                benchmark_variance = np.var(benchmark_returns)
                beta = covariance / benchmark_variance if benchmark_variance > 0 else 1.0
                
                # 추적 오차 계산
                tracking_diff = portfolio_returns - benchmark_returns
                tracking_error = tracking_diff.std() * np.sqrt(252) * 100
                
                # 정보 비율 계산
                excess_return = tracking_diff.mean() * 252
                information_ratio = excess_return / (tracking_error / 100) if tracking_error > 0 else 0
            else:
                beta = 1.0
                tracking_error = 0.0
                information_ratio = 0.0
            
            # 상관관계 리스크 계산
            correlation_risk = self._calculate_correlation_risk(user_id)
            
            # 집중도 리스크 계산
            concentration_risk = self._calculate_concentration_risk(user_id)
            
            return RiskMetrics(
                volatility=round(volatility, 2),
                var_95=round(var_95, 2),
                var_99=round(var_99, 2),
                cvar_95=round(cvar_95, 2),
                max_drawdown=round(max_drawdown, 2),
                beta=round(beta, 2),
                correlation_risk=round(correlation_risk, 2),
                concentration_risk=round(concentration_risk, 2),
                tracking_error=round(tracking_error, 2),
                information_ratio=round(information_ratio, 2)
            )
            
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 리스크 계산 실패: {e}")
            return None
    
    def _get_portfolio_returns(self, user_id: str, days: int = 252) -> Optional[pd.Series]:
        """포트폴리오 수익률 데이터 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 포트폴리오 성과 데이터 조회
            query = '''
                SELECT date, price
                FROM etf_performance p
                JOIN positions pos ON p.code = pos.etf_code
                JOIN portfolios port ON pos.portfolio_id = port.id
                WHERE port.user_id = ? AND port.is_active = 1
                ORDER BY date DESC
                LIMIT ?
            '''
            
            df = pd.read_sql_query(query, conn, params=(user_id, days + 1))
            conn.close()
            
            if len(df) < 30:  # 최소 30일 데이터 필요
                # 실제 데이터가 없으면 시뮬레이션 데이터 생성
                return self._generate_portfolio_returns_simulation(days)
            
            # 일일 수익률 계산
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            df['returns'] = df['price'].pct_change().fillna(0)
            
            return df['returns'].iloc[1:]  # 첫 번째 NaN 제거
            
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 수익률 조회 실패: {e}")
            return self._generate_portfolio_returns_simulation(days)
    
    def _generate_portfolio_returns_simulation(self, days: int = 252) -> pd.Series:
        """포트폴리오 수익률 시뮬레이션 생성"""
        try:
            # 균형잡힌 포트폴리오 가정 (주식 70%, 채권 30%)
            np.random.seed(42)
            
            # 주식 수익률 (연 8%, 변동성 18%)
            stock_returns = np.random.normal(0.08/252, 0.18/np.sqrt(252), days)
            
            # 채권 수익률 (연 3%, 변동성 5%)
            bond_returns = np.random.normal(0.03/252, 0.05/np.sqrt(252), days)
            
            # 포트폴리오 수익률 (70% 주식 + 30% 채권)
            portfolio_returns = 0.7 * stock_returns + 0.3 * bond_returns
            
            return pd.Series(portfolio_returns)
            
        except Exception as e:
            self.logger.error(f"❌ 수익률 시뮬레이션 생성 실패: {e}")
            return pd.Series([])
    
    def _get_benchmark_returns(self, benchmark_type: str, days: int) -> Optional[pd.Series]:
        """벤치마크 수익률 조회"""
        try:
            if benchmark_type == 'balanced':
                # 균형형 벤치마크 (KODEX 200 50% + TIGER 미국S&P500 30% + 국고채 20%)
                weights = [0.5, 0.3, 0.2]
                etf_codes = self.benchmarks['balanced']
            else:
                weights = [1.0]
                etf_codes = [self.benchmarks.get(benchmark_type, '069500')]
            
            conn = sqlite3.connect(self.db_path)
            
            all_returns = []
            for etf_code in etf_codes:
                query = '''
                    SELECT price FROM etf_performance 
                    WHERE code = ? 
                    ORDER BY date DESC 
                    LIMIT ?
                '''
                df = pd.read_sql_query(query, conn, params=(etf_code, days + 1))
                
                if len(df) >= 30:
                    returns = df['price'].pct_change().fillna(0).iloc[1:]
                    all_returns.append(returns)
                else:
                    # 시뮬레이션 데이터
                    if etf_code == '069500':  # 국내 주식
                        sim_returns = np.random.normal(0.08/252, 0.20/np.sqrt(252), days)
                    elif etf_code == '360750':  # 미국 주식
                        sim_returns = np.random.normal(0.12/252, 0.22/np.sqrt(252), days)
                    else:  # 채권
                        sim_returns = np.random.normal(0.03/252, 0.05/np.sqrt(252), days)
                    
                    all_returns.append(pd.Series(sim_returns))
            
            conn.close()
            
            if all_returns:
                # 가중평균 계산
                benchmark_returns = sum(w * ret for w, ret in zip(weights, all_returns))
                return benchmark_returns
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 벤치마크 수익률 조회 실패: {e}")
            return None
    
    def _calculate_correlation_risk(self, user_id: str) -> float:
        """상관관계 리스크 계산"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 포트폴리오 구성 ETF 조회
            query = '''
                SELECT pos.etf_code, pos.target_weight
                FROM positions pos
                JOIN portfolios port ON pos.portfolio_id = port.id
                WHERE port.user_id = ? AND port.is_active = 1
            '''
            
            portfolio_df = pd.read_sql_query(query, conn, params=(user_id,))
            
            if len(portfolio_df) < 2:
                return 0.0  # 단일 자산이면 상관관계 리스크 없음
            
            # ETF 간 상관계수 계산 (시뮬레이션)
            correlations = []
            etf_codes = portfolio_df['etf_code'].tolist()
            
            for i in range(len(etf_codes)):
                for j in range(i + 1, len(etf_codes)):
                    # 실제로는 과거 수익률 데이터로 계산해야 함
                    # 여기서는 자산군별 평균 상관계수 사용
                    corr = self._estimate_correlation(etf_codes[i], etf_codes[j])
                    weight_i = portfolio_df.iloc[i]['target_weight']
                    weight_j = portfolio_df.iloc[j]['target_weight']
                    
                    # 가중 상관계수
                    weighted_corr = corr * weight_i * weight_j
                    correlations.append(weighted_corr)
            
            conn.close()
            
            # 평균 상관계수 반환
            return np.mean(correlations) if correlations else 0.0
            
        except Exception as e:
            self.logger.error(f"❌ 상관관계 리스크 계산 실패: {e}")
            return 0.0
    
    def _estimate_correlation(self, etf1: str, etf2: str) -> float:
        """ETF 간 상관계수 추정"""
        # 자산군별 평균 상관계수 (실제로는 과거 데이터로 계산)
        correlation_matrix = {
            ('069500', '360750'): 0.6,   # 국내주식 - 미국주식
            ('069500', '114260'): -0.1,  # 국내주식 - 국내채권
            ('360750', '114260'): -0.2,  # 미국주식 - 국내채권
            ('069500', '133690'): 0.7,   # 국내주식 - 나스닥
            ('360750', '133690'): 0.8,   # 미국주식 - 나스닥
            ('069500', '195930'): 0.6,   # 국내주식 - 선진국
            ('360750', '195930'): 0.9,   # 미국주식 - 선진국
        }
        
        key1 = (etf1, etf2)
        key2 = (etf2, etf1)
        
        return correlation_matrix.get(key1, correlation_matrix.get(key2, 0.5))
    
    def _calculate_concentration_risk(self, user_id: str) -> float:
        """집중도 리스크 계산"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = '''
                SELECT pos.target_weight
                FROM positions pos
                JOIN portfolios port ON pos.portfolio_id = port.id
                WHERE port.user_id = ? AND port.is_active = 1
                ORDER BY pos.target_weight DESC
            '''
            
            df = pd.read_sql_query(query, conn, params=(user_id,))
            conn.close()
            
            if df.empty:
                return 0.0
            
            weights = df['target_weight'].values
            
            # 허핀달 지수 계산 (HHI)
            hhi = sum(w**2 for w in weights)
            
            # 최대 비중 반환 (백분율)
            max_weight = max(weights) * 100
            
            return max_weight
            
        except Exception as e:
            self.logger.error(f"❌ 집중도 리스크 계산 실패: {e}")
            return 0.0
    
    def assess_risk_alerts(self, user_id: str) -> List[RiskAlert]:
        """리스크 경고 평가"""
        alerts = []
        
        try:
            # 포트폴리오 리스크 계산
            risk_metrics = self.calculate_portfolio_risk(user_id)
            
            if not risk_metrics:
                return alerts
            
            # 각 리스크 지표별 경고 체크
            risk_checks = [
                ('volatility', risk_metrics.volatility, '변동성'),
                ('max_drawdown', risk_metrics.max_drawdown, '최대낙폭'),
                ('var_95', risk_metrics.var_95, '95% VaR'),
                ('concentration', risk_metrics.concentration_risk, '집중도'),
                ('correlation', risk_metrics.correlation_risk, '상관관계'),
                ('beta', risk_metrics.beta, '베타'),
                ('tracking_error', risk_metrics.tracking_error, '추적오차')
            ]
            
            for metric_key, current_value, metric_name in risk_checks:
                if metric_key not in self.risk_limits:
                    continue
                    
                limit = self.risk_limits[metric_key]
                if not limit.enabled:
                    continue
                
                severity = None
                threshold = None
                
                # 임계값 체크 (절댓값 비교)
                abs_current = abs(current_value)
                abs_warning = abs(limit.warning_threshold)
                abs_critical = abs(limit.critical_threshold)
                
                if abs_current >= abs_critical:
                    severity = "critical"
                    threshold = limit.critical_threshold
                elif abs_current >= abs_warning:
                    severity = "high"
                    threshold = limit.warning_threshold
                
                if severity:
                    alert = self._create_risk_alert(
                        metric_key, metric_name, severity, 
                        current_value, threshold
                    )
                    alerts.append(alert)
            
            # 추가 리스크 체크
            additional_alerts = self._check_additional_risks(user_id, risk_metrics)
            alerts.extend(additional_alerts)
            
            self.logger.info(f"⚠️ 리스크 평가 완료: {len(alerts)}개 경고")
            return alerts
            
        except Exception as e:
            self.logger.error(f"❌ 리스크 경고 평가 실패: {e}")
            return alerts
    
    def _create_risk_alert(self, risk_type: str, metric_name: str, 
                          severity: str, current_value: float, 
                          threshold: float) -> RiskAlert:
        """리스크 경고 생성"""
        messages = {
            'volatility': f"포트폴리오 변동성이 {current_value:.1f}%로 높습니다.",
            'max_drawdown': f"최대 낙폭이 {current_value:.1f}%로 큽니다.",
            'var_95': f"95% VaR이 {current_value:.1f}%로 위험도가 높습니다.",
            'concentration': f"단일 자산 집중도가 {current_value:.1f}%로 높습니다.",
            'correlation': f"자산 간 상관관계가 {current_value:.2f}로 높습니다.",
            'beta': f"시장 베타가 {current_value:.2f}로 높습니다.",
            'tracking_error': f"추적 오차가 {current_value:.1f}%로 큽니다."
        }
        
        recommendations = {
            'volatility': "변동성이 낮은 자산(채권, 배당주)의 비중을 늘려보세요.",
            'max_drawdown': "손실 제한 전략을 검토하고 분산투자를 강화하세요.",
            'var_95': "리스크 관리 전략을 점검하고 안전자산 비중을 늘려보세요.",
            'concentration': "단일 자산 비중을 줄이고 분산투자를 강화하세요.",
            'correlation': "상관관계가 낮은 자산군을 추가로 고려해보세요.",
            'beta': "시장 민감도가 높으니 시장 상황을 주의깊게 모니터링하세요.",
            'tracking_error': "벤치마크 추적 전략을 점검해보세요."
        }
        
        return RiskAlert(
            risk_type=risk_type,
            severity=severity,
            message=messages.get(risk_type, f"{metric_name} 지표에 주의가 필요합니다."),
            current_value=current_value,
            threshold=threshold,
            recommendation=recommendations.get(risk_type, "포트폴리오 구성을 재검토해보세요.")
        )
    
    def _check_additional_risks(self, user_id: str, risk_metrics: RiskMetrics) -> List[RiskAlert]:
        """추가 리스크 체크"""
        alerts = []
        
        try:
            # 1. 극단적 성과 체크
            if risk_metrics.var_99 < -15:  # 99% VaR이 -15% 이하
                alerts.append(RiskAlert(
                    risk_type="extreme_risk",
                    severity="critical",
                    message=f"극한 시나리오에서 {risk_metrics.var_99:.1f}%의 큰 손실이 예상됩니다.",
                    current_value=risk_metrics.var_99,
                    threshold=-15.0,
                    recommendation="포트폴리오 전반적인 리스크 수준을 낮추는 것을 고려하세요."
                ))
            
            # 2. 변동성-수익률 불균형 체크
            if risk_metrics.information_ratio < -0.5:  # 정보비율이 매우 낮음
                alerts.append(RiskAlert(
                    risk_type="poor_risk_return",
                    severity="high",
                    message="위험 대비 수익률이 좋지 않습니다.",
                    current_value=risk_metrics.information_ratio,
                    threshold=-0.5,
                    recommendation="포트폴리오 구성을 재검토하여 효율성을 높여보세요."
                ))
            
            # 3. 베타 불안정성 체크
            if risk_metrics.beta > 1.5:
                alerts.append(RiskAlert(
                    risk_type="high_beta",
                    severity="medium",
                    message=f"시장 베타가 {risk_metrics.beta:.2f}로 시장보다 변동성이 큽니다.",
                    current_value=risk_metrics.beta,
                    threshold=1.5,
                    recommendation="시장 하락 시 더 큰 손실이 예상되니 주의하세요."
                ))
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"❌ 추가 리스크 체크 실패: {e}")
            return []
    
    def generate_risk_report(self, user_id: str) -> Dict:
        """종합 리스크 리포트 생성"""
        try:
            # 리스크 지표 계산
            risk_metrics = self.calculate_portfolio_risk(user_id)
            
            # 리스크 경고 평가
            risk_alerts = self.assess_risk_alerts(user_id)
            
            # 리스크 등급 결정
            risk_grade = self._calculate_risk_grade(risk_metrics, risk_alerts)
            
            # 포트폴리오 구성 분석
            portfolio_composition = self._analyze_portfolio_composition(user_id)
            
            # 리스크 개선 권장사항
            recommendations = self._generate_risk_recommendations(risk_metrics, risk_alerts)
            
            report = {
                'user_id': user_id,
                'assessment_date': datetime.now().isoformat(),
                'risk_grade': risk_grade,
                'risk_metrics': risk_metrics.__dict__ if risk_metrics else {},
                'risk_alerts': [alert.__dict__ for alert in risk_alerts],
                'portfolio_composition': portfolio_composition,
                'recommendations': recommendations,
                'summary': self._generate_risk_summary(risk_grade, len(risk_alerts))
            }
            
            self.logger.info(f"📊 리스크 리포트 생성 완료: {user_id} (등급: {risk_grade})")
            return report
            
        except Exception as e:
            self.logger.error(f"❌ 리스크 리포트 생성 실패: {e}")
            return {}
    
    def _calculate_risk_grade(self, risk_metrics: Optional[RiskMetrics], 
                            risk_alerts: List[RiskAlert]) -> str:
        """리스크 등급 계산"""
        if not risk_metrics:
            return "UNKNOWN"
        
        # 경고 개수에 따른 기본 점수
        critical_alerts = len([a for a in risk_alerts if a.severity == "critical"])
        high_alerts = len([a for a in risk_alerts if a.severity == "high"])
        
        if critical_alerts > 0:
            return "HIGH"
        elif high_alerts > 2:
            return "MEDIUM-HIGH"
        elif high_alerts > 0:
            return "MEDIUM"
        elif risk_metrics.volatility > 15:
            return "MEDIUM"
        else:
            return "LOW"
    
    def _analyze_portfolio_composition(self, user_id: str) -> Dict:
        """포트폴리오 구성 분석"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = '''
                SELECT e.category, e.subcategory, pos.target_weight, e.name
                FROM positions pos
                JOIN portfolios port ON pos.portfolio_id = port.id
                JOIN etf_info e ON pos.etf_code = e.code
                WHERE port.user_id = ? AND port.is_active = 1
                ORDER BY pos.target_weight DESC
            '''
            
            df = pd.read_sql_query(query, conn, params=(user_id,))
            conn.close()
            
            if df.empty:
                return {}
            
            # 카테고리별 집계
            category_allocation = df.groupby('category')['target_weight'].sum().to_dict()
            
            # 분산도 계산
            weights = df['target_weight'].values
            diversification_ratio = 1 / sum(w**2 for w in weights)  # 역 허핀달 지수
            
            return {
                'category_allocation': {k: round(v*100, 1) for k, v in category_allocation.items()},
                'diversification_ratio': round(diversification_ratio, 2),
                'number_of_holdings': len(df),
                'largest_holding': {
                    'name': df.iloc[0]['name'],
                    'weight': round(df.iloc[0]['target_weight'] * 100, 1)
                } if not df.empty else None
            }
            
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 구성 분석 실패: {e}")
            return {}
    
    def _generate_risk_recommendations(self, risk_metrics: Optional[RiskMetrics], 
                                     risk_alerts: List[RiskAlert]) -> List[str]:
        """리스크 개선 권장사항 생성"""
        recommendations = []
        
        if not risk_metrics:
            return ["포트폴리오 데이터가 부족하여 권장사항을 제공할 수 없습니다."]
        
        # 높은 변동성 대응
        if risk_metrics.volatility > 20:
            recommendations.append("변동성이 높으니 채권이나 안정적인 배당주 비중을 늘려보세요.")
        
        # 높은 집중도 대응
        if risk_metrics.concentration_risk > 40:
            recommendations.append("특정 자산에 집중되어 있으니 분산투자를 강화하세요.")
        
        # 높은 상관관계 대응
        if risk_metrics.correlation_risk > 0.7:
            recommendations.append("자산 간 상관관계가 높으니 다른 자산군을 고려해보세요.")
        
        # 큰 낙폭 대응
        if risk_metrics.max_drawdown < -20:
            recommendations.append("큰 낙폭에 대비해 손실 제한 전략을 수립하세요.")
        
        # 높은 베타 대응
        if risk_metrics.beta > 1.3:
            recommendations.append("시장 민감도가 높으니 방어적 자산 비중을 늘려보세요.")
        
        # 경고별 권장사항 추가
        for alert in risk_alerts:
            if alert.severity in ["critical", "high"] and alert.recommendation:
                recommendations.append(alert.recommendation)
        
        # 일반적인 권장사항
        if not recommendations:
            recommendations.extend([
                "정기적인 리밸런싱으로 목표 자산배분을 유지하세요.",
                "시장 상황을 모니터링하며 장기 투자 관점을 유지하세요.",
                "리스크 허용도에 맞는 포트폴리오 구성을 점검해보세요."
            ])
        
        return recommendations[:5]  # 최대 5개로 제한
    
    def _generate_risk_summary(self, risk_grade: str, alert_count: int) -> str:
        """리스크 요약 생성"""
        grade_descriptions = {
            "LOW": "낮은 위험도로 안정적인 포트폴리오입니다.",
            "MEDIUM": "적정 수준의 위험도를 가진 균형잡힌 포트폴리오입니다.",
            "MEDIUM-HIGH": "다소 높은 위험도로 주의가 필요한 포트폴리오입니다.",
            "HIGH": "높은 위험도로 즉시 점검이 필요한 포트폴리오입니다.",
            "UNKNOWN": "데이터 부족으로 위험도를 평가할 수 없습니다."
        }
        
        base_summary = grade_descriptions.get(risk_grade, "위험도 평가가 필요합니다.")
        
        if alert_count > 0:
            base_summary += f" 현재 {alert_count}개의 리스크 경고가 있습니다."
        
        return base_summary
    
    def update_risk_limits(self, new_limits: Dict[str, Dict]) -> bool:
        """리스크 한계 업데이트"""
        try:
            for metric_name, limit_config in new_limits.items():
                if metric_name in self.risk_limits:
                    self.risk_limits[metric_name].warning_threshold = limit_config.get(
                        'warning_threshold', self.risk_limits[metric_name].warning_threshold
                    )
                    self.risk_limits[metric_name].critical_threshold = limit_config.get(
                        'critical_threshold', self.risk_limits[metric_name].critical_threshold
                    )
                    self.risk_limits[metric_name].enabled = limit_config.get(
                        'enabled', self.risk_limits[metric_name].enabled
                    )
            
            self.logger.info("⚙️ 리스크 한계 설정 업데이트 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 리스크 한계 업데이트 실패: {e}")
            return False


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("⚠️ 리스크 관리 시스템 테스트")
    print("=" * 60)
    
    # 리스크 관리자 초기화
    risk_manager = RiskManager()
    
    # 테스트 사용자
    test_user = "test_user_risk"
    
    print(f"\n👤 테스트 사용자: {test_user}")
    
    # 1. 포트폴리오 리스크 계산
    print(f"\n📊 포트폴리오 리스크 지표 계산:")
    risk_metrics = risk_manager.calculate_portfolio_risk(test_user)
    
    if risk_metrics:
        print(f"- 변동성: {risk_metrics.volatility:.2f}%")
        print(f"- 95% VaR: {risk_metrics.var_95:.2f}%")
        print(f"- 99% VaR: {risk_metrics.var_99:.2f}%")
        print(f"- 95% CVaR: {risk_metrics.cvar_95:.2f}%")
        print(f"- 최대낙폭: {risk_metrics.max_drawdown:.2f}%")
        print(f"- 베타: {risk_metrics.beta:.2f}")
        print(f"- 상관관계 리스크: {risk_metrics.correlation_risk:.2f}")
        print(f"- 집중도 리스크: {risk_metrics.concentration_risk:.1f}%")
        print(f"- 추적오차: {risk_metrics.tracking_error:.2f}%")
        print(f"- 정보비율: {risk_metrics.information_ratio:.2f}")
    else:
        print("❌ 리스크 지표 계산 실패")
    
    # 2. 리스크 경고 평가
    print(f"\n⚠️ 리스크 경고 평가:")
    risk_alerts = risk_manager.assess_risk_alerts(test_user)
    
    if risk_alerts:
        for i, alert in enumerate(risk_alerts, 1):
            severity_emoji = {
                'low': '🟢',
                'medium': '🟡', 
                'high': '🟠',
                'critical': '🔴'
            }.get(alert.severity, '⚪')
            
            print(f"{i}. {severity_emoji} {alert.message}")
            print(f"   현재값: {alert.current_value:.2f}, 임계값: {alert.threshold:.2f}")
            print(f"   권장사항: {alert.recommendation}")
            print()
    else:
        print("✅ 감지된 리스크 경고가 없습니다")
    
    # 3. 종합 리스크 리포트 생성
    print(f"\n📋 종합 리스크 리포트:")
    risk_report = risk_manager.generate_risk_report(test_user)
    
    if risk_report:
        print(f"- 리스크 등급: {risk_report['risk_grade']}")
        print(f"- 요약: {risk_report['summary']}")
        
        if risk_report.get('portfolio_composition'):
            comp = risk_report['portfolio_composition']
            print(f"- 보유 종목 수: {comp.get('number_of_holdings', 0)}개")
            print(f"- 분산도 지수: {comp.get('diversification_ratio', 0):.2f}")
            
            if comp.get('largest_holding'):
                largest = comp['largest_holding']
                print(f"- 최대 보유 종목: {largest['name']} ({largest['weight']}%)")
        
        if risk_report.get('recommendations'):
            print(f"\n💡 주요 권장사항:")
            for i, rec in enumerate(risk_report['recommendations'][:3], 1):
                print(f"{i}. {rec}")
    
    # 4. 리스크 한계 설정 테스트
    print(f"\n⚙️ 리스크 한계 설정:")
    current_limits = risk_manager.risk_limits
    
    print("현재 리스크 한계:")
    for metric, limit in current_limits.items():
        status = "활성화" if limit.enabled else "비활성화"
        print(f"- {limit.metric_name}: 경고 {limit.warning_threshold}, "
              f"위험 {limit.critical_threshold} ({status})")
    
    # 5. 커스텀 리스크 한계 업데이트 테스트
    print(f"\n🔧 리스크 한계 업데이트 테스트:")
    new_limits = {
        'volatility': {
            'warning_threshold': 18.0,
            'critical_threshold': 25.0,
            'enabled': True
        },
        'max_drawdown': {
            'warning_threshold': -12.0,
            'critical_threshold': -20.0,
            'enabled': True
        }
    }
    
    success = risk_manager.update_risk_limits(new_limits)
    if success:
        print("✅ 리스크 한계 업데이트 성공")
        
        # 업데이트된 설정 확인
        updated_limits = risk_manager.risk_limits
        for metric in ['volatility', 'max_drawdown']:
            limit = updated_limits[metric]
            print(f"- {limit.metric_name}: 경고 {limit.warning_threshold}, "
                  f"위험 {limit.critical_threshold}")
    else:
        print("❌ 리스크 한계 업데이트 실패")
    
    # 6. 리스크 시나리오 분석
    print(f"\n🎲 리스크 시나리오 분석:")
    
    # 다양한 시나리오에서 리스크 등급 변화 확인
    scenarios = [
        ("보수적 포트폴리오", {"volatility": 10, "max_drawdown": -8, "concentration": 25}),
        ("균형 포트폴리오", {"volatility": 15, "max_drawdown": -12, "concentration": 35}),
        ("공격적 포트폴리오", {"volatility": 25, "max_drawdown": -20, "concentration": 45}),
        ("위험 포트폴리오", {"volatility": 35, "max_drawdown": -30, "concentration": 60})
    ]
    
    for scenario_name, metrics in scenarios:
        # 임시 리스크 지표 생성
        temp_metrics = RiskMetrics(
            volatility=metrics["volatility"],
            var_95=-metrics["volatility"]/3,
            var_99=-metrics["volatility"]/2,
            cvar_95=-metrics["volatility"]/2.5,
            max_drawdown=metrics["max_drawdown"],
            beta=1.0 + (metrics["volatility"] - 15) / 30,
            correlation_risk=0.5,
            concentration_risk=metrics["concentration"],
            tracking_error=metrics["volatility"]/5,
            information_ratio=0.5
        )
        
        # 가상의 경고 생성
        temp_alerts = []
        if metrics["volatility"] > 20:
            temp_alerts.append(RiskAlert("volatility", "high", "높은 변동성", 
                                       metrics["volatility"], 20, ""))
        if metrics["concentration"] > 40:
            temp_alerts.append(RiskAlert("concentration", "high", "높은 집중도", 
                                       metrics["concentration"], 40, ""))
        
        grade = risk_manager._calculate_risk_grade(temp_metrics, temp_alerts)
        print(f"- {scenario_name}: {grade} 등급 (변동성: {metrics['volatility']}%)")
    
    print(f"\n✅ 리스크 관리 시스템 테스트 완료!")
    print(f"💡 사용 팁:")
    print(f"   - 정기적으로 리스크 지표를 모니터링하세요")
    print(f"   - 경고가 발생하면 즉시 포트폴리오를 점검하세요")
    print(f"   - 시장 상황에 따라 리스크 한계를 조정할 수 있습니다")
    print(f"   - 분산투자와 정기적 리밸런싱으로 리스크를 관리하세요")