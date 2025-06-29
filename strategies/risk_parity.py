"""
리스크 패리티 투자전략 모듈
각 자산이 동일한 위험을 기여하도록 가중치를 조정하는 고급 자산배분 전략
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import logging
from scipy.optimize import minimize
import warnings

logger = logging.getLogger(__name__)

class RiskParityStrategy:
    """리스크 패리티 전략 클래스"""
    
    def __init__(self, lookback_period: int = 252, 
                 rebalancing_frequency: str = 'monthly',
                 risk_budget_method: str = 'equal'):
        """
        리스크 패리티 전략 초기화
        
        Args:
            lookback_period: 위험 계산에 사용할 과거 데이터 기간 (일)
            rebalancing_frequency: 리밸런싱 주기 ('monthly', 'quarterly')
            risk_budget_method: 위험 예산 방법 ('equal', 'custom', 'volatility_adjusted')
        """
        self.lookback_period = lookback_period
        self.rebalancing_frequency = rebalancing_frequency
        self.risk_budget_method = risk_budget_method
        
        # 기본 자산 풀 설정
        self._setup_asset_universe()
        
        # 위험 예산 설정
        self._setup_risk_budgets()
    
    def _setup_asset_universe(self):
        """투자 가능한 자산 유니버스 설정"""
        
        self.asset_universe = {
            # 주식 자산
            'equity': {
                '069500': {
                    'name': 'KODEX 200',
                    'asset_class': 'domestic_equity',
                    'expected_vol': 0.20,  # 예상 변동성 (연간)
                    'expected_return': 0.08
                },
                '139660': {
                    'name': 'TIGER 미국S&P500',
                    'asset_class': 'us_equity',
                    'expected_vol': 0.18,
                    'expected_return': 0.09
                },
                '195930': {
                    'name': 'TIGER 선진국MSCI World',
                    'asset_class': 'developed_equity',
                    'expected_vol': 0.17,
                    'expected_return': 0.085
                },
                '192090': {
                    'name': 'TIGER 신흥국MSCI',
                    'asset_class': 'emerging_equity',
                    'expected_vol': 0.25,
                    'expected_return': 0.095
                }
            },
            # 채권 자산
            'fixed_income': {
                '114260': {
                    'name': 'KODEX 국고채10년',
                    'asset_class': 'government_bond',
                    'expected_vol': 0.05,
                    'expected_return': 0.035
                },
                '136340': {
                    'name': 'TIGER 미국채10년',
                    'asset_class': 'foreign_bond',
                    'expected_vol': 0.08,
                    'expected_return': 0.04
                },
                '130730': {
                    'name': 'KODEX 글로벌하이일드',
                    'asset_class': 'high_yield_bond',
                    'expected_vol': 0.12,
                    'expected_return': 0.055
                }
            },
            # 대안투자
            'alternatives': {
                '157490': {
                    'name': 'KODEX 리츠',
                    'asset_class': 'reit',
                    'expected_vol': 0.22,
                    'expected_return': 0.07
                },
                '132030': {
                    'name': 'KODEX 골드선물',
                    'asset_class': 'commodity',
                    'expected_vol': 0.18,
                    'expected_return': 0.045
                }
            }
        }
    
    def _setup_risk_budgets(self):
        """위험 예산 설정"""
        
        self.risk_budgets = {
            'equal': {
                # 모든 자산이 동일한 위험 기여
                'domestic_equity': 1/9,
                'us_equity': 1/9,
                'developed_equity': 1/9,
                'emerging_equity': 1/9,
                'government_bond': 1/9,
                'foreign_bond': 1/9,
                'high_yield_bond': 1/9,
                'reit': 1/9,
                'commodity': 1/9
            },
            'strategic': {
                # 전략적 위험 배분
                'domestic_equity': 0.15,
                'us_equity': 0.20,
                'developed_equity': 0.15,
                'emerging_equity': 0.10,
                'government_bond': 0.15,
                'foreign_bond': 0.10,
                'high_yield_bond': 0.05,
                'reit': 0.05,
                'commodity': 0.05
            },
            'conservative': {
                # 보수적 위험 배분 (채권 비중 높음)
                'domestic_equity': 0.10,
                'us_equity': 0.15,
                'developed_equity': 0.10,
                'emerging_equity': 0.05,
                'government_bond': 0.25,
                'foreign_bond': 0.20,
                'high_yield_bond': 0.10,
                'reit': 0.03,
                'commodity': 0.02
            }
        }
    
    def calculate_risk_parity_weights(self, 
                                    price_data: Dict[str, pd.DataFrame],
                                    risk_budget: Optional[Dict[str, float]] = None) -> Dict[str, float]:
        """
        리스크 패리티 가중치 계산
        
        Args:
            price_data: ETF별 가격 데이터
            risk_budget: 커스텀 위험 예산
            
        Returns:
            ETF별 최적 가중치
        """
        try:
            # 수익률 데이터 계산
            returns_data = self._calculate_returns(price_data)
            
            # 공분산 행렬 계산
            cov_matrix = self._calculate_covariance_matrix(returns_data)
            
            # 위험 예산 설정
            if risk_budget is None:
                risk_budget = self.risk_budgets.get(self.risk_budget_method, 
                                                  self.risk_budgets['equal'])
            
            # 자산별 위험 예산 매핑
            asset_risk_budget = self._map_risk_budget_to_assets(risk_budget, list(returns_data.columns))
            
            # 리스크 패리티 최적화
            optimal_weights = self._optimize_risk_parity(cov_matrix, asset_risk_budget)
            
            # ETF 코드별 가중치 딕셔너리로 변환
            weight_dict = {}
            for i, etf_code in enumerate(returns_data.columns):
                weight_dict[etf_code] = optimal_weights[i] * 100  # 퍼센트로 변환
            
            logger.info(f"리스크 패리티 가중치 계산 완료 ({len(weight_dict)}개 자산)")
            return weight_dict
            
        except Exception as e:
            logger.error(f"리스크 패리티 가중치 계산 실패: {e}")
            return self._get_equal_weight_fallback(price_data)
    
    def _calculate_returns(self, price_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """가격 데이터에서 수익률 계산"""
        
        returns_dict = {}
        
        for etf_code, price_df in price_data.items():
            if 'close_price' in price_df.columns and len(price_df) > 1:
                # 일일 수익률 계산
                returns = price_df['close_price'].pct_change().dropna()
                
                # 최근 lookback_period 기간만 사용
                if len(returns) > self.lookback_period:
                    returns = returns.tail(self.lookback_period)
                
                returns_dict[etf_code] = returns
        
        # DataFrame으로 합치기 (공통 날짜만)
        returns_df = pd.DataFrame(returns_dict)
        returns_df = returns_df.dropna()
        
        return returns_df
    
    def _calculate_covariance_matrix(self, returns_data: pd.DataFrame) -> np.ndarray:
        """공분산 행렬 계산"""
        
        # 연간화된 공분산 행렬 계산
        cov_matrix = returns_data.cov() * 252  # 일일 -> 연간
        
        return cov_matrix.values
    
    def _map_risk_budget_to_assets(self, risk_budget: Dict[str, float], 
                                  etf_codes: List[str]) -> np.ndarray:
        """위험 예산을 자산별로 매핑"""
        
        asset_budgets = []
        
        for etf_code in etf_codes:
            # ETF의 자산군 찾기
            asset_class = self._get_asset_class(etf_code)
            budget = risk_budget.get(asset_class, 1/len(etf_codes))  # 기본값: 균등 배분
            asset_budgets.append(budget)
        
        # 정규화 (합이 1이 되도록)
        asset_budgets = np.array(asset_budgets)
        asset_budgets = asset_budgets / asset_budgets.sum()
        
        return asset_budgets
    
    def _get_asset_class(self, etf_code: str) -> str:
        """ETF 코드에서 자산군 추출"""
        
        for category, etfs in self.asset_universe.items():
            for code, info in etfs.items():
                if code == etf_code:
                    return info['asset_class']
        
        return 'other'
    
    def _optimize_risk_parity(self, cov_matrix: np.ndarray, 
                             risk_budget: np.ndarray) -> np.ndarray:
        """리스크 패리티 최적화"""
        
        n_assets = len(cov_matrix)
        
        def risk_contribution(weights, cov_matrix):
            """각 자산의 위험 기여도 계산"""
            portfolio_vol = np.sqrt(np.dot(weights, np.dot(cov_matrix, weights)))
            marginal_contrib = np.dot(cov_matrix, weights) / portfolio_vol
            risk_contrib = weights * marginal_contrib / portfolio_vol
            return risk_contrib
        
        def objective_function(weights):
            """목적함수: 실제 위험 기여도와 목표 위험 예산의 차이"""
            risk_contrib = risk_contribution(weights, cov_matrix)
            return np.sum((risk_contrib - risk_budget) ** 2)
        
        # 제약 조건
        constraints = [
            {'type': 'eq', 'fun': lambda x: np.sum(x) - 1.0}  # 가중치 합 = 1
        ]
        
        # 경계 조건 (각 자산 최소 0.5%, 최대 50%)
        bounds = [(0.005, 0.5) for _ in range(n_assets)]
        
        # 초기값 (균등 가중)
        initial_weights = np.array([1.0 / n_assets] * n_assets)
        
        # 최적화 실행
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            
            result = minimize(
                objective_function,
                initial_weights,
                method='SLSQP',
                bounds=bounds,
                constraints=constraints,
                options={'maxiter': 1000, 'ftol': 1e-9}
            )
        
        if result.success:
            return result.x
        else:
            logger.warning("리스크 패리티 최적화 실패, 균등 가중 사용")
            return initial_weights
    
    def _get_equal_weight_fallback(self, price_data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """최적화 실패시 균등 가중 폴백"""
        
        etf_codes = list(price_data.keys())
        equal_weight = 100.0 / len(etf_codes)
        
        return {etf_code: equal_weight for etf_code in etf_codes}
    
    def generate_portfolio(self, 
                          price_data: Dict[str, pd.DataFrame],
                          custom_risk_budget: Optional[Dict[str, float]] = None,
                          min_weight: float = 0.005,
                          max_weight: float = 0.5) -> Dict[str, float]:
        """
        리스크 패리티 포트폴리오 생성
        
        Args:
            price_data: ETF별 가격 데이터
            custom_risk_budget: 커스텀 위험 예산
            min_weight: 최소 가중치
            max_weight: 최대 가중치
            
        Returns:
            ETF별 가중치 딕셔너리
        """
        try:
            # 데이터 검증
            valid_etfs = self._validate_price_data(price_data)
            
            if len(valid_etfs) < 3:
                logger.warning("유효한 ETF 데이터가 부족합니다")
                return {}
            
            # 유효한 데이터만 사용
            filtered_data = {etf: data for etf, data in price_data.items() if etf in valid_etfs}
            
            # 리스크 패리티 가중치 계산
            weights = self.calculate_risk_parity_weights(filtered_data, custom_risk_budget)
            
            # 가중치 제약 조건 적용
            weights = self._apply_weight_constraints(weights, min_weight * 100, max_weight * 100)
            
            return weights
            
        except Exception as e:
            logger.error(f"리스크 패리티 포트폴리오 생성 실패: {e}")
            return {}
    
    def _validate_price_data(self, price_data: Dict[str, pd.DataFrame]) -> List[str]:
        """가격 데이터 유효성 검증"""
        
        valid_etfs = []
        
        for etf_code, price_df in price_data.items():
            if (not price_df.empty and 
                'close_price' in price_df.columns and 
                len(price_df) >= 30):  # 최소 30일 데이터
                valid_etfs.append(etf_code)
        
        return valid_etfs
    
    def _apply_weight_constraints(self, weights: Dict[str, float], 
                                 min_weight: float, max_weight: float) -> Dict[str, float]:
        """가중치 제약 조건 적용"""
        
        # 최소/최대 가중치 적용
        constrained_weights = {}
        
        for etf_code, weight in weights.items():
            constrained_weight = max(min_weight, min(weight, max_weight))
            constrained_weights[etf_code] = constrained_weight
        
        # 가중치 재정규화
        total_weight = sum(constrained_weights.values())
        if total_weight > 0:
            constrained_weights = {
                etf_code: weight / total_weight * 100
                for etf_code, weight in constrained_weights.items()
            }
        
        return constrained_weights
    
    def calculate_risk_contributions(self, 
                                   weights: Dict[str, float],
                                   price_data: Dict[str, pd.DataFrame]) -> Dict[str, float]:
        """각 자산의 위험 기여도 계산"""
        
        try:
            returns_data = self._calculate_returns(price_data)
            cov_matrix = self._calculate_covariance_matrix(returns_data)
            
            # 가중치를 numpy 배열로 변환
            weight_array = np.array([weights.get(etf, 0) / 100 for etf in returns_data.columns])
            
            # 포트폴리오 변동성 계산
            portfolio_vol = np.sqrt(np.dot(weight_array, np.dot(cov_matrix, weight_array)))
            
            # 한계 위험 기여도 계산
            marginal_contrib = np.dot(cov_matrix, weight_array) / portfolio_vol
            
            # 절대 위험 기여도 계산
            risk_contrib = weight_array * marginal_contrib / portfolio_vol
            
            # ETF별 위험 기여도 딕셔너리
            risk_contributions = {}
            for i, etf_code in enumerate(returns_data.columns):
                risk_contributions[etf_code] = risk_contrib[i] * 100
            
            return risk_contributions
            
        except Exception as e:
            logger.error(f"위험 기여도 계산 실패: {e}")
            return {}
    
    def analyze_portfolio_risk(self, 
                             weights: Dict[str, float],
                             price_data: Dict[str, pd.DataFrame]) -> Dict[str, any]:
        """포트폴리오 위험 분석"""
        
        analysis = {}
        
        try:
            returns_data = self._calculate_returns(price_data)
            cov_matrix = self._calculate_covariance_matrix(returns_data)
            
            # 가중치 배열
            weight_array = np.array([weights.get(etf, 0) / 100 for etf in returns_data.columns])
            
            # 포트폴리오 통계
            portfolio_returns = returns_data.dot(weight_array)
            portfolio_vol = np.sqrt(np.dot(weight_array, np.dot(cov_matrix, weight_array)))
            
            # 위험 기여도
            risk_contributions = self.calculate_risk_contributions(weights, price_data)
            
            # 분산화 효과
            individual_vols = np.sqrt(np.diag(cov_matrix))
            weighted_avg_vol = np.dot(weight_array, individual_vols)
            diversification_ratio = weighted_avg_vol / portfolio_vol
            
            analysis = {
                'portfolio_volatility': portfolio_vol * 100,  # 연간 변동성 (%)
                'expected_return': np.dot(weight_array, [0.08, 0.09, 0.085, 0.095, 0.035, 0.04, 0.055, 0.07, 0.045][:len(weight_array)]) * 100,  # 간단한 예상 수익률
                'risk_contributions': risk_contributions,
                'diversification_ratio': diversification_ratio,
                'concentration_measure': self._calculate_herfindahl_index(risk_contributions),
                'risk_budget_alignment': self._assess_risk_budget_alignment(risk_contributions)
            }
            
        except Exception as e:
            logger.error(f"포트폴리오 위험 분석 실패: {e}")
            analysis = {'error': str(e)}
        
        return analysis
    
    def _calculate_herfindahl_index(self, risk_contributions: Dict[str, float]) -> float:
        """허핀달 지수 계산 (집중도 측정)"""
        
        total_contrib = sum(risk_contributions.values())
        if total_contrib == 0:
            return 0
        
        normalized_contribs = [contrib / total_contrib for contrib in risk_contributions.values()]
        hhi = sum(contrib ** 2 for contrib in normalized_contribs)
        
        return hhi
    
    def _assess_risk_budget_alignment(self, risk_contributions: Dict[str, float]) -> Dict[str, float]:
        """위험 예산 일치도 평가"""
        
        # 목표 위험 예산
        target_budget = self.risk_budgets.get(self.risk_budget_method, self.risk_budgets['equal'])
        
        # 실제 위험 기여도와 목표 예산 비교
        alignment_scores = {}
        
        for etf_code, actual_contrib in risk_contributions.items():
            asset_class = self._get_asset_class(etf_code)
            target_contrib = target_budget.get(asset_class, 0) * 100
            
            # 절대 편차 계산
            deviation = abs(actual_contrib - target_contrib)
            alignment_score = max(0, 100 - deviation * 10)  # 편차에 따른 점수
            
            alignment_scores[etf_code] = alignment_score
        
        return alignment_scores
    
    def get_rebalancing_signals(self, 
                              current_weights: Dict[str, float],
                              target_weights: Dict[str, float],
                              threshold: float = 5.0) -> Dict[str, Dict]:
        """리밸런싱 신호 생성"""
        
        rebalancing_signals = {}
        
        all_etfs = set(list(current_weights.keys()) + list(target_weights.keys()))
        
        for etf_code in all_etfs:
            current_weight = current_weights.get(etf_code, 0)
            target_weight = target_weights.get(etf_code, 0)
            
            deviation = target_weight - current_weight
            
            if abs(deviation) >= threshold:
                action = 'BUY' if deviation > 0 else 'SELL'
                
                rebalancing_signals[etf_code] = {
                    'action': action,
                    'current_weight': current_weight,
                    'target_weight': target_weight,
                    'deviation': deviation,
                    'urgency': 'High' if abs(deviation) >= threshold * 2 else 'Medium'
                }
        
        return rebalancing_signals
    
    def backtest_strategy(self, 
                         price_data: Dict[str, pd.DataFrame],
                         start_date: str,
                         end_date: str,
                         rebalancing_frequency: str = 'monthly') -> Dict[str, any]:
        """전략 백테스팅"""
        
        # 백테스팅 구현 (간단화된 버전)
        # 실제로는 더 복잡한 백테스팅 엔진 필요
        
        backtest_results = {
            'total_return': 0,
            'annualized_return': 0,
            'volatility': 0,
            'sharpe_ratio': 0,
            'max_drawdown': 0,
            'rebalancing_count': 0,
            'transaction_costs': 0
        }
        
        logger.info("백테스팅 기능은 향후 구현 예정")
        
        return backtest_results
    
    def get_strategy_description(self) -> Dict[str, any]:
        """전략 설명 반환"""
        
        return {
            'strategy_name': '리스크 패리티 전략',
            'overview': '각 자산이 포트폴리오 전체 위험에 동일하게 기여하도록 가중치를 조정하는 고급 분산투자 전략',
            'key_principles': [
                '위험 기여도 균등화',
                '변동성 기반 가중치 조정',
                '분산화 효과 극대화',
                '시장 상황 적응형 자산배분'
            ],
            'advantages': [
                '우수한 분산화 효과',
                '시장 집중도 리스크 완화',
                '안정적인 위험-수익 프로필',
                '체계적인 위험 관리'
            ],
            'considerations': [
                '복잡한 계산 과정 필요',
                '과거 데이터 의존성',
                '높은 거래 비용 가능성',
                '변동성이 낮은 자산 편향'
            ],
            'suitable_for': '정교한 위험 관리를 원하는 고급 투자자',
            'minimum_data_requirement': f'{self.lookback_period}일 이상의 가격 데이터',
            'rebalancing_frequency': self.rebalancing_frequency,
            'risk_budget_method': self.risk_budget_method,
            'expected_benefits': [
                '시장 하락기 방어력 향상',
                '장기적 안정성 증대',
                '감정적 투자 실수 방지',
                '체계적인 분산투자'
            ]
        }