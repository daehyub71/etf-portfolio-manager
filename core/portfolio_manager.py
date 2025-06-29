# ==========================================
# core/portfolio_manager.py - 포트폴리오 관리자
# ==========================================

import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import sys
import os

# 상위 모듈 import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 데이터베이스 관리자 import 시도
try:
    from data.database_manager import DatabaseManager
    DATABASE_MANAGER_AVAILABLE = True
    print("✅ DatabaseManager 연동 성공")
except ImportError:
    DATABASE_MANAGER_AVAILABLE = False
    print("⚠️ DatabaseManager를 찾을 수 없습니다 - 기본 모드로 실행")
    
    # 더미 DatabaseManager
    class DatabaseManager:
        def __init__(self, db_path="data"):
            self.db_path = db_path
            print("🔧 더미 DatabaseManager 사용")
        
        def create_portfolio(self, name, strategy_type, target_allocation, risk_level):
            return 1  # 더미 포트폴리오 ID
        
        def get_portfolio_info(self, portfolio_id):
            return {
                'id': portfolio_id,
                'name': '기본 포트폴리오',
                'strategy_type': 'balanced',
                'target_allocation': {'069500': 0.5, '360750': 0.3, '114260': 0.2},
                'total_investment': 10000000,
                'current_value': 10500000
            }

@dataclass
class AssetAllocation:
    """자산배분 정보"""
    etf_code: str
    etf_name: str
    target_weight: float
    current_weight: float
    current_value: float
    target_value: float
    deviation: float
    rebalance_amount: float

@dataclass
class PortfolioSummary:
    """포트폴리오 요약"""
    total_value: float
    total_return: float
    total_return_pct: float
    daily_return: float
    volatility: float
    sharpe_ratio: float
    max_drawdown: float
    num_holdings: int
    last_rebalance: str
    next_rebalance: str

@dataclass
class RebalanceRecommendation:
    """리밸런싱 추천"""
    rebalance_needed: bool
    total_deviation: float
    max_deviation: float
    recommendations: List[AssetAllocation]
    estimated_cost: float
    rebalance_type: str

class PortfolioStrategy:
    """포트폴리오 전략 기본 클래스"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
    
    def get_target_allocation(self, age: int = 35, risk_level: str = "moderate") -> Dict[str, float]:
        """목표 자산배분 반환 (ETF 코드: 비중)"""
        raise NotImplementedError

class CoreSatelliteStrategy(PortfolioStrategy):
    """코어-새틀라이트 전략"""
    
    def __init__(self):
        super().__init__(
            "코어-새틀라이트 전략",
            "안정적인 코어 자산 80% + 성장성 새틀라이트 20%"
        )
    
    def get_target_allocation(self, age: int = 35, risk_level: str = "moderate") -> Dict[str, float]:
        # 기본 코어-새틀라이트 배분
        allocation = {
            "069500": 0.40,  # KODEX 200 (국내 코어)
            "360750": 0.25,  # TIGER 미국S&P500 (해외 코어)
            "114260": 0.15,  # KODEX 국고채10년 (채권 코어)
            "133690": 0.10,  # KODEX 나스닥100 (성장 새틀라이트)
            "195930": 0.10   # KODEX 선진국MSCI (분산 새틀라이트)
        }
        
        # 나이에 따른 조정
        if age < 30:
            allocation["360750"] += 0.05
            allocation["133690"] += 0.05
            allocation["114260"] -= 0.10
        elif age > 50:
            allocation["114260"] += 0.10
            allocation["133690"] -= 0.05
            allocation["360750"] -= 0.05
        
        # 위험성향에 따른 조정
        if risk_level == "conservative":
            allocation["114260"] += 0.10
            allocation["133690"] -= 0.05
            allocation["360750"] -= 0.05
        elif risk_level == "aggressive":
            allocation["133690"] += 0.10
            allocation["360750"] += 0.05
            allocation["114260"] -= 0.15
        
        return allocation

class BalancedStrategy(PortfolioStrategy):
    """균형 전략"""
    
    def __init__(self):
        super().__init__(
            "균형 전략",
            "국내외 주식과 채권의 균형잡힌 분산투자"
        )
    
    def get_target_allocation(self, age: int = 35, risk_level: str = "moderate") -> Dict[str, float]:
        allocation = {
            "069500": 0.35,  # 국내 주식
            "360750": 0.30,  # 해외 주식
            "114260": 0.25,  # 국내 채권
            "305080": 0.10   # 해외 채권
        }
        
        return allocation

class ConservativeStrategy(PortfolioStrategy):
    """보수적 전략"""
    
    def __init__(self):
        super().__init__(
            "보수적 전략",
            "안정적인 채권 중심의 보수적 포트폴리오"
        )
    
    def get_target_allocation(self, age: int = 35, risk_level: str = "moderate") -> Dict[str, float]:
        allocation = {
            "069500": 0.20,  # 국내 주식
            "360750": 0.20,  # 해외 주식
            "114260": 0.40,  # 국내 채권
            "305080": 0.20   # 해외 채권
        }
        
        return allocation

class PortfolioManager:
    """포트폴리오 관리자"""
    
    def __init__(self, database_manager=None, **kwargs):
        """
        포트폴리오 관리자 초기화
        
        Args:
            database_manager: DatabaseManager 인스턴스 (선택적)
            **kwargs: 기타 매개변수
        """
        self.db_manager = database_manager
        if not self.db_manager:
            self.db_manager = DatabaseManager()
        
        # 로깅 설정
        self.logger = logging.getLogger(__name__)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # 사용 가능한 전략들
        self.strategies = {
            "core_satellite": CoreSatelliteStrategy(),
            "balanced": BalancedStrategy(),
            "conservative": ConservativeStrategy()
        }
        
        # ETF 기본 정보 (시뮬레이션용)
        self.etf_info = {
            "069500": {"name": "KODEX 200", "category": "국내주식", "price": 28400},
            "360750": {"name": "TIGER 미국S&P500", "category": "해외주식", "price": 15800},
            "114260": {"name": "KODEX 국고채10년", "category": "국내채권", "price": 108500},
            "133690": {"name": "KODEX 나스닥100", "category": "해외주식", "price": 24500},
            "195930": {"name": "KODEX 선진국MSCI", "category": "해외주식", "price": 13200},
            "305080": {"name": "TIGER 미국채10년", "category": "해외채권", "price": 8900}
        }
        
        self.logger.info("💼 포트폴리오 관리자 초기화 완료")
    
    def set_database_manager(self, db_manager):
        """데이터베이스 관리자 설정"""
        self.db_manager = db_manager
        self.logger.info("🔗 데이터베이스 관리자 연결됨")
    
    def create_portfolio(self, user_id: str, name: str, strategy_name: str, 
                        initial_amount: float, user_profile: Dict) -> Optional[int]:
        """새 포트폴리오 생성"""
        try:
            if strategy_name not in self.strategies:
                self.logger.error(f"지원하지 않는 전략: {strategy_name}")
                return None
            
            strategy = self.strategies[strategy_name]
            target_allocation = strategy.get_target_allocation(
                age=user_profile.get('age', 35),
                risk_level=user_profile.get('risk_level', 'moderate')
            )
            
            # DatabaseManager를 통해 포트폴리오 생성
            portfolio_id = self.db_manager.create_portfolio(
                name=name,
                strategy_type=strategy_name,
                target_allocation=target_allocation,
                risk_level=user_profile.get('risk_level', 'moderate')
            )
            
            if portfolio_id:
                # 초기 투자 시뮬레이션
                self._simulate_initial_investment(portfolio_id, initial_amount, target_allocation)
                self.logger.info(f"✅ 포트폴리오 생성 완료: {name} (ID: {portfolio_id})")
                return portfolio_id
            else:
                self.logger.error("포트폴리오 생성 실패")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 생성 실패: {e}")
            return None
    
    def _simulate_initial_investment(self, portfolio_id: int, amount: float, allocation: Dict):
        """초기 투자 시뮬레이션"""
        try:
            for etf_code, weight in allocation.items():
                etf_amount = amount * weight
                etf_price = self.etf_info.get(etf_code, {}).get('price', 10000)
                shares = int(etf_amount / etf_price)
                
                # 거래 내역 추가
                self.db_manager.add_transaction(
                    portfolio_id=portfolio_id,
                    etf_code=etf_code,
                    transaction_type='BUY',
                    shares=shares,
                    price=etf_price,
                    note='초기 투자'
                )
                
        except Exception as e:
            self.logger.error(f"초기 투자 시뮬레이션 실패: {e}")
    
    def get_portfolio_summary(self, portfolio_id: int) -> Optional[PortfolioSummary]:
        """포트폴리오 요약 정보"""
        try:
            # 포트폴리오 기본 정보 조회
            portfolio_info = self.db_manager.get_portfolio_info(portfolio_id)
            if not portfolio_info:
                return None
            
            # 보유 종목 정보 조회
            holdings_df = self.db_manager.get_portfolio_holdings(portfolio_id)
            if holdings_df.empty:
                return None
            
            # 현재 가치 계산
            total_value = 0
            for _, holding in holdings_df.iterrows():
                current_price = self.etf_info.get(holding['etf_code'], {}).get('price', holding['avg_price'])
                value = holding['shares'] * current_price
                total_value += value
            
            # 수익률 계산
            initial_investment = portfolio_info.get('total_investment', 0)
            total_return = total_value - initial_investment
            total_return_pct = (total_return / initial_investment * 100) if initial_investment > 0 else 0
            
            # 기타 지표 (시뮬레이션)
            daily_return = np.random.normal(0.05, 0.8)  # 일일 수익률
            volatility = 15.2  # 연간 변동성
            sharpe_ratio = total_return_pct / volatility if volatility > 0 else 0
            max_drawdown = -8.5  # 최대 낙폭
            
            # 리밸런싱 일정
            last_rebalance = portfolio_info.get('last_rebalance_date', '없음')
            next_rebalance = self._calculate_next_rebalance_date(last_rebalance)
            
            return PortfolioSummary(
                total_value=round(total_value, 2),
                total_return=round(total_return, 2),
                total_return_pct=round(total_return_pct, 2),
                daily_return=round(daily_return, 2),
                volatility=round(volatility, 2),
                sharpe_ratio=round(sharpe_ratio, 2),
                max_drawdown=round(max_drawdown, 2),
                num_holdings=len(holdings_df),
                last_rebalance=str(last_rebalance) if last_rebalance else '없음',
                next_rebalance=next_rebalance
            )
            
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 요약 조회 실패: {e}")
            return None
    
    def get_rebalance_recommendation(self, portfolio_id: int, 
                                   threshold: float = 5.0) -> Optional[RebalanceRecommendation]:
        """리밸런싱 추천"""
        try:
            # 포트폴리오 정보 조회
            portfolio_info = self.db_manager.get_portfolio_info(portfolio_id)
            if not portfolio_info:
                return None
            
            target_allocation = portfolio_info['target_allocation']
            holdings_df = self.db_manager.get_portfolio_holdings(portfolio_id)
            
            if holdings_df.empty:
                return None
            
            # 현재 총 가치 계산
            total_current_value = 0
            current_values = {}
            
            for _, holding in holdings_df.iterrows():
                current_price = self.etf_info.get(holding['etf_code'], {}).get('price', holding['avg_price'])
                current_value = holding['shares'] * current_price
                current_values[holding['etf_code']] = current_value
                total_current_value += current_value
            
            # 자산배분 분석
            allocations = []
            max_deviation = 0
            total_deviation = 0
            
            for etf_code, target_weight in target_allocation.items():
                current_value = current_values.get(etf_code, 0)
                current_weight = (current_value / total_current_value * 100) if total_current_value > 0 else 0
                target_weight_pct = target_weight * 100
                deviation = abs(current_weight - target_weight_pct)
                
                target_value = total_current_value * target_weight
                rebalance_amount = target_value - current_value
                
                etf_name = self.etf_info.get(etf_code, {}).get('name', f'ETF {etf_code}')
                
                allocation = AssetAllocation(
                    etf_code=etf_code,
                    etf_name=etf_name,
                    target_weight=round(target_weight_pct, 2),
                    current_weight=round(current_weight, 2),
                    current_value=round(current_value, 2),
                    target_value=round(target_value, 2),
                    deviation=round(deviation, 2),
                    rebalance_amount=round(rebalance_amount, 2)
                )
                
                allocations.append(allocation)
                max_deviation = max(max_deviation, deviation)
                total_deviation += deviation
            
            # 리밸런싱 필요 여부
            rebalance_needed = max_deviation > threshold
            
            # 비용 추정
            total_trade_amount = sum(abs(a.rebalance_amount) for a in allocations) / 2
            estimated_cost = total_trade_amount * 0.001  # 0.1% 수수료 가정
            
            # 리밸런싱 타입
            buy_amounts = sum(a.rebalance_amount for a in allocations if a.rebalance_amount > 0)
            sell_amounts = sum(abs(a.rebalance_amount) for a in allocations if a.rebalance_amount < 0)
            
            if buy_amounts > 0 and sell_amounts == 0:
                rebalance_type = "buy_only"
            elif sell_amounts > 0 and buy_amounts == 0:
                rebalance_type = "sell_only"
            else:
                rebalance_type = "mixed"
            
            return RebalanceRecommendation(
                rebalance_needed=rebalance_needed,
                total_deviation=round(total_deviation, 2),
                max_deviation=round(max_deviation, 2),
                recommendations=allocations,
                estimated_cost=round(estimated_cost, 2),
                rebalance_type=rebalance_type
            )
            
        except Exception as e:
            self.logger.error(f"❌ 리밸런싱 추천 실패: {e}")
            return None
    
    def execute_rebalance(self, portfolio_id: int, 
                         additional_investment: float = 0) -> bool:
        """리밸런싱 실행"""
        try:
            recommendation = self.get_rebalance_recommendation(portfolio_id)
            
            if not recommendation or not recommendation.rebalance_needed:
                self.logger.info("리밸런싱이 필요하지 않습니다")
                return True
            
            # 리밸런싱 실행 시뮬레이션
            for rec in recommendation.recommendations:
                if abs(rec.rebalance_amount) > 1000:  # 최소 거래 금액
                    etf_price = self.etf_info.get(rec.etf_code, {}).get('price', 10000)
                    shares_change = rec.rebalance_amount / etf_price
                    
                    if shares_change > 0:
                        # 매수
                        self.db_manager.add_transaction(
                            portfolio_id=portfolio_id,
                            etf_code=rec.etf_code,
                            transaction_type='BUY',
                            shares=int(shares_change),
                            price=etf_price,
                            note='리밸런싱 매수'
                        )
                    else:
                        # 매도
                        self.db_manager.add_transaction(
                            portfolio_id=portfolio_id,
                            etf_code=rec.etf_code,
                            transaction_type='SELL',
                            shares=int(abs(shares_change)),
                            price=etf_price,
                            note='리밸런싱 매도'
                        )
            
            self.logger.info(f"✅ 리밸런싱 실행 완료: 포트폴리오 {portfolio_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 리밸런싱 실행 실패: {e}")
            return False
    
    def get_portfolio_performance(self, portfolio_id: int, days: int = 30) -> Optional[pd.DataFrame]:
        """포트폴리오 성과 히스토리"""
        try:
            # DatabaseManager에서 성과 데이터 조회
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            performance_df = self.db_manager.get_portfolio_performance(portfolio_id, start_date)
            
            if performance_df.empty:
                # 시뮬레이션 데이터 생성
                dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
                
                # 랜덤 수익률 생성 (시드 고정으로 일관성 유지)
                np.random.seed(portfolio_id)
                daily_returns = np.random.normal(0.0005, 0.015, days)
                cumulative_returns = (1 + daily_returns).cumprod() - 1
                
                portfolio_values = 1000000 * (1 + cumulative_returns)
                
                performance_df = pd.DataFrame({
                    'date': dates,
                    'total_value': portfolio_values,
                    'daily_return': daily_returns * 100,
                    'cumulative_return': cumulative_returns * 100
                })
                
                # 성과 데이터 저장 (실제 구현에서)
                for _, row in performance_df.iterrows():
                    self.db_manager.update_portfolio_performance(
                        portfolio_id=portfolio_id,
                        date=row['date'].strftime('%Y-%m-%d'),
                        total_value=row['total_value'],
                        total_investment=1000000,
                        daily_return=row['daily_return']
                    )
            
            return performance_df
            
        except Exception as e:
            self.logger.error(f"❌ 성과 히스토리 조회 실패: {e}")
            return None
    
    def _calculate_next_rebalance_date(self, last_rebalance) -> str:
        """다음 리밸런싱 일정 계산"""
        try:
            if last_rebalance and last_rebalance != '없음':
                if isinstance(last_rebalance, str):
                    last_date = datetime.fromisoformat(last_rebalance.replace('Z', ''))
                else:
                    last_date = last_rebalance
                next_date = last_date + timedelta(days=90)  # 3개월 후
            else:
                next_date = datetime.now() + timedelta(days=90)
            
            return next_date.strftime('%Y-%m-%d')
            
        except Exception:
            next_date = datetime.now() + timedelta(days=90)
            return next_date.strftime('%Y-%m-%d')
    
    def get_available_strategies(self) -> Dict[str, str]:
        """사용 가능한 전략 목록"""
        return {name: strategy.description for name, strategy in self.strategies.items()}
    
    def add_strategy(self, name: str, strategy: PortfolioStrategy):
        """새 전략 추가"""
        self.strategies[name] = strategy
        self.logger.info(f"새 전략 추가: {name}")
    
    def get_etf_allocation_breakdown(self, portfolio_id: int) -> Dict:
        """ETF별 자산배분 상세 분석"""
        try:
            holdings_df = self.db_manager.get_portfolio_holdings(portfolio_id)
            if holdings_df.empty:
                return {}
            
            breakdown = {}
            total_value = 0
            
            for _, holding in holdings_df.iterrows():
                current_price = self.etf_info.get(holding['etf_code'], {}).get('price', holding['avg_price'])
                current_value = holding['shares'] * current_price
                total_value += current_value
                
                breakdown[holding['etf_code']] = {
                    'name': self.etf_info.get(holding['etf_code'], {}).get('name', holding['etf_code']),
                    'category': self.etf_info.get(holding['etf_code'], {}).get('category', '기타'),
                    'shares': holding['shares'],
                    'avg_price': holding['avg_price'],
                    'current_price': current_price,
                    'current_value': current_value,
                    'unrealized_pnl': (current_price - holding['avg_price']) * holding['shares'],
                    'target_weight': holding.get('target_weight', 0) * 100,
                    'current_weight': 0  # 계산 후 업데이트
                }
            
            # 현재 비중 계산
            for etf_code in breakdown:
                breakdown[etf_code]['current_weight'] = (breakdown[etf_code]['current_value'] / total_value * 100) if total_value > 0 else 0
            
            return breakdown
            
        except Exception as e:
            self.logger.error(f"❌ ETF 배분 분석 실패: {e}")
            return {}
    
    def backup_portfolio_data(self, backup_path: str = "backups") -> bool:
        """포트폴리오 데이터 백업"""
        try:
            success = self.db_manager.backup_database(backup_path)
            if success:
                self.logger.info(f"✅ 포트폴리오 데이터 백업 완료: {backup_path}")
            return success
        except Exception as e:
            self.logger.error(f"❌ 포트폴리오 데이터 백업 실패: {e}")
            return False


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("💼 포트폴리오 관리자 테스트")
    print("=" * 60)
    
    # 포트폴리오 관리자 초기화
    manager = PortfolioManager()
    
    # 사용 가능한 전략 출력
    print("\n🎯 사용 가능한 투자 전략:")
    strategies = manager.get_available_strategies()
    for i, (name, desc) in enumerate(strategies.items(), 1):
        print(f"{i}. {name}: {desc}")
    
    # 테스트 데이터
    test_user = "test_user_001"
    user_profile = {
        "age": 35,
        "risk_level": "moderate",
        "investment_goal": "retirement",
        "investment_horizon": 20
    }
    
    print(f"\n👤 테스트 사용자: {test_user}")
    print(f"- 나이: {user_profile['age']}세")
    print(f"- 위험성향: {user_profile['risk_level']}")
    print(f"- 투자목표: {user_profile['investment_goal']}")
    print(f"- 투자기간: {user_profile['investment_horizon']}년")
    
    # 1. 포트폴리오 생성 테스트
    print(f"\n🏗️ 포트폴리오 생성 테스트:")
    initial_amount = 10000000  # 1000만원
    strategy_name = "core_satellite"
    portfolio_name = "테스트 포트폴리오"
    
    portfolio_id = manager.create_portfolio(
        user_id=test_user,
        name=portfolio_name,
        strategy_name=strategy_name,
        initial_amount=initial_amount,
        user_profile=user_profile
    )
    
    if portfolio_id:
        print(f"✅ 포트폴리오 생성 성공 (ID: {portfolio_id})")
        print(f"- 이름: {portfolio_name}")
        print(f"- 전략: {strategy_name}")
        print(f"- 초기 투자금: {initial_amount:,}원")
    else:
        print(f"❌ 포트폴리오 생성 실패")
    
    # 2. 포트폴리오 요약 테스트
    if portfolio_id:
        print(f"\n📊 포트폴리오 요약:")
        summary = manager.get_portfolio_summary(portfolio_id)
        
        if summary:
            print(f"- 현재 가치: {summary.total_value:,.0f}원")
            print(f"- 총 수익: {summary.total_return:,.0f}원 ({summary.total_return_pct:+.2f}%)")
            print(f"- 일일 수익률: {summary.daily_return:+.2f}%")
            print(f"- 변동성: {summary.volatility:.2f}%")
            print(f"- 샤프 비율: {summary.sharpe_ratio:.2f}")
            print(f"- 보유 종목: {summary.num_holdings}개")
            print(f"- 마지막 리밸런싱: {summary.last_rebalance}")
            print(f"- 다음 리밸런싱: {summary.next_rebalance}")
        else:
            print("❌ 포트폴리오 요약 조회 실패")
    
    # 3. 리밸런싱 추천 테스트
    if portfolio_id:
        print(f"\n⚖️ 리밸런싱 추천:")
        recommendation = manager.get_rebalance_recommendation(portfolio_id, threshold=3.0)
        
        if recommendation:
            print(f"- 리밸런싱 필요: {'예' if recommendation.rebalance_needed else '아니오'}")
            print(f"- 최대 편차: {recommendation.max_deviation:.2f}%")
            print(f"- 총 편차: {recommendation.total_deviation:.2f}%")
            print(f"- 예상 비용: {recommendation.estimated_cost:,.0f}원")
            print(f"- 리밸런싱 타입: {recommendation.rebalance_type}")
            
            if recommendation.rebalance_needed:
                print(f"\n📋 개별 ETF 리밸런싱 상세:")
                for rec in recommendation.recommendations:
                    if abs(rec.deviation) > 1.0:  # 1% 이상 편차만 표시
                        print(f"- {rec.etf_name} ({rec.etf_code}):")
                        print(f"  목표: {rec.target_weight:.1f}% → 현재: {rec.current_weight:.1f}% "
                              f"(편차: {rec.deviation:.1f}%)")
                        print(f"  조정 금액: {rec.rebalance_amount:+,.0f}원")
        else:
            print("❌ 리밸런싱 추천 조회 실패")
    
    # 4. ETF 배분 상세 분석
    if portfolio_id:
        print(f"\n📈 ETF별 상세 분석:")
        breakdown = manager.get_etf_allocation_breakdown(portfolio_id)
        
        if breakdown:
            for etf_code, info in breakdown.items():
                pnl_pct = (info['unrealized_pnl'] / (info['avg_price'] * info['shares']) * 100) if info['shares'] > 0 else 0
                print(f"- {info['name']} ({etf_code}):")
                print(f"  보유: {info['shares']}주, 평균단가: {info['avg_price']:,.0f}원")
                print(f"  현재가: {info['current_price']:,.0f}원, 평가액: {info['current_value']:,.0f}원")
                print(f"  평가손익: {info['unrealized_pnl']:+,.0f}원 ({pnl_pct:+.2f}%)")
                print(f"  비중: {info['current_weight']:.1f}% (목표: {info['target_weight']:.1f}%)")
        else:
            print("❌ ETF 배분 분석 실패")
    
    print(f"\n✅ 포트폴리오 관리자 테스트 완료!")
    print(f"💡 다음 단계:")
    print(f"   - 실제 투자금액으로 포트폴리오 생성")
    print(f"   - 정기적 리밸런싱 스케줄 설정")
    print(f"   - 성과 추적 및 전략 조정")
    print(f"   - 백업 및 복원 기능 활용")