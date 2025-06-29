"""
RESTful API 서버
- 포트폴리오 데이터 API
- 백테스팅 API
- 리밸런싱 API
- 실시간 모니터링 API
- 인증 및 보안
"""

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import yaml
import uvicorn
import jwt
from passlib.context import CryptContext
import sqlite3
from contextlib import asynccontextmanager

# 데이터 모델 정의
class PortfolioRequest(BaseModel):
    """포트폴리오 요청 모델"""
    etf_codes: List[str] = Field(..., description="ETF 코드 리스트")
    target_allocation: Dict[str, float] = Field(..., description="목표 자산배분")
    investment_amount: float = Field(..., gt=0, description="투자금액")
    rebalancing_threshold: float = Field(default=0.05, ge=0.01, le=0.2, description="리밸런싱 임계치")

class BacktestRequest(BaseModel):
    """백테스팅 요청 모델"""
    strategy: str = Field(..., description="전략명")
    start_date: str = Field(..., description="시작일 (YYYY-MM-DD)")
    end_date: str = Field(..., description="종료일 (YYYY-MM-DD)")
    initial_amount: float = Field(..., gt=0, description="초기 투자금액")
    rebalancing_frequency: str = Field(default="quarterly", description="리밸런싱 주기")

class UserModel(BaseModel):
    """사용자 모델"""
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., description="이메일")
    age: Optional[int] = Field(None, ge=18, le=100, description="나이")
    risk_tolerance: Optional[str] = Field(None, description="위험성향")

class APIResponse(BaseModel):
    """API 응답 모델"""
    success: bool
    message: str
    data: Optional[Any] = None
    timestamp: datetime = Field(default_factory=datetime.now)

# 보안 설정
security = HTTPBearer()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
SECRET_KEY = "your-secret-key-here"  # 실제 배포시 환경변수로 관리
ALGORITHM = "HS256"

class ETFAPIServer:
    """ETF 투자 관리 API 서버"""
    
    def __init__(self, config_path: str = "config.yaml"):
        """초기화"""
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        self.app = FastAPI(
            title="ETF 장기투자 관리 API",
            description="직장인을 위한 ETF 포트폴리오 관리 시스템 API",
            version="1.0.0",
            docs_url="/docs",
            redoc_url="/redoc"
        )
        
        # CORS 설정
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # 실제 배포시 제한 필요
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # 데이터베이스 연결
        self.db_path = "portfolio_data.db"
        
        # 라우트 설정
        self._setup_routes()
        
    def _setup_routes(self):
        """API 라우트 설정"""
        
        # 헬스 체크
        @self.app.get("/", response_model=APIResponse)
        async def root():
            return APIResponse(
                success=True,
                message="ETF 투자 관리 API 서버가 정상 동작 중입니다."
            )
        
        @self.app.get("/health")
        async def health_check():
            return {"status": "healthy", "timestamp": datetime.now()}
        
        # 인증 관련
        @self.app.post("/auth/login")
        async def login(username: str, password: str):
            # 실제로는 데이터베이스에서 사용자 확인
            if username == "admin" and password == "password":  # 예시
                token = self._create_access_token({"sub": username})
                return {"access_token": token, "token_type": "bearer"}
            raise HTTPException(status_code=401, detail="인증 실패")
        
        # 포트폴리오 관련 API
        @self.app.get("/portfolio/summary", response_model=APIResponse)
        async def get_portfolio_summary(current_user: dict = Depends(self._get_current_user)):
            """포트폴리오 요약 정보"""
            try:
                summary = self._get_portfolio_summary()
                return APIResponse(
                    success=True,
                    message="포트폴리오 요약 조회 성공",
                    data=summary
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/portfolio/holdings", response_model=APIResponse)
        async def get_portfolio_holdings(current_user: dict = Depends(self._get_current_user)):
            """보유 종목 정보"""
            try:
                holdings = self._get_portfolio_holdings()
                return APIResponse(
                    success=True,
                    message="보유 종목 조회 성공",
                    data=holdings
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/portfolio/create", response_model=APIResponse)
        async def create_portfolio(
            request: PortfolioRequest,
            current_user: dict = Depends(self._get_current_user)
        ):
            """새 포트폴리오 생성"""
            try:
                result = self._create_portfolio(request)
                return APIResponse(
                    success=True,
                    message="포트폴리오 생성 성공",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/portfolio/performance", response_model=APIResponse)
        async def get_portfolio_performance(
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            current_user: dict = Depends(self._get_current_user)
        ):
            """포트폴리오 성과 분석"""
            try:
                performance = self._get_portfolio_performance(start_date, end_date)
                return APIResponse(
                    success=True,
                    message="성과 분석 조회 성공",
                    data=performance
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # 리밸런싱 관련 API
        @self.app.get("/rebalancing/check", response_model=APIResponse)
        async def check_rebalancing(current_user: dict = Depends(self._get_current_user)):
            """리밸런싱 필요성 체크"""
            try:
                rebalancing_data = self._check_rebalancing_needed()
                return APIResponse(
                    success=True,
                    message="리밸런싱 체크 완료",
                    data=rebalancing_data
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/rebalancing/suggest", response_model=APIResponse)
        async def suggest_rebalancing(
            threshold: float = 0.05,
            current_user: dict = Depends(self._get_current_user)
        ):
            """리밸런싱 제안"""
            try:
                suggestions = self._suggest_rebalancing(threshold)
                return APIResponse(
                    success=True,
                    message="리밸런싱 제안 생성 완료",
                    data=suggestions
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # ETF 정보 관련 API
        @self.app.get("/etf/list", response_model=APIResponse)
        async def get_etf_list(
            category: Optional[str] = None,
            min_aum: Optional[float] = None
        ):
            """ETF 목록 조회"""
            try:
                etf_list = self._get_etf_list(category, min_aum)
                return APIResponse(
                    success=True,
                    message="ETF 목록 조회 성공",
                    data=etf_list
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/etf/{etf_code}/info", response_model=APIResponse)
        async def get_etf_info(etf_code: str):
            """특정 ETF 정보 조회"""
            try:
                etf_info = self._get_etf_info(etf_code)
                if not etf_info:
                    raise HTTPException(status_code=404, detail="ETF를 찾을 수 없습니다")
                
                return APIResponse(
                    success=True,
                    message="ETF 정보 조회 성공",
                    data=etf_info
                )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/etf/{etf_code}/price", response_model=APIResponse)
        async def get_etf_price(etf_code: str, days: int = 30):
            """ETF 가격 이력 조회"""
            try:
                price_data = self._get_etf_price_history(etf_code, days)
                return APIResponse(
                    success=True,
                    message="가격 이력 조회 성공",
                    data=price_data
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # 백테스팅 관련 API
        @self.app.post("/backtesting/run", response_model=APIResponse)
        async def run_backtest(
            request: BacktestRequest,
            current_user: dict = Depends(self._get_current_user)
        ):
            """백테스팅 실행"""
            try:
                result = self._run_backtest(request)
                return APIResponse(
                    success=True,
                    message="백테스팅 실행 완료",
                    data=result
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/backtesting/strategies", response_model=APIResponse)
        async def get_available_strategies():
            """사용 가능한 전략 목록"""
            strategies = [
                {
                    "name": "core_satellite",
                    "description": "코어-새틀라이트 전략",
                    "risk_level": "medium"
                },
                {
                    "name": "global_diversified",
                    "description": "글로벌 분산 전략",
                    "risk_level": "medium"
                },
                {
                    "name": "lifecycle",
                    "description": "생애주기 전략",
                    "risk_level": "adaptive"
                }
            ]
            
            return APIResponse(
                success=True,
                message="전략 목록 조회 성공",
                data=strategies
            )
        
        # 알림 관련 API
        @self.app.get("/notifications", response_model=APIResponse)
        async def get_notifications(current_user: dict = Depends(self._get_current_user)):
            """알림 목록 조회"""
            try:
                notifications = self._get_user_notifications()
                return APIResponse(
                    success=True,
                    message="알림 목록 조회 성공",
                    data=notifications
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.post("/notifications/{notification_id}/read", response_model=APIResponse)
        async def mark_notification_read(
            notification_id: int,
            current_user: dict = Depends(self._get_current_user)
        ):
            """알림 읽음 처리"""
            try:
                self._mark_notification_read(notification_id)
                return APIResponse(
                    success=True,
                    message="알림 읽음 처리 완료"
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        # 리포트 관련 API
        @self.app.post("/reports/generate", response_model=APIResponse)
        async def generate_report(
            report_type: str,
            period: str = "monthly",
            current_user: dict = Depends(self._get_current_user)
        ):
            """리포트 생성"""
            try:
                report = self._generate_report(report_type, period)
                return APIResponse(
                    success=True,
                    message="리포트 생성 완료",
                    data=report
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
    
    def _create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None):
        """JWT 토큰 생성"""
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(hours=24)
        
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt
    
    async def _get_current_user(self, credentials: HTTPAuthorizationCredentials = Security(security)):
        """현재 사용자 정보 확인"""
        try:
            payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            if username is None:
                raise HTTPException(status_code=401, detail="인증 토큰이 유효하지 않습니다")
            return {"username": username}
        except jwt.PyJWTError:
            raise HTTPException(status_code=401, detail="인증 토큰이 유효하지 않습니다")
    
    def _get_portfolio_summary(self) -> Dict:
        """포트폴리오 요약 정보 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 총 자산 가치
            total_value_query = """
                SELECT SUM(current_value) as total_value
                FROM portfolio_holdings
                WHERE is_active = 1
            """
            total_value = pd.read_sql_query(total_value_query, conn)['total_value'].iloc[0] or 0
            
            # 총 투자원금
            total_cost_query = """
                SELECT SUM(total_cost) as total_cost
                FROM portfolio_holdings
                WHERE is_active = 1
            """
            total_cost = pd.read_sql_query(total_cost_query, conn)['total_cost'].iloc[0] or 0
            
            # 총 수익률
            total_return = (total_value - total_cost) / total_cost * 100 if total_cost > 0 else 0
            
            # 보유 종목 수
            holdings_count_query = """
                SELECT COUNT(*) as count
                FROM portfolio_holdings
                WHERE is_active = 1 AND quantity > 0
            """
            holdings_count = pd.read_sql_query(holdings_count_query, conn)['count'].iloc[0]
            
            conn.close()
            
            return {
                "total_value": total_value,
                "total_cost": total_cost,
                "total_return_pct": round(total_return, 2),
                "total_return_amount": total_value - total_cost,
                "holdings_count": holdings_count,
                "last_updated": datetime.now().isoformat()
            }
            
        except Exception as e:
            raise Exception(f"포트폴리오 요약 조회 실패: {str(e)}")
    
    def _get_portfolio_holdings(self) -> List[Dict]:
        """보유 종목 정보 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = """
                SELECT 
                    etf_code,
                    etf_name,
                    quantity,
                    avg_price,
                    current_price,
                    current_value,
                    total_cost,
                    unrealized_gain,
                    unrealized_gain_pct,
                    target_allocation,
                    current_allocation
                FROM portfolio_holdings
                WHERE is_active = 1 AND quantity > 0
                ORDER BY current_value DESC
            """
            
            holdings_df = pd.read_sql_query(query, conn)
            conn.close()
            
            return holdings_df.to_dict('records')
            
        except Exception as e:
            raise Exception(f"보유 종목 조회 실패: {str(e)}")
    
    def _create_portfolio(self, request: PortfolioRequest) -> Dict:
        """새 포트폴리오 생성"""
        # 실제 구현에서는 portfolio_manager.py 사용
        return {
            "portfolio_id": f"portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "etf_codes": request.etf_codes,
            "target_allocation": request.target_allocation,
            "investment_amount": request.investment_amount,
            "created_at": datetime.now().isoformat()
        }
    
    def _get_portfolio_performance(self, start_date: str = None, end_date: str = None) -> Dict:
        """포트폴리오 성과 분석"""
        # 실제 구현에서는 성과 분석 로직 사용
        return {
            "period": f"{start_date} ~ {end_date}" if start_date and end_date else "전체",
            "total_return": 12.5,
            "annualized_return": 8.3,
            "volatility": 15.2,
            "sharpe_ratio": 0.55,
            "max_drawdown": -8.7,
            "benchmark_comparison": {
                "portfolio_return": 12.5,
                "kospi_return": 9.8,
                "sp500_return": 15.2
            }
        }
    
    def _check_rebalancing_needed(self) -> Dict:
        """리밸런싱 필요성 체크"""
        return {
            "rebalancing_needed": True,
            "max_deviation": 7.2,
            "threshold": 5.0,
            "deviations": [
                {"etf_code": "KODEX 200", "target": 30.0, "current": 37.2, "deviation": 7.2},
                {"etf_code": "TIGER 미국S&P500", "target": 40.0, "current": 35.8, "deviation": -4.2},
                {"etf_code": "KODEX 국고채10년", "target": 30.0, "current": 27.0, "deviation": -3.0}
            ]
        }
    
    def _suggest_rebalancing(self, threshold: float) -> Dict:
        """리밸런싱 제안"""
        return {
            "threshold": threshold,
            "total_trade_amount": 1250000,
            "estimated_cost": 2500,
            "trades": [
                {
                    "etf_code": "KODEX 200",
                    "action": "sell",
                    "amount": 720000,
                    "reason": "목표 비중 초과"
                },
                {
                    "etf_code": "TIGER 미국S&P500",
                    "action": "buy",
                    "amount": 420000,
                    "reason": "목표 비중 미달"
                },
                {
                    "etf_code": "KODEX 국고채10년",
                    "action": "buy",
                    "amount": 300000,
                    "reason": "목표 비중 미달"
                }
            ]
        }
    
    def _get_etf_list(self, category: str = None, min_aum: float = None) -> List[Dict]:
        """ETF 목록 조회"""
        # 샘플 데이터
        etf_list = [
            {
                "etf_code": "KODEX 200",
                "etf_name": "KODEX 코스피200",
                "category": "국내주식",
                "aum": 15000000000,
                "expense_ratio": 0.17,
                "tracking_error": 0.12
            },
            {
                "etf_code": "TIGER 미국S&P500",
                "etf_name": "TIGER 미국S&P500",
                "category": "해외주식",
                "aum": 8000000000,
                "expense_ratio": 0.08,
                "tracking_error": 0.15
            }
        ]
        
        # 필터링
        if category:
            etf_list = [etf for etf in etf_list if etf['category'] == category]
        
        if min_aum:
            etf_list = [etf for etf in etf_list if etf['aum'] >= min_aum]
        
        return etf_list
    
    def _get_etf_info(self, etf_code: str) -> Optional[Dict]:
        """특정 ETF 정보 조회"""
        # 실제 구현에서는 데이터베이스에서 조회
        etf_info = {
            "etf_code": etf_code,
            "etf_name": f"{etf_code} ETF",
            "category": "주식",
            "inception_date": "2020-01-01",
            "aum": 5000000000,
            "expense_ratio": 0.15,
            "dividend_yield": 2.5,
            "tracking_error": 0.10,
            "holdings_count": 200,
            "replication_method": "완전복제"
        }
        
        return etf_info
    
    def _get_etf_price_history(self, etf_code: str, days: int) -> List[Dict]:
        """ETF 가격 이력 조회"""
        # 샘플 데이터
        dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
        prices = np.random.uniform(95, 105, days)
        
        price_history = []
        for date, price in zip(dates, prices):
            price_history.append({
                "date": date.strftime('%Y-%m-%d'),
                "price": round(price, 2),
                "volume": np.random.randint(100000, 1000000)
            })
        
        return price_history
    
    def _run_backtest(self, request: BacktestRequest) -> Dict:
        """백테스팅 실행"""
        # 실제 구현에서는 backtesting_engine.py 사용
        return {
            "strategy": request.strategy,
            "period": f"{request.start_date} ~ {request.end_date}",
            "initial_amount": request.initial_amount,
            "final_amount": request.initial_amount * 1.125,
            "total_return": 12.5,
            "annualized_return": 8.3,
            "volatility": 15.2,
            "sharpe_ratio": 0.55,
            "max_drawdown": -8.7,
            "win_rate": 65.0,
            "trades_count": 24
        }
    
    def _get_user_notifications(self) -> List[Dict]:
        """사용자 알림 목록 조회"""
        return [
            {
                "id": 1,
                "type": "rebalancing",
                "title": "리밸런싱 필요",
                "message": "포트폴리오가 목표 배분에서 5% 이상 벗어났습니다.",
                "is_read": False,
                "created_at": "2025-06-22T10:30:00"
            },
            {
                "id": 2,
                "type": "dividend",
                "title": "배당금 지급",
                "message": "KODEX 200에서 15,000원의 배당금이 지급되었습니다.",
                "is_read": True,
                "created_at": "2025-06-20T14:20:00"
            }
        ]
    
    def _mark_notification_read(self, notification_id: int):
        """알림 읽음 처리"""
        # 실제 구현에서는 데이터베이스 업데이트
        pass
    
    def _generate_report(self, report_type: str, period: str) -> Dict:
        """리포트 생성"""
        return {
            "report_type": report_type,
            "period": period,
            "generated_at": datetime.now().isoformat(),
            "summary": "리포트가 성공적으로 생성되었습니다.",
            "file_path": f"reports/{report_type}_{period}_{datetime.now().strftime('%Y%m%d')}.pdf"
        }
    
    def run(self, host: str = "127.0.0.1", port: int = 8000, reload: bool = False):
        """API 서버 실행"""
        uvicorn.run(
            "api_server:api_server.app",
            host=host,
            port=port,
            reload=reload,
            log_level="info"
        )

# 글로벌 API 서버 인스턴스
api_server = ETFAPIServer()

# 사용 예시
if __name__ == "__main__":
    print("ETF 투자 관리 API 서버 시작...")
    print("API 문서: http://127.0.0.1:8000/docs")
    print("Redoc 문서: http://127.0.0.1:8000/redoc")
    
    # 개발 모드로 서버 실행
    api_server.run(host="127.0.0.1", port=8000, reload=True)