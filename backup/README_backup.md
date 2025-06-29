# 🚀 ETF 포트폴리오 매니저

> **직장인을 위한 장기 ETF 자산배분 관리 시스템**  
> 퇴근 후나 주말에 간편하게 사용할 수 있는 5-10년 장기투자 목표 종합 솔루션

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Beta-yellow.svg)]()

## 📋 목차

- [✨ 주요 기능](#-주요-기능)
- [🏗️ 시스템 구조](#️-시스템-구조)
- [⚡ 빠른 시작](#-빠른-시작)
- [📊 투자 전략](#-투자-전략)
- [🎯 사용법](#-사용법)
- [🔧 설정](#-설정)
- [📈 백테스팅](#-백테스팅)
- [🤝 기여하기](#-기여하기)
- [📞 지원](#-지원)

## ✨ 주요 기능

### 🎯 자산배분 전략 엔진
- **5가지 검증된 전략**: 코어-새틀라이트, 글로벌 분산, 생애주기, 리스크 패리티, 커스텀
- **연령별 맞춤**: 20대 공격적 성장부터 50대+ 안정적 수익까지
- **위험 성향 반영**: 보수적/균형/공격적 투자 성향별 포트폴리오

### 📊 한국 ETF 유니버스
- **체계적 분류**: 국내/해외 주식, 채권, 대안투자, 테마별 구성
- **90개+ ETF 데이터**: 운용보수, 추적오차, 유동성 등 상세 정보
- **스마트 필터링**: 비용/성과/유동성 기준 최적 ETF 선택

### 🔄 자동 리밸런싱
- **3단계 시나리오**: 신규자금 활용 → 부분 조정 → 완전 리밸런싱
- **비용 최적화**: 거래 수수료 최소화 알고리즘
- **세금 효율성**: 손실 ETF 우선 매도로 세금 절약

### 📈 실시간 모니터링
- **포트폴리오 대시보드**: 총 자산, 손익, 자산배분 현황 한눈에
- **성과 분석**: 벤치마크 대비, 샤프 비율, 최대낙폭 등 전문 지표
- **알림 시스템**: 리밸런싱 필요시, 급락 기회, 배당 지급일 알림

## 🏗️ 시스템 구조

```
etf-portfolio-manager/
├── 🚀 main.py                    # 메인 실행 파일
├── ⚙️ config.yaml                # 시스템 설정
├── 
├── 💼 core/                      # 핵심 엔진
│   ├── portfolio_manager.py      # 포트폴리오 관리
│   ├── risk_manager.py           # 리스크 관리
│   ├── backtesting_engine.py     # 백테스팅
│   └── notification_system.py    # 알림 시스템
├── 
├── 📊 data/                      # 데이터 관리
│   ├── etf_universe.py           # ETF 유니버스
│   ├── market_data_collector.py  # 시장 데이터 수집
│   └── database_manager.py       # 데이터베이스 관리
├── 
├── 🧪 strategies/                # 투자 전략
│   ├── core_satellite.py         # 코어-새틀라이트
│   ├── global_diversified.py     # 글로벌 분산
│   ├── lifecycle_strategy.py     # 생애주기
│   ├── risk_parity.py            # 리스크 패리티
│   └── custom_strategy.py        # 커스텀 전략
├── 
├── 🌐 web/                       # 웹 인터페이스
│   ├── dashboard.py              # 대시보드
│   └── api_server.py             # API 서버
└── 
└── 🔧 utils/                     # 유틸리티
    ├── cost_calculator.py        # 비용 계산
    ├── performance_metrics.py    # 성과 지표
    └── tax_optimizer.py          # 세금 최적화
```

## ⚡ 빠른 시작

### 1. 환경 설정

```bash
# 저장소 클론
git clone https://github.com/your-username/etf-portfolio-manager.git
cd etf-portfolio-manager

# 가상환경 생성 (Windows)
python -m venv venv
venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt
```

### 2. 설정 파일 수정

```yaml
# config.yaml
portfolio:
  default_strategy: "lifecycle"  # 기본 전략
  risk_tolerance: "moderate"     # 위험 성향
  rebalancing_threshold: 5.0     # 리밸런싱 임계치 (%)

user:
  age: 35                        # 나이
  monthly_investment: 1000000    # 월 투자금액 (원)
  investment_goal: "retirement"  # 투자 목표
```

### 3. 실행

```bash
# 간편 실행 (Windows)
run.bat

# 또는 직접 실행
python main.py

# 웹 대시보드 실행
python -m streamlit run web/dashboard.py
```

### 4. 첫 포트폴리오 생성

```python
from core.portfolio_manager import PortfolioManager
from strategies.lifecycle_strategy import LifecycleStrategy

# 생애주기 전략으로 포트폴리오 생성
strategy = LifecycleStrategy(age=35, risk_tolerance='moderate')
portfolio = strategy.generate_portfolio(investment_amount=10000000)

print("추천 포트폴리오:")
for etf_code, weight in portfolio.items():
    print(f"{etf_code}: {weight:.1f}%")
```

## 📊 투자 전략

### 🎯 1. 코어-새틀라이트 전략
```
코어 (80%): 안정적 광범위 분산
├── KODEX 200 (30%)
├── TIGER 미국S&P500 (30%)
└── KODEX 국고채10년 (20%)

새틀라이트 (20%): 성장 및 테마
├── KODEX 코스닥150 (10%)
└── KODEX 2차전지산업 (10%)
```

### 🌍 2. 글로벌 분산 전략
```
국내 (25%) + 미국 (30%) + 선진국 (25%) + 채권 (20%)
- 지역별 리스크 분산
- 환율 다변화 효과
```

### 📅 3. 생애주기 전략
```
20대: 주식 90% (공격적 성장)
30대: 주식 80% (균형 성장)  
40대: 주식 70% (안정 성장)
50대+: 주식 60% (자본 보전)
```

### ⚖️ 4. 리스크 패리티
```
각 자산이 동일한 위험 기여
- 변동성 기반 가중치 조정
- 고급 분산투자 효과
```

## 🎯 사용법

### 포트폴리오 생성
```python
# 1. 전략 선택
from strategies.core_satellite import CoreSatelliteStrategy
strategy = CoreSatelliteStrategy(core_ratio=0.8, risk_level='moderate')

# 2. 포트폴리오 생성
portfolio = strategy.generate_portfolio(investment_amount=5000000)

# 3. 포트폴리오 평가
evaluation = strategy.evaluate_current_portfolio(portfolio)
print(f"전략 일치도: {evaluation['strategy_alignment']:.1f}점")
```

### 리밸런싱
```python
from core.portfolio_manager import PortfolioManager

pm = PortfolioManager()
current_weights = {'069500': 25, '139660': 35, '114260': 40}
target_weights = {'069500': 30, '139660': 40, '114260': 30}

# 리밸런싱 계획 수립
rebalancing_plan = pm.generate_rebalancing_plan(current_weights, target_weights)

for etf_code, plan in rebalancing_plan.items():
    print(f"{etf_code}: {plan['action']} {abs(plan['deviation']):.1f}%")
```

### 성과 분석
```python
from utils.performance_metrics import PerformanceMetrics

pm = PerformanceMetrics()
returns = get_portfolio_returns()  # 수익률 데이터

# 종합 성과 지표
metrics = pm.calculate_comprehensive_metrics(returns)
print(f"연수익률: {metrics['annualized_return']:.2%}")
print(f"샤프비율: {metrics['sharpe_ratio']:.3f}")
print(f"최대낙폭: {metrics['max_drawdown']:.2%}")
```

## 🔧 설정

### 주요 설정 옵션

```yaml
# config.yaml
data_sources:
  price_data: "krx_api"          # 가격 데이터 소스
  update_frequency: "daily"      # 업데이트 주기
  
portfolio:
  min_etf_count: 3              # 최소 ETF 개수
  max_etf_count: 15             # 최대 ETF 개수
  min_position_size: 2.0        # 최소 종목 비중 (%)
  max_position_size: 40.0       # 최대 종목 비중 (%)
  
notifications:
  email_enabled: true           # 이메일 알림
  rebalancing_threshold: 5.0    # 리밸런싱 알림 임계치
  market_crash_threshold: -10.0 # 급락 알림 임계치
```

### 환경 변수
```bash
# .env 파일 생성
EMAIL_SMTP_SERVER=smtp.gmail.com
EMAIL_USERNAME=your-email@gmail.com
EMAIL_PASSWORD=your-app-password
DATABASE_URL=sqlite:///data/portfolio.db
```

## 📈 백테스팅

### 과거 성과 검증
```python
from core.backtesting_engine import BacktestingEngine

backtester = BacktestingEngine()

# 2020-2024년 백테스팅
results = backtester.run_backtest(
    strategy='core_satellite',
    start_date='2020-01-01',
    end_date='2024-12-31',
    initial_investment=10000000,
    monthly_contribution=1000000
)

print(f"연평균 수익률: {results['annualized_return']:.2%}")
print(f"최대 낙폭: {results['max_drawdown']:.2%}")
print(f"샤프 비율: {results['sharpe_ratio']:.3f}")
```

### 시나리오 분석
```python
# 경제 위기 시나리오
crisis_results = backtester.stress_test(
    portfolio=my_portfolio,
    scenarios=['2008_crisis', '2020_covid', 'inflation_spike']
)

for scenario, result in crisis_results.items():
    print(f"{scenario}: {result['max_loss']:.2%} 최대손실")
```

## 🔔 알림 설정

### 이메일 알림
- **리밸런싱 알림**: 목표 비중에서 5% 이상 이탈시
- **급락 기회**: 시장 10% 이상 하락시 추가 매수 기회
- **월간 리포트**: 매월 말 포트폴리오 성과 요약
- **배당 알림**: ETF 배당 지급일 사전 알림

### 모바일 푸시 (향후 지원)
- 중요 알림을 모바일로 즉시 전송
- 리밸런싱 체크리스트 제공

## 🛠️ 고급 기능

### 세금 최적화
```python
from utils.tax_optimizer import TaxOptimizer

optimizer = TaxOptimizer()

# 세금 효율적 리밸런싱
tax_plan = optimizer.optimize_rebalancing(
    current_portfolio=current_weights,
    target_portfolio=target_weights,
    tax_loss_harvesting=True
)
```

### 비용 분석
```python
from utils.cost_calculator import CostCalculator

calc = CostCalculator()

# 거래 비용 계산
trading_cost = calc.calculate_trading_cost(
    trade_amount=1000000,
    etf_type='domestic_etf'
)

print(f"거래 비용: {trading_cost['total_cost']:,.0f}원")
print(f"비용 비율: {trading_cost['cost_percentage']:.3f}%")
```

## 📚 학습 자료

### 추천 도서
- 📖 **자산배분 투자**: 개념과 실전 적용
- 📖 **ETF 투자 가이드**: 한국 시장 중심
- 📖 **행동재무학**: 감정적 투자 실수 방지

### 온라인 자료
- 📺 **유튜브 채널**: ETF 투자 기초부터 고급까지
- 📊 **블로그**: 월간 시장 분석 및 전략 업데이트
- 💬 **커뮤니티**: 투자자 경험 공유

## 🐛 알려진 이슈

### 현재 제한사항
1. **실시간 데이터**: 일부 지연 발생 가능
2. **해외 ETF**: 환율 정보 수동 업데이트 필요
3. **백테스팅**: 2020년 이전 데이터 제한적

### 향후 개선 계획
- [ ] 실시간 API 연동 강화
- [ ] 모바일 앱 개발
- [ ] AI 기반 시장 예측 모델
- [ ] 소셜 트레이딩 기능

## 🤝 기여하기

### 개발 참여
```bash
# 개발용 의존성 설치
pip install -r requirements-dev.txt

# 테스트 실행
python -m pytest tests/

# 코드 스타일 검사
flake8 .
```

### 버그 리포트
- 🐛 **이슈 등록**: GitHub Issues에 상세한 재현 방법과 함께
- 📧 **이메일**: etf-manager@example.com
- 💬 **디스코드**: ETF 투자자 커뮤니티

### 기능 제안
새로운 투자 전략이나 기능 아이디어가 있으시면 언제든 제안해주세요!

## 📞 지원

### 기술 지원
- 📧 **이메일**: support@etf-manager.com
- 📖 **위키**: 상세한 사용법과 FAQ
- 💬 **챗봇**: 24/7 자동 문의 응답

### 커뮤니티
- 🗣️ **포럼**: 투자 경험 공유
- 📱 **텔레그램**: 실시간 시장 정보
- 🎯 **스터디**: 월 1회 온라인 투자 스터디

## ⚠️ 면책조항

본 소프트웨어는 교육 및 정보 제공 목적으로 제작되었습니다.

- 📊 **투자 권유 아님**: 개별 투자 결정은 본인의 책임
- ⚖️ **정확성 보장 불가**: 데이터 오류 가능성 존재  
- 💼 **전문가 상담**: 중요한 투자 결정 전 전문가와 상담 권장

## 📄 라이선스

MIT License - 자세한 내용은 [LICENSE](LICENSE) 파일 참조

---

### 🌟 이 프로젝트가 도움이 되셨나요?

⭐ **Star**를 눌러 프로젝트를 응원해주세요!  
🔔 **Watch**로 업데이트 소식을 받아보세요!  
🍴 **Fork**해서 나만의 버전을 만들어보세요!

---

**Made with ❤️ for Korean ETF Investors**

> 💡 **직장인의, 직장인에 의한, 직장인을 위한 ETF 투자 솔루션**