# ==========================================
# report_generator.py - 자동 리포트 생성기
# ==========================================

import pandas as pd
import numpy as np
import sqlite3
import json
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging
import os

# 리포트 생성 라이브러리 (선택적)
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    plt.style.use('seaborn-v0_8')
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("⚠️ matplotlib 없음 - 차트 없는 리포트만 생성")

try:
    from jinja2 import Template
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    print("⚠️ jinja2 없음 - 기본 HTML 템플릿 사용")

# 프로젝트 모듈 import
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from portfolio_manager import PortfolioManager
from update_manager import ETFUpdateManager
from data.etf_analyzer import ETFAnalyzer

@dataclass
class ReportData:
    """리포트 데이터"""
    period: str
    start_date: str
    end_date: str
    portfolio_summary: dict
    performance_metrics: dict
    holdings_analysis: list
    market_overview: dict
    rebalancing_recommendation: dict
    charts: dict
    insights: list

class ReportGenerator:
    """자동 리포트 생성기"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        self.portfolio_manager = PortfolioManager(db_path)
        self.update_manager = ETFUpdateManager(db_path)
        self.analyzer = ETFAnalyzer(db_path)
        
        self.logger = logging.getLogger(__name__)
        
        # 리포트 설정
        self.output_dir = "reports"
        self.template_dir = "templates"
        
        # 디렉토리 생성
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.template_dir, exist_ok=True)
        
        self.logger.info("📄 리포트 생성기 초기화 완료")
    
    def collect_report_data(self, user_id: str, period: str = "monthly") -> ReportData:
        """리포트 데이터 수집"""
        try:
            # 기간 설정
            end_date = datetime.now()
            if period == "monthly":
                start_date = end_date - timedelta(days=30)
            elif period == "quarterly":
                start_date = end_date - timedelta(days=90)
            elif period == "yearly":
                start_date = end_date - timedelta(days=365)
            else:
                start_date = end_date - timedelta(days=30)
            
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            self.logger.info(f"📊 리포트 데이터 수집: {user_id} ({start_date_str} ~ {end_date_str})")
            
            # 1. 포트폴리오 요약
            portfolio_summary = self.portfolio_manager.get_portfolio_summary(user_id)
            if portfolio_summary:
                portfolio_dict = {
                    'total_value': portfolio_summary.total_value,
                    'total_return': portfolio_summary.total_return,
                    'total_return_pct': portfolio_summary.total_return_pct,
                    'daily_return': portfolio_summary.daily_return,
                    'volatility': portfolio_summary.volatility,
                    'sharpe_ratio': portfolio_summary.sharpe_ratio,
                    'max_drawdown': portfolio_summary.max_drawdown,
                    'num_holdings': portfolio_summary.num_holdings,
                    'last_rebalance': portfolio_summary.last_rebalance,
                    'next_rebalance': portfolio_summary.next_rebalance
                }
            else:
                portfolio_dict = {}
            
            # 2. 성과 지표
            performance_data = self.portfolio_manager.get_portfolio_performance(user_id, 
                                                                               days=(end_date - start_date).days)
            if performance_data is not None:
                performance_metrics = {
                    'period_return': performance_data['cumulative_return'].iloc[-1],
                    'best_day': performance_data['daily_return'].max(),
                    'worst_day': performance_data['daily_return'].min(),
                    'positive_days': len(performance_data[performance_data['daily_return'] > 0]),
                    'total_days': len(performance_data),
                    'avg_daily_return': performance_data['daily_return'].mean(),
                    'volatility': performance_data['daily_return'].std() * np.sqrt(252) * 100
                }
            else:
                performance_metrics = {}
            
            # 3. 보유 종목 분석
            holdings_analysis = self._analyze_holdings(user_id)
            
            # 4. 시장 개요
            market_overview = self.analyzer.generate_universe_dashboard()
            
            # 5. 리밸런싱 추천
            rebalancing_rec = self.portfolio_manager.get_rebalance_recommendation(user_id)
            if rebalancing_rec:
                rebalancing_dict = {
                    'rebalance_needed': rebalancing_rec.rebalance_needed,
                    'total_deviation': rebalancing_rec.total_deviation,
                    'max_deviation': rebalancing_rec.max_deviation,
                    'estimated_cost': rebalancing_rec.estimated_cost,
                    'rebalance_type': rebalancing_rec.rebalance_type,
                    'recommendations': [
                        {
                            'etf_code': rec.etf_code,
                            'etf_name': rec.etf_name,
                            'target_weight': rec.target_weight,
                            'current_weight': rec.current_weight,
                            'deviation': rec.deviation,
                            'rebalance_amount': rec.rebalance_amount
                        }
                        for rec in rebalancing_rec.recommendations
                    ]
                }
            else:
                rebalancing_dict = {}
            
            # 6. 차트 생성
            charts = self._generate_charts(user_id, performance_data)
            
            # 7. 인사이트 생성
            insights = self._generate_insights(portfolio_dict, performance_metrics, 
                                             market_overview, rebalancing_dict)
            
            return ReportData(
                period=period,
                start_date=start_date_str,
                end_date=end_date_str,
                portfolio_summary=portfolio_dict,
                performance_metrics=performance_metrics,
                holdings_analysis=holdings_analysis,
                market_overview=market_overview,
                rebalancing_recommendation=rebalancing_dict,
                charts=charts,
                insights=insights
            )
            
        except Exception as e:
            self.logger.error(f"❌ 리포트 데이터 수집 실패: {e}")
            return None
    
    def _analyze_holdings(self, user_id: str) -> List[Dict]:
        """보유 종목 분석"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 포트폴리오 보유 종목 조회
            query = '''
                SELECT p.etf_code, e.name, pos.shares, pos.avg_price, 
                       pos.current_price, pos.current_value, pos.target_weight
                FROM portfolios port
                JOIN positions pos ON port.id = pos.portfolio_id
                JOIN etf_info e ON pos.etf_code = e.code
                LEFT JOIN etf_performance p ON pos.etf_code = p.code
                WHERE port.user_id = ? AND port.is_active = 1
                ORDER BY pos.current_value DESC
            '''
            
            df = pd.read_sql_query(query, conn, params=(user_id,))
            conn.close()
            
            holdings = []
            for _, row in df.iterrows():
                current_return = 0
                if row['avg_price'] and row['avg_price'] > 0:
                    current_return = (row['current_price'] / row['avg_price'] - 1) * 100
                
                holdings.append({
                    'etf_code': row['etf_code'],
                    'etf_name': row['name'],
                    'shares': round(row['shares'], 2),
                    'avg_price': round(row['avg_price'], 0),
                    'current_price': round(row['current_price'], 0),
                    'current_value': round(row['current_value'], 0),
                    'target_weight': round(row['target_weight'] * 100, 1),
                    'current_return': round(current_return, 2)
                })
            
            return holdings
            
        except Exception as e:
            self.logger.error(f"❌ 보유 종목 분석 실패: {e}")
            return []
    
    def _generate_charts(self, user_id: str, performance_data: pd.DataFrame) -> Dict:
        """차트 생성"""
        charts = {}
        
        if not MATPLOTLIB_AVAILABLE or performance_data is None or performance_data.empty:
            return charts
        
        try:
            # 1. 포트폴리오 가치 추이 차트
            plt.figure(figsize=(12, 6))
            plt.plot(performance_data.index, performance_data['portfolio_value'], 
                    linewidth=2, color='#1f77b4')
            plt.title('포트폴리오 가치 추이', fontsize=16, fontweight='bold')
            plt.xlabel('날짜')
            plt.ylabel('포트폴리오 가치 (원)')
            plt.grid(True, alpha=0.3)
            plt.ticklabel_format(style='plain', axis='y')
            
            # 차트를 base64로 인코딩
            chart_path = f"{self.output_dir}/portfolio_value_{user_id}.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            with open(chart_path, 'rb') as f:
                chart_b64 = base64.b64encode(f.read()).decode()
            charts['portfolio_value'] = chart_b64
            
            # 2. 일일 수익률 분포
            plt.figure(figsize=(10, 6))
            plt.hist(performance_data['daily_return'], bins=50, alpha=0.7, 
                    color='#2ca02c', edgecolor='black')
            plt.axvline(performance_data['daily_return'].mean(), color='red', 
                       linestyle='--', label=f'평균: {performance_data["daily_return"].mean():.3f}%')
            plt.title('일일 수익률 분포', fontsize=16, fontweight='bold')
            plt.xlabel('일일 수익률 (%)')
            plt.ylabel('빈도')
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            chart_path = f"{self.output_dir}/return_distribution_{user_id}.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            with open(chart_path, 'rb') as f:
                chart_b64 = base64.b64encode(f.read()).decode()
            charts['return_distribution'] = chart_b64
            
            # 3. 누적 수익률 차트
            plt.figure(figsize=(12, 6))
            plt.plot(performance_data.index, performance_data['cumulative_return'], 
                    linewidth=2, color='#ff7f0e')
            plt.title('누적 수익률 추이', fontsize=16, fontweight='bold')
            plt.xlabel('날짜')
            plt.ylabel('누적 수익률 (%)')
            plt.grid(True, alpha=0.3)
            plt.axhline(y=0, color='black', linestyle='-', alpha=0.5)
            
            chart_path = f"{self.output_dir}/cumulative_return_{user_id}.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            with open(chart_path, 'rb') as f:
                chart_b64 = base64.b64encode(f.read()).decode()
            charts['cumulative_return'] = chart_b64
            
        except Exception as e:
            self.logger.error(f"❌ 차트 생성 실패: {e}")
        
        return charts
    
    def _generate_insights(self, portfolio_summary: dict, performance_metrics: dict,
                          market_overview: dict, rebalancing_rec: dict) -> List[str]:
        """인사이트 생성"""
        insights = []
        
        try:
            # 포트폴리오 성과 인사이트
            if portfolio_summary:
                total_return = portfolio_summary.get('total_return_pct', 0)
                if total_return > 10:
                    insights.append(f"🎉 뛰어난 성과! 포트폴리오가 {total_return:.1f}%의 높은 수익률을 기록했습니다.")
                elif total_return > 5:
                    insights.append(f"👍 양호한 성과! 포트폴리오가 {total_return:.1f}%의 수익률을 달성했습니다.")
                elif total_return > 0:
                    insights.append(f"📈 긍정적 성과! 포트폴리오가 {total_return:.1f}%의 수익을 기록했습니다.")
                else:
                    insights.append(f"📉 단기 변동성! 포트폴리오가 {total_return:.1f}%의 일시적 손실을 보이고 있습니다.")
                
                # 샤프 비율 인사이트
                sharpe = portfolio_summary.get('sharpe_ratio', 0)
                if sharpe > 1.5:
                    insights.append(f"⭐ 우수한 위험조정수익률! 샤프비율 {sharpe:.2f}로 효율적인 투자를 하고 있습니다.")
                elif sharpe > 1.0:
                    insights.append(f"✅ 양호한 위험조정수익률! 샤프비율 {sharpe:.2f}로 적절한 리스크 관리가 이루어지고 있습니다.")
                elif sharpe < 0.5:
                    insights.append(f"⚠️ 위험조정수익률 개선 필요! 샤프비율 {sharpe:.2f}로 리스크 대비 수익이 낮습니다.")
            
            # 성과 지표 인사이트
            if performance_metrics:
                positive_days = performance_metrics.get('positive_days', 0)
                total_days = performance_metrics.get('total_days', 1)
                win_rate = (positive_days / total_days) * 100
                
                if win_rate > 60:
                    insights.append(f"📊 높은 승률! {win_rate:.1f}%의 날에서 수익을 기록했습니다.")
                elif win_rate > 50:
                    insights.append(f"📊 균형잡힌 성과! {win_rate:.1f}%의 승률을 보이고 있습니다.")
                else:
                    insights.append(f"📊 변동성 주의! {win_rate:.1f}%의 승률로 시장 변동성이 큽니다.")
            
            # 리밸런싱 인사이트
            if rebalancing_rec:
                if rebalancing_rec.get('rebalance_needed'):
                    max_dev = rebalancing_rec.get('max_deviation', 0)
                    insights.append(f"⚖️ 리밸런싱 권장! 최대 {max_dev:.1f}% 편차로 자산배분 조정이 필요합니다.")
                else:
                    insights.append(f"✅ 자산배분 균형! 현재 포트폴리오 구성이 목표에 잘 맞춰져 있습니다.")
            
            # 시장 상황 인사이트
            if market_overview and 'total_stats' in market_overview:
                stats = market_overview['total_stats']
                avg_expense = stats.get('avg_expense_ratio', 0)
                if avg_expense < 0.3:
                    insights.append(f"💰 저비용 투자! 평균 운용보수 {avg_expense:.2f}%로 비용 효율적인 투자를 하고 있습니다.")
            
            # 일반적인 투자 조언
            insights.append("📚 장기투자 관점을 유지하고 정기적인 리밸런싱을 통해 목표 자산배분을 지켜주세요.")
            insights.append("🎯 시장 변동성에 일희일비하지 말고 투자 목표와 계획을 지속적으로 점검해주세요.")
            
        except Exception as e:
            self.logger.error(f"❌ 인사이트 생성 실패: {e}")
            insights.append("📊 투자 성과를 정기적으로 모니터링하고 있습니다.")
        
        return insights
    
    def generate_html_report(self, report_data: ReportData, user_id: str) -> str:
        """HTML 리포트 생성"""
        try:
            self.logger.info(f"📄 HTML 리포트 생성: {user_id}")
            
            # HTML 템플릿
            html_template = """
            <!DOCTYPE html>
            <html lang="ko">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>ETF 포트폴리오 {{ period_korean }} 리포트</title>
                <style>
                    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
                    .container { max-width: 1200px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                    .header { text-align: center; margin-bottom: 40px; border-bottom: 2px solid #eee; padding-bottom: 20px; }
                    .title { color: #2c3e50; font-size: 28px; margin-bottom: 10px; }
                    .subtitle { color: #7f8c8d; font-size: 16px; }
                    .section { margin-bottom: 40px; }
                    .section-title { color: #34495e; font-size: 20px; margin-bottom: 20px; border-left: 4px solid #3498db; padding-left: 15px; }
                    .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
                    .metric-card { background: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #3498db; }
                    .metric-label { color: #6c757d; font-size: 14px; margin-bottom: 5px; }
                    .metric-value { color: #2c3e50; font-size: 24px; font-weight: bold; }
                    .positive { color: #27ae60; }
                    .negative { color: #e74c3c; }
                    .table { width: 100%; border-collapse: collapse; margin-bottom: 20px; }
                    .table th, .table td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
                    .table th { background-color: #f8f9fa; font-weight: 600; color: #495057; }
                    .chart-container { text-align: center; margin: 20px 0; }
                    .chart-img { max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
                    .insights { background: #e8f4f8; padding: 20px; border-radius: 8px; border-left: 4px solid #17a2b8; }
                    .insight-item { margin-bottom: 10px; }
                    .footer { text-align: center; margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; color: #6c757d; font-size: 14px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <!-- 헤더 -->
                    <div class="header">
                        <h1 class="title">🚀 ETF 포트폴리오 {{ period_korean }} 리포트</h1>
                        <p class="subtitle">{{ start_date }} ~ {{ end_date }}</p>
                    </div>
                    
                    <!-- 포트폴리오 요약 -->
                    <div class="section">
                        <h2 class="section-title">📊 포트폴리오 요약</h2>
                        <div class="metric-grid">
                            <div class="metric-card">
                                <div class="metric-label">총 자산 가치</div>
                                <div class="metric-value">{{ "{:,.0f}".format(portfolio_summary.total_value) }}원</div>
                            </div>
                            <div class="metric-card">
                                <div class="metric-label">총 수익률</div>
                                <div class="metric-value {{ 'positive' if portfolio_summary.total_return_pct > 0 else 'negative' }}">
                                    {{ "{:+.2f}".format(portfolio_summary.total_return_pct) }}%
                                </div>
                            </div>
                            <div class="metric-card">
                                <div class="metric-label">변동성</div>
                                <div class="metric-value">{{ "{:.2f}".format(portfolio_summary.volatility) }}%</div>
                            </div>
                            <div class="metric-card">
                                <div class="metric-label">샤프 비율</div>
                                <div class="metric-value">{{ "{:.2f}".format(portfolio_summary.sharpe_ratio) }}</div>
                            </div>
                        </div>
                    </div>
                    
                    <!-- 차트 섹션 -->
                    {% if charts %}
                    <div class="section">
                        <h2 class="section-title">📈 성과 차트</h2>
                        {% if charts.portfolio_value %}
                        <div class="chart-container">
                            <img src="data:image/png;base64,{{ charts.portfolio_value }}" alt="포트폴리오 가치 추이" class="chart-img">
                        </div>
                        {% endif %}
                        {% if charts.cumulative_return %}
                        <div class="chart-container">
                            <img src="data:image/png;base64,{{ charts.cumulative_return }}" alt="누적 수익률 추이" class="chart-img">
                        </div>
                        {% endif %}
                    </div>
                    {% endif %}
                    
                    <!-- 보유 종목 -->
                    {% if holdings_analysis %}
                    <div class="section">
                        <h2 class="section-title">🎯 보유 종목 분석</h2>
                        <table class="table">
                            <thead>
                                <tr>
                                    <th>ETF 명</th>
                                    <th>코드</th>
                                    <th>보유 수량</th>
                                    <th>현재가</th>
                                    <th>평가 금액</th>
                                    <th>목표 비중</th>
                                    <th>수익률</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for holding in holdings_analysis %}
                                <tr>
                                    <td>{{ holding.etf_name }}</td>
                                    <td>{{ holding.etf_code }}</td>
                                    <td>{{ "{:.2f}".format(holding.shares) }}</td>
                                    <td>{{ "{:,.0f}".format(holding.current_price) }}원</td>
                                    <td>{{ "{:,.0f}".format(holding.current_value) }}원</td>
                                    <td>{{ holding.target_weight }}%</td>
                                    <td class="{{ 'positive' if holding.current_return > 0 else 'negative' }}">
                                        {{ "{:+.2f}".format(holding.current_return) }}%
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                    {% endif %}
                    
                    <!-- 리밸런싱 추천 -->
                    {% if rebalancing_recommendation.rebalance_needed %}
                    <div class="section">
                        <h2 class="section-title">⚖️ 리밸런싱 권장사항</h2>
                        <p><strong>최대 편차:</strong> {{ "{:.2f}".format(rebalancing_recommendation.max_deviation) }}%</p>
                        <p><strong>예상 거래비용:</strong> {{ "{:,.0f}".format(rebalancing_recommendation.estimated_cost) }}원</p>
                        <table class="table">
                            <thead>
                                <tr>
                                    <th>ETF 명</th>
                                    <th>목표 비중</th>
                                    <th>현재 비중</th>
                                    <th>편차</th>
                                    <th>조정 금액</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for rec in rebalancing_recommendation.recommendations %}
                                <tr>
                                    <td>{{ rec.etf_name }}</td>
                                    <td>{{ "{:.1f}".format(rec.target_weight) }}%</td>
                                    <td>{{ "{:.1f}".format(rec.current_weight) }}%</td>
                                    <td>{{ "{:.1f}".format(rec.deviation) }}%</td>
                                    <td class="{{ 'positive' if rec.rebalance_amount > 0 else 'negative' }}">
                                        {{ "{:+,.0f}".format(rec.rebalance_amount) }}원
                                    </td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                    {% endif %}
                    
                    <!-- 인사이트 -->
                    {% if insights %}
                    <div class="section">
                        <h2 class="section-title">💡 투자 인사이트</h2>
                        <div class="insights">
                            {% for insight in insights %}
                            <div class="insight-item">{{ insight }}</div>
                            {% endfor %}
                        </div>
                    </div>
                    {% endif %}
                    
                    <!-- 푸터 -->
                    <div class="footer">
                        <p>본 리포트는 {{ datetime.now().strftime('%Y-%m-%d %H:%M') }}에 자동 생성되었습니다.</p>
                        <p>ETF 장기투자 관리 시스템 v1.0.0</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # 기간 한국어 변환
            period_korean = {
                'monthly': '월간',
                'quarterly': '분기',
                'yearly': '연간'
            }.get(report_data.period, '정기')
            
            # 템플릿 렌더링
            if JINJA2_AVAILABLE:
                template = Template(html_template)
                html_content = template.render(
                    period_korean=period_korean,
                    start_date=report_data.start_date,
                    end_date=report_data.end_date,
                    portfolio_summary=report_data.portfolio_summary,
                    holdings_analysis=report_data.holdings_analysis,
                    rebalancing_recommendation=report_data.rebalancing_recommendation,
                    charts=report_data.charts,
                    insights=report_data.insights,
                    datetime=datetime
                )
            else:
                # 기본 템플릿 (jinja2 없는 경우)
                html_content = html_template.replace("{{ period_korean }}", period_korean)
                html_content = html_content.replace("{{ start_date }}", report_data.start_date)
                html_content = html_content.replace("{{ end_date }}", report_data.end_date)
                # 간단한 치환만 수행 (복잡한 로직은 생략)
            
            # 파일 저장
            filename = f"portfolio_report_{user_id}_{report_data.period}_{datetime.now().strftime('%Y%m%d')}.html"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            self.logger.info(f"✅ HTML 리포트 생성 완료: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ HTML 리포트 생성 실패: {e}")
            return None
    
    def generate_text_report(self, report_data: ReportData, user_id: str) -> str:
        """텍스트 리포트 생성"""
        try:
            self.logger.info(f"📝 텍스트 리포트 생성: {user_id}")
            
            period_korean = {
                'monthly': '월간',
                'quarterly': '분기',
                'yearly': '연간'
            }.get(report_data.period, '정기')
            
            report_lines = []
            report_lines.append("=" * 60)
            report_lines.append(f"🚀 ETF 포트폴리오 {period_korean} 리포트")
            report_lines.append(f"📅 기간: {report_data.start_date} ~ {report_data.end_date}")
            report_lines.append("=" * 60)
            report_lines.append("")
            
            # 포트폴리오 요약
            if report_data.portfolio_summary:
                ps = report_data.portfolio_summary
                report_lines.append("📊 포트폴리오 요약")
                report_lines.append("-" * 30)
                report_lines.append(f"총 자산 가치: {ps.get('total_value', 0):,.0f}원")
                report_lines.append(f"총 수익률: {ps.get('total_return_pct', 0):+.2f}%")
                report_lines.append(f"변동성: {ps.get('volatility', 0):.2f}%")
                report_lines.append(f"샤프 비율: {ps.get('sharpe_ratio', 0):.2f}")
                report_lines.append(f"최대 낙폭: {ps.get('max_drawdown', 0):.2f}%")
                report_lines.append(f"보유 종목 수: {ps.get('num_holdings', 0)}개")
                report_lines.append("")
            
            # 보유 종목
            if report_data.holdings_analysis:
                report_lines.append("🎯 보유 종목 분석")
                report_lines.append("-" * 30)
                for holding in report_data.holdings_analysis:
                    report_lines.append(f"{holding['etf_name']} ({holding['etf_code']})")
                    report_lines.append(f"  평가금액: {holding['current_value']:,.0f}원")
                    report_lines.append(f"  목표비중: {holding['target_weight']}%")
                    report_lines.append(f"  수익률: {holding['current_return']:+.2f}%")
                    report_lines.append("")
            
            # 리밸런싱 추천
            if report_data.rebalancing_recommendation.get('rebalance_needed'):
                rr = report_data.rebalancing_recommendation
                report_lines.append("⚖️ 리밸런싱 권장사항")
                report_lines.append("-" * 30)
                report_lines.append(f"최대 편차: {rr.get('max_deviation', 0):.2f}%")
                report_lines.append(f"예상 거래비용: {rr.get('estimated_cost', 0):,.0f}원")
                report_lines.append("개별 조정 내역:")
                for rec in rr.get('recommendations', []):
                    if abs(rec['rebalance_amount']) > 10000:  # 1만원 이상만 표시
                        report_lines.append(f"  {rec['etf_name']}: {rec['rebalance_amount']:+,.0f}원")
                report_lines.append("")
            
            # 인사이트
            if report_data.insights:
                report_lines.append("💡 투자 인사이트")
                report_lines.append("-" * 30)
                for insight in report_data.insights:
                    report_lines.append(f"• {insight}")
                report_lines.append("")
            
            # 푸터
            report_lines.append("=" * 60)
            report_lines.append(f"리포트 생성 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append("ETF 장기투자 관리 시스템 v1.0.0")
            
            # 파일 저장
            filename = f"portfolio_report_{user_id}_{report_data.period}_{datetime.now().strftime('%Y%m%d')}.txt"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            
            self.logger.info(f"✅ 텍스트 리포트 생성 완료: {filepath}")
            return filepath
            
        except Exception as e:
            self.logger.error(f"❌ 텍스트 리포트 생성 실패: {e}")
            return None
    
    def generate_report(self, user_id: str, period: str = "monthly", 
                       format_type: str = "html") -> str:
        """종합 리포트 생성"""
        try:
            # 데이터 수집
            report_data = self.collect_report_data(user_id, period)
            
            if not report_data:
                self.logger.error(f"❌ 리포트 데이터 수집 실패: {user_id}")
                return None
            
            # 형식에 따른 리포트 생성
            if format_type.lower() == "html":
                return self.generate_html_report(report_data, user_id)
            elif format_type.lower() == "text":
                return self.generate_text_report(report_data, user_id)
            else:
                self.logger.error(f"❌ 지원하지 않는 형식: {format_type}")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ 리포트 생성 실패: {e}")
            return None
    
    def schedule_reports(self, user_list: List[str], 
                        schedule_config: Dict[str, Dict]) -> bool:
        """리포트 스케줄링"""
        try:
            self.logger.info(f"📅 리포트 스케줄링: {len(user_list)}명 사용자")
            
            for period, config in schedule_config.items():
                if config.get('enabled', False):
                    for user_id in user_list:
                        report_path = self.generate_report(
                            user_id=user_id,
                            period=period,
                            format_type=config.get('format', 'html')
                        )
                        
                        if report_path:
                            # 이메일 발송 등 추가 처리
                            if config.get('email_send', False):
                                self._send_email_report(user_id, report_path)
            
            self.logger.info(f"✅ 리포트 스케줄링 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 리포트 스케줄링 실패: {e}")
            return False
    
    def _send_email_report(self, user_id: str, report_path: str):
        """이메일 리포트 발송 (구현 예정)"""
        # 실제 이메일 발송 로직 구현
        self.logger.info(f"📧 이메일 리포트 발송 예정: {user_id}")


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("📄 리포트 생성기 테스트")
    print("=" * 60)
    
    # 리포트 생성기 초기화
    generator = ReportGenerator()
    
    # 테스트 사용자
    test_user = "test_user_001"
    
    print(f"\n👤 테스트 사용자: {test_user}")
    
    # 1. 월간 HTML 리포트 생성
    print(f"\n📊 월간 HTML 리포트 생성...")
    html_report = generator.generate_report(
        user_id=test_user,
        period="monthly",
        format_type="html"
    )
    
    if html_report:
        print(f"✅ HTML 리포트 생성 완료: {html_report}")
    else:
        print(f"❌ HTML 리포트 생성 실패")
    
    # 2. 월간 텍스트 리포트 생성
    print(f"\n📝 월간 텍스트 리포트 생성...")
    text_report = generator.generate_report(
        user_id=test_user,
        period="monthly",
        format_type="text"
    )
    
    if text_report:
        print(f"✅ 텍스트 리포트 생성 완료: {text_report}")
        
        # 텍스트 리포트 미리보기
        print(f"\n📋 텍스트 리포트 미리보기:")
        try:
            with open(text_report, 'r', encoding='utf-8') as f:
                lines = f.readlines()[:20]  # 처음 20줄만
                for line in lines:
                    print(line.rstrip())
                if len(lines) == 20:
                    print("... (이하 생략)")
        except Exception as e:
            print(f"❌ 파일 읽기 실패: {e}")
    else:
        print(f"❌ 텍스트 리포트 생성 실패")
    
    # 3. 분기 리포트 생성
    print(f"\n📈 분기 리포트 생성...")
    quarterly_report = generator.generate_report(
        user_id=test_user,
        period="quarterly",
        format_type="html"
    )
    
    if quarterly_report:
        print(f"✅ 분기 리포트 생성 완료: {quarterly_report}")
    else:
        print(f"❌ 분기 리포트 생성 실패")
    
    # 4. 생성된 파일 목록
    print(f"\n📁 생성된 리포트 파일:")
    import os
    if os.path.exists("reports"):
        files = os.listdir("reports")
        report_files = [f for f in files if f.startswith("portfolio_report")]
        
        for i, filename in enumerate(report_files, 1):
            filepath = os.path.join("reports", filename)
            file_size = os.path.getsize(filepath)
            print(f"{i}. {filename} ({file_size:,} bytes)")
    else:
        print("reports 디렉토리가 없습니다.")
    
    print(f"\n✅ 리포트 생성기 테스트 완료!")
    print(f"💡 사용 팁:")
    print(f"   - HTML 리포트는 브라우저에서 열어볼 수 있습니다")
    print(f"   - 텍스트 리포트는 이메일이나 메신저로 전송하기 좋습니다")
    print(f"   - 차트가 포함된 리포트는 matplotlib 설치 후 생성됩니다")
    print(f"   - 스케줄러와 연동하여 정기적인 리포트 발송이 가능합니다")