# ==========================================
# data/etf_analyzer.py - ETF 분석 도구 (수정 버전)
# ==========================================

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta

# Plotly 선택적 import (없어도 기본 분석은 가능)
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
    print("✅ Plotly 라이브러리 사용 가능")
except ImportError:
    PLOTLY_AVAILABLE = False
    print("⚠️ Plotly 없음 - 차트 생성 불가, 데이터 분석만 제공")

class ETFAnalyzer:
    """ETF 분석 도구"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        print(f"📊 ETF 분석 도구 초기화 (DB: {db_path})")
    
    def analyze_category_trends(self) -> pd.DataFrame:
        """카테고리별 트렌드 분석"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT 
                category,
                subcategory,
                COUNT(*) as etf_count,
                SUM(aum) as total_aum,
                AVG(expense_ratio) as avg_expense_ratio,
                AVG(tracking_error) as avg_tracking_error,
                AVG(dividend_yield) as avg_dividend_yield,
                MIN(expense_ratio) as min_expense_ratio,
                MAX(expense_ratio) as max_expense_ratio
            FROM etf_info
            GROUP BY category, subcategory
            ORDER BY total_aum DESC
        '''
        
        try:
            df = pd.read_sql_query(query, conn)
            print(f"✅ 카테고리별 트렌드 분석 완료: {len(df)}개 카테고리")
        except Exception as e:
            print(f"❌ 카테고리 트렌드 분석 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def analyze_market_concentration(self) -> dict:
        """시장 집중도 분석"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 전체 시장 규모
            total_aum = pd.read_sql_query('SELECT SUM(aum) as total FROM etf_info', conn).iloc[0]['total']
            
            # 상위 ETF 집중도
            top_etfs = pd.read_sql_query('''
                SELECT name, code, aum, 
                       (aum / (SELECT SUM(aum) FROM etf_info)) * 100 as market_share
                FROM etf_info
                ORDER BY aum DESC
                LIMIT 10
            ''', conn)
            
            # 운용사별 집중도
            manager_concentration = pd.read_sql_query('''
                SELECT fund_manager, 
                       COUNT(*) as etf_count,
                       SUM(aum) as total_aum,
                       (SUM(aum) / (SELECT SUM(aum) FROM etf_info)) * 100 as market_share
                FROM etf_info
                GROUP BY fund_manager
                ORDER BY total_aum DESC
            ''', conn)
            
            # 카테고리별 집중도
            category_concentration = pd.read_sql_query('''
                SELECT category,
                       COUNT(*) as etf_count,
                       SUM(aum) as total_aum,
                       (SUM(aum) / (SELECT SUM(aum) FROM etf_info)) * 100 as market_share
                FROM etf_info
                GROUP BY category
                ORDER BY total_aum DESC
            ''', conn)
            
            # HHI (허핀달 지수) 계산
            market_shares = top_etfs['market_share'].values
            hhi = sum(share**2 for share in market_shares)
            
            print(f"✅ 시장 집중도 분석 완료")
            
            return {
                'total_market_aum': total_aum,
                'top_etfs': top_etfs.to_dict('records'),
                'manager_concentration': manager_concentration.to_dict('records'),
                'category_concentration': category_concentration.to_dict('records'),
                'hhi_index': round(hhi, 2),
                'top_10_market_share': round(top_etfs['market_share'].sum(), 2)
            }
            
        except Exception as e:
            print(f"❌ 시장 집중도 분석 오류: {e}")
            return {}
        finally:
            conn.close()
    
    def analyze_cost_efficiency(self) -> pd.DataFrame:
        """비용 효율성 분석"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            query = '''
                SELECT *,
                       (aum / expense_ratio) as efficiency_ratio,
                       CASE 
                           WHEN expense_ratio <= 0.1 THEN 'Ultra Low'
                           WHEN expense_ratio <= 0.2 THEN 'Low'
                           WHEN expense_ratio <= 0.5 THEN 'Medium'
                           ELSE 'High'
                       END as cost_tier,
                       CASE
                           WHEN aum >= 10000 THEN 'Large'
                           WHEN aum >= 1000 THEN 'Medium'
                           ELSE 'Small'
                       END as size_tier
                FROM etf_info
                WHERE expense_ratio > 0
                ORDER BY efficiency_ratio DESC
            '''
            
            df = pd.read_sql_query(query, conn)
            
            # 비용 티어별 통계
            cost_stats = df.groupby('cost_tier').agg({
                'etf_count': 'size',
                'avg_aum': 'aum',
                'avg_tracking_error': 'tracking_error'
            }).round(2)
            
            print(f"✅ 비용 효율성 분석 완료: {len(df)}개 ETF")
            
        except Exception as e:
            print(f"❌ 비용 효율성 분석 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def generate_expense_ratio_analysis(self) -> dict:
        """운용보수 분석 (차트 데이터 포함)"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            df = pd.read_sql_query('SELECT * FROM etf_info', conn)
            
            # 기본 통계
            expense_stats = {
                'mean': df['expense_ratio'].mean(),
                'median': df['expense_ratio'].median(),
                'std': df['expense_ratio'].std(),
                'min': df['expense_ratio'].min(),
                'max': df['expense_ratio'].max()
            }
            
            # 카테고리별 평균 운용보수
            category_avg = df.groupby('category')['expense_ratio'].mean().sort_values(ascending=True)
            
            # 운용사별 평균 운용보수
            manager_avg = df.groupby('fund_manager')['expense_ratio'].mean().sort_values(ascending=True)
            
            # 운용보수 구간별 분포
            df['expense_tier'] = pd.cut(df['expense_ratio'], 
                                      bins=[0, 0.1, 0.2, 0.3, 0.5, 1.0], 
                                      labels=['0-0.1%', '0.1-0.2%', '0.2-0.3%', '0.3-0.5%', '0.5%+'])
            tier_distribution = df['expense_tier'].value_counts()
            
            analysis = {
                'expense_stats': expense_stats,
                'category_averages': category_avg.to_dict(),
                'manager_averages': manager_avg.to_dict(),
                'tier_distribution': tier_distribution.to_dict(),
                'raw_data': df[['name', 'code', 'category', 'expense_ratio', 'aum']].to_dict('records')
            }
            
            print(f"✅ 운용보수 분석 완료")
            
            # Plotly 차트 생성 (가능한 경우)
            if PLOTLY_AVAILABLE:
                fig = self._create_expense_ratio_chart(df, category_avg, manager_avg)
                analysis['chart'] = fig
            
            return analysis
            
        except Exception as e:
            print(f"❌ 운용보수 분석 오류: {e}")
            return {}
        finally:
            conn.close()
    
    def _create_expense_ratio_chart(self, df, category_avg, manager_avg):
        """운용보수 분석 차트 생성 (Plotly 필요)"""
        if not PLOTLY_AVAILABLE:
            return None
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=['카테고리별 평균 운용보수', '운용보수 분포', 
                          '운용보수 vs 순자산', '운용사별 평균 운용보수'],
            specs=[[{"type": "bar"}, {"type": "histogram"}],
                   [{"type": "scatter"}, {"type": "bar"}]]
        )
        
        # 카테고리별 평균 운용보수
        fig.add_trace(
            go.Bar(x=category_avg.values, y=category_avg.index, orientation='h', name='평균 운용보수'),
            row=1, col=1
        )
        
        # 운용보수 분포
        fig.add_trace(
            go.Histogram(x=df['expense_ratio'], nbinsx=20, name='운용보수 분포'),
            row=1, col=2
        )
        
        # 운용보수 vs 순자산
        fig.add_trace(
            go.Scatter(
                x=df['expense_ratio'], y=df['aum'],
                mode='markers',
                text=df['name'],
                name='운용보수 vs AUM'
            ),
            row=2, col=1
        )
        
        # 운용사별 평균 운용보수 (상위 5개만)
        top_managers = manager_avg.head(5)
        fig.add_trace(
            go.Bar(x=top_managers.values, y=top_managers.index, orientation='h', name='운용사별 평균'),
            row=2, col=2
        )
        
        fig.update_layout(
            title='ETF 운용보수 분석',
            height=800,
            showlegend=False
        )
        
        return fig
    
    def generate_universe_dashboard(self) -> dict:
        """ETF 유니버스 대시보드 데이터"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 전체 통계
            total_stats = pd.read_sql_query('''
                SELECT 
                    COUNT(*) as total_etfs,
                    SUM(aum) as total_aum,
                    AVG(expense_ratio) as avg_expense_ratio,
                    AVG(tracking_error) as avg_tracking_error,
                    AVG(dividend_yield) as avg_dividend_yield,
                    MIN(inception_date) as oldest_etf,
                    MAX(inception_date) as newest_etf
                FROM etf_info
            ''', conn).iloc[0].to_dict()
            
            # 카테고리별 분포
            category_dist = pd.read_sql_query('''
                SELECT category, 
                       COUNT(*) as count, 
                       SUM(aum) as total_aum,
                       AVG(expense_ratio) as avg_expense_ratio
                FROM etf_info
                GROUP BY category
                ORDER BY total_aum DESC
            ''', conn)
            
            # 최신 업데이트 정보
            try:
                last_update = pd.read_sql_query('''
                    SELECT MAX(last_updated) as last_update
                    FROM etf_info
                    WHERE last_updated IS NOT NULL
                ''', conn).iloc[0]['last_update']
            except:
                last_update = datetime.now().isoformat()
            
            # 상위/하위 ETF
            top_etfs = pd.read_sql_query('''
                SELECT name, code, aum, expense_ratio, category
                FROM etf_info
                ORDER BY aum DESC
                LIMIT 10
            ''', conn)
            
            low_cost_etfs = pd.read_sql_query('''
                SELECT name, code, expense_ratio, aum, category
                FROM etf_info
                WHERE aum > 1000
                ORDER BY expense_ratio ASC
                LIMIT 10
            ''', conn)
            
            # 성장성 분석 (AUM 기준)
            growth_analysis = pd.read_sql_query('''
                SELECT 
                    category,
                    COUNT(*) as etf_count,
                    AVG(aum) as avg_aum,
                    SUM(aum) as total_aum
                FROM etf_info
                GROUP BY category
                ORDER BY avg_aum DESC
            ''', conn)
            
            dashboard_data = {
                'total_stats': total_stats,
                'category_distribution': category_dist.to_dict('records'),
                'last_update': last_update,
                'top_etfs_by_aum': top_etfs.to_dict('records'),
                'low_cost_etfs': low_cost_etfs.to_dict('records'),
                'growth_analysis': growth_analysis.to_dict('records'),
                'summary_insights': self._generate_insights(total_stats, category_dist)
            }
            
            print(f"✅ 대시보드 데이터 생성 완료")
            return dashboard_data
            
        except Exception as e:
            print(f"❌ 대시보드 데이터 생성 오류: {e}")
            return {}
        finally:
            conn.close()
    
    def _generate_insights(self, total_stats, category_dist) -> list:
        """대시보드 인사이트 생성"""
        insights = []
        
        # 시장 규모 인사이트
        total_aum = total_stats['total_aum']
        insights.append(f"📊 총 ETF 시장 규모: {total_aum:,.0f}억원")
        
        # 평균 운용보수 인사이트
        avg_expense = total_stats['avg_expense_ratio']
        if avg_expense < 0.3:
            insights.append(f"💰 평균 운용보수 {avg_expense:.2f}% - 저비용 친화적")
        else:
            insights.append(f"💰 평균 운용보수 {avg_expense:.2f}% - 비용 최적화 여지")
        
        # 카테고리 다양성 인사이트
        category_count = len(category_dist)
        insights.append(f"🎯 투자 카테고리: {category_count}개 - 분산투자 가능")
        
        # 최대 카테고리 인사이트
        if not category_dist.empty:
            top_category = category_dist.iloc[0]
            share = (top_category['total_aum'] / total_aum) * 100
            insights.append(f"🔥 최대 카테고리: {top_category['category']} ({share:.1f}%)")
        
        return insights
    
    def compare_etfs(self, etf_codes: list) -> pd.DataFrame:
        """ETF 간 비교 분석"""
        if not etf_codes:
            print("❌ 비교할 ETF 코드가 없습니다")
            return pd.DataFrame()
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            codes_str = "', '".join(etf_codes)
            query = f'''
                SELECT 
                    name, code, category, subcategory,
                    expense_ratio, aum, tracking_error, dividend_yield,
                    fund_manager, benchmark
                FROM etf_info 
                WHERE code IN ('{codes_str}')
                ORDER BY aum DESC
            '''
            
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print("❌ 비교할 ETF 정보를 찾을 수 없습니다")
                return df
            
            # 비교 메트릭 추가
            df['aum_rank'] = df['aum'].rank(ascending=False)
            df['cost_rank'] = df['expense_ratio'].rank(ascending=True)
            df['tracking_rank'] = df['tracking_error'].rank(ascending=True)
            
            print(f"✅ ETF 비교 완료: {len(df)}개")
            return df
            
        except Exception as e:
            print(f"❌ ETF 비교 오류: {e}")
            return pd.DataFrame()
        finally:
            conn.close()
    
    def generate_performance_summary(self) -> dict:
        """성과 요약 생성"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 카테고리별 성과 요약
            performance_by_category = pd.read_sql_query('''
                SELECT 
                    category,
                    COUNT(*) as etf_count,
                    AVG(dividend_yield) as avg_dividend_yield,
                    AVG(tracking_error) as avg_tracking_error,
                    SUM(aum) as total_aum
                FROM etf_info
                GROUP BY category
                ORDER BY avg_dividend_yield DESC
            ''', conn)
            
            # 운용사별 성과 요약
            performance_by_manager = pd.read_sql_query('''
                SELECT 
                    fund_manager,
                    COUNT(*) as etf_count,
                    AVG(expense_ratio) as avg_expense_ratio,
                    AVG(tracking_error) as avg_tracking_error,
                    SUM(aum) as total_aum
                FROM etf_info
                GROUP BY fund_manager
                ORDER BY total_aum DESC
            ''', conn)
            
            # 효율성 지표
            efficiency_leaders = pd.read_sql_query('''
                SELECT 
                    name, code, category,
                    (aum / expense_ratio) as efficiency_score,
                    expense_ratio, aum
                FROM etf_info
                WHERE expense_ratio > 0
                ORDER BY efficiency_score DESC
                LIMIT 10
            ''', conn)
            
            summary = {
                'performance_by_category': performance_by_category.to_dict('records'),
                'performance_by_manager': performance_by_manager.to_dict('records'),
                'efficiency_leaders': efficiency_leaders.to_dict('records'),
                'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            print(f"✅ 성과 요약 생성 완료")
            return summary
            
        except Exception as e:
            print(f"❌ 성과 요약 생성 오류: {e}")
            return {}
        finally:
            conn.close()


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("📊 ETF 분석 도구 테스트")
    print("=" * 50)
    
    # 분석 도구 초기화
    analyzer = ETFAnalyzer()
    
    # 1. 카테고리별 트렌드 분석
    print("\n📈 카테고리별 트렌드 분석:")
    trends = analyzer.analyze_category_trends()
    if not trends.empty:
        print(f"총 {len(trends)}개 카테고리 분석 완료")
        for _, row in trends.head(5).iterrows():
            print(f"- {row['category']}/{row['subcategory']}: {row['etf_count']}개, {row['total_aum']:,.0f}억원, 평균비용 {row['avg_expense_ratio']:.2f}%")
    
    # 2. 시장 집중도 분석
    print("\n🎯 시장 집중도 분석:")
    concentration = analyzer.analyze_market_concentration()
    if concentration:
        print(f"총 시장 규모: {concentration['total_market_aum']:,.0f}억원")
        print(f"상위 10개 ETF 점유율: {concentration['top_10_market_share']:.1f}%")
        print(f"HHI 지수: {concentration['hhi_index']}")
        
        print("\n상위 5개 ETF:")
        for etf in concentration['top_etfs'][:5]:
            print(f"- {etf['name']}: {etf['market_share']:.1f}% ({etf['aum']:,.0f}억원)")
        
        print("\n운용사별 점유율:")
        for manager in concentration['manager_concentration'][:3]:
            print(f"- {manager['fund_manager']}: {manager['market_share']:.1f}% ({manager['etf_count']}개)")
    
    # 3. 비용 효율성 분석
    print("\n💰 비용 효율성 분석:")
    efficiency = analyzer.analyze_cost_efficiency()
    if not efficiency.empty:
        print(f"효율성 분석 완료: {len(efficiency)}개 ETF")
        
        # 비용 티어별 통계
        cost_tier_stats = efficiency.groupby('cost_tier').agg({
            'name': 'count',
            'aum': 'mean',
            'tracking_error': 'mean'
        }).round(2)
        
        print("\n비용 티어별 통계:")
        for tier, stats in cost_tier_stats.iterrows():
            print(f"- {tier}: {stats['name']}개, 평균 AUM {stats['aum']:,.0f}억원")
        
        print("\n효율성 상위 5개 ETF:")
        for _, etf in efficiency.head(5).iterrows():
            print(f"- {etf['name']}: 효율성 {etf['efficiency_ratio']:,.0f}, {etf['expense_ratio']}%")
    
    # 4. 운용보수 분석
    print("\n📊 운용보수 분석:")
    expense_analysis = analyzer.generate_expense_ratio_analysis()
    if expense_analysis:
        stats = expense_analysis['expense_stats']
        print(f"- 평균 운용보수: {stats['mean']:.3f}%")
        print(f"- 중앙값: {stats['median']:.3f}%")
        print(f"- 표준편차: {stats['std']:.3f}%")
        print(f"- 범위: {stats['min']:.3f}% ~ {stats['max']:.3f}%")
        
        print("\n카테고리별 평균 운용보수:")
        for category, avg_expense in list(expense_analysis['category_averages'].items())[:5]:
            print(f"- {category}: {avg_expense:.3f}%")
    
    # 5. 대시보드 데이터 생성
    print("\n🔍 대시보드 데이터 생성:")
    dashboard = analyzer.generate_universe_dashboard()
    if dashboard:
        stats = dashboard['total_stats']
        print(f"- 총 ETF: {stats['total_etfs']}개")
        print(f"- 총 AUM: {stats['total_aum']:,.0f}억원")
        print(f"- 평균 운용보수: {stats['avg_expense_ratio']:.3f}%")
        print(f"- 평균 배당수익률: {stats['avg_dividend_yield']:.2f}%")
        
        print("\n주요 인사이트:")
        for insight in dashboard['summary_insights']:
            print(f"  {insight}")
    
    # 6. ETF 비교 분석
    print("\n⚖️ ETF 비교 분석:")
    comparison_etfs = ['069500', '360750', '114260']  # KODEX 200, TIGER 미국S&P500, KODEX 국고채10년
    comparison = analyzer.compare_etfs(comparison_etfs)
    if not comparison.empty:
        print(f"비교 대상: {len(comparison)}개 ETF")
        for _, etf in comparison.iterrows():
            print(f"- {etf['name']}: {etf['expense_ratio']}%, {etf['aum']:,.0f}억원, 추적오차 {etf['tracking_error']}%")
    
    # 7. 성과 요약 생성
    print("\n🏆 성과 요약:")
    performance = analyzer.generate_performance_summary()
    if performance:
        print("카테고리별 배당수익률 순위:")
        for cat in performance['performance_by_category'][:3]:
            print(f"- {cat['category']}: {cat['avg_dividend_yield']:.2f}% (평균)")
        
        print("\n효율성 리더:")
        for etf in performance['efficiency_leaders'][:3]:
            print(f"- {etf['name']}: 효율성 점수 {etf['efficiency_score']:,.0f}")
    
    print(f"\n✅ ETF 분석 도구 테스트 완료!")
    print(f"💡 다음 단계: python data/market_data_collector.py")