# ==========================================
# dashboard.py - 실시간 모니터링 대시보드 (수정 버전)
# ==========================================

import pandas as pd
import numpy as np
import sqlite3
import json
import time
from datetime import datetime, timedelta
import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Streamlit import 시도
try:
    import streamlit as st
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    STREAMLIT_AVAILABLE = True
    print("✅ Streamlit 사용 가능")
except ImportError:
    STREAMLIT_AVAILABLE = False
    print("⚠️ Streamlit 없음 - CLI 모드로 실행")
    print("pip install streamlit plotly 후 웹 대시보드 사용 가능")

# 프로젝트 모듈 import (오류 처리 포함)
modules_loaded = {}

# ETFUpdateManager import 시도 (올바른 클래스 이름 사용)
try:
    from core.update_manager import ETFUpdateManager
    modules_loaded['ETFUpdateManager'] = True
    print("✅ ETFUpdateManager 로드 성공")
except ImportError as e:
    modules_loaded['ETFUpdateManager'] = False
    print(f"⚠️ ETFUpdateManager 로드 실패: {e}")
    
    # 더미 ETFUpdateManager
    class ETFUpdateManager:
        def __init__(self, db_path="etf_universe.db"):
            self.db_path = db_path
            print(f"🔧 더미 ETFUpdateManager 사용 (DB: {db_path})")
        
        def get_current_status(self):
            return {
                'is_updating': False,
                'progress': 100.0,
                'last_update': '2024-12-22 18:00:00'
            }
        
        def quick_health_check(self):
            return {
                'total_etfs': 17,
                'updated_etfs': 15,
                'price_available': 15,
                'recent_updates_24h': 10,
                'health_score': 85.0,
                'status': 'healthy'
            }
        
        def get_update_history(self, limit=5):
            return [
                {
                    'start_time': '2024-12-22 18:00:00',
                    'success_rate': 85.0,
                    'successful_updates': 15,
                    'failed_updates': 2,
                    'total_etfs': 17,
                    'total_duration': 45.0
                },
                {
                    'start_time': '2024-12-21 18:00:00',
                    'success_rate': 90.0,
                    'successful_updates': 16,
                    'failed_updates': 1,
                    'total_etfs': 17,
                    'total_duration': 38.0
                }
            ]
        
        def batch_update_all_etfs(self, max_etfs=None, delay_between_updates=1.0):
            print(f"🔄 더미 업데이트 실행 (최대 {max_etfs}개 ETF)")
            time.sleep(2)  # 시뮬레이션
            
            class Summary:
                total_etfs = max_etfs or 17
                successful_updates = int(0.8 * (max_etfs or 17))
                failed_updates = int(0.2 * (max_etfs or 17))
                success_rate = 80.0
                total_duration = 45.0
            
            return Summary()
        
        def update_single_etf(self, code, name):
            class Result:
                status = "success"
                code = code
                name = name
            
            return Result()

# DatabaseManager import 시도
try:
    from data.database_manager import DatabaseManager
    modules_loaded['DatabaseManager'] = True
    print("✅ DatabaseManager 로드 성공")
except ImportError:
    modules_loaded['DatabaseManager'] = False
    print("⚠️ DatabaseManager 로드 실패")
    
    class DatabaseManager:
        def __init__(self, db_path="data"):
            self.db_path = db_path

# ETFUniverse import 시도
try:
    from data.etf_universe import ETFUniverse
    modules_loaded['ETFUniverse'] = True
    print("✅ ETFUniverse 로드 성공")
except ImportError:
    modules_loaded['ETFUniverse'] = False
    print("⚠️ ETFUniverse 로드 실패")
    
    class ETFUniverse:
        def __init__(self, db_path=None):
            self.db_path = db_path

# 간단한 ETF 분석기 클래스 구현
class SimpleETFAnalyzer:
    """간단한 ETF 분석기"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
    
    def analyze_category_trends(self) -> pd.DataFrame:
        """카테고리별 트렌드 분석"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query('''
                SELECT 
                    category,
                    COUNT(*) as etf_count,
                    SUM(aum) as total_aum,
                    AVG(expense_ratio) as avg_expense_ratio,
                    AVG(dividend_yield) as avg_dividend_yield
                FROM etf_info
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY total_aum DESC
            ''', conn)
            conn.close()
            return df
        except Exception as e:
            print(f"카테고리 트렌드 분석 실패: {e}")
            return pd.DataFrame()
    
    def analyze_cost_efficiency(self) -> pd.DataFrame:
        """비용 효율성 분석"""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query('''
                SELECT 
                    code, name, category, expense_ratio, aum, dividend_yield,
                    CASE 
                        WHEN expense_ratio > 0 THEN (aum / expense_ratio) 
                        ELSE 0 
                    END as efficiency_ratio
                FROM etf_info
                WHERE expense_ratio > 0 AND aum > 0
                ORDER BY efficiency_ratio DESC
            ''', conn)
            conn.close()
            return df
        except Exception as e:
            print(f"비용 효율성 분석 실패: {e}")
            return pd.DataFrame()
    
    def compare_etfs(self, etf_codes: list) -> pd.DataFrame:
        """ETF 비교"""
        try:
            conn = sqlite3.connect(self.db_path)
            codes_str = "','".join(etf_codes)
            df = pd.read_sql_query(f'''
                SELECT 
                    code, name, category, expense_ratio, aum, 
                    dividend_yield, market_price, fund_company
                FROM etf_info
                WHERE code IN ('{codes_str}')
                ORDER BY aum DESC
            ''', conn)
            conn.close()
            return df
        except Exception as e:
            print(f"ETF 비교 실패: {e}")
            return pd.DataFrame()

# 간단한 ETF 스크리너 클래스 구현
class SimpleETFScreener:
    """간단한 ETF 스크리너"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
    
    def screen_by_criteria(self, criteria: dict) -> pd.DataFrame:
        """조건별 ETF 스크리닝"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 기본 쿼리
            query = "SELECT * FROM etf_info WHERE 1=1"
            params = []
            
            # 조건 추가
            if 'expense_ratio_max' in criteria:
                query += " AND expense_ratio <= ?"
                params.append(criteria['expense_ratio_max'])
            
            if 'aum_min' in criteria:
                query += " AND aum >= ?"
                params.append(criteria['aum_min'])
            
            if 'category' in criteria:
                query += " AND category = ?"
                params.append(criteria['category'])
            
            # 정렬
            sort_by = criteria.get('sort_by', 'aum')
            sort_direction = criteria.get('sort_direction', 'DESC')
            query += f" ORDER BY {sort_by} {sort_direction}"
            
            # 제한
            limit = criteria.get('limit', 20)
            query += f" LIMIT {limit}"
            
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
            
        except Exception as e:
            print(f"ETF 스크리닝 실패: {e}")
            return pd.DataFrame()

class ETFDashboard:
    """ETF 실시간 모니터링 대시보드"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        self.update_manager = ETFUpdateManager(db_path)
        self.analyzer = SimpleETFAnalyzer(db_path)
        self.screener = SimpleETFScreener(db_path)
        
        print(f"🎯 대시보드 초기화 완료 (DB: {db_path})")
    
    def get_market_overview(self) -> dict:
        """시장 전체 현황"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 기본 통계
            overview = pd.read_sql_query('''
                SELECT 
                    COUNT(*) as total_etfs,
                    SUM(COALESCE(aum, 0)) as total_aum,
                    AVG(COALESCE(expense_ratio, 0)) as avg_expense_ratio,
                    COUNT(CASE WHEN market_price > 0 THEN 1 END) as price_available,
                    COUNT(CASE WHEN last_updated > datetime('now', '-1 day') THEN 1 END) as updated_24h
                FROM etf_info
            ''', conn).iloc[0]
            
            # 카테고리별 분포
            category_stats = pd.read_sql_query('''
                SELECT 
                    COALESCE(category, 'Unknown') as category,
                    COUNT(*) as count,
                    SUM(COALESCE(aum, 0)) as total_aum,
                    AVG(COALESCE(expense_ratio, 0)) as avg_expense
                FROM etf_info
                GROUP BY category
                ORDER BY total_aum DESC
            ''', conn)
            
            # 성과 지표
            performance = pd.read_sql_query('''
                SELECT 
                    AVG(COALESCE(dividend_yield, 0)) as avg_dividend,
                    AVG(COALESCE(tracking_error, 0)) as avg_tracking_error,
                    COUNT(CASE WHEN dividend_yield > 3 THEN 1 END) as high_dividend_count
                FROM etf_info
                WHERE dividend_yield > 0
            ''', conn).iloc[0]
            
            conn.close()
            
            return {
                'overview': overview.to_dict(),
                'category_stats': category_stats.to_dict('records'),
                'performance': performance.to_dict(),
                'last_update': datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"❌ 시장 현황 조회 실패: {e}")
            return {}
    
    def get_top_etfs(self, metric: str = 'aum', limit: int = 10) -> pd.DataFrame:
        """상위 ETF 조회"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            order_direction = "DESC" if metric in ['aum', 'dividend_yield', 'market_price'] else "ASC"
            
            query = f'''
                SELECT 
                    name, code, category, 
                    COALESCE(aum, 0) as aum, 
                    COALESCE(expense_ratio, 0) as expense_ratio, 
                    COALESCE(dividend_yield, 0) as dividend_yield, 
                    COALESCE(market_price, 0) as market_price, 
                    last_updated
                FROM etf_info
                WHERE COALESCE({metric}, 0) > 0
                ORDER BY {metric} {order_direction}
                LIMIT ?
            '''
            
            df = pd.read_sql_query(query, conn, params=(limit,))
            conn.close()
            
            return df
            
        except Exception as e:
            print(f"❌ 상위 ETF 조회 실패: {e}")
            return pd.DataFrame()
    
    def get_update_status(self) -> dict:
        """업데이트 상태 조회"""
        try:
            # 현재 업데이트 상태
            current_status = self.update_manager.get_current_status()
            
            # 시스템 건강 상태
            health = self.update_manager.quick_health_check()
            
            # 최근 업데이트 히스토리
            history = self.update_manager.get_update_history(5)
            
            return {
                'current_status': current_status,
                'health': health,
                'recent_history': history,
                'scheduler': {'is_running': False, 'jobs_info': []}  # 기본값
            }
            
        except Exception as e:
            print(f"❌ 업데이트 상태 조회 실패: {e}")
            return {}
    
    def run_streamlit_dashboard(self):
        """Streamlit 웹 대시보드 실행"""
        if not STREAMLIT_AVAILABLE:
            print("❌ Streamlit이 설치되지 않았습니다")
            return
        
        st.set_page_config(
            page_title="ETF 모니터링 대시보드",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
        
        st.title("📊 ETF 실시간 모니터링 대시보드")
        st.markdown("---")
        
        # 사이드바 - 제어판
        st.sidebar.title("🎛️ 제어판")
        
        # 실시간 업데이트 토글
        auto_refresh = st.sidebar.checkbox("🔄 자동 새로고침 (30초)", value=False)
        
        if auto_refresh:
            time.sleep(30)
            st.rerun()
        
        # 수동 새로고침
        if st.sidebar.button("🔄 수동 새로고침"):
            st.rerun()
        
        # 데이터베이스 상태 표시
        st.sidebar.markdown("### 📊 시스템 상태")
        health = self.update_manager.quick_health_check()
        st.sidebar.metric("시스템 건강도", f"{health.get('health_score', 0):.1f}%")
        st.sidebar.metric("총 ETF", f"{health.get('total_etfs', 0)}개")
        
        # 메인 탭 구성
        tab1, tab2, tab3, tab4 = st.tabs([
            "🏠 시장 현황", "📈 업데이트 상태", "🔍 ETF 검색", "📊 성과 분석"
        ])
        
        with tab1:
            self._render_market_overview()
        
        with tab2:
            self._render_update_status()
        
        with tab3:
            self._render_etf_search()
        
        with tab4:
            self._render_performance_analysis()
    
    def _render_market_overview(self):
        """시장 현황 탭 렌더링"""
        st.header("🏠 ETF 시장 현황")
        
        # 시장 데이터 로드
        market_data = self.get_market_overview()
        
        if not market_data:
            st.error("❌ 시장 데이터를 불러올 수 없습니다")
            return
        
        overview = market_data['overview']
        
        # 주요 지표 카드
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "총 ETF 개수",
                f"{overview['total_etfs']:,}개"
            )
        
        with col2:
            st.metric(
                "총 순자산",
                f"{overview['total_aum']:,.0f}억원"
            )
        
        with col3:
            st.metric(
                "평균 운용보수",
                f"{overview['avg_expense_ratio']:.3f}%"
            )
        
        with col4:
            st.metric(
                "24시간 내 업데이트",
                f"{overview['updated_24h']}개"
            )
        
        # 카테고리별 분포
        st.subheader("📊 카테고리별 분포")
        
        if market_data['category_stats']:
            category_df = pd.DataFrame(market_data['category_stats'])
            
            if not category_df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    # 파이 차트
                    fig_pie = px.pie(
                        category_df, 
                        values='total_aum', 
                        names='category',
                        title="카테고리별 순자산 비중"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                with col2:
                    # 바 차트
                    fig_bar = px.bar(
                        category_df, 
                        x='category', 
                        y='count',
                        title="카테고리별 ETF 개수"
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
        
        # 상위 ETF 리스트
        st.subheader("🏆 상위 ETF (순자산 기준)")
        top_etfs = self.get_top_etfs('aum', 10)
        
        if not top_etfs.empty:
            # 표시용 데이터 가공
            display_df = top_etfs.copy()
            display_df['순자산'] = display_df['aum'].apply(lambda x: f"{x:,.0f}억원")
            display_df['운용보수'] = display_df['expense_ratio'].apply(lambda x: f"{x:.3f}%")
            display_df['배당수익률'] = display_df['dividend_yield'].apply(lambda x: f"{x:.2f}%")
            
            st.dataframe(
                display_df[['name', 'code', 'category', '순자산', '운용보수', '배당수익률']],
                use_container_width=True
            )
        else:
            st.warning("📊 ETF 데이터가 없습니다")
    
    def _render_update_status(self):
        """업데이트 상태 탭 렌더링"""
        st.header("📈 업데이트 상태 모니터링")
        
        # 업데이트 상태 로드
        status_data = self.get_update_status()
        
        if not status_data:
            st.error("❌ 업데이트 상태 데이터를 불러올 수 없습니다")
            return
        
        # 현재 상태
        current = status_data.get('current_status', {})
        health = status_data.get('health', {})
        
        # 상태 표시
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if current.get('is_updating'):
                st.error(f"🔄 업데이트 진행 중 ({current.get('progress', 0):.1f}%)")
            else:
                st.success("✅ 업데이트 대기 중")
        
        with col2:
            health_score = health.get('health_score', 0)
            if health_score > 80:
                st.success(f"💚 시스템 건강: {health_score:.1f}%")
            elif health_score > 50:
                st.warning(f"💛 시스템 주의: {health_score:.1f}%")
            else:
                st.error(f"❤️ 시스템 위험: {health_score:.1f}%")
        
        with col3:
            last_update = current.get('last_update', '알 수 없음')
            st.info(f"🕐 마지막 업데이트: {last_update}")
        
        # 수동 업데이트 버튼
        st.subheader("🔧 수동 업데이트")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("⚡ 빠른 업데이트 (5개 ETF)"):
                with st.spinner("업데이트 실행 중..."):
                    try:
                        summary = self.update_manager.batch_update_all_etfs(max_etfs=5, delay_between_updates=0.5)
                        if summary:
                            st.success(f"✅ 완료: {summary.success_rate:.1f}% 성공")
                        else:
                            st.error("❌ 업데이트 실패")
                    except Exception as e:
                        st.error(f"❌ 오류: {e}")
        
        with col2:
            if st.button("🔄 일반 업데이트 (10개 ETF)"):
                with st.spinner("업데이트 실행 중..."):
                    try:
                        summary = self.update_manager.batch_update_all_etfs(max_etfs=10, delay_between_updates=1.0)
                        if summary:
                            st.success(f"✅ 완료: {summary.success_rate:.1f}% 성공")
                        else:
                            st.error("❌ 업데이트 실패")
                    except Exception as e:
                        st.error(f"❌ 오류: {e}")
        
        with col3:
            if st.button("🚀 전체 업데이트"):
                st.warning("⚠️ 전체 업데이트는 시간이 오래 걸립니다")
        
        # 업데이트 히스토리
        st.subheader("📋 최근 업데이트 히스토리")
        
        history = status_data.get('recent_history', [])
        if history:
            history_df = pd.DataFrame(history)
            history_df['시작시간'] = pd.to_datetime(history_df['start_time']).dt.strftime('%m-%d %H:%M')
            history_df['성공률'] = history_df['success_rate'].apply(lambda x: f"{x:.1f}%")
            history_df['소요시간'] = history_df['total_duration'].apply(lambda x: f"{x:.1f}초")
            
            st.dataframe(
                history_df[['시작시간', 'total_etfs', 'successful_updates', 'failed_updates', '성공률', '소요시간']],
                use_container_width=True
            )
        else:
            st.info("📝 업데이트 히스토리가 없습니다")
    
    def _render_etf_search(self):
        """ETF 검색 탭 렌더링"""
        st.header("🔍 ETF 검색 및 비교")
        
        # 검색 필터
        st.subheader("🎯 검색 필터")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            max_expense = st.slider("최대 운용보수 (%)", 0.0, 1.0, 0.5, 0.01)
        
        with col2:
            min_aum = st.number_input("최소 순자산 (억원)", 0, 100000, 1000, 100)
        
        with col3:
            categories = ['전체', 'domestic_equity', 'foreign_equity', 'bonds', 'alternatives', 'thematic']
            selected_category = st.selectbox("카테고리", categories)
        
        # 검색 실행
        if st.button("🔍 검색 실행"):
            criteria = {
                'expense_ratio_max': max_expense,
                'aum_min': min_aum,
                'sort_by': 'aum',
                'sort_direction': 'DESC',
                'limit': 20
            }
            
            if selected_category != '전체':
                criteria['category'] = selected_category
            
            search_results = self.screener.screen_by_criteria(criteria)
            
            if not search_results.empty:
                st.subheader(f"📊 검색 결과 ({len(search_results)}개)")
                
                # 결과 표시
                display_df = search_results.copy()
                display_df['순자산'] = display_df['aum'].apply(lambda x: f"{x:,.0f}억원")
                display_df['운용보수'] = display_df['expense_ratio'].apply(lambda x: f"{x:.3f}%")
                
                st.dataframe(
                    display_df[['name', 'code', 'category', '순자산', '운용보수', 'fund_company']],
                    use_container_width=True
                )
            else:
                st.warning("🔍 검색 조건에 맞는 ETF가 없습니다")
        
        # ETF 비교
        st.subheader("⚖️ ETF 비교")
        
        etf_codes = st.text_input(
            "비교할 ETF 코드 입력 (쉼표로 구분)",
            placeholder="예: 069500,360750,114260"
        )
        
        if st.button("⚖️ 비교 실행") and etf_codes:
            codes = [code.strip() for code in etf_codes.split(',')]
            comparison_df = self.analyzer.compare_etfs(codes)
            
            if not comparison_df.empty:
                st.subheader("📊 비교 결과")
                st.dataframe(comparison_df, use_container_width=True)
                
                # 비교 차트
                if len(comparison_df) > 1:
                    fig = go.Figure()
                    
                    fig.add_trace(go.Bar(
                        x=comparison_df['name'],
                        y=comparison_df['expense_ratio'],
                        name='운용보수 (%)',
                        yaxis='y'
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=comparison_df['name'],
                        y=comparison_df['aum'],
                        mode='lines+markers',
                        name='순자산 (억원)',
                        yaxis='y2'
                    ))
                    
                    fig.update_layout(
                        title="ETF 비교 차트",
                        yaxis=dict(title="운용보수 (%)"),
                        yaxis2=dict(title="순자산 (억원)", overlaying='y', side='right')
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("❌ 비교할 ETF 정보를 찾을 수 없습니다")
    
    def _render_performance_analysis(self):
        """성과 분석 탭 렌더링"""
        st.header("📊 ETF 성과 분석")
        
        # 카테고리별 분석
        st.subheader("📈 카테고리별 트렌드")
        
        trends = self.analyzer.analyze_category_trends()
        if not trends.empty:
            # 순자산 기준 상위 카테고리
            fig = px.bar(
                trends.head(10),
                x='category',
                y='total_aum',
                title="카테고리별 총 순자산"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # 상세 테이블
            st.dataframe(trends, use_container_width=True)
        else:
            st.info("📊 분석할 데이터가 충분하지 않습니다")
        
        # 비용 효율성 분석
        st.subheader("💰 비용 효율성 분석")
        
        efficiency = self.analyzer.analyze_cost_efficiency()
        if not efficiency.empty:
            # 효율성 상위 ETF
            top_efficient = efficiency.head(15)
            
            fig = px.scatter(
                top_efficient,
                x='expense_ratio',
                y='aum',
                size='efficiency_ratio',
                hover_name='name',
                title="ETF 비용 효율성 (크기: 효율성 점수)"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📊 비용 효율성 데이터가 없습니다")
    
    def run_cli_dashboard(self):
        """CLI 버전 대시보드"""
        print("📊 ETF 모니터링 CLI 대시보드")
        print("=" * 60)
        
        while True:
            try:
                print("\n🎯 메뉴 선택:")
                print("1. 시장 현황")
                print("2. 업데이트 상태")
                print("3. ETF 검색")
                print("4. 수동 업데이트")
                print("5. 시스템 상태")
                print("6. 종료")
                
                choice = input("\n선택 (1-6): ").strip()
                
                if choice == "1":
                    self._cli_market_overview()
                elif choice == "2":
                    self._cli_update_status()
                elif choice == "3":
                    self._cli_etf_search()
                elif choice == "4":
                    self._cli_manual_update()
                elif choice == "5":
                    self._cli_system_status()
                elif choice == "6":
                    print("👋 대시보드를 종료합니다")
                    break
                else:
                    print("❌ 1-6 중에서 선택해주세요")
            
            except KeyboardInterrupt:
                print("\n\n👋 사용자에 의해 종료됨")
                break
            except Exception as e:
                print(f"\n❌ 오류 발생: {e}")
    
    def _cli_market_overview(self):
        """CLI 시장 현황"""
        print("\n🏠 ETF 시장 현황")
        print("-" * 40)
        
        market_data = self.get_market_overview()
        if market_data:
            overview = market_data['overview']
            print(f"총 ETF: {overview['total_etfs']}개")
            print(f"총 순자산: {overview['total_aum']:,.0f}억원")
            print(f"평균 운용보수: {overview['avg_expense_ratio']:.3f}%")
            print(f"24시간 내 업데이트: {overview['updated_24h']}개")
            
            print("\n📊 카테고리별 분포:")
            for cat in market_data['category_stats']:
                print(f"- {cat['category']}: {cat['count']}개, {cat['total_aum']:,.0f}억원")
    
    def _cli_update_status(self):
        """CLI 업데이트 상태"""
        print("\n📈 업데이트 상태")
        print("-" * 40)
        
        status_data = self.get_update_status()
        if status_data:
            current = status_data['current_status']
            health = status_data['health']
            
            print(f"업데이트 진행 중: {'예' if current.get('is_updating') else '아니오'}")
            print(f"시스템 건강도: {health.get('health_score', 0):.1f}%")
            print(f"마지막 업데이트: {current.get('last_update', '알 수 없음')}")
            
            history = status_data.get('recent_history', [])
            if history:
                print("\n📋 최근 업데이트 히스토리:")
                for i, record in enumerate(history[:3]):
                    print(f"{i+1}. {record['start_time'][:19]} - "
                          f"성공률 {record['success_rate']:.1f}%")
    
    def _cli_etf_search(self):
        """CLI ETF 검색"""
        print("\n🔍 ETF 검색")
        print("-" * 40)
        
        try:
            max_expense = float(input("최대 운용보수 (%, 예: 0.5): ") or "0.5")
            min_aum = int(input("최소 순자산 (억원, 예: 1000): ") or "1000")
            
            criteria = {
                'expense_ratio_max': max_expense,
                'aum_min': min_aum,
                'sort_by': 'aum',
                'limit': 10
            }
            
            results = self.screener.screen_by_criteria(criteria)
            
            if not results.empty:
                print(f"\n📊 검색 결과 ({len(results)}개):")
                for _, etf in results.iterrows():
                    print(f"- {etf['name']} ({etf['code']}): {etf['aum']:,.0f}억원, {etf['expense_ratio']:.3f}%")
            else:
                print("🔍 검색 조건에 맞는 ETF가 없습니다")
                
        except ValueError:
            print("❌ 올바른 숫자를 입력해주세요")
    
    def _cli_manual_update(self):
        """CLI 수동 업데이트"""
        print("\n🔧 수동 업데이트")
        print("-" * 40)
        
        print("1. 빠른 업데이트 (5개 ETF)")
        print("2. 일반 업데이트 (10개 ETF)")
        print("3. 개별 ETF 업데이트")
        
        choice = input("선택: ").strip()
        
        try:
            if choice == "1":
                print("⚡ 빠른 업데이트 실행 중...")
                summary = self.update_manager.batch_update_all_etfs(max_etfs=5)
                if summary:
                    print(f"✅ 완료: {summary.success_rate:.1f}% 성공")
            
            elif choice == "2":
                print("🔄 일반 업데이트 실행 중...")
                summary = self.update_manager.batch_update_all_etfs(max_etfs=10)
                if summary:
                    print(f"✅ 완료: {summary.success_rate:.1f}% 성공")
            
            elif choice == "3":
                code = input("ETF 코드 입력: ").strip()
                if code:
                    print(f"🔄 {code} 업데이트 중...")
                    result = self.update_manager.update_single_etf(code, f"ETF_{code}")
                    print(f"결과: {result.status}")
                    
        except Exception as e:
            print(f"❌ 업데이트 실패: {e}")
    
    def _cli_system_status(self):
        """CLI 시스템 상태"""
        print("\n⚙️ 시스템 상태")
        print("-" * 40)
        
        health = self.update_manager.quick_health_check()
        if health.get('status') != 'error':
            print(f"총 ETF: {health['total_etfs']}개")
            print(f"업데이트된 ETF: {health['updated_etfs']}개")
            print(f"가격 정보 보유: {health['price_available']}개")
            print(f"24시간 내 업데이트: {health['recent_updates_24h']}개")
            print(f"시스템 건강도: {health['health_score']:.1f}%")
        else:
            print(f"❌ 시스템 상태 확인 실패: {health.get('error')}")


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

def main():
    """메인 실행 함수"""
    print("📊 ETF 모니터링 대시보드")
    print("=" * 60)
    
    dashboard = ETFDashboard()
    
    if STREAMLIT_AVAILABLE:
        print("🌐 Streamlit 웹 대시보드를 시작합니다...")
        print("브라우저에서 http://localhost:8501 으로 접속하세요")
        dashboard.run_streamlit_dashboard()
    else:
        print("💻 CLI 대시보드를 시작합니다...")
        dashboard.run_cli_dashboard()

if __name__ == "__main__":
    # Streamlit으로 실행되는 경우
    if STREAMLIT_AVAILABLE and len(sys.argv) > 1 and 'streamlit' in sys.argv[0]:
        dashboard = ETFDashboard()
        dashboard.run_streamlit_dashboard()
    else:
        main()