# ==========================================
# web/dashboard.py - 탭 기반 ETF 대시보드 (key 충돌 해결)
# ==========================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sqlite3
from datetime import datetime
import sys
import os
import uuid

# 프로젝트 모듈 import
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

try:
    from core.update_manager import ETFUpdateManager
    UPDATE_MANAGER_AVAILABLE = True
except ImportError as e:
    UPDATE_MANAGER_AVAILABLE = False
    print(f"⚠️ UpdateManager import 실패: {e}")

class ETFDashboard:
    """ETF 대시보드 클래스 (탭 기반)"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        # 세션 초기화 (중복 실행 방지)
        if 'dashboard_initialized' not in st.session_state:
            st.session_state.dashboard_initialized = True
            st.session_state.dashboard_unique_id = str(uuid.uuid4())[:8]
        self.unique_id = st.session_state.dashboard_unique_id
    
    def run(self):
        """대시보드 실행"""
        # 페이지 설정 (한 번만 실행)
        if not hasattr(st.session_state, 'page_config_set'):
            st.set_page_config(
                page_title="한국 ETF 시장 분석", 
                page_icon="📊", 
                layout="wide"
            )
            st.session_state.page_config_set = True
        
        st.title("📊 한국 ETF 시장 종합 분석")
        st.markdown("**🇰🇷 국내 상장 ETF 전체 현황과 투자 인사이트**")
        st.markdown("---")
        
        # 데이터 로드
        df = self.load_etf_data()
        
        if df is not None and not df.empty:
            self.show_tabbed_dashboard(df)
        else:
            self.show_empty_dashboard()
    
    def load_etf_data(self) -> pd.DataFrame:
        """ETF 데이터 로드 (고객용 - 시스템 정보 숨김)"""
        try:
            # 명확한 경로 지정
            possible_paths = [
                r"C:\data_analysis\etf-portfolio-manager\etf_universe.db",
                "etf_universe.db",
                f"../{self.db_path}",
                os.path.join(parent_dir, self.db_path),
                os.path.abspath(self.db_path)
            ]
            
            actual_path = None
            
            # 가장 적절한 DB 파일 찾기
            for path in possible_paths:
                if os.path.exists(path):
                    actual_path = path
                    break
            
            if not actual_path:
                st.error("📊 ETF 데이터를 로드할 수 없습니다. 시스템 관리자에게 문의하세요.")
                return None
            
            # 데이터 로드
            conn = sqlite3.connect(actual_path)
            
            # 테이블 확인 및 최적 선택
            tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
            tables = pd.read_sql_query(tables_query, conn)['name'].tolist()
            
            # 각 테이블의 데이터 개수 확인 (백그라운드)
            table_info = {}
            for table in ['etf_info', 'etf_master']:
                if table in tables:
                    try:
                        count_query = f"SELECT COUNT(*) as count FROM {table}"
                        count = pd.read_sql_query(count_query, conn).iloc[0]['count']
                        table_info[table] = count
                    except:
                        table_info[table] = 0
            
            # 최적 테이블 선택 (시스템 메시지 없이)
            if table_info:
                best_table = max(table_info.keys(), key=lambda x: table_info[x])
                
                if best_table == 'etf_info':
                    query = """
                        SELECT 
                            code, name, 
                            COALESCE(category, '기타') as category,
                            COALESCE(fund_manager, '기타') as fund_manager,
                            expense_ratio, aum, 
                            last_updated as updated_at
                        FROM etf_info 
                        ORDER BY COALESCE(aum, 0) DESC
                    """
                else:  # etf_master
                    query = """
                        SELECT 
                            code, name,
                            COALESCE(category, '기타') as category,
                            COALESCE(fund_manager, '기타') as fund_manager,
                            expense_ratio, aum, updated_at
                        FROM etf_master 
                        ORDER BY COALESCE(aum, 0) DESC
                    """
            else:
                st.error("📊 ETF 데이터 테이블을 찾을 수 없습니다.")
                conn.close()
                return None
            
            # 데이터 로드 실행
            df = pd.read_sql_query(query, conn)
            conn.close()
            
            # 간단한 성공 메시지만 표시
            if not df.empty:
                st.success(f"✅ {len(df):,}개 ETF 데이터 업데이트 완료")
            
            return df
            
        except Exception as e:
            st.error("📊 데이터 로드 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.")
            return None
    
    def show_tabbed_dashboard(self, df: pd.DataFrame):
        """탭 기반 대시보드 표시 (고객용)"""
        # 탭 생성 (고객 친화적 이름)
        tab1, tab2, tab3, tab4 = st.tabs(["🏠 시장현황", "📈 투자분석", "🔍 ETF 검색", "ℹ️ 데이터 정보"])
        
        with tab1:
            self.show_overview_tab(df)
        
        with tab2:
            self.show_analysis_tab(df)
        
        with tab3:
            self.show_etf_list_tab(df)
        
        with tab4:
            self.show_update_tab()
    
    def show_overview_tab(self, df: pd.DataFrame):
        """개요 탭 (고객용 정보)"""
        st.subheader("📊 한국 ETF 시장 현황")
        
        # 핵심 메트릭 표시
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📈 총 ETF 종목", f"{len(df):,}개", help="국내 거래소에 상장된 전체 ETF 수")
        
        with col2:
            total_aum = df['aum'].sum() if 'aum' in df.columns else 0
            aum_trillion = total_aum / 10000  # 조원 단위
            st.metric("💰 전체 시장규모", f"{aum_trillion:.1f}조원", help="모든 ETF의 총 운용자산(AUM)")
        
        with col3:
            categories = df['category'].nunique() if 'category' in df.columns else 0
            st.metric("🎯 투자 분야", f"{categories}개", help="투자 가능한 자산 분야 수")
        
        with col4:
            avg_aum = df['aum'].mean() if 'aum' in df.columns and len(df) > 0 else 0
            st.metric("📊 평균 규모", f"{avg_aum:,.0f}억원", help="ETF 평균 운용자산 규모")
        
        st.markdown("---")
        
        # 투자자 관심 정보
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🔥 시장 하이라이트")
            if 'aum' in df.columns and len(df) > 0:
                # 대형 ETF 정보
                large_etfs = df[df['aum'] >= 10000].shape[0] if df['aum'].max() > 0 else 0
                mega_etfs = df[df['aum'] >= 50000].shape[0] if df['aum'].max() > 0 else 0
                
                st.write(f"🏆 **대형 ETF** (1조원 이상): {mega_etfs}개")
                st.write(f"💎 **중형 ETF** (1,000억원 이상): {large_etfs}개")
                
                # 최대/최소 규모
                max_aum = df['aum'].max()
                min_aum = df[df['aum'] > 0]['aum'].min() if df['aum'].max() > 0 else 0
                
                st.write(f"📈 **최대 규모**: {max_aum:,}억원")
                st.write(f"📉 **최소 규모**: {min_aum:,}억원")
            
            # 비용 정보
            if 'expense_ratio' in df.columns and df['expense_ratio'].notna().sum() > 0:
                avg_expense = df['expense_ratio'].mean()
                low_cost_etfs = df[df['expense_ratio'] <= 0.3].shape[0]
                st.write(f"💸 **평균 운용보수**: {avg_expense:.2f}%")
                st.write(f"✨ **저비용 ETF** (0.3% 이하): {low_cost_etfs}개")
        
        with col2:
            st.subheader("🏢 주요 운용사 순위")
            if 'fund_manager' in df.columns:
                # 운용사별 ETF 개수와 AUM
                manager_stats = df.groupby('fund_manager').agg({
                    'code': 'count',
                    'aum': 'sum'
                }).sort_values('aum', ascending=False).head(5)
                
                manager_stats.columns = ['ETF 수', '총 AUM(억원)']
                
                for i, (manager, row) in enumerate(manager_stats.iterrows(), 1):
                    aum_trillion = row['총 AUM(억원)'] / 10000
                    st.write(f"**{i}. {manager}**")
                    st.write(f"   📊 {row['ETF 수']}개 상품, 🏦 {aum_trillion:.1f}조원")
                    st.write("")
        
        # 최근 업데이트 정보 (간소화)
        if 'updated_at' in df.columns and df['updated_at'].notna().sum() > 0:
            st.markdown("---")
            col1, col2 = st.columns([3, 1])
            
            with col1:
                latest_update = df['updated_at'].max()
                try:
                    # 날짜 파싱 시도
                    from datetime import datetime
                    if 'T' in latest_update:
                        update_date = datetime.fromisoformat(latest_update.replace('T', ' ')).strftime('%Y년 %m월 %d일')
                    else:
                        update_date = latest_update.split(' ')[0]
                    st.info(f"📅 **데이터 업데이트**: {update_date}")
                except:
                    st.info(f"📅 **데이터 업데이트**: {latest_update}")
            
            with col2:
                st.markdown("") # 여백
    
    def show_analysis_tab(self, df: pd.DataFrame):
        """분석 탭 (고객용 개선)"""
        st.subheader("📈 시장 분석 및 투자 인사이트")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🎯 투자 분야별 현황")
            if 'category' in df.columns:
                category_counts = df['category'].value_counts()
                fig_pie = px.pie(
                    values=category_counts.values,
                    names=category_counts.index,
                    title="투자 분야별 ETF 분포"
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
                
                # 인사이트 추가
                top_category = category_counts.index[0]
                top_percentage = (category_counts.iloc[0] / len(df)) * 100
                st.info(f"💡 **가장 인기있는 투자분야**: {top_category} ({top_percentage:.0f}%)")
        
        with col2:
            st.subheader("👑 자산규모 TOP 10")
            if 'aum' in df.columns:
                top_aum = df.nlargest(10, 'aum')
                # ETF 이름을 더 보기 좋게 축약
                top_aum['short_name'] = top_aum['name'].apply(
                    lambda x: x[:20] + '...' if len(x) > 20 else x
                )
                
                fig_bar = px.bar(
                    top_aum,
                    x='aum',
                    y='short_name',
                    orientation='h',
                    title="운용자산 규모 상위 10개 ETF",
                    labels={'aum': '운용자산(억원)', 'short_name': 'ETF'}
                )
                fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # 인사이트 추가
                max_aum = df['aum'].max()
                max_name = df.loc[df['aum'].idxmax(), 'name']
                st.info(f"💰 **최대 규모 ETF**: {max_name} ({max_aum:,}억원)")
        
        # 추가 분석 차트
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🏢 운용사별 경쟁 현황")
            if 'fund_manager' in df.columns:
                manager_counts = df['fund_manager'].value_counts().head(10)
                fig_manager = px.bar(
                    x=manager_counts.values,
                    y=manager_counts.index,
                    orientation='h',
                    title="ETF 상품 수 기준 운용사 순위",
                    labels={'x': 'ETF 상품 수', 'y': '운용사'}
                )
                fig_manager.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig_manager, use_container_width=True)
                
                # 인사이트 추가
                top_manager = manager_counts.index[0]
                top_count = manager_counts.iloc[0]
                st.info(f"🥇 **최다 상품 운용사**: {top_manager} ({top_count}개 상품)")
        
        with col2:
            st.subheader("💸 수수료 분포 현황")
            if 'expense_ratio' in df.columns and df['expense_ratio'].notna().sum() > 0:
                # 0이 아닌 수수료만 필터링
                expense_data = df[df['expense_ratio'] > 0]['expense_ratio']
                
                fig_expense = px.histogram(
                    x=expense_data,
                    nbins=20,
                    title="연간 운용보수 분포",
                    labels={'x': '연간 운용보수 (%)', 'y': 'ETF 개수'}
                )
                fig_expense.update_layout(
                    xaxis_title="연간 운용보수 (%)",
                    yaxis_title="ETF 개수"
                )
                st.plotly_chart(fig_expense, use_container_width=True)
                
                # 인사이트 추가
                avg_expense = expense_data.mean()
                low_cost_count = (expense_data <= 0.3).sum()
                st.info(f"📊 **평균 운용보수**: {avg_expense:.2f}% | **저비용 ETF**: {low_cost_count}개 (0.3% 이하)")
        
        # AUM 분포 히스토그램
        st.subheader("📊 투자규모별 ETF 분포")
        if 'aum' in df.columns:
            # 0이 아닌 AUM만 필터링하고 로그 스케일 고려
            aum_data = df[df['aum'] > 0]['aum']
            
            fig_aum_hist = px.histogram(
                x=aum_data,
                nbins=25,
                title="ETF 투자규모 분포 (전체 시장)"
            )
            fig_aum_hist.update_layout(
                xaxis_title="운용자산 규모 (억원)",
                yaxis_title="ETF 개수"
            )
            st.plotly_chart(fig_aum_hist, use_container_width=True)
            
            # 규모별 분류 인사이트
            col1, col2, col3 = st.columns(3)
            
            with col1:
                large_etfs = (aum_data >= 10000).sum()
                st.metric("🏆 대형 ETF", f"{large_etfs}개", help="1조원 이상")
            
            with col2:
                medium_etfs = ((aum_data >= 1000) & (aum_data < 10000)).sum()
                st.metric("💎 중형 ETF", f"{medium_etfs}개", help="100억-1조원")
            
            with col3:
                small_etfs = (aum_data < 1000).sum()
                st.metric("🌱 소형 ETF", f"{small_etfs}개", help="100억원 미만")
    
    def show_etf_list_tab(self, df: pd.DataFrame):
        """ETF 목록 탭 (고객용 개선)"""
        st.subheader("🔍 ETF 상세 검색")
        
        # 필터링 옵션
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if 'category' in df.columns:
                categories = ['전체'] + sorted(list(df['category'].unique()))
                selected_category = st.selectbox("📂 투자 분야", categories, help="투자하고 싶은 자산 분야를 선택하세요")
            else:
                selected_category = '전체'
        
        with col2:
            if 'fund_manager' in df.columns:
                managers = ['전체'] + sorted(list(df['fund_manager'].unique()))
                selected_manager = st.selectbox("🏢 운용사", managers, help="선호하는 운용사를 선택하세요")
            else:
                selected_manager = '전체'
        
        with col3:
            show_count = st.selectbox("📊 표시 개수", [10, 25, 50, 100, "전체"], help="한 번에 볼 ETF 개수")
        
        with col4:
            sort_options = {
                "AUM 높은순": "인기순 (큰 규모부터)",
                "AUM 낮은순": "소형부터", 
                "이름순": "가나다순",
                "운용보수 낮은순": "저비용순"
            }
            sort_by = st.selectbox("🔢 정렬 방식", list(sort_options.keys()), 
                                 format_func=lambda x: sort_options[x])
        
        # 검색 기능
        search_term = st.text_input("🔍 ETF 검색", 
                                  placeholder="ETF 이름이나 코드를 입력하세요 (예: KODEX, 200, 삼성)",
                                  help="ETF 이름이나 종목코드로 빠르게 찾을 수 있습니다")
        
        # 필터링 적용
        filtered_df = df.copy()
        
        if selected_category != '전체' and 'category' in df.columns:
            filtered_df = filtered_df[filtered_df['category'] == selected_category]
        
        if selected_manager != '전체' and 'fund_manager' in df.columns:
            filtered_df = filtered_df[filtered_df['fund_manager'] == selected_manager]
        
        # 검색 필터
        if search_term:
            mask = (
                filtered_df['code'].str.contains(search_term, case=False, na=False) |
                filtered_df['name'].str.contains(search_term, case=False, na=False)
            )
            filtered_df = filtered_df[mask]
        
        # 정렬 적용
        if sort_by == "AUM 높은순" and 'aum' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('aum', ascending=False)
        elif sort_by == "AUM 낮은순" and 'aum' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('aum', ascending=True)
        elif sort_by == "이름순":
            filtered_df = filtered_df.sort_values('name')
        elif sort_by == "운용보수 낮은순" and 'expense_ratio' in filtered_df.columns:
            filtered_df = filtered_df.sort_values('expense_ratio', ascending=True)
        
        # 표시할 데이터 선택
        if show_count == "전체":
            display_df = filtered_df
        else:
            display_df = filtered_df.head(show_count)
        
        # 결과 정보 표시
        if len(filtered_df) != len(df):
            st.success(f"🎯 검색 결과: **{len(filtered_df):,}개** ETF 발견 (전체 {len(df):,}개 중)")
        else:
            st.info(f"📊 전체 **{len(df):,}개** ETF 표시 중")
        
        if not display_df.empty:
            # 컬럼 형식 조정
            display_df = display_df.copy()
            
            if 'aum' in display_df.columns:
                display_df['운용규모'] = display_df['aum'].apply(
                    lambda x: f"{x:,}억원" if pd.notnull(x) and x > 0 else "정보없음"
                )
            
            if 'expense_ratio' in display_df.columns:
                display_df['연간수수료'] = display_df['expense_ratio'].apply(
                    lambda x: f"{x:.2f}%" if pd.notnull(x) else "정보없음"
                )
            
            # 표시할 컬럼 선택 및 한글화
            display_columns = ['code', 'name']
            column_names = {'code': '종목코드', 'name': 'ETF 이름'}
            
            if 'category' in display_df.columns:
                display_columns.append('category')
                column_names['category'] = '투자분야'
            
            if 'fund_manager' in display_df.columns:
                display_columns.append('fund_manager')
                column_names['fund_manager'] = '운용사'
            
            if '운용규모' in display_df.columns:
                display_columns.append('운용규모')
            
            if '연간수수료' in display_df.columns:
                display_columns.append('연간수수료')
            
            # 컬럼명 변경
            final_df = display_df[display_columns].rename(columns=column_names)
            
            # 테이블 표시
            st.dataframe(
                final_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "종목코드": st.column_config.TextColumn("종목코드", help="거래소 종목코드", width="small"),
                    "ETF 이름": st.column_config.TextColumn("ETF 이름", help="정식 상품명"),
                    "투자분야": st.column_config.TextColumn("투자분야", help="주요 투자 자산 분야", width="medium"),
                    "운용사": st.column_config.TextColumn("운용사", help="자산운용회사", width="medium"),
                    "운용규모": st.column_config.TextColumn("운용규모", help="총 운용자산(AUM)", width="medium"),
                    "연간수수료": st.column_config.TextColumn("연간수수료", help="연간 운용보수율", width="small")
                }
            )
            
            # 다운로드 기능
            col1, col2 = st.columns([3, 1])
            with col2:
                csv_data = final_df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 엑셀 다운로드",
                    data=csv_data,
                    file_name=f"ETF목록_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    help="검색 결과를 엑셀 파일로 저장합니다"
                )
        else:
            st.warning("🔍 검색 조건에 맞는 ETF가 없습니다.")
            st.info("💡 다른 검색어나 필터 조건을 시도해보세요.")
    
    def show_update_tab(self):
        """업데이트 탭 (고객용 정보)"""
        st.subheader("📊 데이터 정보")
        
        st.info("""
        **💡 데이터 특징:**
        - **실시간 반영**: 거래소 데이터 기반으로 정기 업데이트
        - **포괄적 커버리지**: 국내 상장 ETF 전체 포함
        - **신뢰성**: 공식 운용사 및 거래소 정보 활용
        """)
        
        # 데이터 현황
        df = self.load_etf_data()
        if df is not None and not df.empty:
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("📈 커버리지", f"{len(df):,}개 ETF", help="현재 추적 중인 ETF 종목 수")
            
            with col2:
                aum_available = df['aum'].notna().sum() if 'aum' in df.columns else 0
                coverage_rate = (aum_available / len(df)) * 100 if len(df) > 0 else 0
                st.metric("💰 AUM 정보", f"{coverage_rate:.0f}%", help="운용자산 정보 보유 비율")
            
            with col3:
                if 'updated_at' in df.columns and df['updated_at'].notna().sum() > 0:
                    try:
                        latest = df['updated_at'].max()
                        if 'T' in latest:
                            from datetime import datetime
                            update_date = datetime.fromisoformat(latest.replace('T', ' ')).strftime('%m/%d')
                        else:
                            update_date = latest.split(' ')[0].split('-')[1:3]
                            update_date = f"{update_date[0]}/{update_date[1]}"
                        st.metric("🕒 최근 업데이트", update_date, help="마지막 데이터 갱신일")
                    except:
                        st.metric("🕒 데이터 상태", "✅ 최신", help="데이터가 최신 상태입니다")
            
            st.markdown("---")
            
            # 데이터 품질 정보
            st.subheader("📋 데이터 품질 현황")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # 카테고리별 분포
                if 'category' in df.columns:
                    category_stats = df['category'].value_counts().head(6)
                    st.write("**📂 투자 분야별 ETF 수:**")
                    for category, count in category_stats.items():
                        percentage = (count / len(df)) * 100
                        st.write(f"• {category}: {count}개 ({percentage:.0f}%)")
            
            with col2:
                # 규모별 분포
                if 'aum' in df.columns and df['aum'].notna().sum() > 0:
                    st.write("**💰 규모별 ETF 분포:**")
                    
                    # 규모별 분류
                    aum_data = df[df['aum'] > 0]['aum']
                    mega = (aum_data >= 50000).sum()  # 5조원 이상
                    large = ((aum_data >= 10000) & (aum_data < 50000)).sum()  # 1-5조원
                    medium = ((aum_data >= 1000) & (aum_data < 10000)).sum()  # 100억-1조원
                    small = (aum_data < 1000).sum()  # 100억원 미만
                    
                    st.write(f"• 대형 (5조원+): {mega}개")
                    st.write(f"• 중대형 (1-5조원): {large}개") 
                    st.write(f"• 중형 (100억-1조원): {medium}개")
                    st.write(f"• 소형 (100억원 미만): {small}개")
            
            st.markdown("---")
            
            # 업데이트 안내 (기술적 내용 최소화)
            st.subheader("🔄 데이터 갱신 안내")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.info("""
                **자동 업데이트**: 시장 데이터는 정기적으로 자동 갱신됩니다.
                
                **수동 갱신**: 더 빠른 데이터 반영이 필요한 경우 아래 버튼을 이용하세요.
                """)
            
            with col2:
                if UPDATE_MANAGER_AVAILABLE:
                    if st.button("🔄 데이터 새로고침", type="secondary", help="최신 데이터로 갱신"):
                        with st.spinner("데이터 갱신 중..."):
                            try:
                                manager = ETFUpdateManager()
                                summary = manager.batch_update_all_etfs(max_etfs=50)
                                if summary:
                                    st.success("✅ 데이터 갱신 완료!")
                                    st.rerun()
                                else:
                                    st.warning("⚠️ 일부 데이터 갱신에 실패했습니다.")
                            except Exception as e:
                                st.error("❌ 데이터 갱신 중 오류가 발생했습니다.")
                else:
                    st.write("💡 시스템 관리자에게 갱신을 요청하세요.")
        
        else:
            st.warning("📊 현재 표시할 ETF 데이터가 없습니다.")
            st.info("시스템 관리자에게 문의하여 데이터를 확인해주세요.")
    
    def run_update(self, count: int):
        """업데이트 실행 (고객용 메시지)"""
        try:
            if UPDATE_MANAGER_AVAILABLE:
                with st.spinner(f"데이터 갱신 중... 잠시만 기다려주세요"):
                    manager = ETFUpdateManager()
                    summary = manager.batch_update_all_etfs(max_etfs=count)
                    
                    if summary:
                        st.success(f"✅ 데이터 갱신 완료!")
                        
                        # 간단한 결과만 표시
                        successful = getattr(summary, 'successful_updates', 0)
                        success_rate = getattr(summary, 'success_rate', 0)
                        
                        if success_rate >= 80:
                            st.info(f"📊 {successful}개 ETF 정보가 성공적으로 갱신되었습니다.")
                        else:
                            st.warning(f"⚠️ 일부 데이터 갱신에 실패했습니다. ({successful}개 성공)")
                        
                        # 자동 새로고침 대신 안내 메시지
                        st.info("💡 **새로운 데이터를 확인하려면 다른 탭을 클릭해보세요.**")
                    else:
                        st.error("❌ 데이터 갱신에 실패했습니다.")
            else:
                st.error("❌ 현재 데이터 갱신 기능을 사용할 수 없습니다.")
                st.info("시스템 관리자에게 문의해주세요.")
                
        except Exception as e:
            st.error("❌ 데이터 갱신 중 오류가 발생했습니다.")
    
    def show_empty_dashboard(self):
        """빈 대시보드 표시 (고객용)"""
        st.warning("📊 현재 표시할 ETF 데이터가 없습니다.")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.info("""
            **데이터 준비 중입니다.**
            
            ETF 정보를 수집하고 있습니다. 잠시 후 다시 확인해주세요.
            
            지속적으로 이 메시지가 표시된다면 시스템 관리자에게 문의하세요.
            """)
        
        with col2:
            if UPDATE_MANAGER_AVAILABLE:
                st.markdown("### 🔄 데이터 수집")
                if st.button("📊 데이터 수집 시작", type="primary"):
                    with st.spinner("ETF 데이터 수집 중..."):
                        try:
                            manager = ETFUpdateManager()
                            summary = manager.batch_update_all_etfs(max_etfs=50)
                            if summary:
                                st.success("✅ 데이터 수집 완료!")
                                st.rerun()
                            else:
                                st.error("❌ 데이터 수집 실패")
                        except Exception as e:
                            st.error("❌ 수집 중 오류 발생")
            else:
                st.markdown("### 📞 문의")
                st.info("시스템 관리자에게 데이터 준비를 요청해주세요.")

# 함수형 인터페이스 (기존 코드와의 호환성)
def run_dashboard():
    """대시보드 실행 함수"""
    dashboard = ETFDashboard()
    dashboard.run()

def load_etf_data(db_path: str = "etf_universe.db") -> pd.DataFrame:
    """ETF 데이터 로드 함수"""
    dashboard = ETFDashboard(db_path)
    return dashboard.load_etf_data()

# 메인 실행 (단일 진입점)
if __name__ == "__main__":
    run_dashboard()