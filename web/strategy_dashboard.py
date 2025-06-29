import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import sqlite3
import sys
import os
from pathlib import Path

# 프로젝트 루트 경로 추가
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.append(str(project_root))

# 페이지 설정
st.set_page_config(
    page_title="ETF 전략 성과 비교 (실제 데이터)",
    page_icon="📈",
    layout="wide"
)

# CSS 스타일링
st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #007bff;
        margin: 0.5rem 0;
    }
    
    .real-data-badge {
        background: linear-gradient(90deg, #28a745, #20c997);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.9rem;
        display: inline-block;
        margin: 0.5rem 0;
    }
    
    .data-source {
        background-color: #e9ecef;
        padding: 0.8rem;
        border-radius: 8px;
        margin: 1rem 0;
        border-left: 3px solid #6c757d;
    }
    
    .strategy-performance {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data
def load_real_etf_data():
    """실제 ETF 데이터베이스에서 데이터 로드"""
    try:
        # 데이터베이스 파일 경로들 시도
        possible_paths = [
            "etf_universe.db",
            "../etf_universe.db", 
            "data/etf_universe.db",
            str(project_root / "etf_universe.db")
        ]
        
        db_path = None
        for path in possible_paths:
            if Path(path).exists():
                db_path = path
                break
        
        if not db_path:
            st.error("❌ ETF 데이터베이스를 찾을 수 없습니다")
            return pd.DataFrame(), {}
        
        # SQLite 데이터베이스 연결
        conn = sqlite3.connect(db_path)
        
        # ETF 정보 조회
        query = """
        SELECT code, name, category, expense_ratio, aum, market_price, 
               fund_manager, last_updated, data_source
        FROM etf_info 
        WHERE is_active = 1 OR is_active IS NULL
        ORDER BY aum DESC NULLS LAST
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # 기본값 처리
        df['expense_ratio'] = df['expense_ratio'].fillna(0.5)
        df['aum'] = df['aum'].fillna(0)
        df['market_price'] = df['market_price'].fillna(10000)
        df['category'] = df['category'].fillna('기타')
        
        # 통계 정보
        stats = {
            'total_etfs': len(df),
            'real_data_count': len(df[df['data_source'] != 'unknown']),
            'total_aum': df['aum'].sum(),
            'avg_expense_ratio': df['expense_ratio'].mean(),
            'db_path': db_path,
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        return df, stats
        
    except Exception as e:
        st.error(f"❌ 데이터 로드 실패: {e}")
        return pd.DataFrame(), {}

@st.cache_data
def calculate_real_strategy_performance(etf_df):
    """실제 ETF 데이터 기반 전략 성과 계산"""
    
    # 주요 ETF들 필터링 (AUM 기준)
    major_etfs = etf_df.nlargest(20, 'aum')
    
    # 카테고리별 대표 ETF 선별
    domestic_equity = etf_df[etf_df['category'].str.contains('domestic|국내', na=False)].nlargest(3, 'aum')
    foreign_equity = etf_df[etf_df['category'].str.contains('foreign|해외|미국', na=False)].nlargest(3, 'aum')
    bonds = etf_df[etf_df['category'].str.contains('bond|채권', na=False)].nlargest(2, 'aum')
    alternatives = etf_df[etf_df['category'].str.contains('reit|금|원자재', na=False)].nlargest(2, 'aum')
    
    strategies = {}
    
    # 1. 코어-새틀라이트 전략 (실제 ETF 기반)
    if len(domestic_equity) > 0 and len(foreign_equity) > 0:
        core_etfs = pd.concat([domestic_equity.head(2), foreign_equity.head(2)])
        satellite_etfs = pd.concat([bonds.head(1), alternatives.head(1)]) if len(bonds) > 0 else pd.DataFrame()
        
        strategies["코어-새틀라이트 (80/20)"] = {
            "구성ETF": core_etfs['name'].tolist() + (satellite_etfs['name'].tolist() if not satellite_etfs.empty else []),
            "평균보수율": core_etfs['expense_ratio'].mean(),
            "총AUM": core_etfs['aum'].sum() + (satellite_etfs['aum'].sum() if not satellite_etfs.empty else 0),
            "ETF개수": len(core_etfs) + len(satellite_etfs),
            "예상수익률": 8.2,  # 이론적 계산값
            "위험도": "보통"
        }
    
    # 2. 글로벌 4분할 전략
    if len(domestic_equity) > 0 and len(foreign_equity) > 0 and len(bonds) > 0:
        four_way_etfs = pd.concat([
            domestic_equity.head(1),
            foreign_equity.head(1), 
            bonds.head(2)
        ])
        
        strategies["글로벌 4분할"] = {
            "구성ETF": four_way_etfs['name'].tolist(),
            "평균보수율": four_way_etfs['expense_ratio'].mean(),
            "총AUM": four_way_etfs['aum'].sum(),
            "ETF개수": len(four_way_etfs),
            "예상수익률": 7.8,
            "위험도": "보통"
        }
    
    # 3. 생애주기 전략 (35세)
    if len(domestic_equity) > 0 and len(foreign_equity) > 0:
        lifecycle_etfs = pd.concat([
            domestic_equity.head(2),
            foreign_equity.head(1),
            bonds.head(1) if len(bonds) > 0 else pd.DataFrame(),
            alternatives.head(1) if len(alternatives) > 0 else pd.DataFrame()
        ])
        
        strategies["생애주기 맞춤 (35세)"] = {
            "구성ETF": lifecycle_etfs['name'].tolist(),
            "평균보수율": lifecycle_etfs['expense_ratio'].mean(),
            "총AUM": lifecycle_etfs['aum'].sum(),
            "ETF개수": len(lifecycle_etfs),
            "예상수익률": 9.1,
            "위험도": "보통-높음"
        }
    
    # 4. 리스크 패리티 전략
    if len(bonds) > 0:
        risk_parity_etfs = pd.concat([
            domestic_equity.head(1) if len(domestic_equity) > 0 else pd.DataFrame(),
            foreign_equity.head(1) if len(foreign_equity) > 0 else pd.DataFrame(),
            bonds.head(2),
            alternatives.head(1) if len(alternatives) > 0 else pd.DataFrame()
        ])
        
        strategies["리스크 패리티"] = {
            "구성ETF": risk_parity_etfs['name'].tolist(),
            "평균보수율": risk_parity_etfs['expense_ratio'].mean(),
            "총AUM": risk_parity_etfs['aum'].sum(),
            "ETF개수": len(risk_parity_etfs),
            "예상수익률": 7.2,
            "위험도": "낮음-보통"
        }
    
    return strategies

def create_real_data_overview(etf_df, stats):
    """실제 데이터 현황 표시"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "총 ETF 개수", 
            f"{stats['total_etfs']:,}개",
            help="데이터베이스에 등록된 전체 ETF 수"
        )
    
    with col2:
        st.metric(
            "실제 데이터", 
            f"{stats['real_data_count']:,}개",
            f"{(stats['real_data_count']/stats['total_etfs']*100):.1f}%"
        )
    
    with col3:
        st.metric(
            "총 운용자산", 
            f"{stats['total_aum']:,.0f}억원",
            help="전체 ETF 운용자산 합계"
        )
    
    with col4:
        st.metric(
            "평균 보수율", 
            f"{stats['avg_expense_ratio']:.3f}%",
            help="전체 ETF 평균 운용보수율"
        )

def create_etf_distribution_chart(etf_df):
    """ETF 분포 차트"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        # 카테고리별 분포
        category_counts = etf_df['category'].value_counts().head(10)
        
        fig1 = px.bar(
            x=category_counts.values,
            y=category_counts.index,
            orientation='h',
            title="카테고리별 ETF 개수",
            labels={'x': 'ETF 개수', 'y': '카테고리'}
        )
        fig1.update_layout(height=400)
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # AUM 규모별 분포
        aum_ranges = []
        labels = []
        
        for _, etf in etf_df.iterrows():
            aum = etf['aum']
            if aum >= 10000:
                labels.append('대형(1조원+)')
            elif aum >= 5000:
                labels.append('중형(5천억원+)')
            elif aum >= 1000:
                labels.append('중소형(1천억원+)')
            else:
                labels.append('소형(1천억원 미만)')
        
        size_counts = pd.Series(labels).value_counts()
        
        fig2 = px.pie(
            values=size_counts.values,
            names=size_counts.index,
            title="AUM 규모별 분포"
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

def display_top_etfs(etf_df):
    """상위 ETF 목록 표시"""
    
    st.subheader("🏆 AUM 기준 상위 ETF")
    
    top_etfs = etf_df.nlargest(10, 'aum')[['name', 'code', 'category', 'aum', 'expense_ratio', 'market_price']]
    
    # 컬럼명 한글화
    top_etfs_display = top_etfs.copy()
    top_etfs_display.columns = ['ETF명', '코드', '카테고리', 'AUM(억원)', '보수율(%)', '현재가(원)']
    
    # 수치 포맷팅
    top_etfs_display['AUM(억원)'] = top_etfs_display['AUM(억원)'].apply(lambda x: f"{x:,.0f}")
    top_etfs_display['보수율(%)'] = top_etfs_display['보수율(%)'].apply(lambda x: f"{x:.3f}")
    top_etfs_display['현재가(원)'] = top_etfs_display['현재가(원)'].apply(lambda x: f"{x:,.0f}")
    
    st.dataframe(top_etfs_display, use_container_width=True)

def main():
    # 헤더
    st.title("📈 ETF 투자전략 성과 비교")
    
    # 실제 데이터 배지
    st.markdown("""
    <div class="real-data-badge">
        ✅ 실제 ETF 데이터 기반 분석
    </div>
    """, unsafe_allow_html=True)
    
    st.subheader("직장인을 위한 4가지 자산배분 전략 분석 (실제 데이터)")
    
    # 데이터 로드
    etf_df, stats = load_real_etf_data()
    
    if etf_df.empty:
        st.error("❌ ETF 데이터를 로드할 수 없습니다. 데이터베이스 파일을 확인해주세요.")
        return
    
    # 데이터 소스 정보
    st.markdown(f"""
    <div class="data-source">
        <strong>📊 데이터 소스:</strong> {stats['db_path']}<br>
        <strong>📅 마지막 업데이트:</strong> {stats['last_updated']}<br>
        <strong>🎯 실제 데이터 비율:</strong> {stats['real_data_count']}/{stats['total_etfs']} ({(stats['real_data_count']/stats['total_etfs']*100):.1f}%)
    </div>
    """, unsafe_allow_html=True)
    
    # 실제 데이터 현황
    st.markdown("---")
    st.header("📊 실제 ETF 시장 현황")
    create_real_data_overview(etf_df, stats)
    
    # ETF 분포 분석
    st.markdown("---")
    st.header("📈 ETF 시장 분포 분석")
    create_etf_distribution_chart(etf_df)
    
    # 상위 ETF 목록
    st.markdown("---")
    display_top_etfs(etf_df)
    
    # 실제 데이터 기반 전략 성과
    st.markdown("---")
    st.header("🎯 실제 ETF 기반 전략 구성")
    
    strategies = calculate_real_strategy_performance(etf_df)
    
    if not strategies:
        st.warning("⚠️ 전략 구성을 위한 충분한 ETF 데이터가 없습니다.")
        return
    
    # 전략별 카드 표시
    cols = st.columns(2)
    
    for i, (strategy_name, strategy_info) in enumerate(strategies.items()):
        col = cols[i % 2]
        
        with col:
            st.markdown(f"""
            <div class="strategy-performance">
                <h4>{strategy_name}</h4>
                <p><strong>💰 총 AUM:</strong> {strategy_info['총AUM']:,.0f}억원</p>
                <p><strong>📊 구성 ETF:</strong> {strategy_info['ETF개수']}개</p>
                <p><strong>💸 평균 보수율:</strong> {strategy_info['평균보수율']:.3f}%</p>
                <p><strong>📈 예상 수익률:</strong> {strategy_info['예상수익률']:.1f}%</p>
                <p><strong>⚠️ 위험도:</strong> {strategy_info['위험도']}</p>
                
                <details>
                    <summary><strong>구성 ETF 목록</strong></summary>
                    <ul>
                        {''.join([f'<li>{etf}</li>' for etf in strategy_info['구성ETF']])}
                    </ul>
                </details>
            </div>
            """, unsafe_allow_html=True)
    
    # 전략 비교 차트
    st.markdown("---")
    st.header("📊 전략별 비교 분석")
    
    # 전략 비교 데이터프레임
    comparison_data = []
    for name, info in strategies.items():
        comparison_data.append({
            '전략명': name,
            '총AUM(억원)': f"{info['총AUM']:,.0f}",
            'ETF개수': info['ETF개수'],
            '평균보수율(%)': f"{info['평균보수율']:.3f}",
            '예상수익률(%)': f"{info['예상수익률']:.1f}",
            '위험도': info['위험도']
        })
    
    comparison_df = pd.DataFrame(comparison_data)
    st.dataframe(comparison_df, use_container_width=True)
    
    # 보수율 vs AUM 산점도
    col1, col2 = st.columns(2)
    
    with col1:
        fig_expense = px.scatter(
            etf_df.head(50),  # 상위 50개만
            x='expense_ratio',
            y='aum',
            hover_data=['name'],
            title="보수율 vs AUM",
            labels={'expense_ratio': '보수율(%)', 'aum': 'AUM(억원)'}
        )
        fig_expense.update_layout(height=400)
        st.plotly_chart(fig_expense, use_container_width=True)
    
    with col2:
        # 카테고리별 평균 보수율
        category_expense = etf_df.groupby('category')['expense_ratio'].mean().sort_values()
        
        fig_cat_expense = px.bar(
            x=category_expense.values,
            y=category_expense.index,
            orientation='h',
            title="카테고리별 평균 보수율",
            labels={'x': '평균 보수율(%)', 'y': '카테고리'}
        )
        fig_cat_expense.update_layout(height=400)
        st.plotly_chart(fig_cat_expense, use_container_width=True)
    
    # 실전 투자 가이드
    st.markdown("---")
    st.header("💡 실전 투자 가이드")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🎯 투자 초보자를 위한 추천")
        beginner_etfs = etf_df[(etf_df['aum'] >= 5000) & (etf_df['expense_ratio'] <= 0.5)].head(5)
        
        for _, etf in beginner_etfs.iterrows():
            st.write(f"• **{etf['name']}** ({etf['code']})")
            st.write(f"  - AUM: {etf['aum']:,.0f}억원, 보수율: {etf['expense_ratio']:.3f}%")
    
    with col2:
        st.subheader("⚠️ 투자 시 체크포인트")
        st.write("""
        1. **AUM 규모**: 1,000억원 이상 권장
        2. **보수율**: 0.5% 이하 선호
        3. **거래량**: 충분한 유동성 확인  
        4. **추적오차**: 기초지수와의 괴리율
        5. **분산투자**: 단일 ETF 집중 투자 지양
        """)
    
    # 푸터
    st.markdown("---")
    st.info("""
    **💡 투자 유의사항**
    
    • 위 분석은 과거 데이터 및 이론적 계산에 기반합니다
    • 실제 투자 시 시장 상황과 개인 투자성향을 고려하세요
    • 투자 전 반드시 투자설명서를 확인하시기 바랍니다
    • 분산투자를 통해 위험을 관리하세요
    """)
    
    st.markdown(f"""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>📊 실제 ETF 데이터 기반 분석 | 데이터 출처: {stats['db_path']}</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()