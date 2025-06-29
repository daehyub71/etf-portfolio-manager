import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json

# 페이지 설정
st.set_page_config(
    page_title="ETF 전략 성과 비교",
    page_icon="📈",
    layout="wide"
)

# 간단한 CSS (모바일 반응형)
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    
    .metric-card {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #007bff;
        margin: 0.5rem 0;
    }
    
    .strategy-header {
        background: linear-gradient(90deg, #4CAF50, #45a049);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        margin: 1rem 0;
    }
    
    .best-strategy {
        background: linear-gradient(90deg, #ff6b6b, #ee5a52);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        text-align: center;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# 샘플 전략 데이터 생성 함수
@st.cache_data
def get_strategy_data():
    """4가지 전략의 성과 데이터"""
    return {
        "코어-새틀라이트 (80/20)": {
            "연간수익률": 8.2,
            "변동성": 12.5,
            "최대손실": -15.2,
            "샤프비율": 0.62,
            "위험도": "보통",
            "설명": "안정적인 코어 자산 80% + 성장성 새틀라이트 20%",
            "주요ETF": ["KODEX 200", "TIGER 미국S&P500", "KODEX 국고채10년"]
        },
        "글로벌 4분할": {
            "연간수익률": 7.8,
            "변동성": 11.2,
            "최대손실": -13.8,
            "샤프비율": 0.68,
            "위험도": "보통",
            "설명": "국내주식 25% + 해외주식 25% + 국내채권 25% + 해외채권 25%",
            "주요ETF": ["KODEX 200", "TIGER 미국S&P500", "KODEX 국고채10년", "TIGER 해외채권"]
        },
        "생애주기 맞춤 (35세)": {
            "연간수익률": 9.1,
            "변동성": 14.8,
            "최대손실": -18.5,
            "샤프비율": 0.58,
            "위험도": "보통-높음",
            "설명": "연령 기반 자산배분 (주식 65% + 채권 25% + 대안투자 10%)",
            "주요ETF": ["KODEX 200", "KODEX 나스닥100", "KODEX 리츠"]
        },
        "리스크 패리티": {
            "연간수익률": 7.2,
            "변동성": 9.8,
            "최대손실": -11.2,
            "샤프비율": 0.71,
            "위험도": "낮음-보통",
            "설명": "각 자산의 위험 기여도를 동일하게 조정한 고급 분산투자",
            "주요ETF": ["KODEX 200", "KODEX 국고채10년", "KODEX 금"]
        }
    }

@st.cache_data
def generate_performance_data():
    """성과 히스토리 생성 (3년)"""
    dates = pd.date_range(start='2021-01-01', end='2024-01-01', freq='M')
    strategies = get_strategy_data()
    
    np.random.seed(42)
    performance_data = {}
    
    for strategy_name, info in strategies.items():
        annual_return = info['연간수익률']
        volatility = info['변동성']
        
        monthly_returns = np.random.normal(
            annual_return/12, volatility/np.sqrt(12), len(dates)
        )
        cumulative_returns = (1 + monthly_returns/100).cumprod()
        performance_data[strategy_name] = cumulative_returns
    
    return pd.DataFrame(performance_data, index=dates)

def main():
    # 헤더
    st.title("📈 ETF 투자전략 성과 비교")
    st.subheader("직장인을 위한 4가지 자산배분 전략 분석")
    
    strategies = get_strategy_data()
    
    # 전략 카드들 (2x2 그리드)
    st.markdown("---")
    st.header("🎯 전략별 성과 요약")
    
    col1, col2 = st.columns(2)
    strategy_list = list(strategies.items())
    
    for i, (strategy_name, info) in enumerate(strategy_list):
        col = col1 if i % 2 == 0 else col2
        
        with col:
            # 위험도에 따른 색상
            risk_color = {
                "낮음": "🟢",
                "낮음-보통": "🟡", 
                "보통": "🟡",
                "보통-높음": "🟠",
                "높음": "🔴"
            }.get(info['위험도'], "🟡")
            
            st.markdown(f"""
            <div class="metric-card">
                <h4>{strategy_name}</h4>
                <p><strong>📊 연간수익률:</strong> {info['연간수익률']:.1f}%</p>
                <p><strong>📈 샤프비율:</strong> {info['샤프비율']:.2f}</p>
                <p><strong>📉 변동성:</strong> {info['변동성']:.1f}%</p>
                <p><strong>⚠️ 위험도:</strong> {risk_color} {info['위험도']}</p>
                <p style="font-size: 0.9em; color: #666;">{info['설명']}</p>
            </div>
            """, unsafe_allow_html=True)
    
    # 성과 차트
    st.markdown("---")
    st.header("📊 3년간 누적 수익률 비교")
    
    performance_df = generate_performance_data()
    
    fig = go.Figure()
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    for i, strategy in enumerate(performance_df.columns):
        fig.add_trace(go.Scatter(
            x=performance_df.index,
            y=performance_df[strategy],
            mode='lines',
            name=strategy,
            line=dict(color=colors[i], width=3)
        ))
    
    fig.update_layout(
        title="전략별 성과 비교 (2021-2024)",
        xaxis_title="기간",
        yaxis_title="누적 수익률",
        height=500,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # 위험-수익률 분석
    st.markdown("---")
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("🎯 위험 대비 수익률")
        
        strategy_names = list(strategies.keys())
        returns = [strategies[name]['연간수익률'] for name in strategy_names]
        risks = [strategies[name]['변동성'] for name in strategy_names]
        
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=risks,
            y=returns,
            mode='markers+text',
            text=[name.split()[0] for name in strategy_names],  # 짧은 이름
            textposition="top center",
            marker=dict(size=15, color=colors)
        ))
        
        fig2.update_layout(
            xaxis_title="위험도 (변동성, %)",
            yaxis_title="연간 수익률 (%)",
            height=400
        )
        
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        st.header("🏆 추천 전략")
        
        # 샤프 비율이 가장 높은 전략 찾기
        best_strategy = max(strategies.items(), key=lambda x: x[1]['샤프비율'])
        
        st.markdown(f"""
        <div class="best-strategy">
            <h3>{best_strategy[0]}</h3>
            <h2>샤프 비율: {best_strategy[1]['샤프비율']:.2f}</h2>
            <p>연간 수익률: <strong>{best_strategy[1]['연간수익률']:.1f}%</strong></p>
            <p>{best_strategy[1]['설명']}</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.subheader("📋 주요 ETF 구성")
        for etf in best_strategy[1]['주요ETF']:
            st.write(f"• {etf}")
    
    # 성과 지표 비교표
    st.markdown("---")
    st.header("📋 상세 성과 지표 비교")
    
    # 데이터프레임으로 변환
    comparison_data = []
    for strategy_name, info in strategies.items():
        comparison_data.append({
            '전략명': strategy_name,
            '연간수익률(%)': f"{info['연간수익률']:.1f}",
            '변동성(%)': f"{info['변동성']:.1f}",
            '최대손실(%)': f"{info['최대손실']:.1f}",
            '샤프비율': f"{info['샤프비율']:.2f}",
            '위험도': info['위험도']
        })
    
    df = pd.DataFrame(comparison_data)
    st.dataframe(df, use_container_width=True)
    
    # 커스텀 전략 섹션
    st.markdown("---")
    st.header("🎨 나만의 커스텀 전략")
    
    with st.expander("💡 커스텀 전략 시뮬레이터", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.subheader("자산배분 설정")
            domestic_equity = st.slider("국내 주식 (%)", 0, 70, 30)
            foreign_equity = st.slider("해외 주식 (%)", 0, 70, 30)
            bonds = st.slider("채권 (%)", 0, 50, 25)
            alternatives = st.slider("대안투자 (%)", 0, 30, 15)
        
        with col2:
            total = domestic_equity + foreign_equity + bonds + alternatives
            
            if total == 100:
                st.success("✅ 총 합계: 100%")
                
                # 간단한 예상 성과 계산
                expected_return = (domestic_equity * 0.08 + foreign_equity * 0.09 + 
                                 bonds * 0.04 + alternatives * 0.06) / 100
                expected_volatility = (domestic_equity * 0.18 + foreign_equity * 0.16 + 
                                     bonds * 0.08 + alternatives * 0.12) / 100
                expected_sharpe = expected_return / expected_volatility if expected_volatility > 0 else 0
                
                st.metric("예상 연간 수익률", f"{expected_return*100:.1f}%")
                st.metric("예상 변동성", f"{expected_volatility*100:.1f}%")
                st.metric("예상 샤프 비율", f"{expected_sharpe:.2f}")
                
            else:
                st.error(f"❌ 총 합계: {total}% (100%로 맞춰주세요)")
        
        with col3:
            if total == 100:
                # 파이 차트
                fig3 = go.Figure(data=[go.Pie(
                    labels=['국내주식', '해외주식', '채권', '대안투자'],
                    values=[domestic_equity, foreign_equity, bonds, alternatives],
                    hole=.3
                )])
                
                fig3.update_layout(
                    title="내 포트폴리오 구성",
                    height=300
                )
                
                st.plotly_chart(fig3, use_container_width=True)
    
    # 투자 유의사항
    st.markdown("---")
    st.info("""
    **💡 투자 유의사항**
    
    • 과거 성과는 미래 수익을 보장하지 않습니다
    • 투자 전 반드시 투자설명서를 확인하세요
    • 개인의 투자성향과 재무상황을 고려하여 투자결정을 내리세요
    • 분산투자를 통해 위험을 관리하세요
    """)
    
    # 푸터
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <p>📊 ETF 포트폴리오 관리 시스템 | 📱 모바일 최적화 | 🔄 실시간 분석</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()