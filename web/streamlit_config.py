# ==============================================
# 1. web/streamlit_config.py - Streamlit 설정
# ==============================================

import streamlit as st
import os

def configure_streamlit():
    """Streamlit 기본 설정"""
    
    # 페이지 설정
    st.set_page_config(
        page_title="ETF 투자전략 분석",
        page_icon="📈",
        layout="wide",
        initial_sidebar_state="collapsed",
        menu_items={
            'Get Help': 'https://github.com/your-repo',
            'Report a bug': 'https://github.com/your-repo/issues',
            'About': """
            # ETF 포트폴리오 관리 시스템
            
            직장인을 위한 스마트한 ETF 투자전략 분석 도구
            
            - 📊 4가지 검증된 투자전략 비교
            - 📱 모바일 최적화 인터페이스  
            - 🎯 개인 맞춤형 포트폴리오 추천
            - 📈 실시간 성과 모니터링
            """
        }
    )
    
    # 사이드바 숨기기 (모바일 최적화)
    hide_sidebar = """
    <style>
        section[data-testid="stSidebar"] {
            display: none !important;
        }
    </style>
    """
    st.markdown(hide_sidebar, unsafe_allow_html=True)