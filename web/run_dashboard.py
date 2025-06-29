# ==============================================
# 3. web/run_dashboard.py - 실행 스크립트
# ==============================================

import subprocess
import sys
import os

def check_dependencies():
    """필요한 라이브러리 설치 확인"""
    required_packages = [
        'streamlit',
        'plotly', 
        'pandas',
        'numpy'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} 설치됨")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} 설치 필요")
    
    if missing_packages:
        print(f"\n📦 누락된 패키지 설치 중...")
        for package in missing_packages:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print("✅ 모든 패키지 설치 완료!")

def run_dashboard():
    """대시보드 실행"""
    print("🚀 ETF 전략 대시보드를 시작합니다...")
    print("📱 모바일에서도 접속 가능합니다!")
    print("🔗 브라우저에서 자동으로 열립니다...")
    
    # 현재 파일의 디렉토리 기준으로 strategy_dashboard.py 실행
    dashboard_path = os.path.join(os.path.dirname(__file__), 'strategy_dashboard.py')
    
    subprocess.run([
        sys.executable, 
        "-m", 
        "streamlit", 
        "run", 
        dashboard_path,
        "--server.headless", "false",
        "--server.port", "8501",
        "--server.address", "0.0.0.0"  # 모바일 접속 허용
    ])

if __name__ == "__main__":
    print("=" * 50)
    print("📈 ETF 투자전략 대시보드")
    print("=" * 50)
    
    # 의존성 확인
    check_dependencies()
    
    # 대시보드 실행
    run_dashboard()
