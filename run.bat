REM ==========================================
REM run.bat - Windows 실행 스크립트
REM ==========================================

@echo off
chcp 65001 >nul
title ETF 장기투자 관리 시스템

echo.
echo ╔══════════════════════════════════════════════════════════════╗
echo ║                🚀 ETF 장기투자 관리 시스템                   ║
echo ║                     Windows 실행 스크립트                     ║
echo ╚══════════════════════════════════════════════════════════════╝
echo.

REM 파이썬 설치 확인
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python이 설치되지 않았습니다.
    echo 💡 https://python.org 에서 Python 3.8+ 버전을 설치해주세요.
    pause
    exit /b 1
)

echo ✅ Python 버전 확인:
python --version

REM 가상환경 생성 및 활성화 (선택적)
if not exist "venv" (
    echo.
    echo 🔧 가상환경을 생성하시겠습니까? (권장)
    set /p create_venv="y/N: "
    if /i "!create_venv!"=="y" (
        echo 📦 가상환경 생성 중...
        python -m venv venv
        echo ✅ 가상환경 생성 완료
    )
)

if exist "venv" (
    echo 🔄 가상환경 활성화 중...
    call venv\Scripts\activate.bat
    echo ✅ 가상환경 활성화 완료
)

REM 필수 패키지 설치 확인
echo.
echo 📦 필요한 패키지 설치 확인 중...
pip show pandas >nul 2>&1
if errorlevel 1 (
    echo ⚠️  필수 패키지가 설치되지 않았습니다.
    echo 💡 패키지를 설치하시겠습니까?
    set /p install_packages="Y/n: "
    if not "!install_packages!"=="n" (
        echo 📥 패키지 설치 중...
        pip install pandas numpy scipy PyYAML requests schedule
        echo ✅ 기본 패키지 설치 완료
        
        echo.
        echo 📊 웹 대시보드용 패키지를 설치하시겠습니까? (권장)
        set /p install_web="Y/n: "
        if not "!install_web!"=="n" (
            pip install streamlit plotly
            echo ✅ 웹 대시보드 패키지 설치 완료
        )
        
        echo.
        echo 🌐 실시간 데이터 수집용 패키지를 설치하시겠습니까?
        set /p install_data="Y/n: "
        if not "!install_data!"=="n" (
            pip install pykrx apscheduler
            echo ✅ 데이터 수집 패키지 설치 완료
        )
    )
)

REM 실행 모드 선택
echo.
echo 🎯 실행 모드를 선택하세요:
echo 1. 대화형 메뉴 (추천)
echo 2. 웹 대시보드
echo 3. ETF 데이터 업데이트
echo 4. 스케줄러 시작
echo 5. 시스템 상태 확인
echo.
set /p mode="선택 (1-5): "

if "%mode%"=="1" (
    echo 🚀 대화형 메뉴 시작...
    python main.py
) else if "%mode%"=="2" (
    echo 🌐 웹 대시보드 시작...
    python main.py --mode dashboard --dashboard-mode web
) else if "%mode%"=="3" (
    echo 📊 ETF 데이터 업데이트...
    python main.py --mode update
) else if "%mode%"=="4" (
    echo ⏰ 스케줄러 시작...
    python main.py --mode scheduler
) else if "%mode%"=="5" (
    echo 🔍 시스템 상태 확인...
    python main.py --mode status
) else (
    echo ❌ 잘못된 선택입니다. 기본 대화형 메뉴를 시작합니다.
    python main.py
)

echo.
echo 📝 실행이 완료되었습니다.
pause