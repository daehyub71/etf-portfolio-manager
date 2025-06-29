"""
Web 모듈 - ETF 포트폴리오 관리 시스템의 웹 인터페이스

이 모듈은 Streamlit 기반 대시보드와 FastAPI 기반 REST API를 제공합니다.
사용자는 웹 브라우저를 통해 포트폴리오를 관리하고 모니터링할 수 있습니다.
"""

# FastAPI와 Streamlit은 선택적 의존성이므로 try-except로 처리
try:
    from .dashboard import main as run_dashboard
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False
    run_dashboard = None

try:
    from .api_server import app as api_app
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    api_app = None

__version__ = "1.0.0"
__author__ = "ETF Portfolio Manager Team"

# 웹 모듈 컴포넌트 export
__all__ = [
    'run_dashboard',
    'api_app',
    'get_web_capabilities',
    'start_web_services',
    'check_web_dependencies'
]

# 모듈 메타데이터
MODULE_INFO = {
    'name': 'ETF Portfolio Manager Web Interface',
    'description': '웹 기반 사용자 인터페이스 및 API 서버',
    'version': __version__,
    'components': {
        'dashboard': 'Streamlit 기반 실시간 대시보드',
        'api_server': 'FastAPI 기반 REST API 서버'
    },
    'features': [
        '실시간 포트폴리오 모니터링',
        '인터랙티브 차트 및 그래프',
        '전략 비교 및 백테스팅',
        'RESTful API 엔드포인트',
        '모바일 반응형 디자인'
    ]
}

# 웹 서비스 설정
WEB_CONFIG = {
    'dashboard': {
        'title': 'ETF 포트폴리오 매니저',
        'layout': 'wide',
        'initial_sidebar_state': 'expanded',
        'page_icon': '📊',
        'theme': {
            'primaryColor': '#FF6B6B',
            'backgroundColor': '#FFFFFF',
            'secondaryBackgroundColor': '#F0F2F6',
            'textColor': '#262730'
        }
    },
    'api_server': {
        'title': 'ETF Portfolio Manager API',
        'description': 'RESTful API for ETF portfolio management',
        'version': __version__,
        'contact': {
            'name': 'Support Team',
            'email': 'support@etf-manager.com'
        },
        'license': {
            'name': 'MIT',
            'url': 'https://opensource.org/licenses/MIT'
        }
    }
}

def get_web_capabilities():
    """웹 모듈 기능 확인"""
    capabilities = {
        'streamlit_dashboard': STREAMLIT_AVAILABLE,
        'fastapi_server': FASTAPI_AVAILABLE,
        'full_web_stack': STREAMLIT_AVAILABLE and FASTAPI_AVAILABLE
    }
    
    if not capabilities['full_web_stack']:
        missing = []
        if not STREAMLIT_AVAILABLE:
            missing.append('streamlit')
        if not FASTAPI_AVAILABLE:
            missing.append('fastapi')
        
        capabilities['missing_dependencies'] = missing
    
    return capabilities

def check_web_dependencies():
    """웹 의존성 확인"""
    dependencies = {
        'streamlit': False,
        'fastapi': False,
        'uvicorn': False,
        'plotly': False,
        'altair': False
    }
    
    # Streamlit 확인
    try:
        import streamlit
        dependencies['streamlit'] = True
    except ImportError:
        pass
    
    # FastAPI 확인
    try:
        import fastapi
        dependencies['fastapi'] = True
    except ImportError:
        pass
    
    # Uvicorn 확인
    try:
        import uvicorn
        dependencies['uvicorn'] = True
    except ImportError:
        pass
    
    # Plotly 확인
    try:
        import plotly
        dependencies['plotly'] = True
    except ImportError:
        pass
    
    # Altair 확인
    try:
        import altair
        dependencies['altair'] = True
    except ImportError:
        pass
    
    return dependencies

def start_web_services(service_type='dashboard', **kwargs):
    """웹 서비스 시작"""
    if service_type == 'dashboard':
        if not STREAMLIT_AVAILABLE:
            raise ImportError("Streamlit이 설치되지 않았습니다. 'pip install streamlit'으로 설치하세요.")
        
        import subprocess
        import sys
        
        # Streamlit 앱 실행
        dashboard_path = __file__.replace('__init__.py', 'dashboard.py')
        cmd = [sys.executable, '-m', 'streamlit', 'run', dashboard_path]
        
        # 추가 옵션
        if 'port' in kwargs:
            cmd.extend(['--server.port', str(kwargs['port'])])
        if 'host' in kwargs:
            cmd.extend(['--server.address', kwargs['host']])
        
        return subprocess.Popen(cmd)
    
    elif service_type == 'api':
        if not FASTAPI_AVAILABLE:
            raise ImportError("FastAPI가 설치되지 않았습니다. 'pip install fastapi uvicorn'으로 설치하세요.")
        
        import uvicorn
        
        # API 서버 설정
        config = {
            'app': 'web.api_server:app',
            'host': kwargs.get('host', '127.0.0.1'),
            'port': kwargs.get('port', 8000),
            'reload': kwargs.get('reload', True)
        }
        
        uvicorn.run(**config)
    
    else:
        raise ValueError(f"지원하지 않는 서비스 타입: {service_type}")

def get_dashboard_url(host='localhost', port=8501):
    """대시보드 URL 반환"""
    return f"http://{host}:{port}"

def get_api_url(host='localhost', port=8000):
    """API 서버 URL 반환"""
    return f"http://{host}:{port}"

def get_api_docs_url(host='localhost', port=8000):
    """API 문서 URL 반환"""
    return f"http://{host}:{port}/docs"

# 웹 유틸리티 함수들
def format_number_for_display(value, format_type='currency'):
    """웹 표시용 숫자 포맷팅"""
    if format_type == 'currency':
        if value >= 100000000:  # 1억 이상
            return f"{value/100000000:.1f}억원"
        elif value >= 10000:  # 1만 이상
            return f"{value/10000:.0f}만원"
        else:
            return f"{value:,.0f}원"
    
    elif format_type == 'percentage':
        return f"{value:.2f}%"
    
    elif format_type == 'ratio':
        return f"{value:.3f}"
    
    else:
        return f"{value:,.2f}"

def create_color_palette(n_colors=10):
    """차트용 색상 팔레트 생성"""
    colors = [
        '#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7',
        '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9'
    ]
    
    if n_colors <= len(colors):
        return colors[:n_colors]
    else:
        # 색상이 부족하면 반복
        return (colors * ((n_colors // len(colors)) + 1))[:n_colors]

def get_chart_config():
    """차트 기본 설정 반환"""
    return {
        'displayModeBar': False,
        'responsive': True,
        'theme': 'plotly_white',
        'font': {'family': 'Arial, sans-serif'},
        'showTips': False
    }

# 보안 설정
def setup_web_security():
    """웹 보안 설정"""
    security_headers = {
        'X-Content-Type-Options': 'nosniff',
        'X-Frame-Options': 'DENY',
        'X-XSS-Protection': '1; mode=block',
        'Strict-Transport-Security': 'max-age=31536000; includeSubDomains'
    }
    
    return security_headers

# 로깅 설정
import logging

def setup_web_logging(level=logging.INFO):
    """웹 모듈 로깅 설정"""
    logger = logging.getLogger('web')
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    logger.setLevel(level)
    return logger

# 모듈 초기화
logger = setup_web_logging()

def web_health_check():
    """웹 모듈 상태 확인"""
    health_status = {
        'module_loaded': True,
        'dependencies': check_web_dependencies(),
        'capabilities': get_web_capabilities()
    }
    
    # 필수 의존성 확인
    essential_deps = ['streamlit', 'fastapi', 'plotly']
    missing_deps = [dep for dep in essential_deps 
                   if not health_status['dependencies'].get(dep, False)]
    
    if missing_deps:
        health_status['warning'] = f"누락된 의존성: {', '.join(missing_deps)}"
        health_status['recommendation'] = "pip install streamlit fastapi plotly uvicorn"
    
    return health_status

# 설치 가이드
def print_installation_guide():
    """설치 가이드 출력"""
    guide = """
    📦 ETF Portfolio Manager 웹 인터페이스 설치 가이드
    
    필수 패키지 설치:
    pip install streamlit fastapi uvicorn plotly altair
    
    대시보드 실행:
    streamlit run web/dashboard.py
    
    API 서버 실행:
    uvicorn web.api_server:app --reload
    
    또는 Python에서:
    from web import start_web_services
    start_web_services('dashboard', port=8501)
    start_web_services('api', port=8000)
    """
    print(guide)

# 버전 정보
def get_version():
    """현재 버전 반환"""
    return __version__

def get_module_info():
    """모듈 정보 반환"""
    return MODULE_INFO.copy()

def get_web_config():
    """웹 설정 반환"""
    return WEB_CONFIG.copy()

# 초기화 로그
capabilities = get_web_capabilities()
if capabilities['full_web_stack']:
    logger.info(f"Web 모듈 로드 완료 (버전: {__version__})")
else:
    missing = capabilities.get('missing_dependencies', [])
    logger.warning(f"Web 모듈 일부 기능 제한 - 누락된 패키지: {', '.join(missing)}")

# 개발 모드 확인
def is_development_mode():
    """개발 모드 여부 확인"""
    import os
    return os.getenv('ENVIRONMENT', 'production').lower() in ['dev', 'development']

# 프로덕션 설정
def get_production_config():
    """프로덕션 환경 설정"""
    return {
        'debug': False,
        'reload': False,
        'workers': 4,
        'access_log': True,
        'ssl_keyfile': None,
        'ssl_certfile': None
    }