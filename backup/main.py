# ==========================================
# main.py - 안전한 ETF 시스템 런처 (문법 오류 해결)
# ==========================================

import sys
import os
import time
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path

# 콘솔 인코딩 설정 (Windows)
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# 프로젝트 루트 경로 설정
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

# 모듈 import 상태 추적
modules_loaded = {}

# ETFUpdateManager import 시도
try:
    from core.update_manager import ETFUpdateManager
    modules_loaded['UpdateManager'] = True
    print("✅ ETFUpdateManager 사용 가능")
except ImportError as e:
    modules_loaded['UpdateManager'] = False
    print(f"⚠️ UpdateManager import 실패: {e}")

class SafeETFLauncher:
    """안전한 ETF 시스템 런처"""
    
    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # 기본 설정
        self.db_path = "etf_universe.db"
        self.max_workers = 5
        
        # 컴포넌트 초기화
        self.update_manager = None
        
        print("🚀 ETF 시스템 런처 초기화 완료")
    
    def setup_logging(self):
        """로깅 설정"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # 로그 디렉토리 생성
        log_dir = PROJECT_ROOT / 'logs'
        log_dir.mkdir(exist_ok=True)
        
        # 로그 파일 설정
        log_file = log_dir / f'etf_system_{datetime.now().strftime("%Y%m%d")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
    
    def initialize_components(self) -> bool:
        """시스템 컴포넌트 초기화"""
        print("🔧 시스템 컴포넌트 초기화 중...")
        
        try:
            # UpdateManager 초기화
            if modules_loaded.get('UpdateManager'):
                self.update_manager = ETFUpdateManager(
                    db_path=self.db_path,
                    max_workers=self.max_workers
                )
                print("✅ UpdateManager 초기화 완료")
                return True
            else:
                print("❌ UpdateManager 초기화 실패")
                return False
                
        except Exception as e:
            self.logger.error(f"컴포넌트 초기화 실패: {e}")
            print(f"❌ 컴포넌트 초기화 실패: {e}")
            return False
    
    def run_update(self, max_etfs: int = None, force: bool = False) -> bool:
        """안전한 ETF 업데이트 실행"""
        print("\n" + "="*60)
        print("🚀 ETF 업데이트 시작")
        print("="*60)
        
        if not self.update_manager:
            print("❌ UpdateManager가 초기화되지 않았습니다")
            return False
        
        # 설정 표시
        target_etfs = max_etfs if max_etfs else 683
        print(f"📊 업데이트 설정:")
        print(f"  - 대상 ETF: {target_etfs}개")
        print(f"  - 데이터베이스: {self.db_path}")
        print(f"  - 예상 소요시간: {target_etfs * 0.5 / 60:.1f}분")
        
        # 사용자 확인 (force 모드가 아닌 경우)
        if not force and target_etfs > 50:
            print(f"\n⚠️ 경고: {target_etfs}개 ETF 업데이트는 시간이 소요됩니다")
            confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                print("❌ 업데이트가 취소되었습니다")
                return False
        
        try:
            # 시작 시간 기록
            start_time = datetime.now()
            print(f"⏰ 업데이트 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 실제 업데이트 실행
            summary = self.update_manager.batch_update_all_etfs(
                max_etfs=max_etfs,
                batch_size=50,
                delay_between_updates=0.3,
                delay_between_batches=1.0
            )
            
            # 결과 처리
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            if summary:
                # 안전한 속성 접근
                total_etfs = getattr(summary, 'total_etfs', 0)
                successful = getattr(summary, 'successful_updates', 0)
                failed = getattr(summary, 'failed_updates', 0)
                success_rate = getattr(summary, 'success_rate', 0.0)
                total_aum = getattr(summary, 'total_aum', 0)
                
                print(f"\n🎉 업데이트 완료!")
                print(f"⏰ 총 소요시간: {duration/60:.1f}분")
                print(f"📊 총 ETF: {total_etfs}개")
                print(f"✅ 성공: {successful}개 ({success_rate:.1f}%)")
                print(f"❌ 실패: {failed}개")
                
                if total_aum > 0:
                    print(f"💰 총 AUM: {total_aum:,}억원")
                
                # 성공률에 따른 메시지
                if success_rate >= 80:
                    print("🌟 성공적으로 업데이트되었습니다!")
                elif success_rate >= 60:
                    print("👍 대부분의 ETF가 업데이트되었습니다.")
                else:
                    print("⚠️ 일부 ETF 업데이트에 실패했습니다.")
                
                # 결과 저장
                self._save_simple_summary(summary, duration)
                
                return True
            else:
                print("❌ 업데이트 결과를 받지 못했습니다")
                return False
            
        except Exception as e:
            self.logger.error(f"업데이트 실행 실패: {e}")
            print(f"❌ 업데이트 실행 실패: {e}")
            return False
    
    def _save_simple_summary(self, summary, duration: float):
        """간단한 요약 저장"""
        try:
            results_dir = PROJECT_ROOT / 'results'
            results_dir.mkdir(exist_ok=True)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            result_file = results_dir / f'update_summary_{timestamp}.json'
            
            # 안전한 데이터 추출
            summary_data = {
                'timestamp': timestamp,
                'duration_minutes': round(duration / 60, 2),
                'total_etfs': getattr(summary, 'total_etfs', 0),
                'successful_updates': getattr(summary, 'successful_updates', 0),
                'failed_updates': getattr(summary, 'failed_updates', 0),
                'success_rate': getattr(summary, 'success_rate', 0.0),
                'total_aum': getattr(summary, 'total_aum', 0)
            }
            
            with open(result_file, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, indent=2, ensure_ascii=False)
            
            print(f"📄 업데이트 요약 저장: {result_file}")
            
        except Exception as e:
            print(f"⚠️ 요약 저장 실패 (업데이트는 완료): {e}")
    
    def run_dashboard(self, port: int = 8501, host: str = 'localhost') -> bool:
        """웹 대시보드 실행"""
        print(f"\n📊 ETF 웹 대시보드 시작")
        print(f"🌐 호스트: {host}")
        print(f"🔌 포트: {port}")
        print(f"📱 브라우저 주소: http://{host}:{port}")
        
        # dashboard.py 파일 경로 확인
        dashboard_file = PROJECT_ROOT / 'web' / 'dashboard.py'
        
        if not dashboard_file.exists():
            print("❌ 대시보드 파일을 찾을 수 없습니다")
            print(f"   경로: {dashboard_file}")
            return False
        
        try:
            import subprocess
            import sys
            
            print("🚀 Streamlit 대시보드 실행 중...")
            print("   (브라우저가 자동으로 열립니다)")
            print("   (종료하려면 Ctrl+C 를 누르세요)")
            
            # Streamlit 실행 명령
            cmd = [
                sys.executable, '-m', 'streamlit', 'run', 
                str(dashboard_file),
                '--server.port', str(port),
                '--server.address', host,
                '--server.headless', 'false',  # 브라우저 자동 열기
                '--theme.base', 'light'
            ]
            
            # 실행
            result = subprocess.run(cmd, check=True)
            return result.returncode == 0
            
        except subprocess.CalledProcessError as e:
            print(f"❌ 대시보드 실행 실패: {e}")
            print("\n🔧 해결 방법:")
            print("1. Streamlit 설치: pip install streamlit")
            print("2. 포트 변경: python main.py dashboard --port 8502")
            print("3. 직접 실행: streamlit run web/dashboard.py --server.port 8502")
            return False
        except KeyboardInterrupt:
            print("\n👋 대시보드가 종료되었습니다")
            return True
        except Exception as e:
            print(f"❌ 대시보드 실행 중 오류: {e}")
            return False
    
    def show_status(self):
        """시스템 상태 표시"""
        print("\n📋 ETF 시스템 상태")
        print("-" * 50)
        
        # 모듈 상태
        print("📦 모듈 상태:")
        for module, loaded in modules_loaded.items():
            status = "✅ 사용가능" if loaded else "❌ 불가능"
            print(f"  - {module}: {status}")
        
        # 파일 상태
        print(f"\n📁 파일 상태:")
        files_to_check = [
            ('데이터베이스', self.db_path),
            ('로그디렉토리', 'logs/'),
            ('결과디렉토리', 'results/')
        ]
        
        for desc, path in files_to_check:
            file_path = PROJECT_ROOT / path
            exists = file_path.exists()
            status = "✅ 존재" if exists else "❌ 없음"
            print(f"  - {desc}: {status}")
        
        # ETF 통계 (가능한 경우)
        if self.update_manager:
            try:
                stats = self.update_manager.get_etf_statistics()
                if stats and 'basic_stats' in stats:
                    basic = stats['basic_stats']
                    print(f"\n📊 ETF 통계:")
                    print(f"  - 총 ETF 수: {basic.get('total_etfs', 0):,}개")
                    print(f"  - 총 AUM: {basic.get('total_aum', 0):,}억원")
            except Exception as e:
                print(f"  - ETF 통계: 조회 실패 ({e})")
        
        print(f"\n🖥️ 시스템 정보:")
        print(f"  - Python 버전: {sys.version.split()[0]}")
        print(f"  - 프로젝트 경로: {PROJECT_ROOT}")

def create_parser():
    """명령행 인수 파서 생성"""
    parser = argparse.ArgumentParser(
        description="ETF 포트폴리오 관리 시스템 (683개 ETF 지원)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='실행할 명령')
    
    # update 명령
    update_parser = subparsers.add_parser('update', help='ETF 데이터 업데이트')
    update_parser.add_argument('--max-etfs', type=int, default=683, 
                              help='업데이트할 최대 ETF 수 (기본: 683)')
    update_parser.add_argument('--force', action='store_true',
                              help='확인 없이 강제 실행')
    
    # quick 명령
    quick_parser = subparsers.add_parser('quick', help='빠른 업데이트')
    quick_parser.add_argument('--count', type=int, default=50,
                             help='업데이트할 ETF 수 (기본: 50)')
    
    # status 명령
    subparsers.add_parser('status', help='시스템 상태 확인')
    
    # dashboard 명령 추가
    dashboard_parser = subparsers.add_parser('dashboard', help='웹 대시보드 실행')
    dashboard_parser.add_argument('--port', type=int, default=8501,
                                 help='대시보드 포트 (기본: 8501)')
    dashboard_parser.add_argument('--host', type=str, default='localhost',
                                 help='대시보드 호스트 (기본: localhost)')
    
    return parser

def main():
    """메인 함수"""
    print("🚀 ETF 포트폴리오 관리 시스템 v3.0")
    print("📊 683개 ETF 완전 수집 지원")
    print("=" * 60)
    
    # 명령행 인수 파싱
    parser = create_parser()
    args = parser.parse_args()
    
    # 시스템 런처 초기화
    launcher = SafeETFLauncher()
    
    # 명령 처리
    if args.command == 'update':
        if launcher.initialize_components():
            success = launcher.run_update(
                max_etfs=args.max_etfs if args.max_etfs != 683 else None,
                force=args.force
            )
            sys.exit(0 if success else 1)
        else:
            print("❌ 시스템 초기화 실패")
            sys.exit(1)
            
    elif args.command == 'quick':
        if launcher.initialize_components():
            count = getattr(args, 'count', 50)
            success = launcher.run_update(max_etfs=count, force=True)
            sys.exit(0 if success else 1)
        else:
            print("❌ 시스템 초기화 실패")
            sys.exit(1)
            
    elif args.command == 'status':
        if launcher.initialize_components():
            launcher.show_status()
        else:
            print("❌ 시스템 초기화 실패")
        sys.exit(0)
    
    elif args.command == 'dashboard':
        # 대시보드는 초기화 없이도 실행 가능
        port = getattr(args, 'port', 8501)
        host = getattr(args, 'host', 'localhost')
        success = launcher.run_dashboard(port=port, host=host)
        sys.exit(0 if success else 1)
        
    else:
        # 명령이 없으면 대화형 메뉴
        run_interactive_menu(launcher)

def run_interactive_menu(launcher):
    """대화형 메뉴 실행"""
    while True:
        print(f"\n{'='*50}")
        print("🎯 ETF 시스템 메인 메뉴")
        print("="*50)
        print("1. 📊 시스템 상태 확인")
        print("2. ⚡ 빠른 업데이트 (50개)")
        print("3. 📈 중간 업데이트 (200개)")
        print("4. 🌐 전체 업데이트 (683개)")
        print("5. 📊 웹 대시보드 실행")
        print("6. 👋 종료")
        
        try:
            choice = input("\n메뉴 선택 (1-6): ").strip()
            
            if choice == "1":
                if launcher.initialize_components():
                    launcher.show_status()
                
            elif choice == "2":
                if launcher.initialize_components():
                    launcher.run_update(max_etfs=50, force=True)
                
            elif choice == "3":
                if launcher.initialize_components():
                    print("📈 200개 ETF 업데이트")
                    confirm = input("계속 진행하시겠습니까? (y/N): ")
                    if confirm.lower() == 'y':
                        launcher.run_update(max_etfs=200, force=False)
                
            elif choice == "4":
                if launcher.initialize_components():
                    print("🌐 전체 683개 ETF 업데이트")
                    print("⚠️ 이 작업은 30-45분 소요될 수 있습니다")
                    confirm = input("정말로 전체 업데이트를 실행하시겠습니까? (y/N): ")
                    if confirm.lower() == 'y':
                        launcher.run_update(max_etfs=None, force=False)
            
            elif choice == "5":
                print("📊 웹 대시보드 실행")
                port_input = input("포트 번호 (기본: 8501): ").strip()
                port = int(port_input) if port_input.isdigit() else 8501
                launcher.run_dashboard(port=port)
                
            elif choice == "6":
                print("👋 ETF 시스템을 종료합니다")
                break
                
            else:
                print("❌ 1-6 중에서 선택해주세요")
                
        except KeyboardInterrupt:
            print("\n👋 ETF 시스템을 종료합니다")
            break
        except Exception as e:
            print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()