# ==========================================
# main.py - 안전한 ETF 시스템 런처 (완전 수정 버전)
# ==========================================

import sys
import os
import time
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path

# 콘솔 인코딩 설정 (Windows 안전)
try:
    if sys.platform.startswith('win'):
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())
except Exception:
    pass  # 인코딩 설정 실패해도 계속 진행

# 프로젝트 루트 경로 설정
PROJECT_ROOT = Path(__file__).parent.absolute()
sys.path.insert(0, str(PROJECT_ROOT))

# 모듈 import 상태 추적
modules_loaded = {}

# ETFUpdateManager import 시도 (안전하게)
try:
    from core.update_manager import ETFUpdateManager
    modules_loaded['UpdateManager'] = True
    print("✅ ETFUpdateManager 사용 가능")
except ImportError as e:
    modules_loaded['UpdateManager'] = False
    print(f"⚠️ UpdateManager import 실패: {e}")
    
    # 더미 UpdateManager 클래스
    class DummyUpdateManager:
        def __init__(self, **kwargs):
            self.db_path = kwargs.get('db_path', 'etf_universe.db')
        
        def batch_update_all_etfs(self, **kwargs):
            max_etfs = kwargs.get('max_etfs', 683)
            delay = kwargs.get('delay_between_updates', 1.0)
            
            print(f"🔧 더미 모드: {max_etfs}개 ETF 업데이트 시뮬레이션")
            print(f"⏱️ 지연시간: {delay}초")
            
            # 시뮬레이션
            import random
            time.sleep(1)  # 간단한 시뮬레이션
            
            class DummySummary:
                def __init__(self):
                    self.total_etfs = max_etfs or 683
                    self.successful_updates = int(self.total_etfs * 0.85)
                    self.failed_updates = self.total_etfs - self.successful_updates
                    self.success_rate = (self.successful_updates / self.total_etfs) * 100
                    self.total_aum = self.successful_updates * 3000  # 평균 3000억원
                    self.total_duration = self.total_etfs * 0.1  # 0.1초씩
            
            return DummySummary()
        
        def get_etf_statistics(self):
            return {
                'basic_stats': {
                    'total_etfs': 683,
                    'total_aum': 2000000
                }
            }
    
    ETFUpdateManager = DummyUpdateManager

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
        """로깅 설정 (안전하게)"""
        try:
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
        except Exception as e:
            # 로깅 설정 실패해도 계속 진행
            print(f"⚠️ 로깅 설정 실패 (계속 진행): {e}")
            logging.basicConfig(level=logging.INFO)
    
    def initialize_components(self) -> bool:
        """시스템 컴포넌트 초기화"""
        print("🔧 시스템 컴포넌트 초기화 중...")
        
        try:
            # UpdateManager 초기화 (더미든 실제든)
            self.update_manager = ETFUpdateManager(
                db_path=self.db_path,
                max_workers=self.max_workers
            )
            
            if modules_loaded.get('UpdateManager'):
                print("✅ 실제 UpdateManager 초기화 완료")
            else:
                print("🔧 더미 UpdateManager 초기화 완료")
            
            return True
                
        except Exception as e:
            self.logger.error(f"컴포넌트 초기화 실패: {e}")
            print(f"❌ 컴포넌트 초기화 실패: {e}")
            return False
    
    def run_update(self, max_etfs: int = None, force: bool = False, delay: float = 1.0) -> bool:
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
        print(f"  - 지연시간: {delay}초")
        print(f"  - 데이터베이스: {self.db_path}")
        print(f"  - 예상 소요시간: {target_etfs * delay / 60:.1f}분")
        print(f"  - 모드: {'실제 데이터' if modules_loaded.get('UpdateManager') else '더미 모드'}")
        
        # 사용자 확인 (force 모드가 아닌 경우)
        if not force and target_etfs > 50:
            print(f"\n⚠️ 경고: {target_etfs}개 ETF 업데이트는 시간이 소요됩니다")
            try:
                confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
                if confirm not in ['y', 'yes']:
                    print("❌ 업데이트가 취소되었습니다")
                    return False
            except (KeyboardInterrupt, EOFError):
                print("\n❌ 업데이트가 취소되었습니다")
                return False
        
        try:
            # 시작 시간 기록
            start_time = datetime.now()
            print(f"⏰ 업데이트 시작: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 실제 업데이트 실행
            summary = self.update_manager.batch_update_all_etfs(
                max_etfs=max_etfs,
                delay_between_updates=delay,
                batch_size=50,
                delay_between_batches=max(delay, 1.0)
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
                    print(f"💰 평균 AUM: {total_aum/successful:,.0f}억원" if successful > 0 else "")
                
                # 성공률에 따른 메시지
                if success_rate >= 90:
                    print("🌟 훌륭한 성과로 업데이트되었습니다!")
                elif success_rate >= 80:
                    print("👍 성공적으로 업데이트되었습니다!")
                elif success_rate >= 60:
                    print("✅ 대부분의 ETF가 업데이트되었습니다.")
                else:
                    print("⚠️ 일부 ETF 업데이트에 실패했습니다.")
                
                # 683개 전체 업데이트인 경우
                if target_etfs >= 683:
                    print("\n🌐 전체 ETF 수집 완료!")
                    print("📊 이제 대시보드에서 결과를 확인할 수 있습니다:")
                    print("   python main.py dashboard")
                
                # 결과 저장
                self._save_simple_summary(summary, duration)
                
                return True
            else:
                print("❌ 업데이트 결과를 받지 못했습니다")
                return False
            
        except KeyboardInterrupt:
            print("\n❌ 사용자에 의해 업데이트가 중단되었습니다")
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
                'total_aum': getattr(summary, 'total_aum', 0),
                'mode': 'real' if modules_loaded.get('UpdateManager') else 'dummy'
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
            print("\n🔧 해결 방법:")
            print("1. web/dashboard.py 파일이 있는지 확인")
            print("2. 올바른 디렉토리에서 실행하고 있는지 확인")
            return False
        
        try:
            import subprocess
            
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
            status = "✅ 사용가능" if loaded else "🔧 더미모드"
            print(f"  - {module}: {status}")
        
        # 파일 상태
        print(f"\n📁 파일 상태:")
        files_to_check = [
            ('데이터베이스', self.db_path),
            ('로그디렉토리', 'logs/'),
            ('결과디렉토리', 'results/'),
            ('대시보드', 'web/dashboard.py')
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
        print(f"  - 실행 모드: {'실제 데이터' if modules_loaded.get('UpdateManager') else '더미 모드'}")

def create_parser():
    """명령행 인수 파서 생성"""
    parser = argparse.ArgumentParser(
        description="ETF 포트폴리오 관리 시스템 (683개 ETF 지원)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
사용 예시:
  python main.py update --max-etfs 683 --force --delay 1.5
  python main.py quick --count 50
  python main.py dashboard --port 8501
  python main.py status
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='실행할 명령')
    
    # update 명령
    update_parser = subparsers.add_parser('update', help='ETF 데이터 업데이트')
    update_parser.add_argument('--max-etfs', type=int, default=683, 
                              help='업데이트할 최대 ETF 수 (기본: 683)')
    update_parser.add_argument('--force', action='store_true',
                              help='확인 없이 강제 실행')
    update_parser.add_argument('--delay', type=float, default=1.0,
                              help='ETF간 지연시간(초) (기본: 1.0)')
    
    # quick 명령
    quick_parser = subparsers.add_parser('quick', help='빠른 업데이트')
    quick_parser.add_argument('--count', type=int, default=50,
                             help='업데이트할 ETF 수 (기본: 50)')
    quick_parser.add_argument('--delay', type=float, default=0.5,
                             help='ETF간 지연시간(초) (기본: 0.5)')
    
    # status 명령
    subparsers.add_parser('status', help='시스템 상태 확인')
    
    # dashboard 명령
    dashboard_parser = subparsers.add_parser('dashboard', help='웹 대시보드 실행')
    dashboard_parser.add_argument('--port', type=int, default=8501,
                                 help='대시보드 포트 (기본: 8501)')
    dashboard_parser.add_argument('--host', type=str, default='localhost',
                                 help='대시보드 호스트 (기본: localhost)')
    
    return parser

def main():
    """메인 함수"""
    print("🚀 ETF 포트폴리오 관리 시스템 v3.1")
    print("📊 683개 ETF 완전 수집 지원")
    print("=" * 60)
    
    try:
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
                    force=args.force,
                    delay=args.delay
                )
                sys.exit(0 if success else 1)
            else:
                print("❌ 시스템 초기화 실패")
                sys.exit(1)
                
        elif args.command == 'quick':
            if launcher.initialize_components():
                count = getattr(args, 'count', 50)
                delay = getattr(args, 'delay', 0.5)
                success = launcher.run_update(max_etfs=count, force=True, delay=delay)
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
    
    except KeyboardInterrupt:
        print("\n👋 사용자에 의해 종료되었습니다")
        sys.exit(0)
    except Exception as e:
        print(f"❌ 시스템 실행 중 오류: {e}")
        sys.exit(1)

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
                    print("⚡ 빠른 업데이트 (50개 ETF)")
                    launcher.run_update(max_etfs=50, force=True, delay=0.5)
                
            elif choice == "3":
                if launcher.initialize_components():
                    print("📈 200개 ETF 업데이트")
                    confirm = input("계속 진행하시겠습니까? (y/N): ")
                    if confirm.lower() == 'y':
                        launcher.run_update(max_etfs=200, force=False, delay=1.0)
                
            elif choice == "4":
                if launcher.initialize_components():
                    print("🌐 전체 683개 ETF 업데이트")
                    print("⚠️ 이 작업은 30-45분 소요될 수 있습니다")
                    confirm = input("정말로 전체 업데이트를 실행하시겠습니까? (y/N): ")
                    if confirm.lower() == 'y':
                        launcher.run_update(max_etfs=None, force=False, delay=1.5)
            
            elif choice == "5":
                print("📊 웹 대시보드 실행")
                try:
                    port_input = input("포트 번호 (기본: 8501): ").strip()
                    port = int(port_input) if port_input.isdigit() else 8501
                    launcher.run_dashboard(port=port)
                except ValueError:
                    launcher.run_dashboard(port=8501)
                
            elif choice == "6":
                print("👋 ETF 시스템을 종료합니다")
                break
                
            else:
                print("❌ 1-6 중에서 선택해주세요")
                
        except (KeyboardInterrupt, EOFError):
            print("\n👋 ETF 시스템을 종료합니다")
            break
        except Exception as e:
            print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    main()