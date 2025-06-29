# ==========================================
# scheduler.py - 정기적 스케줄링 시스템
# ==========================================

import schedule
import time
import threading
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable
import sys
import os

# APScheduler import 시도
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.interval import IntervalTrigger
    APSCHEDULER_AVAILABLE = True
    print("✅ APScheduler 사용 가능")
except ImportError:
    APSCHEDULER_AVAILABLE = False
    print("⚠️ APScheduler 없음 - 기본 스케줄러 사용")
    print("pip install apscheduler 로 설치하면 고급 스케줄링 기능 사용 가능")

# 프로젝트 모듈 import 시도
try:
    from update_manager import ETFUpdateManager
except ImportError:
    print("⚠️ update_manager를 찾을 수 없습니다. 기본 업데이트 관리자를 사용합니다.")
    
    class DummyUpdateManager:
        """더미 업데이트 관리자 (실제 모듈이 없을 때 사용)"""
        def batch_update_all_etfs(self, max_etfs=None, delay_between_updates=1.0):
            print(f"🔄 더미 업데이트 실행 (최대 {max_etfs}개 ETF)")
            time.sleep(1)  # 시뮬레이션
            
            # 더미 결과 반환
            class DummySummary:
                total_etfs = max_etfs or 100
                successful_updates = int(0.8 * (max_etfs or 100))
                failed_updates = int(0.2 * (max_etfs or 100))
                success_rate = 80.0
            
            return DummySummary()
        
        def quick_health_check(self):
            return {
                "health_score": 85.0,
                "status": "healthy",
                "total_etfs": 100,
                "updated_etfs": 80
            }
    
    ETFUpdateManager = DummyUpdateManager

class ETFScheduler:
    """ETF 데이터 업데이트 스케줄러"""
    
    def __init__(self, config_file: str = "scheduler_config.json"):
        self.config_file = config_file
        self.update_manager = ETFUpdateManager()
        self.scheduler = None
        self.is_running = False
        self.jobs = {}
        
        # 로깅 설정
        self.logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        
        # 설정 로드
        self.config = self.load_config()
        
        # 스케줄러 초기화
        self.init_scheduler()
        
        self.logger.info("📅 ETF 스케줄러 초기화 완료")
    
    def load_config(self) -> Dict:
        """스케줄러 설정 로드"""
        default_config = {
            "schedules": {
                "daily_update": {
                    "enabled": True,
                    "time": "18:00",  # 장마감 후
                    "timezone": "Asia/Seoul",
                    "max_etfs": None,
                    "delay": 1.0
                },
                "weekly_full_update": {
                    "enabled": True,
                    "day": "sunday",
                    "time": "02:00",
                    "timezone": "Asia/Seoul",
                    "max_etfs": None,
                    "delay": 0.5
                },
                "quick_check": {
                    "enabled": True,
                    "interval_hours": 4,
                    "max_etfs": 10,
                    "delay": 0.3
                }
            },
            "notifications": {
                "email_enabled": False,
                "slack_enabled": False,
                "log_level": "INFO"
            },
            "safety": {
                "max_consecutive_failures": 5,
                "auto_disable_on_failure": True,
                "health_check_interval": 1  # 시간
            }
        }
        
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # 기본값과 병합
                    for key, value in default_config.items():
                        if key not in config:
                            config[key] = value
                        elif isinstance(value, dict):
                            for subkey, subvalue in value.items():
                                if subkey not in config[key]:
                                    config[key][subkey] = subvalue
                self.logger.info(f"📋 설정 파일 로드: {self.config_file}")
                return config
            else:
                # 기본 설정 파일 생성
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(default_config, f, ensure_ascii=False, indent=2)
                self.logger.info(f"📝 기본 설정 파일 생성: {self.config_file}")
                return default_config
                
        except Exception as e:
            self.logger.error(f"❌ 설정 로드 실패: {e}")
            return default_config
    
    def save_config(self):
        """설정 저장"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.config, f, ensure_ascii=False, indent=2)
            self.logger.info(f"💾 설정 저장 완료: {self.config_file}")
        except Exception as e:
            self.logger.error(f"❌ 설정 저장 실패: {e}")
    
    def init_scheduler(self):
        """스케줄러 초기화"""
        if APSCHEDULER_AVAILABLE:
            self.scheduler = BackgroundScheduler()
            self.logger.info("🔧 APScheduler 초기화")
        else:
            self.scheduler = None
            self.logger.info("🔧 기본 스케줄러 사용")
    
    def schedule_daily_update(self):
        """일일 업데이트 스케줄"""
        config = self.config["schedules"]["daily_update"]
        
        if not config["enabled"]:
            return
        
        def daily_job():
            try:
                self.logger.info("🌅 일일 업데이트 시작")
                summary = self.update_manager.batch_update_all_etfs(
                    max_etfs=config.get("max_etfs"),
                    delay_between_updates=config.get("delay", 1.0)
                )
                if summary:
                    self.logger.info(f"🌅 일일 업데이트 완료: {summary.success_rate:.1f}% 성공")
                    self.send_notification(f"일일 업데이트 완료: {summary.success_rate:.1f}% 성공")
            except Exception as e:
                self.logger.error(f"❌ 일일 업데이트 실패: {e}")
                self.send_notification(f"일일 업데이트 실패: {e}")
        
        if APSCHEDULER_AVAILABLE and self.scheduler:
            hour, minute = map(int, config["time"].split(":"))
            self.scheduler.add_job(
                daily_job,
                CronTrigger(hour=hour, minute=minute),
                id="daily_update",
                replace_existing=True
            )
            self.jobs["daily_update"] = daily_job
            self.logger.info(f"📅 일일 업데이트 스케줄 설정: 매일 {config['time']}")
        else:
            # 기본 스케줄러 사용
            schedule.every().day.at(config["time"]).do(daily_job)
            self.jobs["daily_update"] = daily_job
            self.logger.info(f"📅 일일 업데이트 스케줄 설정: 매일 {config['time']} (기본 스케줄러)")
    
    def schedule_weekly_update(self):
        """주간 전체 업데이트 스케줄"""
        config = self.config["schedules"]["weekly_full_update"]
        
        if not config["enabled"]:
            return
        
        def weekly_job():
            try:
                self.logger.info("📅 주간 전체 업데이트 시작")
                summary = self.update_manager.batch_update_all_etfs(
                    max_etfs=config.get("max_etfs"),
                    delay_between_updates=config.get("delay", 0.5)
                )
                if summary:
                    self.logger.info(f"📅 주간 업데이트 완료: {summary.success_rate:.1f}% 성공")
                    self.send_notification(f"주간 전체 업데이트 완료: {summary.success_rate:.1f}% 성공")
            except Exception as e:
                self.logger.error(f"❌ 주간 업데이트 실패: {e}")
                self.send_notification(f"주간 업데이트 실패: {e}")
        
        if APSCHEDULER_AVAILABLE and self.scheduler:
            day_map = {
                "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
                "friday": 4, "saturday": 5, "sunday": 6
            }
            hour, minute = map(int, config["time"].split(":"))
            day_of_week = day_map.get(config["day"].lower(), 6)
            
            self.scheduler.add_job(
                weekly_job,
                CronTrigger(day_of_week=day_of_week, hour=hour, minute=minute),
                id="weekly_update",
                replace_existing=True
            )
            self.jobs["weekly_update"] = weekly_job
            self.logger.info(f"📅 주간 업데이트 스케줄 설정: 매주 {config['day']} {config['time']}")
        else:
            # 기본 스케줄러 사용
            getattr(schedule.every(), config["day"].lower()).at(config["time"]).do(weekly_job)
            self.jobs["weekly_update"] = weekly_job
            self.logger.info(f"📅 주간 업데이트 스케줄 설정: 매주 {config['day']} {config['time']} (기본 스케줄러)")
    
    def schedule_quick_check(self):
        """빠른 상태 체크 스케줄"""
        config = self.config["schedules"]["quick_check"]
        
        if not config["enabled"]:
            return
        
        def quick_job():
            try:
                self.logger.info("⚡ 빠른 상태 체크 시작")
                
                # 시스템 상태 체크
                health = self.update_manager.quick_health_check()
                
                if health.get("health_score", 0) < 50:
                    # 상태가 좋지 않으면 일부 ETF만 업데이트
                    summary = self.update_manager.batch_update_all_etfs(
                        max_etfs=config.get("max_etfs", 10),
                        delay_between_updates=config.get("delay", 0.3)
                    )
                    if summary:
                        self.logger.info(f"⚡ 빠른 업데이트 완료: {summary.successful_updates}개 성공")
                else:
                    self.logger.info(f"⚡ 시스템 상태 양호: {health.get('health_score', 0):.1f}%")
                    
            except Exception as e:
                self.logger.error(f"❌ 빠른 체크 실패: {e}")
        
        if APSCHEDULER_AVAILABLE and self.scheduler:
            self.scheduler.add_job(
                quick_job,
                IntervalTrigger(hours=config["interval_hours"]),
                id="quick_check",
                replace_existing=True
            )
            self.jobs["quick_check"] = quick_job
            self.logger.info(f"⚡ 빠른 체크 스케줄 설정: {config['interval_hours']}시간 간격")
        else:
            # 기본 스케줄러 사용
            schedule.every(config["interval_hours"]).hours.do(quick_job)
            self.jobs["quick_check"] = quick_job
            self.logger.info(f"⚡ 빠른 체크 스케줄 설정: {config['interval_hours']}시간 간격 (기본 스케줄러)")
    
    def start(self):
        """스케줄러 시작"""
        if self.is_running:
            self.logger.warning("⚠️ 스케줄러가 이미 실행 중입니다")
            return
        
        try:
            # 모든 스케줄 설정
            self.schedule_daily_update()
            self.schedule_weekly_update()
            self.schedule_quick_check()
            
            if APSCHEDULER_AVAILABLE and self.scheduler:
                self.scheduler.start()
                self.is_running = True
                self.logger.info("🚀 APScheduler 시작됨")
            else:
                # 기본 스케줄러 백그라운드 실행
                self.is_running = True
                self.schedule_thread = threading.Thread(target=self._run_basic_scheduler, daemon=True)
                self.schedule_thread.start()
                self.logger.info("🚀 기본 스케줄러 시작됨")
            
            print("🚀 ETF 스케줄러가 시작되었습니다!")
            self.print_schedule_info()
            
        except Exception as e:
            self.logger.error(f"❌ 스케줄러 시작 실패: {e}")
            raise
    
    def _run_basic_scheduler(self):
        """기본 스케줄러 실행 (schedule 라이브러리)"""
        while self.is_running:
            schedule.run_pending()
            time.sleep(60)  # 1분마다 체크
    
    def stop(self):
        """스케줄러 중지"""
        if not self.is_running:
            self.logger.warning("⚠️ 스케줄러가 실행 중이 아닙니다")
            return
        
        try:
            if APSCHEDULER_AVAILABLE and self.scheduler:
                self.scheduler.shutdown()
            
            self.is_running = False
            self.logger.info("🛑 스케줄러 중지됨")
            print("🛑 ETF 스케줄러가 중지되었습니다!")
            
        except Exception as e:
            self.logger.error(f"❌ 스케줄러 중지 실패: {e}")
    
    def print_schedule_info(self):
        """스케줄 정보 출력"""
        print("\n📅 설정된 스케줄:")
        
        config = self.config["schedules"]
        
        if config["daily_update"]["enabled"]:
            print(f"   🌅 일일 업데이트: 매일 {config['daily_update']['time']}")
        
        if config["weekly_full_update"]["enabled"]:
            print(f"   📅 주간 전체 업데이트: 매주 {config['weekly_full_update']['day']} {config['weekly_full_update']['time']}")
        
        if config["quick_check"]["enabled"]:
            print(f"   ⚡ 빠른 체크: {config['quick_check']['interval_hours']}시간 간격")
        
        if APSCHEDULER_AVAILABLE and self.scheduler:
            print(f"\n📋 활성 작업:")
            for job in self.scheduler.get_jobs():
                print(f"   - {job.id}: {job.next_run_time}")
    
    def send_notification(self, message: str):
        """알림 발송"""
        try:
            # 로그 메시지
            self.logger.info(f"📢 알림: {message}")
            
            # 이메일 알림 (구현 예정)
            if self.config["notifications"]["email_enabled"]:
                pass  # 이메일 발송 로직
            
            # 슬랙 알림 (구현 예정)
            if self.config["notifications"]["slack_enabled"]:
                pass  # 슬랙 발송 로직
                
        except Exception as e:
            self.logger.error(f"❌ 알림 발송 실패: {e}")
    
    def manual_trigger(self, job_name: str):
        """수동 작업 트리거"""
        if job_name in self.jobs:
            try:
                self.logger.info(f"🔧 수동 실행: {job_name}")
                self.jobs[job_name]()
                return True
            except Exception as e:
                self.logger.error(f"❌ 수동 실행 실패 ({job_name}): {e}")
                return False
        else:
            self.logger.warning(f"⚠️ 알 수 없는 작업: {job_name}")
            return False
    
    def get_status(self) -> Dict:
        """스케줄러 상태 조회"""
        status = {
            "is_running": self.is_running,
            "scheduler_type": "APScheduler" if APSCHEDULER_AVAILABLE else "Basic",
            "active_jobs": list(self.jobs.keys()),
            "config": self.config
        }
        
        if APSCHEDULER_AVAILABLE and self.scheduler:
            status["jobs_info"] = [
                {
                    "id": job.id,
                    "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
                    "trigger": str(job.trigger)
                }
                for job in self.scheduler.get_jobs()
            ]
        
        return status


# ==========================================
# 이름 호환성을 위한 별칭
# ==========================================

# main.py에서 Scheduler로 import할 수 있도록 별칭 생성
Scheduler = ETFScheduler

# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("📅 ETF 스케줄러 테스트")
    print("=" * 60)
    
    # 스케줄러 초기화
    scheduler = ETFScheduler()
    
    # 현재 설정 출력
    print("\n⚙️ 현재 설정:")
    config = scheduler.config
    for category, settings in config.items():
        print(f"📋 {category}:")
        if isinstance(settings, dict):
            for key, value in settings.items():
                print(f"   - {key}: {value}")
        else:
            print(f"   {settings}")
    
    # 상태 확인
    print("\n📊 스케줄러 상태:")
    status = scheduler.get_status()
    print(f"- 실행 중: {'예' if status['is_running'] else '아니오'}")
    print(f"- 스케줄러 타입: {status['scheduler_type']}")
    print(f"- 활성 작업: {', '.join(status['active_jobs']) if status['active_jobs'] else '없음'}")
    
    # 사용자 선택
    print("\n🎯 테스트 옵션:")
    print("1. 스케줄러 시작")
    print("2. 수동 작업 실행")
    print("3. 설정 수정")
    print("4. 종료")
    
    try:
        while True:
            choice = input("\n선택 (1-4): ").strip()
            
            if choice == "1":
                print("\n🚀 스케줄러 시작...")
                scheduler.start()
                
                print("\n⏰ 스케줄러가 백그라운드에서 실행 중입니다.")
                print("Ctrl+C로 중지할 수 있습니다.")
                
                try:
                    while True:
                        time.sleep(10)
                        # 10초마다 상태 출력
                        current_time = datetime.now().strftime("%H:%M:%S")
                        print(f"[{current_time}] 스케줄러 실행 중... (Ctrl+C로 중지)")
                        
                except KeyboardInterrupt:
                    print("\n\n🛑 사용자에 의해 중지됨")
                    scheduler.stop()
                    break
            
            elif choice == "2":
                print("\n🔧 수동 작업 실행:")
                jobs = list(scheduler.jobs.keys())
                if jobs:
                    for i, job in enumerate(jobs):
                        print(f"{i+1}. {job}")
                    
                    try:
                        job_choice = int(input("작업 선택: ")) - 1
                        if 0 <= job_choice < len(jobs):
                            job_name = jobs[job_choice]
                            print(f"\n실행 중: {job_name}")
                            success = scheduler.manual_trigger(job_name)
                            if success:
                                print(f"✅ {job_name} 완료")
                            else:
                                print(f"❌ {job_name} 실패")
                        else:
                            print("❌ 잘못된 선택")
                    except ValueError:
                        print("❌ 숫자를 입력해주세요")
                else:
                    print("❌ 설정된 작업이 없습니다")
            
            elif choice == "3":
                print("\n⚙️ 설정 수정:")
                print("현재 설정 파일:", scheduler.config_file)
                print("설정 파일을 직접 수정하고 스케줄러를 재시작하세요.")
                
            elif choice == "4":
                print("\n👋 종료합니다")
                if scheduler.is_running:
                    scheduler.stop()
                break
            
            else:
                print("❌ 1-4 중에서 선택해주세요")
    
    except KeyboardInterrupt:
        print("\n\n🛑 프로그램 중단됨")
        if scheduler.is_running:
            scheduler.stop()
    
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        if scheduler.is_running:
            scheduler.stop()
    
    print(f"\n✅ ETF 스케줄러 테스트 완료!")
    print(f"💡 다음 단계:")
    print(f"   - streamlit run dashboard.py : 실시간 모니터링 대시보드")
    print(f"   - 설정 파일 수정으로 스케줄 커스터마이징")