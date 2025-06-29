# ==========================================
# notification_system.py - 알림 시스템
# ==========================================

import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from typing import Dict, List, Optional, Union
from dataclasses import dataclass
import logging
import os

@dataclass
class NotificationMessage:
    """알림 메시지"""
    title: str
    content: str
    priority: str = "normal"  # low, normal, high, urgent
    category: str = "general"  # general, rebalancing, performance, system
    recipient: str = ""
    attachments: List[str] = None
    
@dataclass
class NotificationConfig:
    """알림 설정"""
    email_enabled: bool = False
    email_smtp_server: str = ""
    email_smtp_port: int = 587
    email_username: str = ""
    email_password: str = ""
    email_from: str = ""
    email_to: List[str] = None
    
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = ""
    
    webhook_enabled: bool = False
    webhook_url: str = ""
    webhook_headers: Dict[str, str] = None

class NotificationSystem:
    """통합 알림 시스템"""
    
    def __init__(self, config_path: str = "notification_config.json"):
        self.config_path = config_path
        self.config = self.load_config()
        self.logger = logging.getLogger(__name__)
        
        # 알림 히스토리
        self.notification_history = []
        self.max_history = 1000
        
        self.logger.info("📢 알림 시스템 초기화 완료")
    
    def load_config(self) -> NotificationConfig:
        """설정 로드"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
                
                return NotificationConfig(
                    email_enabled=config_data.get('email', {}).get('enabled', False),
                    email_smtp_server=config_data.get('email', {}).get('smtp_server', ''),
                    email_smtp_port=config_data.get('email', {}).get('smtp_port', 587),
                    email_username=config_data.get('email', {}).get('username', ''),
                    email_password=config_data.get('email', {}).get('password', ''),
                    email_from=config_data.get('email', {}).get('from_email', ''),
                    email_to=config_data.get('email', {}).get('to_emails', []),
                    
                    slack_enabled=config_data.get('slack', {}).get('enabled', False),
                    slack_webhook_url=config_data.get('slack', {}).get('webhook_url', ''),
                    slack_channel=config_data.get('slack', {}).get('channel', ''),
                    
                    webhook_enabled=config_data.get('webhook', {}).get('enabled', False),
                    webhook_url=config_data.get('webhook', {}).get('url', ''),
                    webhook_headers=config_data.get('webhook', {}).get('headers', {})
                )
            else:
                # 기본 설정 파일 생성
                self.create_default_config()
                return NotificationConfig()
                
        except Exception as e:
            self.logger.error(f"❌ 알림 설정 로드 실패: {e}")
            return NotificationConfig()
    
    def create_default_config(self):
        """기본 설정 파일 생성"""
        default_config = {
            "email": {
                "enabled": False,
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587,
                "username": "your_email@gmail.com",
                "password": "your_app_password",
                "from_email": "your_email@gmail.com",
                "to_emails": ["recipient@gmail.com"]
            },
            "slack": {
                "enabled": False,
                "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL",
                "channel": "#etf-alerts"
            },
            "webhook": {
                "enabled": False,
                "url": "https://your-webhook-url.com",
                "headers": {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer YOUR_TOKEN"
                }
            },
            "preferences": {
                "rebalancing_threshold": 5.0,
                "performance_alert_enabled": True,
                "system_alert_enabled": True,
                "daily_summary_enabled": False,
                "weekly_summary_enabled": True
            }
        }
        
        try:
            with open(self.config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, ensure_ascii=False, indent=2)
            self.logger.info(f"📝 기본 알림 설정 파일 생성: {self.config_path}")
        except Exception as e:
            self.logger.error(f"❌ 기본 설정 파일 생성 실패: {e}")
    
    def send_notification(self, message: NotificationMessage) -> bool:
        """통합 알림 발송"""
        success = True
        
        try:
            # 알림 히스토리에 추가
            self.notification_history.append({
                'timestamp': datetime.now().isoformat(),
                'title': message.title,
                'category': message.category,
                'priority': message.priority,
                'recipient': message.recipient
            })
            
            # 히스토리 크기 제한
            if len(self.notification_history) > self.max_history:
                self.notification_history = self.notification_history[-self.max_history:]
            
            # 이메일 발송
            if self.config.email_enabled:
                email_success = self.send_email(message)
                success = success and email_success
            
            # 슬랙 발송
            if self.config.slack_enabled:
                slack_success = self.send_slack(message)
                success = success and slack_success
            
            # 웹훅 발송
            if self.config.webhook_enabled:
                webhook_success = self.send_webhook(message)
                success = success and webhook_success
            
            if success:
                self.logger.info(f"✅ 알림 발송 성공: {message.title}")
            else:
                self.logger.warning(f"⚠️ 일부 알림 발송 실패: {message.title}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ 알림 발송 실패: {e}")
            return False
    
    def send_email(self, message: NotificationMessage) -> bool:
        """이메일 발송"""
        try:
            if not self.config.email_enabled or not self.config.email_to:
                return True  # 비활성화시 성공으로 처리
            
            # 이메일 메시지 구성
            msg = MIMEMultipart()
            msg['From'] = self.config.email_from
            msg['To'] = ', '.join(self.config.email_to)
            msg['Subject'] = f"[ETF 시스템] {message.title}"
            
            # 우선순위에 따른 제목 꾸미기
            priority_prefix = {
                'low': '📘',
                'normal': '📊',
                'high': '⚠️',
                'urgent': '🚨'
            }.get(message.priority, '📊')
            
            msg['Subject'] = f"{priority_prefix} [ETF 시스템] {message.title}"
            
            # HTML 이메일 본문 생성
            html_body = f"""
            <html>
            <head></head>
            <body>
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px;">
                        <h2 style="color: #2c3e50; margin-bottom: 20px;">
                            {priority_prefix} {message.title}
                        </h2>
                        <div style="background-color: white; padding: 20px; border-radius: 5px; 
                                    border-left: 4px solid #3498db;">
                            <p style="color: #34495e; line-height: 1.6; margin-bottom: 15px;">
                                {message.content.replace('\n', '<br>')}
                            </p>
                        </div>
                        <div style="margin-top: 20px; padding-top: 15px; border-top: 1px solid #e9ecef;">
                            <p style="color: #6c757d; font-size: 12px; margin: 0;">
                                발송 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
                                카테고리: {message.category} | 우선순위: {message.priority}<br>
                                ETF 장기투자 관리 시스템 v1.0.0
                            </p>
                        </div>
                    </div>
                </div>
            </body>
            </html>
            """
            
            msg.attach(MIMEText(html_body, 'html', 'utf-8'))
            
            # 첨부파일 처리
            if message.attachments:
                for file_path in message.attachments:
                    if os.path.exists(file_path):
                        with open(file_path, "rb") as attachment:
                            part = MIMEBase('application', 'octet-stream')
                            part.set_payload(attachment.read())
                        
                        encoders.encode_base64(part)
                        filename = os.path.basename(file_path)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename= {filename}'
                        )
                        msg.attach(part)
            
            # SMTP 서버 연결 및 발송
            server = smtplib.SMTP(self.config.email_smtp_server, self.config.email_smtp_port)
            server.starttls()
            server.login(self.config.email_username, self.config.email_password)
            
            text = msg.as_string()
            server.sendmail(self.config.email_from, self.config.email_to, text)
            server.quit()
            
            self.logger.info(f"📧 이메일 발송 성공: {message.title}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 이메일 발송 실패: {e}")
            return False
    
    def send_slack(self, message: NotificationMessage) -> bool:
        """슬랙 메시지 발송"""
        try:
            if not self.config.slack_enabled or not self.config.slack_webhook_url:
                return True  # 비활성화시 성공으로 처리
            
            # 우선순위에 따른 색상 설정
            colors = {
                'low': '#36a64f',      # 녹색
                'normal': '#3498db',   # 파란색
                'high': '#ff9900',     # 주황색
                'urgent': '#e74c3c'    # 빨간색
            }
            
            # 슬랙 메시지 구성
            slack_data = {
                "channel": self.config.slack_channel,
                "username": "ETF Bot",
                "icon_emoji": ":chart_with_upwards_trend:",
                "attachments": [
                    {
                        "color": colors.get(message.priority, '#3498db'),
                        "title": message.title,
                        "text": message.content,
                        "fields": [
                            {
                                "title": "카테고리",
                                "value": message.category,
                                "short": True
                            },
                            {
                                "title": "우선순위",
                                "value": message.priority.upper(),
                                "short": True
                            }
                        ],
                        "footer": "ETF 장기투자 관리 시스템",
                        "ts": int(datetime.now().timestamp())
                    }
                ]
            }
            
            # 웹훅으로 발송
            response = requests.post(
                self.config.slack_webhook_url,
                json=slack_data,
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.info(f"💬 슬랙 메시지 발송 성공: {message.title}")
                return True
            else:
                self.logger.error(f"❌ 슬랙 메시지 발송 실패: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 슬랙 메시지 발송 실패: {e}")
            return False
    
    def send_webhook(self, message: NotificationMessage) -> bool:
        """웹훅 발송"""
        try:
            if not self.config.webhook_enabled or not self.config.webhook_url:
                return True  # 비활성화시 성공으로 처리
            
            # 웹훅 데이터 구성
            webhook_data = {
                "timestamp": datetime.now().isoformat(),
                "title": message.title,
                "content": message.content,
                "priority": message.priority,
                "category": message.category,
                "recipient": message.recipient,
                "source": "ETF_SYSTEM"
            }
            
            # 헤더 설정
            headers = self.config.webhook_headers or {}
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
            
            # 웹훅 발송
            response = requests.post(
                self.config.webhook_url,
                json=webhook_data,
                headers=headers,
                timeout=10
            )
            
            if response.status_code in [200, 201, 202]:
                self.logger.info(f"🔗 웹훅 발송 성공: {message.title}")
                return True
            else:
                self.logger.error(f"❌ 웹훅 발송 실패: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 웹훅 발송 실패: {e}")
            return False
    
    def send_rebalancing_alert(self, user_id: str, max_deviation: float, 
                             estimated_cost: float, recommendations: List[Dict]) -> bool:
        """리밸런싱 알림"""
        priority = "high" if max_deviation > 10 else "normal"
        
        content = f"""
포트폴리오 리밸런싱이 권장됩니다.

📊 현황:
• 최대 편차: {max_deviation:.2f}%
• 예상 거래비용: {estimated_cost:,.0f}원
• 조정 필요 종목: {len([r for r in recommendations if abs(r.get('rebalance_amount', 0)) > 10000])}개

💡 권장사항:
• 편차가 5% 이상인 자산이 있어 리밸런싱을 권장합니다
• 신규 투자금이 있다면 부족한 자산 위주로 매수하세요
• 거래비용을 고려하여 필요시에만 매도하세요

자세한 내용은 대시보드에서 확인하세요.
        """
        
        message = NotificationMessage(
            title=f"포트폴리오 리밸런싱 권장 (편차: {max_deviation:.1f}%)",
            content=content.strip(),
            priority=priority,
            category="rebalancing",
            recipient=user_id
        )
        
        return self.send_notification(message)
    
    def send_performance_alert(self, user_id: str, period_return: float, 
                             benchmark_return: float, period: str = "월간") -> bool:
        """성과 알림"""
        outperformance = period_return - benchmark_return
        priority = "high" if abs(outperformance) > 5 else "normal"
        
        performance_emoji = "📈" if period_return > 0 else "📉"
        vs_benchmark = "초과달성" if outperformance > 0 else "미달성"
        
        content = f"""
{period} 포트폴리오 성과를 알려드립니다.

{performance_emoji} 성과 요약:
• {period} 수익률: {period_return:+.2f}%
• 벤치마크 수익률: {benchmark_return:+.2f}%
• 초과 수익률: {outperformance:+.2f}% ({vs_benchmark})

💡 분석:
{'🎉 우수한 성과입니다!' if period_return > 5 else 
 '👍 양호한 성과입니다.' if period_return > 0 else 
 '📊 단기 변동성이 있지만 장기 관점을 유지하세요.'}

자세한 분석은 성과 리포트에서 확인하세요.
        """
        
        message = NotificationMessage(
            title=f"{period} 성과 리포트 ({period_return:+.2f}%)",
            content=content.strip(),
            priority=priority,
            category="performance",
            recipient=user_id
        )
        
        return self.send_notification(message)
    
    def send_system_alert(self, alert_type: str, message_content: str, 
                         priority: str = "normal") -> bool:
        """시스템 알림"""
        alert_emojis = {
            "error": "🚨",
            "warning": "⚠️",
            "info": "ℹ️",
            "success": "✅"
        }
        
        emoji = alert_emojis.get(alert_type, "📊")
        
        message = NotificationMessage(
            title=f"{emoji} 시스템 알림: {alert_type.upper()}",
            content=message_content,
            priority=priority,
            category="system"
        )
        
        return self.send_notification(message)
    
    def send_daily_summary(self, user_id: str, summary_data: Dict) -> bool:
        """일일 요약 알림"""
        content = f"""
📊 일일 포트폴리오 요약

💰 현재 상황:
• 총 자산: {summary_data.get('total_value', 0):,.0f}원
• 일일 수익률: {summary_data.get('daily_return', 0):+.2f}%
• 누적 수익률: {summary_data.get('total_return', 0):+.2f}%

📈 주요 지표:
• 변동성: {summary_data.get('volatility', 0):.2f}%
• 샤프 비율: {summary_data.get('sharpe_ratio', 0):.2f}

🔄 상태:
• 마지막 리밸런싱: {summary_data.get('last_rebalance', '없음')}
• 다음 점검일: {summary_data.get('next_check', '미정')}

좋은 투자 되세요! 📈
        """
        
        message = NotificationMessage(
            title="📊 일일 포트폴리오 요약",
            content=content.strip(),
            priority="low",
            category="general",
            recipient=user_id
        )
        
        return self.send_notification(message)
    
    def send_data_update_complete(self, updated_count: int, failed_count: int, 
                                success_rate: float) -> bool:
        """데이터 업데이트 완료 알림"""
        status_emoji = "✅" if success_rate > 90 else "⚠️" if success_rate > 70 else "❌"
        priority = "normal" if success_rate > 90 else "high"
        
        content = f"""
{status_emoji} ETF 데이터 업데이트가 완료되었습니다.

📊 업데이트 결과:
• 성공: {updated_count}개
• 실패: {failed_count}개
• 성공률: {success_rate:.1f}%

⏰ 업데이트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'✅ 모든 데이터가 정상적으로 업데이트되었습니다.' if success_rate > 95 else 
 '⚠️ 일부 데이터 업데이트에 실패했습니다. 시스템 점검이 필요할 수 있습니다.' if success_rate < 70 else 
 '👍 대부분의 데이터가 정상적으로 업데이트되었습니다.'}
        """
        
        message = NotificationMessage(
            title=f"데이터 업데이트 완료 ({success_rate:.1f}% 성공)",
            content=content.strip(),
            priority=priority,
            category="system"
        )
        
        return self.send_notification(message)
    
    def get_notification_history(self, limit: int = 50) -> List[Dict]:
        """알림 히스토리 조회"""
        return self.notification_history[-limit:]
    
    def test_notifications(self) -> Dict[str, bool]:
        """알림 시스템 테스트"""
        results = {}
        
        test_message = NotificationMessage(
            title="알림 시스템 테스트",
            content="이것은 ETF 시스템 알림 테스트 메시지입니다.\n모든 알림 채널이 정상적으로 작동하는지 확인합니다.",
            priority="normal",
            category="system"
        )
        
        # 이메일 테스트
        if self.config.email_enabled:
            results['email'] = self.send_email(test_message)
        else:
            results['email'] = None
        
        # 슬랙 테스트
        if self.config.slack_enabled:
            results['slack'] = self.send_slack(test_message)
        else:
            results['slack'] = None
        
        # 웹훅 테스트
        if self.config.webhook_enabled:
            results['webhook'] = self.send_webhook(test_message)
        else:
            results['webhook'] = None
        
        return results


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("📢 알림 시스템 테스트")
    print("=" * 60)
    
    # 알림 시스템 초기화
    notifier = NotificationSystem()
    
    # 현재 설정 확인
    print("\n⚙️ 현재 알림 설정:")
    print(f"- 이메일: {'활성화' if notifier.config.email_enabled else '비활성화'}")
    print(f"- 슬랙: {'활성화' if notifier.config.slack_enabled else '비활성화'}")
    print(f"- 웹훅: {'활성화' if notifier.config.webhook_enabled else '비활성화'}")
    
    # 테스트 메시지들
    print("\n📨 테스트 메시지 발송:")
    
    # 1. 일반 알림 테스트
    print("\n1. 일반 알림 테스트...")
    general_message = NotificationMessage(
        title="시스템 시작 알림",
        content="ETF 장기투자 관리 시스템이 성공적으로 시작되었습니다.\n모든 모듈이 정상 작동하고 있습니다.",
        priority="normal",
        category="system"
    )
    
    result = notifier.send_notification(general_message)
    print(f"결과: {'✅ 성공' if result else '❌ 실패'}")
    
    # 2. 리밸런싱 알림 테스트
    print("\n2. 리밸런싱 알림 테스트...")
    rebalancing_result = notifier.send_rebalancing_alert(
        user_id="test_user",
        max_deviation=7.5,
        estimated_cost=15000,
        recommendations=[
            {"etf_code": "069500", "rebalance_amount": 50000},
            {"etf_code": "360750", "rebalance_amount": -30000}
        ]
    )
    print(f"결과: {'✅ 성공' if rebalancing_result else '❌ 실패'}")
    
    # 3. 성과 알림 테스트
    print("\n3. 성과 알림 테스트...")
    performance_result = notifier.send_performance_alert(
        user_id="test_user",
        period_return=3.2,
        benchmark_return=2.8,
        period="월간"
    )
    print(f"결과: {'✅ 성공' if performance_result else '❌ 실패'}")
    
    # 4. 시스템 알림 테스트
    print("\n4. 시스템 알림 테스트...")
    system_result = notifier.send_system_alert(
        alert_type="warning",
        message_content="일부 ETF 데이터 수집에 지연이 발생하고 있습니다. 자동으로 재시도 중입니다.",
        priority="high"
    )
    print(f"결과: {'✅ 성공' if system_result else '❌ 실패'}")
    
    # 5. 일일 요약 테스트
    print("\n5. 일일 요약 테스트...")
    summary_data = {
        'total_value': 12450000,
        'daily_return': 0.85,
        'total_return': 24.5,
        'volatility': 15.2,
        'sharpe_ratio': 1.35,
        'last_rebalance': '2024-01-15',
        'next_check': '2024-02-15'
    }
    
    summary_result = notifier.send_daily_summary("test_user", summary_data)
    print(f"결과: {'✅ 성공' if summary_result else '❌ 실패'}")
    
    # 6. 데이터 업데이트 완료 알림 테스트
    print("\n6. 데이터 업데이트 완료 알림 테스트...")
    update_result = notifier.send_data_update_complete(
        updated_count=23,
        failed_count=2,
        success_rate=92.0
    )
    print(f"결과: {'✅ 성공' if update_result else '❌ 실패'}")
    
    # 7. 알림 시스템 연결 테스트
    print("\n7. 알림 채널 연결 테스트...")
    test_results = notifier.test_notifications()
    
    for channel, result in test_results.items():
        if result is None:
            print(f"- {channel}: 비활성화")
        elif result:
            print(f"- {channel}: ✅ 연결 성공")
        else:
            print(f"- {channel}: ❌ 연결 실패")
    
    # 8. 알림 히스토리 확인
    print("\n8. 알림 히스토리:")
    history = notifier.get_notification_history(5)
    
    if history:
        for i, notification in enumerate(history, 1):
            print(f"{i}. [{notification['timestamp'][:19]}] {notification['title']} "
                  f"({notification['category']}/{notification['priority']})")
    else:
        print("알림 히스토리가 없습니다.")
    
    print(f"\n✅ 알림 시스템 테스트 완료!")
    print(f"💡 사용 팁:")
    print(f"   - notification_config.json 파일에서 설정을 변경할 수 있습니다")
    print(f"   - 이메일 사용시 Gmail의 경우 앱 비밀번호를 사용하세요")
    print(f"   - 슬랙 웹훅 URL은 슬랙 앱에서 생성할 수 있습니다")
    print(f"   - 우선순위에 따라 알림 색상과 아이콘이 다르게 표시됩니다")