"""
이메일 발송 유틸리티 모듈
포트폴리오 리포트, 알림, 리밸런싱 신호 등을 이메일로 발송하는 기능
"""

import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.utils import formataddr
import os
import pandas as pd
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path

logger = logging.getLogger(__name__)

class EmailSender:
    """이메일 발송 클래스"""
    
    def __init__(self, smtp_config: Optional[Dict[str, str]] = None):
        """
        이메일 발송기 초기화
        
        Args:
            smtp_config: SMTP 설정 딕셔너리
        """
        self.smtp_config = smtp_config or self._get_default_smtp_config()
        
        # 이메일 템플릿 설정
        self._setup_email_templates()
        
        # 발송 이력 관리
        self.send_history = []
        
    def _get_default_smtp_config(self) -> Dict[str, str]:
        """기본 SMTP 설정 (환경변수에서 읽기)"""
        
        return {
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'email': os.getenv('EMAIL_ADDRESS', ''),
            'password': os.getenv('EMAIL_PASSWORD', ''),
            'sender_name': os.getenv('SENDER_NAME', 'ETF Portfolio Manager')
        }
    
    def _setup_email_templates(self):
        """이메일 템플릿 설정"""
        
        self.templates = {
            'portfolio_report': {
                'subject': '[ETF Portfolio] 포트폴리오 리포트 - {date}',
                'template': '''
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .header {{ background-color: #f0f8ff; padding: 20px; border-radius: 10px; }}
                        .summary {{ background-color: #f9f9f9; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                        .table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                        .table th, .table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                        .table th {{ background-color: #f2f2f2; }}
                        .positive {{ color: #28a745; }}
                        .negative {{ color: #dc3545; }}
                        .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h2>📊 ETF 포트폴리오 리포트</h2>
                        <p>리포트 생성일: {date}</p>
                    </div>
                    
                    <div class="summary">
                        <h3>📈 포트폴리오 요약</h3>
                        <ul>
                            <li><strong>총 자산:</strong> {total_value:,}원</li>
                            <li><strong>총 수익률:</strong> <span class="{return_class}">{total_return:+.2f}%</span></li>
                            <li><strong>일일 수익률:</strong> <span class="{daily_return_class}">{daily_return:+.2f}%</span></li>
                            <li><strong>보유 ETF 수:</strong> {etf_count}개</li>
                        </ul>
                    </div>
                    
                    <h3>📋 보유 종목 현황</h3>
                    {holdings_table}
                    
                    {rebalancing_section}
                    
                    <div class="footer">
                        <p>이 리포트는 ETF Portfolio Manager에서 자동 생성되었습니다.</p>
                        <p>투자 결정은 신중히 내리시기 바랍니다.</p>
                    </div>
                </body>
                </html>
                '''
            },
            'rebalancing_alert': {
                'subject': '[ETF Portfolio] 🔄 리밸런싱 알림',
                'template': '''
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .alert {{ background-color: #fff3cd; border: 1px solid #ffeaa7; padding: 20px; border-radius: 10px; }}
                        .table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                        .table th, .table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                        .table th {{ background-color: #f2f2f2; }}
                        .buy {{ color: #28a745; font-weight: bold; }}
                        .sell {{ color: #dc3545; font-weight: bold; }}
                        .high-priority {{ background-color: #ffebee; }}
                    </style>
                </head>
                <body>
                    <div class="alert">
                        <h2>🔄 리밸런싱이 필요합니다</h2>
                        <p>포트폴리오가 목표 배분에서 {threshold}% 이상 벗어났습니다.</p>
                        <p><strong>감지 시점:</strong> {detection_time}</p>
                    </div>
                    
                    <h3>📊 리밸런싱 필요 종목</h3>
                    {rebalancing_table}
                    
                    <div style="margin-top: 30px; padding: 15px; background-color: #e8f5e8; border-radius: 5px;">
                        <h4>💡 리밸런싱 가이드</h4>
                        <ul>
                            <li>우선순위가 높은 종목부터 조정하세요</li>
                            <li>거래 비용을 고려하여 신중히 결정하세요</li>
                            <li>시장 상황을 확인 후 실행하세요</li>
                        </ul>
                    </div>
                </body>
                </html>
                '''
            },
            'market_alert': {
                'subject': '[ETF Portfolio] 🚨 시장 알림',
                'template': '''
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .alert {{ padding: 20px; border-radius: 10px; margin: 20px 0; }}
                        .alert-danger {{ background-color: #f8d7da; border: 1px solid #f5c6cb; }}
                        .alert-warning {{ background-color: #fff3cd; border: 1px solid #ffeaa7; }}
                        .alert-info {{ background-color: #d1ecf1; border: 1px solid #bee5eb; }}
                    </style>
                </head>
                <body>
                    <div class="alert alert-{alert_type}">
                        <h2>🚨 {alert_title}</h2>
                        <p>{alert_message}</p>
                        <p><strong>발생 시점:</strong> {timestamp}</p>
                    </div>
                    
                    <h3>📊 영향받는 포트폴리오</h3>
                    {affected_portfolios}
                    
                    <div style="margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px;">
                        <h4>📋 권장 조치사항</h4>
                        {recommendations}
                    </div>
                </body>
                </html>
                '''
            },
            'monthly_summary': {
                'subject': '[ETF Portfolio] 📅 월간 투자 리포트 - {month}',
                'template': '''
                <html>
                <head>
                    <style>
                        body {{ font-family: Arial, sans-serif; margin: 20px; }}
                        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                                  color: white; padding: 30px; border-radius: 15px; text-align: center; }}
                        .metric-box {{ background-color: #f8f9fa; padding: 20px; margin: 10px; 
                                     border-radius: 10px; display: inline-block; width: 200px; text-align: center; }}
                        .performance-section {{ margin: 30px 0; }}
                        .table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
                        .table th, .table td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                        .table th {{ background-color: #f2f2f2; }}
                        .positive {{ color: #28a745; font-weight: bold; }}
                        .negative {{ color: #dc3545; font-weight: bold; }}
                    </style>
                </head>
                <body>
                    <div class="header">
                        <h1>📅 월간 투자 리포트</h1>
                        <h2>{month}</h2>
                    </div>
                    
                    <div class="performance-section">
                        <h3>📊 이번 달 성과</h3>
                        <div>
                            <div class="metric-box">
                                <h4>월간 수익률</h4>
                                <h2 class="{monthly_return_class}">{monthly_return:+.2f}%</h2>
                            </div>
                            <div class="metric-box">
                                <h4>누적 수익률</h4>
                                <h2 class="{cumulative_return_class}">{cumulative_return:+.2f}%</h2>
                            </div>
                            <div class="metric-box">
                                <h4>벤치마크 대비</h4>
                                <h2 class="{vs_benchmark_class}">{vs_benchmark:+.2f}%p</h2>
                            </div>
                        </div>
                    </div>
                    
                    <h3>🏆 이번 달 베스트/워스트</h3>
                    {performance_table}
                    
                    <h3>🔄 리밸런싱 이력</h3>
                    {rebalancing_history}
                    
                    <div style="margin-top: 30px; padding: 20px; background-color: #e8f5e8; border-radius: 10px;">
                        <h4>💭 다음 달 전망 및 전략</h4>
                        {next_month_strategy}
                    </div>
                </body>
                </html>
                '''
            }
        }
    
    def send_portfolio_report(self, recipient: str, portfolio_data: Dict[str, Any], 
                            attachments: Optional[List[str]] = None) -> bool:
        """
        포트폴리오 리포트 이메일 발송
        
        Args:
            recipient: 수신자 이메일
            portfolio_data: 포트폴리오 데이터
            attachments: 첨부파일 경로 리스트
            
        Returns:
            발송 성공 여부
        """
        try:
            # 이메일 내용 생성
            subject = self.templates['portfolio_report']['subject'].format(
                date=datetime.now().strftime('%Y-%m-%d')
            )
            
            # HTML 내용 생성
            html_content = self._generate_portfolio_report_html(portfolio_data)
            
            return self._send_email(
                recipient=recipient,
                subject=subject,
                html_content=html_content,
                attachments=attachments
            )
            
        except Exception as e:
            logger.error(f"포트폴리오 리포트 발송 실패: {e}")
            return False
    
    def send_rebalancing_alert(self, recipient: str, rebalancing_data: Dict[str, Any]) -> bool:
        """
        리밸런싱 알림 이메일 발송
        
        Args:
            recipient: 수신자 이메일
            rebalancing_data: 리밸런싱 데이터
            
        Returns:
            발송 성공 여부
        """
        try:
            subject = self.templates['rebalancing_alert']['subject']
            
            # HTML 내용 생성
            html_content = self._generate_rebalancing_alert_html(rebalancing_data)
            
            return self._send_email(
                recipient=recipient,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"리밸런싱 알림 발송 실패: {e}")
            return False
    
    def send_market_alert(self, recipient: str, alert_data: Dict[str, Any]) -> bool:
        """
        시장 알림 이메일 발송
        
        Args:
            recipient: 수신자 이메일
            alert_data: 알림 데이터
            
        Returns:
            발송 성공 여부
        """
        try:
            subject = self.templates['market_alert']['subject']
            
            # HTML 내용 생성
            html_content = self._generate_market_alert_html(alert_data)
            
            return self._send_email(
                recipient=recipient,
                subject=subject,
                html_content=html_content
            )
            
        except Exception as e:
            logger.error(f"시장 알림 발송 실패: {e}")
            return False
    
    def send_monthly_summary(self, recipient: str, summary_data: Dict[str, Any],
                           attachments: Optional[List[str]] = None) -> bool:
        """
        월간 요약 리포트 발송
        
        Args:
            recipient: 수신자 이메일
            summary_data: 월간 요약 데이터
            attachments: 첨부파일 경로 리스트
            
        Returns:
            발송 성공 여부
        """
        try:
            subject = self.templates['monthly_summary']['subject'].format(
                month=summary_data.get('month', datetime.now().strftime('%Y년 %m월'))
            )
            
            # HTML 내용 생성
            html_content = self._generate_monthly_summary_html(summary_data)
            
            return self._send_email(
                recipient=recipient,
                subject=subject,
                html_content=html_content,
                attachments=attachments
            )
            
        except Exception as e:
            logger.error(f"월간 요약 리포트 발송 실패: {e}")
            return False
    
    def send_custom_email(self, recipient: str, subject: str, content: str,
                         content_type: str = 'html', attachments: Optional[List[str]] = None) -> bool:
        """
        커스텀 이메일 발송
        
        Args:
            recipient: 수신자 이메일
            subject: 제목
            content: 내용
            content_type: 내용 유형 ('html' 또는 'text')
            attachments: 첨부파일 경로 리스트
            
        Returns:
            발송 성공 여부
        """
        try:
            if content_type == 'html':
                return self._send_email(
                    recipient=recipient,
                    subject=subject,
                    html_content=content,
                    attachments=attachments
                )
            else:
                return self._send_email(
                    recipient=recipient,
                    subject=subject,
                    text_content=content,
                    attachments=attachments
                )
                
        except Exception as e:
            logger.error(f"커스텀 이메일 발송 실패: {e}")
            return False
    
    def _send_email(self, recipient: str, subject: str, 
                   html_content: Optional[str] = None,
                   text_content: Optional[str] = None,
                   attachments: Optional[List[str]] = None) -> bool:
        """실제 이메일 발송 처리"""
        
        try:
            # SMTP 설정 검증
            if not self.smtp_config.get('email') or not self.smtp_config.get('password'):
                logger.error("SMTP 설정이 불완전합니다 (이메일 주소 또는 비밀번호 누락)")
                return False
            
            # 이메일 메시지 생성
            message = MIMEMultipart('alternative')
            message['From'] = formataddr((self.smtp_config['sender_name'], self.smtp_config['email']))
            message['To'] = recipient
            message['Subject'] = subject
            
            # 텍스트 및 HTML 내용 추가
            if text_content:
                text_part = MIMEText(text_content, 'plain', 'utf-8')
                message.attach(text_part)
            
            if html_content:
                html_part = MIMEText(html_content, 'html', 'utf-8')
                message.attach(html_part)
            
            # 첨부파일 추가
            if attachments:
                for file_path in attachments:
                    if os.path.exists(file_path):
                        self._add_attachment(message, file_path)
                    else:
                        logger.warning(f"첨부파일을 찾을 수 없습니다: {file_path}")
            
            # 이메일 발송
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_config['smtp_server'], self.smtp_config['smtp_port']) as server:
                server.starttls(context=context)
                server.login(self.smtp_config['email'], self.smtp_config['password'])
                server.send_message(message)
            
            # 발송 이력 기록
            self.send_history.append({
                'timestamp': datetime.now(),
                'recipient': recipient,
                'subject': subject,
                'status': 'success'
            })
            
            logger.info(f"이메일 발송 성공: {recipient}")
            return True
            
        except Exception as e:
            # 발송 실패 이력 기록
            self.send_history.append({
                'timestamp': datetime.now(),
                'recipient': recipient,
                'subject': subject,
                'status': 'failed',
                'error': str(e)
            })
            
            logger.error(f"이메일 발송 실패: {e}")
            return False
    
    def _add_attachment(self, message: MIMEMultipart, file_path: str):
        """이메일에 첨부파일 추가"""
        
        try:
            with open(file_path, 'rb') as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            
            filename = os.path.basename(file_path)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {filename}'
            )
            
            message.attach(part)
            
        except Exception as e:
            logger.error(f"첨부파일 추가 실패 ({file_path}): {e}")
    
    def _generate_portfolio_report_html(self, portfolio_data: Dict[str, Any]) -> str:
        """포트폴리오 리포트 HTML 생성"""
        
        try:
            # 보유 종목 테이블 생성
            holdings_table = self._create_holdings_table(portfolio_data.get('holdings', []))
            
            # 리밸런싱 섹션 생성
            rebalancing_section = ""
            if portfolio_data.get('rebalancing_needed'):
                rebalancing_section = f"""
                <div style="background-color: #fff3cd; padding: 15px; margin: 20px 0; border-radius: 5px;">
                    <h4>🔄 리밸런싱 권장</h4>
                    <p>일부 ETF가 목표 비중에서 벗어났습니다. 리밸런싱을 고려해보세요.</p>
                </div>
                """
            
            # 수익률 색상 클래스 결정
            total_return = portfolio_data.get('total_return_pct', 0)
            daily_return = portfolio_data.get('daily_return_pct', 0)
            
            return_class = 'positive' if total_return >= 0 else 'negative'
            daily_return_class = 'positive' if daily_return >= 0 else 'negative'
            
            # HTML 템플릿에 데이터 삽입
            html_content = self.templates['portfolio_report']['template'].format(
                date=datetime.now().strftime('%Y년 %m월 %d일'),
                total_value=portfolio_data.get('total_value', 0),
                total_return=total_return,
                daily_return=daily_return,
                etf_count=portfolio_data.get('etf_count', 0),
                return_class=return_class,
                daily_return_class=daily_return_class,
                holdings_table=holdings_table,
                rebalancing_section=rebalancing_section
            )
            
            return html_content
            
        except Exception as e:
            logger.error(f"포트폴리오 리포트 HTML 생성 실패: {e}")
            return "<p>리포트 생성 중 오류가 발생했습니다.</p>"
    
    def _generate_rebalancing_alert_html(self, rebalancing_data: Dict[str, Any]) -> str:
        """리밸런싱 알림 HTML 생성"""
        
        try:
            # 리밸런싱 테이블 생성
            rebalancing_table = self._create_rebalancing_table(rebalancing_data.get('actions', []))
            
            html_content = self.templates['rebalancing_alert']['template'].format(
                threshold=rebalancing_data.get('threshold', 5),
                detection_time=datetime.now().strftime('%Y년 %m월 %d일 %H:%M'),
                rebalancing_table=rebalancing_table
            )
            
            return html_content
            
        except Exception as e:
            logger.error(f"리밸런싱 알림 HTML 생성 실패: {e}")
            return "<p>알림 생성 중 오류가 발생했습니다.</p>"
    
    def _generate_market_alert_html(self, alert_data: Dict[str, Any]) -> str:
        """시장 알림 HTML 생성"""
        
        try:
            # 영향받는 포트폴리오 정보
            affected_portfolios = ""
            if alert_data.get('affected_etfs'):
                affected_list = "<ul>"
                for etf in alert_data['affected_etfs']:
                    affected_list += f"<li>{etf}</li>"
                affected_list += "</ul>"
                affected_portfolios = affected_list
            
            # 권장사항
            recommendations = ""
            if alert_data.get('recommendations'):
                rec_list = "<ul>"
                for rec in alert_data['recommendations']:
                    rec_list += f"<li>{rec}</li>"
                rec_list += "</ul>"
                recommendations = rec_list
            
            html_content = self.templates['market_alert']['template'].format(
                alert_type=alert_data.get('type', 'warning'),
                alert_title=alert_data.get('title', '시장 알림'),
                alert_message=alert_data.get('message', ''),
                timestamp=datetime.now().strftime('%Y년 %m월 %d일 %H:%M'),
                affected_portfolios=affected_portfolios,
                recommendations=recommendations
            )
            
            return html_content
            
        except Exception as e:
            logger.error(f"시장 알림 HTML 생성 실패: {e}")
            return "<p>알림 생성 중 오류가 발생했습니다.</p>"
    
    def _generate_monthly_summary_html(self, summary_data: Dict[str, Any]) -> str:
        """월간 요약 리포트 HTML 생성"""
        
        try:
            # 성과 테이블 생성
            performance_table = self._create_performance_table(summary_data.get('performance', []))
            
            # 리밸런싱 이력
            rebalancing_history = self._create_rebalancing_history_table(
                summary_data.get('rebalancing_history', [])
            )
            
            # 다음 달 전략
            next_month_strategy = summary_data.get('next_month_strategy', '계속 모니터링하며 목표 자산배분을 유지할 예정입니다.')
            
            # 수익률 색상 클래스
            monthly_return = summary_data.get('monthly_return', 0)
            cumulative_return = summary_data.get('cumulative_return', 0)
            vs_benchmark = summary_data.get('vs_benchmark', 0)
            
            monthly_return_class = 'positive' if monthly_return >= 0 else 'negative'
            cumulative_return_class = 'positive' if cumulative_return >= 0 else 'negative'
            vs_benchmark_class = 'positive' if vs_benchmark >= 0 else 'negative'
            
            html_content = self.templates['monthly_summary']['template'].format(
                month=summary_data.get('month', datetime.now().strftime('%Y년 %m월')),
                monthly_return=monthly_return,
                cumulative_return=cumulative_return,
                vs_benchmark=vs_benchmark,
                monthly_return_class=monthly_return_class,
                cumulative_return_class=cumulative_return_class,
                vs_benchmark_class=vs_benchmark_class,
                performance_table=performance_table,
                rebalancing_history=rebalancing_history,
                next_month_strategy=next_month_strategy
            )
            
            return html_content
            
        except Exception as e:
            logger.error(f"월간 요약 HTML 생성 실패: {e}")
            return "<p>리포트 생성 중 오류가 발생했습니다.</p>"
    
    def _create_holdings_table(self, holdings: List[Dict[str, Any]]) -> str:
        """보유 종목 테이블 HTML 생성"""
        
        if not holdings:
            return "<p>보유 종목이 없습니다.</p>"
        
        table_html = '''
        <table class="table">
            <thead>
                <tr>
                    <th>ETF명</th>
                    <th>현재 비중</th>
                    <th>목표 비중</th>
                    <th>평가금액</th>
                    <th>수익률</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for holding in holdings:
            return_pct = holding.get('return_pct', 0)
            return_class = 'positive' if return_pct >= 0 else 'negative'
            
            table_html += f'''
                <tr>
                    <td>{holding.get('etf_name', '')}</td>
                    <td>{holding.get('current_weight', 0):.1f}%</td>
                    <td>{holding.get('target_weight', 0):.1f}%</td>
                    <td>{holding.get('market_value', 0):,.0f}원</td>
                    <td><span class="{return_class}">{return_pct:+.2f}%</span></td>
                </tr>
            '''
        
        table_html += '</tbody></table>'
        return table_html
    
    def _create_rebalancing_table(self, actions: List[Dict[str, Any]]) -> str:
        """리밸런싱 테이블 HTML 생성"""
        
        if not actions:
            return "<p>현재 리밸런싱이 필요한 종목이 없습니다.</p>"
        
        table_html = '''
        <table class="table">
            <thead>
                <tr>
                    <th>ETF명</th>
                    <th>현재 비중</th>
                    <th>목표 비중</th>
                    <th>편차</th>
                    <th>조치</th>
                    <th>우선순위</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for action in actions:
            priority_class = 'high-priority' if action.get('urgency') == 'High' else ''
            action_class = 'buy' if action.get('action') == 'BUY' else 'sell'
            
            table_html += f'''
                <tr class="{priority_class}">
                    <td>{action.get('etf_name', '')}</td>
                    <td>{action.get('current_weight', 0):.1f}%</td>
                    <td>{action.get('target_weight', 0):.1f}%</td>
                    <td>{action.get('deviation', 0):+.1f}%p</td>
                    <td><span class="{action_class}">{action.get('action', '')}</span></td>
                    <td>{action.get('urgency', 'Medium')}</td>
                </tr>
            '''
        
        table_html += '</tbody></table>'
        return table_html
    
    def _create_performance_table(self, performance: List[Dict[str, Any]]) -> str:
        """성과 테이블 HTML 생성"""
        
        if not performance:
            return "<p>성과 데이터가 없습니다.</p>"
        
        table_html = '''
        <table class="table">
            <thead>
                <tr>
                    <th>ETF명</th>
                    <th>월간 수익률</th>
                    <th>기여도</th>
                    <th>평가</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for perf in performance:
            return_pct = perf.get('monthly_return', 0)
            return_class = 'positive' if return_pct >= 0 else 'negative'
            
            table_html += f'''
                <tr>
                    <td>{perf.get('etf_name', '')}</td>
                    <td><span class="{return_class}">{return_pct:+.2f}%</span></td>
                    <td>{perf.get('contribution', 0):+.2f}%p</td>
                    <td>{perf.get('evaluation', '')}</td>
                </tr>
            '''
        
        table_html += '</tbody></table>'
        return table_html
    
    def _create_rebalancing_history_table(self, history: List[Dict[str, Any]]) -> str:
        """리밸런싱 이력 테이블 HTML 생성"""
        
        if not history:
            return "<p>이번 달 리밸런싱 이력이 없습니다.</p>"
        
        table_html = '''
        <table class="table">
            <thead>
                <tr>
                    <th>날짜</th>
                    <th>ETF</th>
                    <th>조치</th>
                    <th>금액</th>
                </tr>
            </thead>
            <tbody>
        '''
        
        for item in history:
            action_class = 'buy' if item.get('action') == 'BUY' else 'sell'
            
            table_html += f'''
                <tr>
                    <td>{item.get('date', '')}</td>
                    <td>{item.get('etf_name', '')}</td>
                    <td><span class="{action_class}">{item.get('action', '')}</span></td>
                    <td>{item.get('amount', 0):,.0f}원</td>
                </tr>
            '''
        
        table_html += '</tbody></table>'
        return table_html
    
    def get_send_history(self, days: int = 30) -> List[Dict[str, Any]]:
        """
        이메일 발송 이력 조회
        
        Args:
            days: 조회할 기간 (일)
            
        Returns:
            발송 이력 리스트
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        recent_history = [
            record for record in self.send_history
            if record['timestamp'] >= cutoff_date
        ]
        
        return recent_history
    
    def test_smtp_connection(self) -> bool:
        """SMTP 연결 테스트"""
        
        try:
            context = ssl.create_default_context()
            
            with smtplib.SMTP(self.smtp_config['smtp_server'], self.smtp_config['smtp_port']) as server:
                server.starttls(context=context)
                server.login(self.smtp_config['email'], self.smtp_config['password'])
            
            logger.info("SMTP 연결 테스트 성공")
            return True
            
        except Exception as e:
            logger.error(f"SMTP 연결 테스트 실패: {e}")
            return False
    
    def update_smtp_config(self, new_config: Dict[str, str]) -> bool:
        """SMTP 설정 업데이트"""
        
        try:
            # 설정 백업
            old_config = self.smtp_config.copy()
            
            # 새 설정 적용
            self.smtp_config.update(new_config)
            
            # 연결 테스트
            if self.test_smtp_connection():
                logger.info("SMTP 설정 업데이트 성공")
                return True
            else:
                # 실패시 롤백
                self.smtp_config = old_config
                logger.error("SMTP 설정 업데이트 실패 (연결 테스트 실패)")
                return False
                
        except Exception as e:
            logger.error(f"SMTP 설정 업데이트 실패: {e}")
            return False