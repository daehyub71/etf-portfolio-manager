# ==========================================
# real_etf_data_collector.py - 실제 ETF 데이터 수집기
# ==========================================

import requests
import pandas as pd
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Optional
import sqlite3

logger = logging.getLogger(__name__)

class RealETFDataCollector:
    """실제 한국 ETF 데이터 수집기"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def collect_krx_etf_list(self) -> List[Dict]:
        """KRX에서 ETF 목록 수집"""
        try:
            print("🌐 KRX에서 ETF 목록 수집 중...")
            
            # KRX ETF 목록 API (실제 URL)
            url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
            
            data = {
                'bld': 'dbms/MDC/STAT/standard/MDCSTAT04901',
                'locale': 'ko_KR',
                'trdDd': datetime.now().strftime('%Y%m%d'),
                'money': '1',  # 원화
                'csvxls_isNo': 'false'
            }
            
            response = self.session.post(url, data=data)
            
            if response.status_code == 200:
                result = response.json()
                etf_list = result.get('OutBlock_1', [])
                
                print(f"✅ KRX에서 {len(etf_list)}개 ETF 정보 수집 완료")
                
                # 데이터 정제
                processed_etfs = []
                for etf in etf_list:
                    processed_etf = {
                        'code': etf.get('ISU_SRT_CD', ''),  # 종목코드
                        'name': etf.get('ISU_ABBRV', ''),   # 종목명
                        'market_price': self._safe_float(etf.get('TDD_CLSPRC', 0)),  # 종가
                        'change': self._safe_float(etf.get('CMPPREVDD_PRC', 0)),     # 전일대비
                        'change_rate': self._safe_float(etf.get('FLUC_RT', 0)),      # 등락률
                        'volume': self._safe_int(etf.get('ACC_TRDVOL', 0)),          # 거래량
                        'market_cap': self._safe_float(etf.get('MKTCAP', 0)),        # 시가총액
                        'nav': self._safe_float(etf.get('NAV', 0)),                  # NAV
                        'data_source': 'krx_real',
                        'data_quality': 'excellent',
                        'last_updated': datetime.now().isoformat()
                    }
                    processed_etfs.append(processed_etf)
                
                return processed_etfs
            else:
                print(f"❌ KRX API 호출 실패: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ KRX 데이터 수집 실패: {e}")
            return []
    
    def collect_naver_etf_detail(self, etf_code: str) -> Dict:
        """네이버 금융에서 개별 ETF 상세 정보 수집"""
        try:
            url = f"https://finance.naver.com/item/main.naver?code={etf_code}"
            response = self.session.get(url)
            
            if response.status_code != 200:
                return {}
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # ETF 기본 정보 추출
            etf_info = {
                'code': etf_code,
                'data_source': 'naver_real',
                'data_quality': 'good',
                'last_updated': datetime.now().isoformat()
            }
            
            # 현재가 추출
            current_price = soup.select_one('.no_today .blind')
            if current_price:
                etf_info['market_price'] = self._safe_float(current_price.text.replace(',', ''))
            
            # 운용보수 추출 (ETF 요약 정보에서)
            summary_table = soup.select('.tb_type1 tr')
            for row in summary_table:
                th = row.select_one('th')
                td = row.select_one('td')
                if th and td:
                    if '운용보수' in th.text:
                        expense_text = td.text.strip()
                        # "0.15%" 형태에서 숫자 추출
                        expense_ratio = self._extract_percentage(expense_text)
                        if expense_ratio:
                            etf_info['expense_ratio'] = expense_ratio
            
            # 순자산 추출 (시가총액으로 대체)
            market_cap_elem = soup.select_one('em[id*="market_sum"]')
            if market_cap_elem:
                market_cap_text = market_cap_elem.text.replace(',', '').replace('억원', '')
                etf_info['aum'] = self._safe_float(market_cap_text)
            
            # ETF 요약 정보에서 추가 데이터
            info_table = soup.select('.tab_con1 .tb_type1 tr')
            for row in info_table:
                cells = row.select('td')
                if len(cells) >= 2:
                    label = cells[0].text.strip()
                    value = cells[1].text.strip()
                    
                    if '펀드규모' in label or '순자산' in label:
                        # "1,234억원" 형태에서 숫자 추출
                        aum_value = self._extract_amount(value)
                        if aum_value:
                            etf_info['aum'] = aum_value
                    
                    elif '배당수익률' in label:
                        dividend_yield = self._extract_percentage(value)
                        if dividend_yield:
                            etf_info['dividend_yield'] = dividend_yield
                    
                    elif '운용회사' in label:
                        etf_info['fund_manager'] = value
            
            print(f"✅ {etf_code} 네이버 데이터 수집 완료")
            return etf_info
            
        except Exception as e:
            print(f"❌ {etf_code} 네이버 데이터 수집 실패: {e}")
            return {}
    
    def collect_etf_detail_from_multiple_sources(self, etf_code: str) -> Dict:
        """여러 소스에서 ETF 상세 정보 수집 및 병합"""
        print(f"🔍 {etf_code} 상세 정보 수집 중...")
        
        # 네이버에서 데이터 수집
        naver_data = self.collect_naver_etf_detail(etf_code)
        
        # 다른 소스에서도 수집 (예: 다음, 운용사 사이트 등)
        # daum_data = self.collect_daum_etf_detail(etf_code)
        
        # 데이터 병합 (네이버 우선)
        merged_data = naver_data.copy()
        
        # 데이터 품질 평가
        quality_score = self._evaluate_data_quality(merged_data)
        merged_data['quality_score'] = quality_score
        
        if quality_score >= 80:
            merged_data['data_quality'] = 'excellent'
        elif quality_score >= 60:
            merged_data['data_quality'] = 'good'
        elif quality_score >= 40:
            merged_data['data_quality'] = 'fair'
        else:
            merged_data['data_quality'] = 'poor'
        
        return merged_data
    
    def batch_collect_etf_data(self, etf_codes: List[str], delay: float = 1.0) -> List[Dict]:
        """여러 ETF의 데이터를 배치로 수집"""
        print(f"🚀 {len(etf_codes)}개 ETF 실제 데이터 수집 시작...")
        
        collected_data = []
        
        for i, code in enumerate(etf_codes):
            print(f"[{i+1}/{len(etf_codes)}] {code} 수집 중...")
            
            etf_data = self.collect_etf_detail_from_multiple_sources(code)
            if etf_data:
                collected_data.append(etf_data)
            
            # 요청 간 지연
            if i < len(etf_codes) - 1:
                time.sleep(delay)
        
        print(f"✅ {len(collected_data)}개 ETF 실제 데이터 수집 완료")
        return collected_data
    
    def update_database_with_real_data(self, db_path: str, real_data: List[Dict]) -> int:
        """수집한 실제 데이터로 데이터베이스 업데이트"""
        try:
            conn = sqlite3.connect(db_path)
            updated_count = 0
            
            for etf_data in real_data:
                # 기존 레코드 업데이트
                update_query = '''
                    UPDATE etf_info 
                    SET market_price = ?, aum = ?, expense_ratio = ?, dividend_yield = ?,
                        fund_manager = ?, data_source = ?, data_quality = ?, 
                        quality_score = ?, last_updated = ?, last_real_update = ?
                    WHERE code = ?
                '''
                
                cursor = conn.execute(update_query, (
                    etf_data.get('market_price'),
                    etf_data.get('aum'),
                    etf_data.get('expense_ratio'),
                    etf_data.get('dividend_yield'),
                    etf_data.get('fund_manager'),
                    etf_data.get('data_source'),
                    etf_data.get('data_quality'),
                    etf_data.get('quality_score'),
                    etf_data.get('last_updated'),
                    datetime.now().isoformat(),
                    etf_data.get('code')
                ))
                
                if cursor.rowcount > 0:
                    updated_count += 1
            
            conn.commit()
            conn.close()
            
            print(f"✅ {updated_count}개 ETF 데이터베이스 업데이트 완료")
            return updated_count
            
        except Exception as e:
            print(f"❌ 데이터베이스 업데이트 실패: {e}")
            return 0
    
    def collect_major_korean_etfs(self) -> List[Dict]:
        """주요 한국 ETF들의 실제 데이터 수집"""
        major_etf_codes = [
            '069500',  # KODEX 200
            '114800',  # KODEX 인버스
            '229200',  # KODEX 코스닥150
            '360750',  # TIGER 미국S&P500
            '308620',  # TIGER 선진국MSCI World
            '381170',  # KODEX 미국S&P500TR
            '114260',  # KODEX 국고채10년
            '130730',  # KOSEF 국고채10년
            '148070',  # KOSEF 국고채3년
            '091160',  # KODEX 반도체
            '091170',  # KODEX 은행
            '261140',  # KODEX 2차전지산업
            '102110',  # TIGER 200
            '139270',  # TIGER 코스닥150
            '182490',  # TIGER 200TR
        ]
        
        return self.batch_collect_etf_data(major_etf_codes, delay=0.5)
    
    # 유틸리티 메서드들
    def _safe_float(self, value) -> float:
        """안전한 float 변환"""
        try:
            if isinstance(value, str):
                value = value.replace(',', '').replace('%', '')
            return float(value) if value else 0.0
        except:
            return 0.0
    
    def _safe_int(self, value) -> int:
        """안전한 int 변환"""
        try:
            if isinstance(value, str):
                value = value.replace(',', '')
            return int(float(value)) if value else 0
        except:
            return 0
    
    def _extract_percentage(self, text: str) -> Optional[float]:
        """텍스트에서 백분율 추출 (예: "0.15%" -> 0.15)"""
        import re
        match = re.search(r'(\d+\.?\d*)%?', text.replace(',', ''))
        if match:
            return float(match.group(1))
        return None
    
    def _extract_amount(self, text: str) -> Optional[float]:
        """텍스트에서 금액 추출 (예: "1,234억원" -> 1234)"""
        import re
        
        # "1,234억원" 형태
        if '억' in text:
            match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)억', text)
            if match:
                amount_str = match.group(1).replace(',', '')
                return float(amount_str)
        
        # "1.2조원" 형태
        if '조' in text:
            match = re.search(r'(\d+(?:\.\d+)?)조', text)
            if match:
                amount_str = match.group(1)
                return float(amount_str) * 10000  # 조를 억으로 변환
        
        return None
    
    def _evaluate_data_quality(self, etf_data: Dict) -> int:
        """데이터 품질 점수 계산 (0-100)"""
        score = 0
        
        # 필수 데이터 체크
        if etf_data.get('market_price', 0) > 0:
            score += 25
        if etf_data.get('aum', 0) > 0:
            score += 25
        if etf_data.get('expense_ratio', 0) > 0:
            score += 20
        if etf_data.get('fund_manager'):
            score += 10
        if etf_data.get('dividend_yield', 0) >= 0:
            score += 10
        if etf_data.get('last_updated'):
            score += 10
        
        return min(score, 100)


# 사용 예제
def main():
    """실제 ETF 데이터 수집 및 업데이트 실행"""
    print("🌐 실제 ETF 데이터 수집 시작!")
    print("=" * 50)
    
    collector = RealETFDataCollector()
    
    # 주요 ETF 실제 데이터 수집
    real_data = collector.collect_major_korean_etfs()
    
    if real_data:
        print(f"\n📊 수집된 실제 데이터:")
        for etf in real_data[:5]:  # 상위 5개만 표시
            print(f"- {etf.get('code')}: {etf.get('name', 'N/A')}")
            print(f"  현재가: {etf.get('market_price', 0):,.0f}원")
            print(f"  순자산: {etf.get('aum', 0):,.0f}억원")
            print(f"  운용보수: {etf.get('expense_ratio', 0):.3f}%")
            print(f"  품질: {etf.get('data_quality')} ({etf.get('quality_score', 0)}점)")
        
        # 데이터베이스 업데이트
        db_path = "etf_universe.db"
        updated_count = collector.update_database_with_real_data(db_path, real_data)
        
        print(f"\n✅ {updated_count}개 ETF 실제 데이터로 업데이트 완료!")
        print("🎉 이제 대시보드에서 실제 순자산 데이터를 확인하세요!")
    
    else:
        print("❌ 실제 데이터 수집에 실패했습니다")


if __name__ == "__main__":
    main()