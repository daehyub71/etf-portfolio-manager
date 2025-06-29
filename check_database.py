# ==========================================
# 실제 ETF 데이터 수집기 (네이버 금융 + pykrx 연동)
# ==========================================

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import json
import re

# pykrx 설치 및 import
try:
    from pykrx import stock
    from pykrx import bond
    PYKRX_AVAILABLE = True
    print("✅ pykrx 사용 가능")
except ImportError:
    PYKRX_AVAILABLE = False
    print("❌ pykrx 설치 필요: pip install pykrx")

# 추가 라이브러리들
try:
    import FinanceDataReader as fdr
    FDR_AVAILABLE = True
    print("✅ FinanceDataReader 사용 가능")
except ImportError:
    FDR_AVAILABLE = False
    print("⚠️ FinanceDataReader 설치 권장: pip install finance-datareader")

logger = logging.getLogger(__name__)

class RealETFDataCollector:
    """실제 ETF 데이터 수집기"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        self.last_request_time = 0
        self.request_delay = 1.0  # 1초 지연
        
        print("🚀 실제 ETF 데이터 수집기 초기화 완료")
    
    def _wait_for_rate_limit(self):
        """요청 간격 제한"""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < self.request_delay:
            time.sleep(self.request_delay - elapsed)
        self.last_request_time = time.time()
    
    # ==========================================
    # 1. 네이버 금융 실시간 데이터 수집
    # ==========================================
    
    def get_naver_etf_realtime_data(self, code: str) -> Dict:
        """네이버 금융에서 실시간 ETF 데이터 수집"""
        try:
            self._wait_for_rate_limit()
            
            # 네이버 금융 ETF 페이지 URL
            url = f"https://finance.naver.com/item/main.naver?code={code}"
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            etf_data = {
                'code': code,
                'timestamp': datetime.now().isoformat(),
                'source': 'naver'
            }
            
            # 현재가 정보 추출
            try:
                # 현재가
                price_element = soup.select_one('.no_today .blind')
                if price_element:
                    current_price = price_element.get_text().strip()
                    etf_data['current_price'] = float(current_price.replace(',', ''))
                
                # 전일 대비
                change_element = soup.select_one('.no_exday .blind')
                if change_element:
                    change_text = change_element.get_text().strip()
                    change_match = re.search(r'([+-]?\d+)', change_text.replace(',', ''))
                    if change_match:
                        etf_data['price_change'] = float(change_match.group(1))
                
                # 등락률
                rate_element = soup.select_one('.no_exday')
                if rate_element:
                    rate_text = rate_element.get_text()
                    rate_match = re.search(r'([+-]?\d+\.?\d*)%', rate_text)
                    if rate_match:
                        etf_data['change_rate'] = float(rate_match.group(1))
                
            except Exception as e:
                logger.debug(f"가격 정보 추출 실패: {e}")
            
            # ETF 기본 정보 추출
            try:
                # 펀드 기본 정보 테이블
                tables = soup.select('table.tbl_data')
                for table in tables:
                    rows = table.select('tr')
                    for row in rows:
                        cells = row.select('td, th')
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True)
                            value = cells[1].get_text(strip=True)
                            
                            # 운용보수 / 총보수
                            if '보수' in key and ('운용' in key or '총' in key):
                                expense_match = re.search(r'(\d+\.?\d*)%', value)
                                if expense_match:
                                    etf_data['expense_ratio'] = float(expense_match.group(1))
                            
                            # 배당수익률
                            elif '배당' in key and '수익률' in key:
                                dividend_match = re.search(r'(\d+\.?\d*)%', value)
                                if dividend_match:
                                    etf_data['dividend_yield'] = float(dividend_match.group(1))
                            
                            # 순자산 총액 (AUM)
                            elif '순자산' in key:
                                aum_match = re.search(r'(\d+[,\d]*)', value.replace(',', ''))
                                if aum_match:
                                    aum_value = float(aum_match.group(1))
                                    if '조' in value:
                                        aum_value *= 10000  # 조원 -> 억원
                                    elif '억' not in value and '만' in value:
                                        aum_value /= 10000  # 만원 -> 억원
                                    etf_data['aum'] = aum_value
                            
                            # 거래량
                            elif '거래량' in key:
                                volume_match = re.search(r'(\d+[,\d]*)', value.replace(',', ''))
                                if volume_match:
                                    etf_data['volume'] = int(volume_match.group(1))
                
            except Exception as e:
                logger.debug(f"ETF 정보 추출 실패: {e}")
            
            # ETF 이름 추출
            try:
                title_element = soup.select_one('.wrap_company h2')
                if title_element:
                    etf_data['name'] = title_element.get_text(strip=True)
            except Exception as e:
                logger.debug(f"ETF 이름 추출 실패: {e}")
            
            print(f"✅ {code} 네이버 데이터 수집 성공: {etf_data.get('current_price', 0):,}원")
            return etf_data
            
        except Exception as e:
            logger.error(f"네이버 ETF 데이터 수집 실패 {code}: {e}")
            return {'code': code, 'error': str(e), 'source': 'naver'}
    
    # ==========================================
    # 2. pykrx를 통한 공식 데이터 수집
    # ==========================================
    
    def get_krx_etf_data(self, code: str, days: int = 30) -> Dict:
        """pykrx를 통한 KRX 공식 ETF 데이터 수집"""
        if not PYKRX_AVAILABLE:
            return {'code': code, 'error': 'pykrx not available', 'source': 'krx'}
        
        try:
            self._wait_for_rate_limit()
            
            # 날짜 설정
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
            
            etf_data = {
                'code': code,
                'timestamp': datetime.now().isoformat(),
                'source': 'krx'
            }
            
            # OHLCV 데이터 수집
            try:
                df = stock.get_market_ohlcv_by_date(start_date, end_date, code)
                if not df.empty:
                    latest = df.iloc[-1]
                    
                    # 컬럼명 처리 (한글/영어 혼용 대응)
                    if '종가' in df.columns:
                        etf_data['current_price'] = float(latest['종가'])
                        etf_data['volume'] = int(latest['거래량'])
                        etf_data['high_52w'] = float(df['고가'].max())
                        etf_data['low_52w'] = float(df['저가'].min())
                    else:
                        # 영어 컬럼명인 경우
                        etf_data['current_price'] = float(latest.iloc[3])  # close
                        etf_data['volume'] = int(latest.iloc[4])  # volume
                        etf_data['high_52w'] = float(df.iloc[:, 1].max())  # high
                        etf_data['low_52w'] = float(df.iloc[:, 2].min())  # low
                    
                    # 수익률 계산
                    if len(df) >= 2:
                        prev_price = df.iloc[-2].iloc[3]  # 전일 종가
                        price_change = etf_data['current_price'] - prev_price
                        etf_data['price_change'] = price_change
                        etf_data['change_rate'] = (price_change / prev_price) * 100
                    
                    # 가격 히스토리 (최근 30일)
                    price_history = df.iloc[:, 3].tolist()  # 종가 리스트
                    etf_data['price_history'] = price_history[-30:]  # 최근 30일
                    
                    # 변동성 계산
                    returns = df.iloc[:, 3].pct_change().dropna()
                    if len(returns) > 1:
                        etf_data['volatility'] = float(returns.std() * np.sqrt(252) * 100)  # 연환산 변동성
                
            except Exception as e:
                logger.debug(f"KRX OHLCV 데이터 수집 실패: {e}")
            
            # ETF 펀더멘털 정보 (시도만 하고 실패해도 무시)
            try:
                # ETF 기본 정보
                etf_info = stock.get_etf_portfolio_deposit_file(code)
                if not etf_info.empty:
                    etf_data['portfolio_info'] = etf_info.to_dict('records')
            except:
                pass  # 펀더멘털 데이터는 선택사항
            
            print(f"✅ {code} KRX 데이터 수집 성공: {etf_data.get('current_price', 0):,}원")
            return etf_data
            
        except Exception as e:
            logger.error(f"KRX ETF 데이터 수집 실패 {code}: {e}")
            return {'code': code, 'error': str(e), 'source': 'krx'}
    
    # ==========================================
    # 3. FinanceDataReader를 통한 추가 데이터
    # ==========================================
    
    def get_fdr_etf_data(self, code: str, days: int = 30) -> Dict:
        """FinanceDataReader를 통한 ETF 데이터 수집"""
        if not FDR_AVAILABLE:
            return {'code': code, 'error': 'fdr not available', 'source': 'fdr'}
        
        try:
            self._wait_for_rate_limit()
            
            # KRX ETF 코드 형식으로 변환
            krx_code = f"{code}.KS"
            
            # 날짜 설정
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # 데이터 수집
            df = fdr.DataReader(krx_code, start_date, end_date)
            
            if not df.empty:
                latest = df.iloc[-1]
                
                etf_data = {
                    'code': code,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'fdr',
                    'current_price': float(latest['Close']),
                    'volume': int(latest['Volume']),
                    'high_52w': float(df['High'].max()),
                    'low_52w': float(df['Low'].min()),
                }
                
                # 수익률 계산
                if len(df) >= 2:
                    prev_price = df.iloc[-2]['Close']
                    price_change = etf_data['current_price'] - prev_price
                    etf_data['price_change'] = price_change
                    etf_data['change_rate'] = (price_change / prev_price) * 100
                
                # 기술적 지표 계산
                prices = df['Close']
                
                # 이동평균
                etf_data['ma_5'] = float(prices.rolling(5).mean().iloc[-1])
                etf_data['ma_20'] = float(prices.rolling(20).mean().iloc[-1])
                
                # RSI (간단 버전)
                delta = prices.diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                etf_data['rsi'] = float(100 - (100 / (1 + rs.iloc[-1])))
                
                print(f"✅ {code} FDR 데이터 수집 성공: {etf_data['current_price']:,}원")
                return etf_data
            else:
                return {'code': code, 'error': 'no data', 'source': 'fdr'}
                
        except Exception as e:
            logger.error(f"FDR ETF 데이터 수집 실패 {code}: {e}")
            return {'code': code, 'error': str(e), 'source': 'fdr'}
    
    # ==========================================
    # 4. 통합 데이터 수집
    # ==========================================
    
    def collect_comprehensive_etf_data(self, code: str) -> Dict:
        """여러 소스에서 ETF 데이터를 종합 수집"""
        print(f"\n📊 {code} 종합 데이터 수집 시작...")
        
        comprehensive_data = {
            'code': code,
            'collection_time': datetime.now().isoformat(),
            'sources_used': [],
            'data_quality': 'unknown'
        }
        
        # 1. 네이버 금융 데이터 (실시간 가격 + 펀드 정보)
        print(f"  📈 네이버 금융 데이터 수집...")
        naver_data = self.get_naver_etf_realtime_data(code)
        if 'error' not in naver_data:
            comprehensive_data.update(naver_data)
            comprehensive_data['sources_used'].append('naver')
            print(f"    ✅ 네이버: {naver_data.get('current_price', 0):,}원")
        else:
            print(f"    ❌ 네이버 실패: {naver_data['error']}")
        
        # 2. KRX 공식 데이터 (OHLCV + 히스토리)
        print(f"  📊 KRX 공식 데이터 수집...")
        krx_data = self.get_krx_etf_data(code)
        if 'error' not in krx_data:
            # 중복되지 않는 정보만 추가
            for key, value in krx_data.items():
                if key not in comprehensive_data or key in ['price_history', 'volatility', 'high_52w', 'low_52w']:
                    comprehensive_data[key] = value
            comprehensive_data['sources_used'].append('krx')
            print(f"    ✅ KRX: 히스토리 {len(krx_data.get('price_history', []))}일")
        else:
            print(f"    ❌ KRX 실패: {krx_data['error']}")
        
        # 3. FinanceDataReader 데이터 (기술적 지표)
        if FDR_AVAILABLE:
            print(f"  📈 FDR 기술적 지표 수집...")
            fdr_data = self.get_fdr_etf_data(code)
            if 'error' not in fdr_data:
                # 기술적 지표만 추가
                for key in ['ma_5', 'ma_20', 'rsi']:
                    if key in fdr_data:
                        comprehensive_data[key] = fdr_data[key]
                if 'fdr' not in comprehensive_data['sources_used']:
                    comprehensive_data['sources_used'].append('fdr')
                print(f"    ✅ FDR: 기술적 지표 추가")
            else:
                print(f"    ❌ FDR 실패: {fdr_data['error']}")
        
        # 데이터 품질 평가
        quality_score = 0
        if 'current_price' in comprehensive_data and comprehensive_data['current_price'] > 0:
            quality_score += 30
        if 'expense_ratio' in comprehensive_data:
            quality_score += 20
        if 'dividend_yield' in comprehensive_data:
            quality_score += 20
        if 'price_history' in comprehensive_data and len(comprehensive_data['price_history']) > 10:
            quality_score += 20
        if 'volume' in comprehensive_data and comprehensive_data['volume'] > 0:
            quality_score += 10
        
        if quality_score >= 80:
            comprehensive_data['data_quality'] = 'excellent'
        elif quality_score >= 60:
            comprehensive_data['data_quality'] = 'good'
        elif quality_score >= 40:
            comprehensive_data['data_quality'] = 'fair'
        else:
            comprehensive_data['data_quality'] = 'poor'
        
        print(f"  📊 데이터 품질: {comprehensive_data['data_quality']} ({quality_score}점)")
        print(f"  📊 사용된 소스: {', '.join(comprehensive_data['sources_used'])}")
        
        return comprehensive_data
    
    # ==========================================
    # 5. ETF 목록 실시간 수집
    # ==========================================
    
    def get_etf_universe_from_krx(self) -> List[Dict]:
        """KRX에서 전체 ETF 목록 수집"""
        if not PYKRX_AVAILABLE:
            print("❌ pykrx가 필요합니다: pip install pykrx")
            return []
        
        try:
            print("📋 KRX에서 ETF 목록 수집 중...")
            
            # ETF 티커 목록 가져오기
            etf_tickers = stock.get_etf_ticker_list()
            print(f"📋 총 {len(etf_tickers)}개 ETF 발견")
            
            etf_universe = []
            
            # 각 ETF의 기본 정보 수집
            for i, ticker in enumerate(etf_tickers[:20]):  # 처음 20개만 테스트
                try:
                    print(f"  [{i+1:2d}/{len(etf_tickers[:20])}] {ticker} 처리 중...")
                    
                    # 간단한 정보만 수집 (속도 향상)
                    basic_data = self.get_naver_etf_realtime_data(ticker)
                    
                    if 'error' not in basic_data:
                        etf_info = {
                            'code': ticker,
                            'name': basic_data.get('name', f'ETF_{ticker}'),
                            'current_price': basic_data.get('current_price', 0),
                            'change_rate': basic_data.get('change_rate', 0),
                            'volume': basic_data.get('volume', 0),
                            'aum': basic_data.get('aum', 0),
                            'expense_ratio': basic_data.get('expense_ratio', 0),
                            'last_updated': datetime.now().isoformat()
                        }
                        etf_universe.append(etf_info)
                        print(f"    ✅ {basic_data.get('name', ticker)}: {basic_data.get('current_price', 0):,}원")
                    else:
                        print(f"    ❌ {ticker} 실패")
                
                except Exception as e:
                    print(f"    ❌ {ticker} 오류: {e}")
                    continue
                
                # 요청 간격 (서버 부하 방지)
                time.sleep(0.5)
            
            print(f"✅ ETF 목록 수집 완료: {len(etf_universe)}개")
            return etf_universe
            
        except Exception as e:
            print(f"❌ ETF 목록 수집 실패: {e}")
            return []
    
    # ==========================================
    # 6. 배치 업데이트
    # ==========================================
    
    def batch_update_etf_data(self, etf_codes: List[str], max_concurrent: int = 5) -> List[Dict]:
        """여러 ETF 데이터를 배치로 수집"""
        print(f"\n🔄 {len(etf_codes)}개 ETF 배치 업데이트 시작...")
        
        results = []
        
        for i, code in enumerate(etf_codes):
            try:
                print(f"\n[{i+1}/{len(etf_codes)}] {code} 처리 중...")
                
                # 종합 데이터 수집
                data = self.collect_comprehensive_etf_data(code)
                results.append(data)
                
                # 진행률 표시
                progress = ((i + 1) / len(etf_codes)) * 100
                print(f"  📊 진행률: {progress:.1f}% ({i+1}/{len(etf_codes)})")
                
                # 요청 간격 (서버 부하 방지)
                if i < len(etf_codes) - 1:
                    time.sleep(1.0)
                
            except Exception as e:
                print(f"❌ {code} 처리 실패: {e}")
                results.append({
                    'code': code,
                    'error': str(e),
                    'collection_time': datetime.now().isoformat()
                })
                continue
        
        # 결과 요약
        successful = len([r for r in results if 'error' not in r])
        failed = len(results) - successful
        
        print(f"\n✅ 배치 업데이트 완료!")
        print(f"  성공: {successful}개")
        print(f"  실패: {failed}개")
        print(f"  성공률: {(successful/len(results)*100):.1f}%")
        
        return results


# ==========================================
# 사용 예제
# ==========================================

def main():
    """실제 ETF 데이터 수집 테스트"""
    print("🚀 실제 ETF 데이터 수집기 테스트")
    print("=" * 50)
    
    # 데이터 수집기 초기화
    collector = RealETFDataCollector()
    
    # 테스트할 ETF 코드들
    test_etfs = [
        '069500',  # KODEX 200
        '360750',  # TIGER 미국S&P500  
        '114260',  # KODEX 국고채10년
        '102960',  # KODEX 기계장비 (이미지의 ETF)
    ]
    
    print(f"\n1️⃣ 개별 ETF 상세 데이터 수집 테스트")
    for code in test_etfs[:2]:  # 처음 2개만
        print(f"\n{'='*40}")
        comprehensive_data = collector.collect_comprehensive_etf_data(code)
        
        # 결과 요약 출력
        if comprehensive_data.get('data_quality') != 'poor':
            print(f"\n📊 {code} 수집 결과:")
            print(f"  이름: {comprehensive_data.get('name', 'Unknown')}")
            print(f"  현재가: {comprehensive_data.get('current_price', 0):,}원")
            print(f"  등락률: {comprehensive_data.get('change_rate', 0):+.2f}%")
            print(f"  거래량: {comprehensive_data.get('volume', 0):,}주")
            print(f"  운용보수: {comprehensive_data.get('expense_ratio', 0):.2f}%")
            print(f"  배당수익률: {comprehensive_data.get('dividend_yield', 0):.2f}%")
            print(f"  AUM: {comprehensive_data.get('aum', 0):,.0f}억원")
            print(f"  데이터 품질: {comprehensive_data['data_quality']}")
            print(f"  소스: {', '.join(comprehensive_data['sources_used'])}")
        else:
            print(f"❌ {code} 데이터 수집 실패")
    
    print(f"\n2️⃣ 배치 업데이트 테스트")
    batch_results = collector.batch_update_etf_data(test_etfs)
    
    # 결과를 DataFrame으로 변환하여 표시
    summary_data = []
    for result in batch_results:
        if 'error' not in result:
            summary_data.append({
                'ETF코드': result['code'],
                'ETF명': result.get('name', 'Unknown')[:15],  # 15자로 제한
                '현재가': f"{result.get('current_price', 0):,.0f}원",
                '등락률': f"{result.get('change_rate', 0):+.1f}%",
                '운용보수': f"{result.get('expense_ratio', 0):.2f}%",
                '품질': result['data_quality'],
                '소스': '/'.join(result['sources_used'])
            })
        else:
            summary_data.append({
                'ETF코드': result['code'],
                'ETF명': 'ERROR',
                '현재가': '수집실패',
                '등락률': '-',
                '운용보수': '-',
                '품질': 'error',
                '소스': '-'
            })
    
    # 결과 테이블 출력
    if summary_data:
        df = pd.DataFrame(summary_data)
        print(f"\n📊 수집 결과 요약:")
        print(df.to_string(index=False))
    
    print(f"\n✅ 실제 데이터 수집 테스트 완료!")
    print(f"💡 이제 이 수집기를 기존 시스템에 통합하여 사용할 수 있습니다.")

if __name__ == "__main__":
    main()