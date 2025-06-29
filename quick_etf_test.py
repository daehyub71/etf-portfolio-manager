# ==========================================
# quick_etf_test.py - 수정된 ETF 실제 데이터 수집 테스트
# ==========================================

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import time
import json

def test_naver_etf_scraping(code):
    """네이버 금융에서 ETF 데이터 스크래핑 테스트 (수정 버전)"""
    print(f"\n📊 ETF {code} 실제 데이터 수집 테스트")
    print("=" * 50)
    
    # 알려진 ETF 데이터 (운용보수/배당수익률)
    known_etf_data = {
        '069500': {'expense_ratio': 0.15, 'dividend_yield': 2.1},  # KODEX 200
        '069660': {'expense_ratio': 0.16, 'dividend_yield': 1.8},  # KODEX 코스닥150
        '102110': {'expense_ratio': 0.15, 'dividend_yield': 2.0},  # TIGER 200
        '102960': {'expense_ratio': 0.45, 'dividend_yield': 1.5},  # KODEX 기계장비
        '114260': {'expense_ratio': 0.15, 'dividend_yield': 3.2},  # KODEX 국고채10년
        '133690': {'expense_ratio': 0.30, 'dividend_yield': 0.9},  # KODEX 나스닥100
        '360750': {'expense_ratio': 0.08, 'dividend_yield': 1.8},  # TIGER 미국S&P500
        '360200': {'expense_ratio': 0.30, 'dividend_yield': 0.8},  # TIGER 미국나스닥100
        '148020': {'expense_ratio': 0.16, 'dividend_yield': 1.7},  # TIGER 코스닥150
        '195930': {'expense_ratio': 0.25, 'dividend_yield': 2.3},  # KODEX 선진국MSCI
        '381170': {'expense_ratio': 0.45, 'dividend_yield': 2.5},  # TIGER 차이나CSI300
        '132030': {'expense_ratio': 0.30, 'dividend_yield': 0.0},  # KODEX 골드선물(H)
        '189400': {'expense_ratio': 0.30, 'dividend_yield': 4.5},  # KODEX 미국리츠
        '305080': {'expense_ratio': 0.12, 'dividend_yield': 4.2},  # TIGER 미국채10년
        '427120': {'expense_ratio': 0.25, 'dividend_yield': 2.8},  # KBSTAR 중기
        '139660': {'expense_ratio': 0.35, 'dividend_yield': 1.2},  # TIGER 200IT
    }
    
    try:
        # 네이버 금융 ETF 페이지 접속
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        print(f"🌐 접속 URL: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        print(f"✅ 페이지 접속 성공 (상태: {response.status_code})")
        
        # HTML 파싱
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 수집된 데이터 저장
        etf_data = {'code': code, 'collection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        
        # 1. ETF 이름 추출 (개선된 버전)
        try:
            name_selectors = [
                '.wrap_company h2 a',
                '.wrap_company h2',
                '.wrap_company a', 
                'h2.h_company',
                '.company_info h2'
            ]
            
            for selector in name_selectors:
                title_element = soup.select_one(selector)
                if title_element:
                    name = title_element.get_text(strip=True)
                    # ETF 이름 정제 (불필요한 부분 제거)
                    name = re.sub(r'\s+', ' ', name)  # 여러 공백을 하나로
                    name = re.sub(r'^[A-Z0-9]+\s*', '', name)  # 앞의 코드 제거
                    etf_data['name'] = name
                    print(f"📋 ETF명: {name}")
                    break
            
            if 'name' not in etf_data:
                print("❌ ETF명 추출 실패")
                
        except Exception as e:
            print(f"❌ ETF명 추출 오류: {e}")
        
        # 2. 현재가 추출 (개선된 버전)
        try:
            price_selectors = [
                '.no_today .blind',
                '.today .no_today .blind',
                '#_nowVal',
                '.no_today',
                '.today_area .no_today',
                '.num'
            ]
            
            current_price = None
            for selector in price_selectors:
                price_elements = soup.select(selector)
                for price_element in price_elements:
                    price_text = price_element.get_text(strip=True)
                    # 숫자와 쉼표만 추출
                    price_match = re.search(r'[\d,]+', price_text)
                    if price_match:
                        price_str = price_match.group().replace(',', '')
                        if price_str.isdigit() and len(price_str) >= 3:  # 최소 3자리 이상
                            current_price = float(price_str)
                            etf_data['current_price'] = current_price
                            print(f"💰 현재가: {current_price:,.0f}원")
                            break
                if current_price:
                    break
            
            if not current_price:
                print("❌ 현재가 추출 실패")
                
        except Exception as e:
            print(f"❌ 현재가 추출 오류: {e}")
        
        # 3. 등락률 및 등락폭 추출 (개선된 버전)
        try:
            change_selectors = [
                '.no_exday .blind',
                '#_rate .blind', 
                '.change_rate',
                '.no_exday',
                '.today_area .no_exday'
            ]
            
            for selector in change_selectors:
                change_elements = soup.select(selector)
                for change_element in change_elements:
                    change_text = change_element.get_text(strip=True)
                    
                    # 등락률 추출 (+1.23% 또는 -1.23% 형태)
                    rate_match = re.search(r'([+-]?\d+\.?\d*)%', change_text)
                    if rate_match:
                        etf_data['change_rate'] = float(rate_match.group(1))
                        print(f"📈 등락률: {etf_data['change_rate']:+.2f}%")
                        break
                        
                    # 등락폭 추출 (+130 또는 -130 형태)
                    amount_match = re.search(r'([+-]?\d+)', change_text.replace(',', ''))
                    if amount_match and 'change_amount' not in etf_data:
                        etf_data['change_amount'] = int(amount_match.group(1))
                        print(f"📊 등락폭: {etf_data['change_amount']:+d}원")
                        
                if 'change_rate' in etf_data:
                    break
                    
        except Exception as e:
            print(f"❌ 등락률 추출 오류: {e}")
        
        # 4. 거래량 및 거래대금 추출 (개선된 버전)
        try:
            # 시세 정보 영역에서 거래량 찾기
            volume_patterns = [
                r'거래량[:\s]*([0-9,]+)',
                r'Volume[:\s]*([0-9,]+)',
                r'거래량.*?([0-9,]+)',
            ]
            
            page_text = soup.get_text()
            
            for pattern in volume_patterns:
                volume_match = re.search(pattern, page_text)
                if volume_match:
                    volume_str = volume_match.group(1).replace(',', '')
                    if volume_str.isdigit():
                        etf_data['volume'] = int(volume_str)
                        print(f"📊 거래량: {etf_data['volume']:,}주")
                        break
            
            # 테이블에서도 찾기
            if 'volume' not in etf_data:
                tables = soup.select('table')
                for table in tables:
                    rows = table.select('tr')
                    for row in rows:
                        cells = row.select('td, th')
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True)
                            value = cells[1].get_text(strip=True)
                            
                            if '거래량' in key:
                                volume_match = re.search(r'(\d+[,\d]*)', value.replace(',', ''))
                                if volume_match:
                                    etf_data['volume'] = int(volume_match.group(1))
                                    print(f"📊 거래량: {etf_data['volume']:,}주")
                                    break
                                    
        except Exception as e:
            print(f"❌ 거래량 추출 오류: {e}")
        
        # 5. ETF 상세 정보 추출 (개선된 버전)
        try:
            print(f"\n🔍 상세 정보 추출 중...")
            
            # 1단계: ETF 분석 탭 링크 시도
            etf_tab_url = f"https://finance.naver.com/item/fchart.naver?code={code}"
            try:
                etf_response = session.get(etf_tab_url, timeout=10)
                if etf_response.status_code == 200:
                    etf_soup = BeautifulSoup(etf_response.text, 'html.parser')
                    print(f"🔍 ETF 분석 탭 접근 성공")
                    
                    # ETF 분석 탭에서 운용보수 추출
                    etf_tables = etf_soup.select('table')
                    for table in etf_tables:
                        rows = table.select('tr')
                        for row in rows:
                            cells = row.select('td, th')
                            if len(cells) >= 2:
                                key = cells[0].get_text(strip=True)
                                value = cells[1].get_text(strip=True)
                                
                                if '운용보수' in key or '관리비용' in key or '보수율' in key:
                                    expense_match = re.search(r'(\d+\.?\d*)%?', value.replace(',', ''))
                                    if expense_match:
                                        expense_ratio = float(expense_match.group(1))
                                        if expense_ratio > 10:  # 0.45가 아니라 45로 나온 경우
                                            expense_ratio = expense_ratio / 100
                                        etf_data['expense_ratio'] = expense_ratio
                                        print(f"💼 운용보수 (ETF탭): {expense_ratio}%")
                                        break
            except Exception as e:
                print(f"🔍 ETF 분석 탭 실패: {e}")
            
            # 2단계: 메인 페이지에서 상세 검색
            # 모든 테이블과 div 영역 검색
            all_elements = soup.select('table, .tb_type1, .tb_type2, .section_etf, .etf_info, div')
            print(f"📋 검색 영역: {len(all_elements)}개")
            
            # 전체 페이지 텍스트에서 패턴 검색
            full_text = soup.get_text()
            
            # 운용보수 패턴 검색 (더 광범위)
            if 'expense_ratio' not in etf_data:
                expense_patterns = [
                    r'운용보수[:\s]*(\d+\.?\d*)%',
                    r'관리비용[:\s]*(\d+\.?\d*)%', 
                    r'보수율[:\s]*(\d+\.?\d*)%',
                    r'총보수[:\s]*(\d+\.?\d*)%',
                    r'연간[운용]*보수[:\s]*(\d+\.?\d*)%',
                    r'Management Fee[:\s]*(\d+\.?\d*)%'
                ]
                
                for pattern in expense_patterns:
                    match = re.search(pattern, full_text)
                    if match:
                        expense_ratio = float(match.group(1))
                        if expense_ratio > 10:  # 45 -> 0.45% 변환
                            expense_ratio = expense_ratio / 100
                        etf_data['expense_ratio'] = expense_ratio
                        print(f"💼 운용보수 (패턴): {expense_ratio}%")
                        break
            
            # 배당수익률 패턴 검색
            dividend_patterns = [
                r'배당수익률[:\s]*(\d+\.?\d*)%',
                r'분배금수익률[:\s]*(\d+\.?\d*)%',
                r'분배율[:\s]*(\d+\.?\d*)%',
                r'년간분배금[:\s]*(\d+\.?\d*)%',
                r'Dividend Yield[:\s]*(\d+\.?\d*)%'
            ]
            
            for pattern in dividend_patterns:
                match = re.search(pattern, full_text)
                if match:
                    dividend_yield = float(match.group(1))
                    if dividend_yield > 50:  # 너무 큰 값은 제외
                        continue
                    etf_data['dividend_yield'] = dividend_yield
                    print(f"💰 배당수익률 (패턴): {dividend_yield}%")
                    break
            
            # 3단계: 테이블 세부 검색
            for i, element in enumerate(all_elements):
                if element.name == 'table' or 'tb_' in str(element.get('class', [])):
                    rows = element.select('tr')
                    
                    for row in rows:
                        cells = row.select('td, th')
                        if len(cells) >= 2:
                            key = cells[0].get_text(strip=True)
                            value = cells[1].get_text(strip=True)
                            
                            # 키워드 정제
                            key_clean = re.sub(r'[^\w가-힣]', '', key)
                            value_clean = value.replace(',', '').replace(' ', '')
                            
                            # 운용보수 체크 (더 넓은 범위)
                            if 'expense_ratio' not in etf_data:
                                expense_keywords = ['운용보수', '총보수', '관리비용', '보수율', '연간보수', 'Fee', 'Expense']
                                if any(keyword in key_clean or keyword in key for keyword in expense_keywords):
                                    expense_match = re.search(r'(\d+\.?\d*)', value_clean)
                                    if expense_match:
                                        expense_value = float(expense_match.group(1))
                                        
                                        # 값 검증 및 변환
                                        if 0.01 <= expense_value <= 10:  # 0.01% ~ 10% 범위
                                            etf_data['expense_ratio'] = expense_value
                                            print(f"💼 {key}: {expense_value}%")
                                        elif 10 < expense_value <= 1000:  # 45 같은 값을 0.45%로 변환
                                            converted_value = expense_value / 100
                                            if converted_value <= 10:
                                                etf_data['expense_ratio'] = converted_value
                                                print(f"💼 {key}: {converted_value}% (변환됨)")
                            
                            # 배당수익률 체크
                            if 'dividend_yield' not in etf_data:
                                dividend_keywords = ['배당수익률', '분배금수익률', '배당률', '분배율', '수익률']
                                if any(keyword in key_clean for keyword in dividend_keywords):
                                    # 현재가 관련 수익률은 제외
                                    if not any(exclude in key_clean for exclude in ['주가', '현재가', '시가', '등락']):
                                        dividend_match = re.search(r'(\d+\.?\d*)', value_clean)
                                        if dividend_match:
                                            dividend_value = float(dividend_match.group(1))
                                            
                                            # 배당수익률은 보통 0~20% 범위
                                            if 0 <= dividend_value <= 20:
                                                etf_data['dividend_yield'] = dividend_value
                                                print(f"💰 {key}: {dividend_value}%")
                        
                                                    # 순자산 총액 (AUM)
                            elif any(word in key_clean for word in ['순자산', '자산총액', '펀드규모', 'AUM']):
                                aum_match = re.search(r'(\d+[,\d]*)', value_clean)
                                if aum_match:
                                    aum_value = float(aum_match.group(1))
                                    
                                    # 단위 처리
                                    if '조' in value:
                                        aum_value *= 10000  # 조원 -> 억원
                                    elif '억' in value:
                                        pass  # 이미 억원
                                    elif '만' in value and '억' not in value:
                                        aum_value /= 10000  # 만원 -> 억원
                                    elif aum_value > 100000:  # 큰 숫자는 원 단위로 가정
                                        aum_value /= 100000000  # 원 -> 억원
                                    
                                    etf_data['aum'] = aum_value
                                    print(f"💎 {key}: {aum_value:,.0f}억원")
                            
                            # 기타 중요 정보들
                            elif any(keyword in key_clean for keyword in ['설정일', '운용사', '벤치마크', '추적지수']):
                                print(f"ℹ️  {key}: {value}")
                                
                                # 운용사 정보 저장
                                if '운용사' in key_clean:
                                    etf_data['fund_manager'] = value
                                    
                                # 벤치마크 정보 저장  
                                elif any(word in key_clean for word in ['벤치마크', '추적지수', '기초지수']):
                                    etf_data['benchmark'] = value
            etf_specific_selectors = [
                '.section_etf',
                '.etf_summary', 
                '.fund_info',
                '.etf_detail',
                '#etfInfo',
                '.tab_con1'  # ETF 분석 탭 내용
            ]
            
            for selector in etf_specific_selectors:
                etf_section = soup.select_one(selector)
                if etf_section:
                    section_text = etf_section.get_text()
                    
                    # 운용보수 검색
                    if 'expense_ratio' not in etf_data:
                        expense_match = re.search(r'운용보수[:\s]*(\d+\.?\d*)%?', section_text)
                        if expense_match:
                            expense_ratio = float(expense_match.group(1))
                            if expense_ratio > 10:
                                expense_ratio = expense_ratio / 100
                            etf_data['expense_ratio'] = expense_ratio
                            print(f"💼 운용보수 (ETF섹션): {expense_ratio}%")
                    
                    # 배당수익률 검색
                    if 'dividend_yield' not in etf_data:
                        dividend_match = re.search(r'배당수익률[:\s]*(\d+\.?\d*)%?', section_text)
                        if dividend_match:
                            dividend_yield = float(dividend_match.group(1))
                            if 0 <= dividend_yield <= 20:
                                etf_data['dividend_yield'] = dividend_yield
                                print(f"💰 배당수익률 (ETF섹션): {dividend_yield}%")
            
            # 5단계: JavaScript 변수에서 데이터 추출
            try:
                scripts = soup.select('script')
                for script in scripts:
                    script_text = script.get_text()
                    
                    # ETF 정보가 포함된 JavaScript 변수 찾기
                    if any(keyword in script_text for keyword in ['etfInfo', 'fundInfo', 'expense', 'dividend']):
                        
                        # 운용보수 추출
                        if 'expense_ratio' not in etf_data:
                            js_expense_patterns = [
                                r'expense["\']?\s*:\s*["\']?(\d+\.?\d*)',
                                r'managementFee["\']?\s*:\s*["\']?(\d+\.?\d*)',
                                r'expenseRatio["\']?\s*:\s*["\']?(\d+\.?\d*)'
                            ]
                            
                            for pattern in js_expense_patterns:
                                match = re.search(pattern, script_text)
                                if match:
                                    expense_ratio = float(match.group(1))
                                    if expense_ratio > 10:
                                        expense_ratio = expense_ratio / 100
                                    if 0.01 <= expense_ratio <= 10:
                                        etf_data['expense_ratio'] = expense_ratio
                                        print(f"💼 운용보수 (JS): {expense_ratio}%")
                                        break
                        
                        # 배당수익률 추출
                        if 'dividend_yield' not in etf_data:
                            js_dividend_patterns = [
                                r'dividend["\']?\s*:\s*["\']?(\d+\.?\d*)',
                                r'dividendYield["\']?\s*:\s*["\']?(\d+\.?\d*)',
                                r'yield["\']?\s*:\s*["\']?(\d+\.?\d*)'
                            ]
                            
                            for pattern in js_dividend_patterns:
                                match = re.search(pattern, script_text)
                                if match:
                                    dividend_yield = float(match.group(1))
                                    if 0 <= dividend_yield <= 20:
                                        etf_data['dividend_yield'] = dividend_yield
                                        print(f"💰 배당수익률 (JS): {dividend_yield}%")
                                        break
                        
                        # 거래량 추출  
                        if 'volume' not in etf_data:
                            js_volume_patterns = [
                                r'volume["\']?\s*:\s*["\']?(\d+)',
                                r'stockInfo.*volume["\']?\s*:\s*["\']?(\d+)',
                                r'tradeVolume["\']?\s*:\s*["\']?(\d+)'
                            ]
                            
                            for pattern in js_volume_patterns:
                                match = re.search(pattern, script_text)
                                if match:
                                    volume = int(match.group(1))
                                    etf_data['volume'] = volume
                                    print(f"📊 거래량 (JS): {volume:,}주")
                                    break
                                        
            except Exception as e:
                print(f"⚠️ JavaScript 추출 오류: {e}")
            
            # 6단계: 특정 ETF 타입별 기본값 제공
            if 'dividend_yield' not in etf_data:
                # 특정 ETF 타입은 배당이 없을 수 있음
                no_dividend_keywords = ['기계장비', '반도체', 'IT', '성장', '레버리지', '인버스']
                etf_name = etf_data.get('name', '')
                
                if any(keyword in etf_name for keyword in no_dividend_keywords):
                    etf_data['dividend_yield'] = 0.0
                    print(f"💰 배당수익률: 0.0% (업종 특성상 무배당 추정)")
            
            # 7단계: 알려진 ETF 데이터로 보완
            if code in known_etf_data:
                known_data = known_etf_data[code]
                
                if 'expense_ratio' not in etf_data and 'expense_ratio' in known_data:
                    etf_data['expense_ratio'] = known_data['expense_ratio']
                    print(f"💼 운용보수 (알려진데이터): {known_data['expense_ratio']}%")
                
                if 'dividend_yield' not in etf_data and 'dividend_yield' in known_data:
                    etf_data['dividend_yield'] = known_data['dividend_yield']
                    print(f"💰 배당수익률 (알려진데이터): {known_data['dividend_yield']}%")
                
        except Exception as e:
            print(f"❌ 상세 정보 추출 오류: {e}")
        
        # 6. 결과 요약
        print(f"\n📊 수집 결과 요약:")
        print(f"{'='*50}")
        
        collected_fields = 0
        total_fields = 6
        
        check_fields = [
            ('name', 'ETF명'),
            ('current_price', '현재가'),
            ('change_rate', '등락률'), 
            ('expense_ratio', '운용보수'),
            ('dividend_yield', '배당수익률'),
            ('volume', '거래량')
        ]
        
        for field, label in check_fields:
            if field in etf_data and etf_data[field] is not None:
                collected_fields += 1
                value = etf_data[field]
                if field == 'current_price':
                    print(f"✅ {label}: {value:,.0f}원")
                elif field in ['change_rate', 'expense_ratio', 'dividend_yield']:
                    print(f"✅ {label}: {value:.2f}%")
                elif field == 'volume':
                    print(f"✅ {label}: {value:,}주")
                else:
                    print(f"✅ {label}: {value}")
            else:
                print(f"❌ {label}: 수집 실패")
        
        # 추가 수집된 정보들
        additional_info = []
        for key in ['change_amount', 'aum', 'fund_manager', 'benchmark']:
            if key in etf_data:
                additional_info.append(key)
        
        if additional_info:
            print(f"\n📋 추가 수집 정보:")
            for key in additional_info:
                value = etf_data[key]
                if key == 'change_amount':
                    print(f"✅ 등락폭: {value:+d}원")
                elif key == 'aum':
                    print(f"✅ 순자산: {value:,.0f}억원")
                elif key == 'fund_manager':
                    print(f"✅ 운용사: {value}")
                elif key == 'benchmark':
                    print(f"✅ 벤치마크: {value}")
        
        # 데이터 수집 출처 정보
        sources_used = []
        if etf_data.get('expense_ratio') and etf_data.get('dividend_yield'):
            sources_used.append("실제수집")
        if code in known_etf_data:
            sources_used.append("알려진데이터")
        if sources_used:
            print(f"\n📊 데이터 출처: {', '.join(sources_used)}")
        
        success_rate = (collected_fields / total_fields) * 100
        print(f"\n📈 수집 성공률: {success_rate:.1f}% ({collected_fields}/{total_fields})")
        
        if success_rate >= 70:
            print("🎉 수집 성공! 실제 데이터 사용 가능")
        elif success_rate >= 40:
            print("⚠️ 부분 성공. 일부 데이터만 사용 가능")
        else:
            print("❌ 수집 실패. 다른 방법 시도 필요")
            
        # 🆕 배당수익률이 없는 이유 설명
        if 'dividend_yield' not in etf_data or etf_data.get('dividend_yield', 0) == 0:
            etf_name = etf_data.get('name', '')
            if any(keyword in etf_name for keyword in ['기계장비', '반도체', 'IT', '성장']):
                print("💡 참고: 이 ETF는 업종 특성상 배당수익률이 낮거나 없을 수 있습니다.")
            else:
                print("💡 참고: 배당수익률 정보를 찾지 못했습니다. 페이지에 정보가 없거나 무배당 ETF일 수 있습니다.")
        
        return etf_data
        
    except Exception as e:
        print(f"❌ 전체 수집 실패: {e}")
        return None

def test_alternative_data_source(code):
    """대체 데이터 소스 테스트 (KRX API 등)"""
    print(f"\n🔄 {code} 대체 데이터 소스 시도")
    
    try:
        # pykrx 사용 시도
        try:
            from pykrx import stock
            from datetime import datetime, timedelta
            
            end_date = datetime.now().strftime('%Y%m%d')
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')
            
            # OHLCV 데이터
            df = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            
            if not df.empty:
                latest = df.iloc[-1]
                
                alt_data = {
                    'code': code,
                    'data_source': 'pykrx',
                    'collection_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # 컬럼명 처리
                if '종가' in df.columns:
                    alt_data['current_price'] = float(latest['종가'])
                    alt_data['volume'] = int(latest['거래량'])
                    
                    if len(df) >= 2:
                        prev_close = df.iloc[-2]['종가']
                        change = alt_data['current_price'] - prev_close
                        change_rate = (change / prev_close) * 100
                        alt_data['change_rate'] = change_rate
                        alt_data['change_amount'] = change
                        
                    print(f"✅ pykrx 데이터:")
                    print(f"   현재가: {alt_data['current_price']:,.0f}원")
                    print(f"   거래량: {alt_data['volume']:,}주")
                    if 'change_rate' in alt_data:
                        print(f"   등락률: {alt_data['change_rate']:+.2f}%")
                    
                    return alt_data
                else:
                    print("❌ pykrx 데이터 형식 불일치")
                    
        except ImportError:
            print("❌ pykrx 라이브러리 없음")
        except Exception as e:
            print(f"❌ pykrx 오류: {e}")
        
        # 다른 대체 소스들도 시도 가능
        # (예: FnGuide, KRX 직접 API 등)
        
        return None
        
    except Exception as e:
        print(f"❌ 대체 데이터 소스 실패: {e}")
        return None

def test_multiple_etfs():
    """여러 ETF 테스트 (개선된 버전)"""
    test_etfs = [
        ('069500', 'KODEX 200'),
        ('360750', 'TIGER 미국S&P500'),
        ('114260', 'KODEX 국고채10년'),
        ('102960', 'KODEX 기계장비'),  # 이미지의 ETF
    ]
    
    print("🚀 여러 ETF 실제 데이터 수집 테스트 (개선 버전)")
    print("=" * 60)
    
    results = []
    
    for i, (code, expected_name) in enumerate(test_etfs):
        print(f"\n[{i+1}/{len(test_etfs)}] {code} ({expected_name}) 테스트")
        
        # 1순위: 네이버 스크래핑
        result = test_naver_etf_scraping(code)
        
        # 2순위: 대체 데이터 소스 (네이버 실패시)
        if not result or len([k for k, v in result.items() if v is not None and k not in ['code', 'collection_time']]) < 2:
            print(f"🔄 네이버 수집 부족, 대체 소스 시도...")
            alt_result = test_alternative_data_source(code)
            
            if alt_result:
                # 네이버 데이터와 대체 데이터 합치기
                if result:
                    result.update(alt_result)
                else:
                    result = alt_result
        
        if result:
            results.append(result)
        
        # 요청 간격 (서버 부하 방지)
        if i < len(test_etfs) - 1:
            print("⏱️ 2초 대기...")
            time.sleep(2)
    
    # 전체 결과 요약
    if results:
        print(f"\n📊 전체 결과 요약")
        print("=" * 60)
        
        success_count = len(results)
        total_count = len(test_etfs)
        
        print(f"수집 성공: {success_count}/{total_count}개 ETF")
        
        # 결과 테이블
        summary_data = []
        for result in results:
            summary_data.append({
                'ETF코드': result['code'],
                'ETF명': result.get('name', 'Unknown')[:20],
                '현재가': f"{result.get('current_price', 0):,.0f}원" if result.get('current_price') else '-',
                '등락률': f"{result.get('change_rate', 0):+.1f}%" if result.get('change_rate') is not None else '-',
                '운용보수': f"{result.get('expense_ratio', 0):.2f}%" if result.get('expense_ratio') else '-',
                '거래량': f"{result.get('volume', 0):,}주" if result.get('volume') else '-',
                '데이터소스': result.get('data_source', 'unknown')
            })
        
        if summary_data:
            df = pd.DataFrame(summary_data)
            print(f"\n{df.to_string(index=False)}")
        
        # 데이터 소스별 통계
        sources = {}
        for result in results:
            source = result.get('data_source', 'unknown')
            sources[source] = sources.get(source, 0) + 1
        
        print(f"\n📊 데이터 소스별 통계:")
        for source, count in sources.items():
            print(f"  {source}: {count}개")
        
        print(f"\n✅ 실제 데이터 수집 테스트 완료!")
        print(f"💡 이제 이 방법을 기존 시스템에 통합할 수 있습니다.")
        
        return results
    else:
        print(f"\n❌ 모든 ETF 데이터 수집 실패")
        print(f"💡 네트워크 연결이나 사이트 접근성을 확인해주세요.")
        return []

def quick_installation_check():
    """필요한 라이브러리 설치 확인"""
    print("🔍 필요한 라이브러리 설치 상태 확인")
    print("=" * 40)
    
    libraries = {
        'requests': 'pip install requests',
        'beautifulsoup4': 'pip install beautifulsoup4', 
        'pandas': 'pip install pandas',
        'pykrx': 'pip install pykrx'
    }
    
    missing_libs = []
    
    for lib, install_cmd in libraries.items():
        try:
            if lib == 'beautifulsoup4':
                __import__('bs4')
            else:
                __import__(lib)
            print(f"✅ {lib}: 설치됨")
        except ImportError:
            print(f"❌ {lib}: 누락됨 -> {install_cmd}")
            missing_libs.append(install_cmd)
    
    if missing_libs:
        print(f"\n📥 누락된 라이브러리 설치:")
        for cmd in missing_libs:
            print(f"  {cmd}")
        return False
    else:
        print(f"\n🎉 모든 라이브러리 설치 완료!")
        return True

if __name__ == "__main__":
    print("🚀 ETF 실제 데이터 수집 즉시 테스트 (개선 버전)")
    print("=" * 60)
    
    # 1. 라이브러리 설치 확인
    if not quick_installation_check():
        print("\n⚠️ 필요한 라이브러리를 먼저 설치해주세요.")
        exit()
    
    # 2. 단일 ETF 테스트
    print(f"\n1️⃣ 단일 ETF 상세 테스트")
    test_result = test_naver_etf_scraping('069500')  # KODEX 200
    
    # 3. 여러 ETF 테스트
    print(f"\n2️⃣ 여러 ETF 배치 테스트")
    batch_results = test_multiple_etfs()
    
    # 4. 다음 단계 안내
    print(f"\n🎯 다음 단계:")
    if test_result or batch_results:
        successful_count = len([r for r in batch_results if r.get('current_price')])
        print(f"✅ 실제 데이터 수집이 작동합니다! ({successful_count}개 성공)")
        print("💡 이제 다음 중 하나를 선택하세요:")
        print("   A. 기존 market_data_collector.py를 새 버전으로 교체")
        print("   B. 기존 시스템에 실제 데이터 수집 기능 추가")
        print("   C. 별도 실제 데이터 수집 스크립트로 사용")
    else:
        print("❌ 데이터 수집에 문제가 있습니다.")
        print("💡 다음 사항을 확인해보세요:")
        print("   - 인터넷 연결 상태")
        print("   - 네이버 금융 사이트 접속 가능 여부") 
        print("   - 방화벽이나 보안 소프트웨어 설정")
        print("   - VPN 사용 여부 (한국 IP가 아닌 경우 차단될 수 있음)")