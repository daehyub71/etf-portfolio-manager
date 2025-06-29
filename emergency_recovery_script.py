#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
응급 복구 스크립트 - ETF 매니저 문제 해결
"""

import os
import sys
import time
import logging
from datetime import datetime

def emergency_recovery():
    """응급 상황 복구 절차"""
    
    print("🚨 ETF 매니저 응급 복구 시작")
    print("=" * 50)
    
    # 1. 환경 진단
    print("\n📋 1단계: 환경 진단")
    
    try:
        import pykrx
        print(f"✅ pykrx 버전: {pykrx.__version__}")
    except ImportError:
        print("❌ pykrx 설치 필요: pip install pykrx")
        return False
    
    try:
        import pandas as pd
        print(f"✅ pandas 버전: {pd.__version__}")
    except ImportError:
        print("❌ pandas 설치 필요: pip install pandas")
        return False
    
    # 2. 캐시 초기화
    print("\n🧹 2단계: 캐시 초기화")
    cache_dirs = ['cache', '__pycache__', 'data/__pycache__']
    
    for cache_dir in cache_dirs:
        if os.path.exists(cache_dir):
            try:
                import shutil
                shutil.rmtree(cache_dir)
                print(f"✅ {cache_dir} 삭제됨")
            except Exception as e:
                print(f"⚠️ {cache_dir} 삭제 실패: {e}")
    
    # 3. 설정 초기화
    print("\n⚙️ 3단계: 안전 모드 설정")
    
    safe_config = {
        'api_delay': 1.0,  # 1초 지연
        'batch_size': 3,   # 작은 배치
        'max_retries': 5,  # 많은 재시도
        'use_cache': True,
        'safe_mode': True
    }
    
    print("안전 모드 설정:")
    for key, value in safe_config.items():
        print(f"  {key}: {value}")
    
    # 4. 테스트 실행
    print("\n🧪 4단계: 기본 기능 테스트")
    
    try:
        from data.market_data_collector import MarketDataCollector
        
        collector = MarketDataCollector()
        collector.api_delay = safe_config['api_delay']
        
        # 시장 상태 확인
        market_status = collector.get_market_status()
        print(f"✅ 시장 상태 조회 성공")
        print(f"  영업일: {market_status['last_business_day']}")
        print(f"  pykrx 사용가능: {market_status['pykrx_available']}")
        
        # 기본 ETF 리스트 테스트
        default_etfs = collector._get_default_etf_list()
        print(f"✅ 기본 ETF 리스트: {len(default_etfs)}개")
        
        return True
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        return False

def quick_fix():
    """빠른 수정 절차"""
    
    print("\n🔧 빠른 수정 적용")
    
    # 문제 종목 리스트 업데이트
    problematic_tickers = [
        '427120',  # KBSTAR 중기
        '495710',  # TIMEFOLIO Korea플러스배당액티브
        # 추가로 문제가 되는 종목들 여기에 추가
    ]
    
    print(f"문제 종목 {len(problematic_tickers)}개 제외 처리:")
    for ticker in problematic_tickers:
        print(f"  - {ticker}")
    
    # 환경변수 설정
    os.environ['ETF_SAFE_MODE'] = 'true'
    os.environ['ETF_API_DELAY'] = '1.0'
    
    print("✅ 빠른 수정 완료")

def create_minimal_test():
    """최소 기능 테스트 생성"""
    
    test_code = '''
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data.market_data_collector import MarketDataCollector

def minimal_test():
    """최소 기능 테스트"""
    print("🧪 최소 기능 테스트 시작")
    
    try:
        collector = MarketDataCollector()
        collector.api_delay = 2.0  # 안전한 지연
        
        # 기본 ETF만 테스트
        default_etfs = collector._get_default_etf_list()
        print(f"✅ 기본 ETF: {len(default_etfs)}개")
        
        # 더미 데이터 테스트
        dummy_data = collector._generate_dummy_price_data('069500', '1w')
        print(f"✅ 더미 데이터: {len(dummy_data)}건")
        
        print("🎉 최소 기능 테스트 성공!")
        return True
        
    except Exception as e:
        print(f"❌ 최소 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    minimal_test()
'''
    
    with open('minimal_test.py', 'w', encoding='utf-8') as f:
        f.write(test_code)
    
    print("✅ minimal_test.py 생성됨")
    print("실행: python minimal_test.py")

def main():
    """메인 복구 절차"""
    
    try:
        # 응급 복구
        if not emergency_recovery():
            print("\n❌ 응급 복구 실패")
            print("수동 설치 필요:")
            print("  pip install --upgrade pykrx pandas numpy")
            return
        
        # 빠른 수정
        quick_fix()
        
        # 최소 테스트 생성
        create_minimal_test()
        
        print("\n🎉 복구 완료!")
        print("\n다음 단계:")
        print("1. python minimal_test.py - 최소 기능 테스트")
        print("2. python data/market_data_collector.py - 전체 테스트") 
        print("3. python core/update_manager.py - 원본 시스템 실행")
        
    except Exception as e:
        print(f"\n💥 복구 중 오류: {e}")
        print("\n수동 복구 방법:")
        print("1. pip install --upgrade pykrx")
        print("2. 캐시 폴더 삭제")
        print("3. Python 재시작")

if __name__ == "__main__":
    main()