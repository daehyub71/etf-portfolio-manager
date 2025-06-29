
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
