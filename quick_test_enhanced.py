# ==========================================
# quick_test_enhanced.py - ETF 시스템 빠른 테스트 (AUM & 카테고리 강화)
# ==========================================

import sys
import os
import time
from datetime import datetime

# 콘솔 인코딩 설정 (Windows)
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

def test_market_data_collector():
    """MarketDataCollector 테스트 (AUM & 카테고리 강화)"""
    print("\n🔍 1. MarketDataCollector 테스트 (AUM & 카테고리 강화)")
    print("=" * 60)
    
    try:
        # 새로운 MarketDataCollector import
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        from data.market_data_collector import MarketDataCollector
        
        collector = MarketDataCollector()
        print("✅ MarketDataCollector 초기화 성공")
        
        # 테스트 ETF 코드들 (다양한 카테고리)
        test_codes = ['069500', '360750', '114260', '329200', '132030']
        
        print(f"\n📊 테스트 ETF: {len(test_codes)}개")
        
        for i, code in enumerate(test_codes, 1):
            print(f"\n[{i}/{len(test_codes)}] {code} 테스트:")
            
            try:
                # 종합 데이터 수집
                data = collector.get_comprehensive_etf_details(code)
                
                # 결과 출력
                print(f"   ETF명: {data.get('name', 'Unknown')}")
                print(f"   카테고리: {data.get('category', '기타')} > {data.get('subcategory', '')}")
                print(f"   현재가: {data.get('current_price', 0):,}원")
                print(f"   AUM: {data.get('aum', 0):,}억원")
                print(f"   운용보수: {data.get('expense_ratio', 0)}%")
                print(f"   배당수익률: {data.get('dividend_yield', 0)}%")
                print(f"   데이터 품질: {data.get('data_quality', 'unknown')} ({data.get('final_quality_score', 0)}점)")
                print(f"   데이터 소스: {data.get('data_source', 'unknown')}")
                
            except Exception as e:
                print(f"   ❌ 오류: {e}")
            
            time.sleep(0.5)  # 요청 간격
        
        return True
        
    except ImportError as e:
        print(f"❌ MarketDataCollector import 실패: {e}")
        return False
    except Exception as e:
        print(f"❌ MarketDataCollector 테스트 실패: {e}")
        return False

def test_update_manager():
    """UpdateManager 테스트 (AUM & 카테고리 포함)"""
    print("\n🔄 2. UpdateManager 테스트 (AUM & 카테고리 포함)")
    print("=" * 60)
    
    try:
        from core.update_manager import ETFUpdateManager
        
        manager = ETFUpdateManager()
        print("✅ ETFUpdateManager 초기화 성공")
        
        # 시스템 상태 체크
        print("\n📊 시스템 상태 체크:")
        health = manager.quick_health_check()
        
        if health.get('status') != 'error':
            print(f"   총 ETF: {health['total_etfs']}개")
            print(f"   업데이트된 ETF: {health['updated_etfs']}개")
            print(f"   실제 데이터 ETF: {health['real_data_etfs']}개")
            print(f"   시스템 상태: {health['status']} ({health['health_score']:.1f}%)")
            
            # AUM & 카테고리 정보
            if 'total_aum' in health:
                print(f"   총 AUM: {health['total_aum']:,}억원")
                print(f"   평균 AUM: {health.get('avg_aum', 0):,.0f}억원")
                print(f"   AUM 커버리지: {health.get('aum_coverage', 0):.1f}%")
            
            if 'category_distribution' in health and health['category_distribution']:
                print(f"   카테고리 분포:")
                for cat in health['category_distribution'][:5]:  # 상위 5개만
                    print(f"     - {cat['category']}: {cat['count']}개")
        else:
            print(f"   ❌ 상태 체크 실패: {health.get('error')}")
        
        # 소규모 업데이트 테스트
        print(f"\n🔄 소규모 업데이트 테스트 (3개 ETF):")
        
        try:
            summary = manager.batch_update_all_etfs(max_etfs=3, delay_between_updates=1.0)
            
            if summary:
                print(f"   ✅ 업데이트 완료!")
                print(f"   성공률: {summary.success_rate:.1f}%")
                print(f"   성공: {summary.successful_updates}개")
                print(f"   실패: {summary.failed_updates}개")
                print(f"   실제 데이터: {summary.real_data_count}개")
                print(f"   소요 시간: {summary.total_duration:.1f}초")
                
                # AUM & 카테고리 결과
                if hasattr(summary, 'total_aum') and summary.total_aum > 0:
                    print(f"   총 AUM: {summary.total_aum:,}억원")
                
                if hasattr(summary, 'category_distribution') and summary.category_distribution:
                    print(f"   카테고리: {summary.category_distribution}")
                
                return True
            else:
                print(f"   ❌ 업데이트 실패")
                return False
                
        except Exception as e:
            print(f"   ❌ 업데이트 오류: {e}")
            return False
        
    except ImportError as e:
        print(f"❌ UpdateManager import 실패: {e}")
        return False
    except Exception as e:
        print(f"❌ UpdateManager 테스트 실패: {e}")
        return False

def test_database():
    """데이터베이스 확인 (AUM & 카테고리 데이터)"""
    print("\n🗄️ 3. 데이터베이스 확인 (AUM & 카테고리)")
    print("=" * 60)
    
    try:
        import sqlite3
        import pandas as pd
        
        db_path = "etf_universe.db"
        
        if not os.path.exists(db_path):
            print(f"❌ 데이터베이스 파일이 없습니다: {db_path}")
            return False
        
        conn = sqlite3.connect(db_path)
        
        # ETF 정보 테이블 확인
        try:
            df = pd.read_sql_query('''
                SELECT 
                    COUNT(*) as total_etfs,
                    COUNT(CASE WHEN aum > 0 THEN 1 END) as aum_available,
                    COUNT(CASE WHEN category != '기타' THEN 1 END) as categorized,
                    SUM(COALESCE(aum, 0)) as total_aum,
                    COUNT(DISTINCT category) as unique_categories
                FROM etf_info
            ''', conn)
            
            stats = df.iloc[0]
            print(f"   총 ETF: {stats['total_etfs']}개")
            print(f"   AUM 데이터 보유: {stats['aum_available']}개")
            print(f"   카테고리 분류됨: {stats['categorized']}개")
            print(f"   총 AUM: {stats['total_aum']:,}억원")
            print(f"   고유 카테고리: {stats['unique_categories']}개")
            
            # 카테고리별 분포
            category_df = pd.read_sql_query('''
                SELECT 
                    category,
                    COUNT(*) as count,
                    SUM(COALESCE(aum, 0)) as total_aum,
                    AVG(COALESCE(aum, 0)) as avg_aum
                FROM etf_info 
                WHERE category IS NOT NULL
                GROUP BY category
                ORDER BY count DESC
            ''', conn)
            
            if not category_df.empty:
                print(f"\n   📂 카테고리별 분포:")
                for _, row in category_df.head(10).iterrows():
                    print(f"     {row['category']}: {row['count']}개 (AUM: {row['total_aum']:,.0f}억원)")
            
            # 최근 업데이트 확인
            try:
                recent_df = pd.read_sql_query('''
                    SELECT 
                        code, name, category, aum, data_quality, last_updated
                    FROM etf_info 
                    WHERE last_updated IS NOT NULL
                    ORDER BY last_updated DESC
                    LIMIT 5
                ''', conn)
                
                if not recent_df.empty:
                    print(f"\n   🔄 최근 업데이트된 ETF:")
                    for _, row in recent_df.iterrows():
                        aum_str = f"{row['aum']:,.0f}억원" if row['aum'] > 0 else "N/A"
                        print(f"     {row['code']} ({row['category']}): AUM {aum_str}, 품질 {row['data_quality']}")
                        
            except Exception as e:
                print(f"   ⚠️ 최근 업데이트 조회 실패: {e}")
            
        except Exception as e:
            print(f"   ❌ ETF 정보 조회 실패: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 데이터베이스 확인 실패: {e}")
        return False

def test_dashboard():
    """대시보드 테스트"""
    print("\n📊 4. 대시보드 테스트")
    print("=" * 60)
    
    try:
        # Streamlit 설치 확인
        import streamlit as st
        print("✅ Streamlit 사용 가능")
        
        # 대시보드 파일 확인 (여러 경로)
        possible_paths = [
            "dashboard.py",
            "web/dashboard.py", 
            os.path.join("web", "dashboard.py"),
            os.path.join("dashboard", "dashboard.py"),
            os.path.join("app", "dashboard.py")
        ]
        
        dashboard_file = None
        for path in possible_paths:
            if os.path.exists(path):
                dashboard_file = path
                break
        
        if dashboard_file:
            print(f"✅ 대시보드 파일 존재: {dashboard_file}")
            print(f"💡 다음 명령으로 대시보드 실행:")
            print(f"   streamlit run {dashboard_file}")
            
            # 절대 경로도 표시
            abs_path = os.path.abspath(dashboard_file)
            print(f"   (절대 경로: {abs_path})")
            
            return True
        else:
            print(f"❌ 대시보드 파일을 찾을 수 없습니다")
            print(f"   확인한 경로들:")
            for path in possible_paths:
                abs_path = os.path.abspath(path)
                exists = "✅" if os.path.exists(path) else "❌"
                print(f"   {exists} {path} ({abs_path})")
            
            # 현재 디렉토리의 파일 목록 표시
            print(f"\n📁 현재 디렉토리 파일 목록:")
            try:
                current_files = os.listdir('.')
                dashboard_files = [f for f in current_files if 'dashboard' in f.lower()]
                if dashboard_files:
                    print(f"   대시보드 관련 파일: {dashboard_files}")
                else:
                    print(f"   대시보드 파일 없음")
                
                # web 폴더 확인
                if os.path.exists('web'):
                    web_files = os.listdir('web')
                    web_dashboard_files = [f for f in web_files if 'dashboard' in f.lower()]
                    print(f"   web 폴더 내 파일: {web_dashboard_files if web_dashboard_files else '대시보드 파일 없음'}")
                else:
                    print(f"   web 폴더가 존재하지 않음")
                    
            except Exception as e:
                print(f"   파일 목록 조회 실패: {e}")
            
            return False
            
    except ImportError:
        print("❌ Streamlit이 설치되지 않았습니다")
        print("💡 설치 명령: pip install streamlit")
        return False
    except Exception as e:
        print(f"❌ 대시보드 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 실행"""
    print("🚀 ETF 시스템 종합 테스트 (AUM & 카테고리 강화 버전)")
    print("=" * 80)
    print(f"⏰ 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 테스트 결과 추적
    test_results = {}
    
    # 1. MarketDataCollector 테스트
    test_results['collector'] = test_market_data_collector()
    
    # 2. UpdateManager 테스트
    test_results['update_manager'] = test_update_manager()
    
    # 3. 데이터베이스 확인
    test_results['database'] = test_database()
    
    # 4. 대시보드 테스트
    test_results['dashboard'] = test_dashboard()
    
    # 결과 요약
    print("\n📈 테스트 결과 요약")
    print("=" * 80)
    
    success_count = sum(test_results.values())
    total_count = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ 성공" if result else "❌ 실패"
        print(f"   {test_name}: {status}")
    
    print(f"\n🎯 전체 성공률: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
    
    if success_count == total_count:
        print("\n🎉 모든 테스트가 성공했습니다!")
        print("💡 다음 단계:")
        print("   1. 더 많은 ETF 업데이트: python main.py --mode update --max-etfs 50")
        
        # 대시보드 경로 동적 확인
        dashboard_paths = ["dashboard.py", "web/dashboard.py", "web\\dashboard.py"]
        dashboard_found = None
        for path in dashboard_paths:
            if os.path.exists(path):
                dashboard_found = path
                break
        
        if dashboard_found:
            print(f"   2. 대시보드 실행: streamlit run {dashboard_found}")
        else:
            print("   2. 대시보드 실행: streamlit run web/dashboard.py (파일 경로 확인 필요)")
            
        print("   3. 전체 시스템 실행: python main.py --mode interactive")
    else:
        print("\n⚠️ 일부 테스트가 실패했습니다.")
        print("💡 실패한 구성 요소를 확인하고 필요한 패키지를 설치해주세요.")
        
        if not test_results['collector']:
            print("   - MarketDataCollector: pip install requests beautifulsoup4 pandas")
        
        if not test_results['update_manager']:
            print("   - UpdateManager: 위 패키지들 + sqlite3 (기본 내장)")
        
        if not test_results['dashboard']:
            print("   - Dashboard: pip install streamlit plotly")
            print("   - 대시보드 파일 경로 확인: web/dashboard.py 존재 여부 확인")
    
    print(f"\n⏰ 완료 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

if __name__ == "__main__":
    main()