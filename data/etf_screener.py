# ==========================================
# data/etf_screener.py - ETF 스크리너 (수정 버전)
# ==========================================

import pandas as pd
import numpy as np
import sqlite3

class ETFScreener:
    """ETF 스크리너 클래스"""
    
    def __init__(self, db_path: str = "etf_universe.db"):
        self.db_path = db_path
        print(f"🔍 ETF 스크리너 초기화 (DB: {db_path})")
    
    def screen_by_criteria(self, criteria: dict) -> pd.DataFrame:
        """다중 조건 필터링"""
        conn = sqlite3.connect(self.db_path)
        
        # 기본 쿼리
        base_query = "SELECT * FROM etf_info WHERE 1=1"
        params = []
        
        # 조건별 필터링
        if 'expense_ratio_max' in criteria:
            base_query += " AND expense_ratio <= ?"
            params.append(criteria['expense_ratio_max'])
        
        if 'expense_ratio_min' in criteria:
            base_query += " AND expense_ratio >= ?"
            params.append(criteria['expense_ratio_min'])
        
        if 'aum_min' in criteria:
            base_query += " AND aum >= ?"
            params.append(criteria['aum_min'])
        
        if 'aum_max' in criteria:
            base_query += " AND aum <= ?"
            params.append(criteria['aum_max'])
        
        if 'tracking_error_max' in criteria:
            base_query += " AND tracking_error <= ?"
            params.append(criteria['tracking_error_max'])
        
        if 'dividend_yield_min' in criteria:
            base_query += " AND dividend_yield >= ?"
            params.append(criteria['dividend_yield_min'])
        
        if 'category' in criteria:
            base_query += " AND category = ?"
            params.append(criteria['category'])
        
        if 'subcategory' in criteria:
            base_query += " AND subcategory = ?"
            params.append(criteria['subcategory'])
        
        if 'fund_manager' in criteria:
            base_query += " AND fund_manager = ?"
            params.append(criteria['fund_manager'])
        
        # 정렬 조건
        order_by = criteria.get('sort_by', 'aum')
        order_direction = criteria.get('sort_direction', 'DESC')
        base_query += f" ORDER BY {order_by} {order_direction}"
        
        # 결과 개수 제한
        if 'limit' in criteria:
            base_query += " LIMIT ?"
            params.append(criteria['limit'])
        
        try:
            df = pd.read_sql_query(base_query, conn, params=params)
            print(f"✅ 스크리닝 완료: {len(df)}개 ETF 발견")
        except Exception as e:
            print(f"❌ 스크리닝 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def get_top_etfs_by_category(self, category: str, metric: str = 'aum', top_n: int = 5) -> pd.DataFrame:
        """카테고리별 상위 ETF 조회"""
        conn = sqlite3.connect(self.db_path)
        
        query = f'''
            SELECT * FROM etf_info 
            WHERE category = ?
            ORDER BY {metric} DESC
            LIMIT ?
        '''
        
        try:
            df = pd.read_sql_query(query, conn, params=(category, top_n))
            print(f"✅ {category} 카테고리 상위 {len(df)}개 ETF 조회 완료")
        except Exception as e:
            print(f"❌ 카테고리 조회 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def compare_similar_etfs(self, benchmark: str) -> pd.DataFrame:
        """유사 벤치마크 ETF 비교"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT * FROM etf_info 
            WHERE benchmark LIKE ?
            ORDER BY expense_ratio ASC, aum DESC
        '''
        
        try:
            df = pd.read_sql_query(query, conn, params=(f'%{benchmark}%',))
            print(f"✅ '{benchmark}' 벤치마크 ETF {len(df)}개 비교 완료")
        except Exception as e:
            print(f"❌ 벤치마크 비교 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def find_cost_efficient_etfs(self, category: str = None, max_expense_ratio: float = 0.5) -> pd.DataFrame:
        """비용 효율적인 ETF 찾기"""
        conn = sqlite3.connect(self.db_path)
        
        if category:
            query = '''
                SELECT *, (aum / expense_ratio) as efficiency_score
                FROM etf_info 
                WHERE category = ? AND expense_ratio <= ? AND expense_ratio > 0
                ORDER BY efficiency_score DESC
            '''
            params = (category, max_expense_ratio)
        else:
            query = '''
                SELECT *, (aum / expense_ratio) as efficiency_score
                FROM etf_info 
                WHERE expense_ratio <= ? AND expense_ratio > 0
                ORDER BY efficiency_score DESC
            '''
            params = (max_expense_ratio,)
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            print(f"✅ 비용 효율적 ETF {len(df)}개 발견")
        except Exception as e:
            print(f"❌ 비용 효율 분석 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def get_diversification_candidates(self, existing_etfs: list, target_categories: list = None) -> pd.DataFrame:
        """분산투자 후보 ETF 추천"""
        if not existing_etfs:
            print("❌ 기존 ETF 리스트가 비어있습니다")
            return pd.DataFrame()
            
        conn = sqlite3.connect(self.db_path)
        
        # 기존 ETF의 카테고리 조회
        existing_codes = "', '".join(existing_etfs)
        existing_categories_query = f'''
            SELECT DISTINCT category, subcategory 
            FROM etf_info 
            WHERE code IN ('{existing_codes}')
        '''
        
        try:
            existing_categories = pd.read_sql_query(existing_categories_query, conn)
            
            if existing_categories.empty:
                print("❌ 기존 ETF 정보를 찾을 수 없습니다")
                conn.close()
                return pd.DataFrame()
            
            # 기존 카테고리 제외 조건 생성
            exclude_conditions = []
            for _, row in existing_categories.iterrows():
                exclude_conditions.append(f"(category != '{row['category']}' OR subcategory != '{row['subcategory']}')")
            
            if exclude_conditions:
                exclude_clause = " AND (" + " OR ".join(exclude_conditions) + ")"
            else:
                exclude_clause = ""
            
            # 추천 후보 조회
            if target_categories:
                target_clause = " AND category IN ('" + "', '".join(target_categories) + "')"
            else:
                target_clause = ""
            
            query = f'''
                SELECT * FROM etf_info 
                WHERE 1=1{exclude_clause}{target_clause}
                ORDER BY aum DESC, expense_ratio ASC
                LIMIT 20
            '''
            
            df = pd.read_sql_query(query, conn)
            print(f"✅ 분산투자 후보 {len(df)}개 추천 완료")
            
        except Exception as e:
            print(f"❌ 분산투자 후보 추천 오류: {e}")
            df = pd.DataFrame()
        finally:
            conn.close()
        
        return df
    
    def calculate_portfolio_metrics(self, etf_codes: list, weights: list = None) -> dict:
        """포트폴리오 메트릭 계산"""
        if not etf_codes:
            print("❌ ETF 코드 리스트가 비어있습니다")
            return {}
            
        conn = sqlite3.connect(self.db_path)
        
        # ETF 정보 조회
        codes_str = "', '".join(etf_codes)
        query = f'''
            SELECT code, expense_ratio, aum, tracking_error, dividend_yield
            FROM etf_info 
            WHERE code IN ('{codes_str}')
        '''
        
        try:
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                print("❌ 포트폴리오 ETF 정보를 찾을 수 없습니다")
                return {}
            
            # 가중치 설정 (없으면 균등 가중)
            if weights is None:
                weights = [1/len(etf_codes)] * len(etf_codes)
            elif len(weights) != len(df):
                print("⚠️ 가중치 개수가 ETF 개수와 맞지 않아 균등 가중치 사용")
                weights = [1/len(df)] * len(df)
            
            # 포트폴리오 메트릭 계산
            weighted_expense_ratio = sum(df['expense_ratio'] * weights)
            weighted_dividend_yield = sum(df['dividend_yield'] * weights)
            total_aum = sum(df['aum'])
            avg_tracking_error = df['tracking_error'].mean()
            
            metrics = {
                'portfolio_expense_ratio': round(weighted_expense_ratio, 3),
                'portfolio_dividend_yield': round(weighted_dividend_yield, 2),
                'total_aum': round(total_aum, 0),
                'avg_tracking_error': round(avg_tracking_error, 3),
                'num_etfs': len(etf_codes),
                'diversification_score': len(df['code'].unique()) / len(etf_codes) * 100
            }
            
            print(f"✅ 포트폴리오 메트릭 계산 완료")
            return metrics
            
        except Exception as e:
            print(f"❌ 포트폴리오 메트릭 계산 오류: {e}")
            return {}
        finally:
            conn.close()
    
    def get_screening_summary(self) -> dict:
        """스크리닝 가능한 필드 요약"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 카테고리 목록
            categories = pd.read_sql_query('SELECT DISTINCT category FROM etf_info', conn)['category'].tolist()
            
            # 운용사 목록
            managers = pd.read_sql_query('SELECT DISTINCT fund_manager FROM etf_info', conn)['fund_manager'].tolist()
            
            # 수치형 필드 범위
            stats = pd.read_sql_query('''
                SELECT 
                    MIN(expense_ratio) as min_expense_ratio,
                    MAX(expense_ratio) as max_expense_ratio,
                    MIN(aum) as min_aum,
                    MAX(aum) as max_aum,
                    MIN(tracking_error) as min_tracking_error,
                    MAX(tracking_error) as max_tracking_error,
                    MIN(dividend_yield) as min_dividend_yield,
                    MAX(dividend_yield) as max_dividend_yield
                FROM etf_info
            ''', conn).iloc[0].to_dict()
            
            return {
                'available_categories': categories,
                'available_managers': managers,
                'field_ranges': stats
            }
            
        except Exception as e:
            print(f"❌ 스크리닝 요약 오류: {e}")
            return {}
        finally:
            conn.close()


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("🔍 ETF 스크리너 테스트")
    print("=" * 50)
    
    # 스크리너 초기화
    screener = ETFScreener()
    
    # 1. 스크리닝 요약 정보
    print("\n📊 스크리닝 가능한 필드 요약:")
    summary = screener.get_screening_summary()
    if summary:
        print(f"- 카테고리: {len(summary['available_categories'])}개")
        print(f"  {summary['available_categories']}")
        print(f"- 운용사: {len(summary['available_managers'])}개")
        print(f"  {summary['available_managers']}")
        print(f"- 운용보수 범위: {summary['field_ranges']['min_expense_ratio']:.2f}% ~ {summary['field_ranges']['max_expense_ratio']:.2f}%")
        print(f"- 순자산 범위: {summary['field_ranges']['min_aum']:,.0f}억원 ~ {summary['field_ranges']['max_aum']:,.0f}억원")
    
    # 2. 저비용 ETF 스크리닝
    print("\n💰 저비용 ETF 스크리닝 (운용보수 0.2% 이하):")
    low_cost_criteria = {
        'expense_ratio_max': 0.2,
        'aum_min': 1000,
        'sort_by': 'aum',
        'sort_direction': 'DESC',
        'limit': 8
    }
    
    low_cost_etfs = screener.screen_by_criteria(low_cost_criteria)
    if not low_cost_etfs.empty:
        for _, etf in low_cost_etfs.iterrows():
            print(f"- {etf['name']} ({etf['code']}): {etf['expense_ratio']}%, {etf['aum']:,.0f}억원")
    
    # 3. 카테고리별 상위 ETF
    print("\n🌍 해외 주식 ETF 상위 5개:")
    intl_etfs = screener.get_top_etfs_by_category('international_equity', 'aum', 5)
    if not intl_etfs.empty:
        for _, etf in intl_etfs.iterrows():
            print(f"- {etf['name']} ({etf['code']}): {etf['aum']:,.0f}억원, {etf['expense_ratio']}%")
    
    # 4. 유사 벤치마크 ETF 비교
    print("\n📈 S&P 500 관련 ETF 비교:")
    sp500_etfs = screener.compare_similar_etfs('S&P 500')
    if not sp500_etfs.empty:
        for _, etf in sp500_etfs.iterrows():
            print(f"- {etf['name']} ({etf['code']}): {etf['expense_ratio']}%, {etf['aum']:,.0f}억원")
    
    # 5. 비용 효율적 ETF 찾기
    print("\n⚡ 국내 주식 부문 비용 효율적 ETF:")
    efficient_etfs = screener.find_cost_efficient_etfs('domestic_equity', 0.5)
    if not efficient_etfs.empty:
        for _, etf in efficient_etfs.head(5).iterrows():
            efficiency = etf['efficiency_score']
            print(f"- {etf['name']} ({etf['code']}): 효율성 {efficiency:,.0f}, {etf['expense_ratio']}%")
    
    # 6. 분산투자 후보 추천
    print("\n🎯 분산투자 후보 ETF 추천:")
    existing_portfolio = ['069500', '360750']  # KODEX 200, TIGER 미국S&P500
    print(f"기존 포트폴리오: {existing_portfolio}")
    
    candidates = screener.get_diversification_candidates(existing_portfolio)
    if not candidates.empty:
        for _, etf in candidates.head(5).iterrows():
            print(f"- {etf['name']} ({etf['code']}): {etf['category']}/{etf['subcategory']}, {etf['aum']:,.0f}억원")
    
    # 7. 포트폴리오 메트릭 계산
    print("\n📊 샘플 포트폴리오 메트릭:")
    sample_portfolio = ['069500', '360750', '114260']  # 주식 70%, 채권 30% 가정
    weights = [0.4, 0.3, 0.3]
    
    metrics = screener.calculate_portfolio_metrics(sample_portfolio, weights)
    if metrics:
        print(f"- 포트폴리오 운용보수: {metrics['portfolio_expense_ratio']}%")
        print(f"- 포트폴리오 배당수익률: {metrics['portfolio_dividend_yield']}%")
        print(f"- 총 순자산: {metrics['total_aum']:,.0f}억원")
        print(f"- 평균 추적오차: {metrics['avg_tracking_error']}%")
        print(f"- ETF 개수: {metrics['num_etfs']}개")
        print(f"- 분산도 점수: {metrics['diversification_score']:.1f}%")
    
    # 8. 고배당 ETF 스크리닝
    print("\n💰 고배당 ETF 스크리닝 (배당수익률 3% 이상):")
    high_dividend_criteria = {
        'dividend_yield_min': 3.0,
        'aum_min': 500,
        'sort_by': 'dividend_yield',
        'sort_direction': 'DESC',
        'limit': 5
    }
    
    high_dividend_etfs = screener.screen_by_criteria(high_dividend_criteria)
    if not high_dividend_etfs.empty:
        for _, etf in high_dividend_etfs.iterrows():
            print(f"- {etf['name']} ({etf['code']}): {etf['dividend_yield']}%, {etf['aum']:,.0f}억원")
    
    print(f"\n✅ ETF 스크리너 테스트 완료!")
    print(f"💡 다음 단계: python data/etf_analyzer.py")