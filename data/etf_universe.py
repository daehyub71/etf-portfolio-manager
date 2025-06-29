"""
ETF 유니버스 관리 모듈 (pykrx 연동 버전)
한국에서 거래 가능한 모든 ETF 정보를 체계적으로 관리
"""

import pandas as pd
import logging
import sqlite3
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json

# pykrx 연동을 위한 MarketDataCollector import 시도
try:
    from data.market_data_collector import MarketDataCollector
    MARKET_DATA_AVAILABLE = True
except ImportError:
    MARKET_DATA_AVAILABLE = False
    print("⚠️ MarketDataCollector 없음 - 기본 ETF 데이터 사용")

logger = logging.getLogger(__name__)

class ETFUniverse:
    """ETF 유니버스 관리 클래스 (pykrx 연동)"""

    def __init__(self, db_path=None, auto_refresh=True, **kwargs):
        """
        ETF 유니버스 초기화
        
        Args:
            db_path: 데이터베이스 경로 (선택적)
            auto_refresh: 자동으로 실제 ETF 데이터 로드 여부
            **kwargs: 기타 매개변수
        """
        self.db_path = db_path
        self.etf_data = {}
        self.categories = {}
        self.auto_refresh = auto_refresh
        
        # MarketDataCollector 초기화 (사용 가능한 경우)
        if MARKET_DATA_AVAILABLE and db_path:
            self.collector = MarketDataCollector(db_path)
        else:
            self.collector = None
        
        # ETF 유니버스 초기화
        self._initialize_etf_universe()
        
        # 데이터베이스 연동 지원
        if db_path:
            try:
                self._load_from_database()
            except Exception as e:
                logger.warning(f"데이터베이스에서 ETF 데이터 로드 실패: {e}")
                logger.info("기본 ETF 데이터로 계속 진행합니다.")
        
        # 자동 새로고침 (옵션)
        if auto_refresh and self.collector and self._should_refresh_from_market():
            try:
                self.refresh_from_market()
            except Exception as e:
                logger.warning(f"시장 데이터 자동 새로고침 실패: {e}")
        
        logger.info(f"ETF 유니버스 초기화 완료 (DB: {db_path if db_path else 'N/A'}, ETF: {len(self.etf_data)}개)")

    def _should_refresh_from_market(self) -> bool:
        """시장 데이터 새로고침이 필요한지 확인"""
        if not self.collector:
            return False
        
        # ETF 데이터가 부족하거나 오래된 경우
        if len(self.etf_data) < 10:
            return True
        
        # 마지막 업데이트가 24시간 이상 지난 경우
        try:
            if self.db_path and Path(self.db_path).exists():
                conn = sqlite3.connect(self.db_path)
                cursor = conn.execute('''
                    SELECT MAX(last_updated) as last_update 
                    FROM etf_info 
                    WHERE last_updated IS NOT NULL
                ''')
                result = cursor.fetchone()
                conn.close()
                
                if result and result[0]:
                    last_update = datetime.fromisoformat(result[0])
                    hours_since_update = (datetime.now() - last_update).total_seconds() / 3600
                    return hours_since_update >= 24
        except Exception as e:
            logger.debug(f"마지막 업데이트 시간 확인 실패: {e}")
        
        return False

    def refresh_from_market(self) -> bool:
        """시장에서 최신 ETF 데이터 새로고침"""
        if not self.collector:
            logger.warning("MarketDataCollector가 없어서 시장 데이터 새로고침 불가")
            return False
        
        try:
            logger.info("시장에서 ETF 데이터 새로고침 시작")
            
            # 시장 상태 확인
            market_status = self.collector.get_market_status()
            logger.info(f"시장 상태: 영업일 {market_status.get('last_business_day')}, pykrx 사용가능: {market_status.get('pykrx_available')}")
            
            # 전체 ETF 리스트 가져오기
            etf_list = self.collector.get_all_etf_list()
            
            if not etf_list:
                logger.warning("시장에서 ETF 리스트를 가져올 수 없음")
                return False
            
            # ETF 데이터 업데이트
            updated_count = 0
            for etf in etf_list:
                code = etf['code']
                
                # 기존 데이터와 병합
                if code in self.etf_data:
                    # 기존 데이터 유지하고 새 데이터로 업데이트
                    self.etf_data[code].update(etf)
                else:
                    # 새 ETF 추가
                    self.etf_data[code] = etf
                
                # 카테고리 정보 보강
                if 'category' not in self.etf_data[code] or not self.etf_data[code]['category']:
                    self.etf_data[code]['category'] = self._classify_etf_category(code)
                
                updated_count += 1
            
            logger.info(f"시장 데이터 새로고침 완료: {updated_count}개 ETF 업데이트")
            
            # 데이터베이스에 저장
            if self.db_path:
                self.save_to_database()
            
            return True
            
        except Exception as e:
            logger.error(f"시장 데이터 새로고침 실패: {e}")
            return False

    def _classify_etf_category(self, code: str) -> str:
        """ETF 코드를 기반으로 카테고리 자동 분류"""
        # 실제 ETF 코드 패턴을 기반으로 카테고리 분류
        category_patterns = {
            'domestic_equity': {
                'codes': ['069500', '229200', '148020', '091160', '091170', '114800'],
                'keywords': ['코스피', 'KOSPI', '코스닥', 'KOSDAQ', '코리아', 'Korea', '한국', '국내']
            },
            'foreign_equity': {
                'codes': ['360750', '133690', '195930', '195980', '160570', '322400'],
                'keywords': ['미국', 'US', 'S&P', '나스닥', 'NASDAQ', '선진국', '신흥국', '중국', '일본', '유럽']
            },
            'bonds': {
                'codes': ['114260', '305080', '130730', '148070', '136340'],
                'keywords': ['국고채', '회사채', '채권', 'Bond', 'Treasury', '국채']
            },
            'alternatives': {
                'codes': ['329200', '351590', '132030', '130680', '130730'],
                'keywords': ['리츠', 'REIT', '골드', 'Gold', '원자재', 'Commodity', '인프라']
            },
            'thematic': {
                'codes': ['305540', '091160', '148020', '190770', '233740'],
                'keywords': ['2차전지', '배터리', '반도체', 'ESG', '바이오', '게임', '콘텐츠']
            }
        }
        
        # 코드 직접 매칭
        for category, patterns in category_patterns.items():
            if code in patterns['codes']:
                return category
        
        # ETF 이름으로 키워드 매칭 (이름이 있는 경우)
        etf_name = self.etf_data.get(code, {}).get('name', '')
        if etf_name:
            for category, patterns in category_patterns.items():
                for keyword in patterns['keywords']:
                    if keyword in etf_name:
                        return category
        
        # 기본값
        return 'domestic_equity'

    def _load_from_database(self):
        """데이터베이스에서 ETF 정보 로드 (개선된 버전)"""
        if not self.db_path:
            return
        
        try:
            # db_path가 파일명만 주어진 경우 현재 디렉토리에서 찾음
            if isinstance(self.db_path, str) and not self.db_path.startswith('/') and not ':' in self.db_path:
                db_file = self.db_path
            else:
                db_file = self.db_path
            
            # 데이터베이스 파일 존재 확인
            if not Path(db_file).exists():
                logger.warning(f"데이터베이스 파일이 존재하지 않습니다: {db_file}")
                return
            
            # SQLite 데이터베이스 연결
            conn = sqlite3.connect(db_file)
            
            # ETF 정보 테이블 존재 확인
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='etf_info'
            """)
            
            if not cursor.fetchone():
                logger.warning("etf_info 테이블이 존재하지 않습니다")
                conn.close()
                return
            
            # 테이블의 실제 컬럼 구조 확인
            cursor = conn.execute("PRAGMA table_info(etf_info)")
            available_columns = [column[1] for column in cursor.fetchall()]
            logger.debug(f"사용 가능한 컬럼: {available_columns}")
            
            # 기본 컬럼들 (필수)
            base_columns = ['code', 'name']
            
            # 선택적 컬럼들 (존재하는 경우에만 조회)
            optional_columns = [
                'category', 'subcategory', 'asset_class', 'region', 
                'currency', 'expense_ratio', 'market_price', 'aum',
                'inception_date', 'avg_volume', 'last_updated', 'is_active'
            ]
            
            # 컬럼명 매핑 (다른 이름으로 저장된 경우)
            column_mapping = {
                'tracking_index': 'benchmark',
                'fund_company': 'fund_manager'
            }
            
            # 실제 조회할 컬럼 목록 구성
            query_columns = []
            select_columns = []
            
            for col in base_columns + optional_columns:
                if col in available_columns:
                    query_columns.append(col)
                    select_columns.append(col)
            
            # 매핑된 컬럼들 추가
            for target_col, source_col in column_mapping.items():
                if source_col in available_columns:
                    query_columns.append(f"{source_col} as {target_col}")
                    select_columns.append(target_col)
            
            # 동적 쿼리 생성 (활성 ETF만 조회)
            where_clause = "WHERE is_active = 1" if 'is_active' in available_columns else ""
            query = f"SELECT {', '.join(query_columns)} FROM etf_info {where_clause}"
            
            cursor = conn.execute(query)
            rows = cursor.fetchall()
            
            # 컬럼명 가져오기
            column_names = [desc[0] for desc in cursor.description]
            
            loaded_count = 0
            for row in rows:
                etf_dict = dict(zip(column_names, row))
                etf_code = etf_dict.pop('code')
                
                # None 값들을 적절한 기본값으로 변경
                etf_dict['expense_ratio'] = etf_dict.get('expense_ratio') or 0.0
                etf_dict['market_price'] = etf_dict.get('market_price') or 0.0
                etf_dict['aum'] = etf_dict.get('aum') or 0.0
                
                # 카테고리 자동 분류 (없는 경우)
                if not etf_dict.get('category'):
                    etf_dict['category'] = self._classify_etf_category(etf_code)
                
                # 기존 데이터와 병합 (데이터베이스 데이터가 우선)
                if etf_code in self.etf_data:
                    self.etf_data[etf_code].update(etf_dict)
                else:
                    self.etf_data[etf_code] = etf_dict
                
                loaded_count += 1
            
            conn.close()
            logger.info(f"데이터베이스에서 ETF {loaded_count}개 로드 완료")
            
        except sqlite3.Error as e:
            logger.error(f"SQLite 오류: {e}")
            logger.warning("데이터베이스 로드 실패, 기본 ETF 데이터 사용")
        except Exception as e:
            logger.error(f"데이터베이스 로드 중 오류: {e}")
            logger.warning("데이터베이스 로드 실패, 기본 ETF 데이터 사용")
    
    def _initialize_etf_universe(self):
        """한국 ETF 유니버스 초기화"""
        self.categories = {
            'domestic_equity': {
                'name': '국내 주식',
                'subcategories': {
                    'large_cap': '대형주',
                    'mid_small_cap': '중소형주', 
                    'dividend': '배당주',
                    'value': '가치주',
                    'growth': '성장주',
                    'sector': '섹터별'
                }
            },
            'foreign_equity': {
                'name': '해외 주식',
                'subcategories': {
                    'us': '미국',
                    'developed': '선진국',
                    'emerging': '신흥국',
                    'china': '중국',
                    'japan': '일본',
                    'europe': '유럽'
                }
            },
            'bonds': {
                'name': '채권',
                'subcategories': {
                    'government': '국채',
                    'corporate': '회사채',
                    'foreign_government': '해외국채',
                    'foreign_corporate': '해외회사채',
                    'high_yield': '하이일드'
                }
            },
            'alternatives': {
                'name': '대안투자',
                'subcategories': {
                    'reits': '리츠',
                    'commodities': '원자재',
                    'gold': '금',
                    'infrastructure': '인프라'
                }
            },
            'thematic': {
                'name': '테마/섹터',
                'subcategories': {
                    'technology': '기술',
                    'healthcare': '헬스케어',
                    'esg': 'ESG',
                    'battery': '배터리',
                    'semiconductor': '반도체'
                }
            }
        }
        
        # 주요 ETF 데이터 초기화 (기본값)
        self._load_major_etfs()
    
    def _load_major_etfs(self):
        """주요 ETF 데이터 로드 (기본 템플릿)"""
        major_etfs = {
            # 국내 주식 ETF
            '069500': {
                'name': 'KODEX 200',
                'category': 'domestic_equity',
                'subcategory': 'large_cap',
                'asset_class': 'equity',
                'region': 'KR',
                'currency': 'KRW',
                'expense_ratio': 0.15,
                'tracking_index': 'KOSPI 200',
                'fund_company': '삼성자산운용',
                'description': '코스피 200 지수를 추종하는 대표적인 국내 대형주 ETF',
                'market_price': 28400,
                'aum': 20000
            },
            '360750': {
                'name': 'TIGER 미국S&P500',
                'category': 'foreign_equity',
                'subcategory': 'us',
                'asset_class': 'equity',
                'region': 'US',
                'currency': 'USD',
                'expense_ratio': 0.045,
                'tracking_index': 'S&P 500',
                'fund_company': '미래에셋자산운용',
                'description': '미국 S&P 500 지수를 추종하는 대표적인 미국주식 ETF',
                'market_price': 15800,
                'aum': 25000
            },
            '114260': {
                'name': 'KODEX 국고채10년',
                'category': 'bonds',
                'subcategory': 'government',
                'asset_class': 'fixed_income',
                'region': 'KR',
                'currency': 'KRW',
                'expense_ratio': 0.15,
                'tracking_index': 'KTB 10년 국고채',
                'fund_company': '삼성자산운용',
                'description': '10년 만기 국고채에 투자하는 채권 ETF',
                'market_price': 108500,
                'aum': 12000
            },
            '133690': {
                'name': 'KODEX 나스닥100',
                'category': 'foreign_equity',
                'subcategory': 'us',
                'asset_class': 'equity',
                'region': 'US',
                'currency': 'USD',
                'expense_ratio': 0.045,
                'tracking_index': 'NASDAQ 100',
                'fund_company': '삼성자산운용',
                'description': '나스닥 100 지수를 추종하는 미국 기술주 중심 ETF',
                'market_price': 24500,
                'aum': 15000
            },
            '229200': {
                'name': 'KODEX 코스닥150',
                'category': 'domestic_equity',
                'subcategory': 'growth',
                'asset_class': 'equity',
                'region': 'KR',
                'currency': 'KRW',
                'expense_ratio': 0.15,
                'tracking_index': 'KOSDAQ 150',
                'fund_company': '삼성자산운용',
                'description': '코스닥 150 지수를 추종하는 성장주 중심 ETF',
                'market_price': 9800,
                'aum': 8000
            },
            # 더 많은 ETF들 추가...
        }
        
        # 기존 데이터가 없는 경우에만 기본 데이터 설정
        if not self.etf_data:
            self.etf_data = major_etfs
            logger.info(f"기본 ETF {len(major_etfs)}개 로드 완료")
    
    def get_live_etf_info(self, etf_code: str) -> Optional[Dict]:
        """실시간 ETF 정보 조회 (pykrx 연동)"""
        if not self.collector:
            return self.get_etf_info(etf_code)
        
        try:
            # 캐시된 정보 먼저 확인
            cached_info = self.get_etf_info(etf_code)
            
            # 실시간 정보 가져오기
            live_info = self.collector.fetch_etf_info(etf_code)
            
            if cached_info and live_info:
                # 캐시된 정보와 실시간 정보 병합
                merged_info = cached_info.copy()
                merged_info.update(live_info)
                return merged_info
            elif live_info:
                return live_info
            else:
                return cached_info
                
        except Exception as e:
            logger.error(f"실시간 ETF 정보 조회 실패 ({etf_code}): {e}")
            return self.get_etf_info(etf_code)
    
    def get_etf_price_history(self, etf_code: str, period: str = "1m") -> pd.DataFrame:
        """ETF 가격 히스토리 조회 (pykrx 연동)"""
        if not self.collector:
            logger.warning(f"MarketDataCollector 없음 - {etf_code} 더미 데이터 반환")
            return pd.DataFrame()
        
        try:
            return self.collector.fetch_etf_price_data(etf_code, period)
        except Exception as e:
            logger.error(f"ETF 가격 히스토리 조회 실패 ({etf_code}): {e}")
            return pd.DataFrame()
    
    def search_etfs_advanced(self, **filters) -> List[Dict]:
        """고급 ETF 검색 (실시간 데이터 포함)"""
        # 기본 검색 수행
        base_results = self.search_etfs(**filters)
        
        # 실시간 정보로 보강 (collector가 있는 경우)
        if self.collector and base_results:
            enhanced_results = []
            
            for etf in base_results:
                try:
                    live_info = self.collector.fetch_etf_info(etf['code'])
                    if live_info:
                        etf.update(live_info)
                    enhanced_results.append(etf)
                except Exception as e:
                    logger.debug(f"실시간 정보 보강 실패 ({etf['code']}): {e}")
                    enhanced_results.append(etf)
            
            return enhanced_results
        
        return base_results
    
    def get_market_overview(self) -> Dict:
        """시장 전체 현황 (pykrx 연동)"""
        overview = self.get_total_market_info()
        
        if self.collector:
            try:
                market_status = self.collector.get_market_status()
                overview['market_status'] = market_status
                overview['data_source'] = 'pykrx' if market_status.get('pykrx_available') else 'cached'
                overview['last_business_day'] = market_status.get('last_business_day')
                overview['is_trading_hours'] = market_status.get('is_trading_hours')
            except Exception as e:
                logger.warning(f"시장 상태 정보 추가 실패: {e}")
                overview['data_source'] = 'cached'
        
        return overview
    
    def get_trending_etfs(self, limit: int = 10) -> List[Dict]:
        """인기/트렌딩 ETF 조회"""
        # AUM과 최근 거래량을 기준으로 정렬
        etfs = []
        
        for code, etf_info in self.etf_data.items():
            etf_copy = etf_info.copy()
            etf_copy['code'] = code
            
            # 트렌딩 점수 계산 (AUM + 거래량 가중)
            aum_score = etf_info.get('aum', 0)
            volume_score = etf_info.get('avg_volume', 0) / 1000000  # 백만주 단위
            etf_copy['trending_score'] = aum_score + volume_score
            
            etfs.append(etf_copy)
        
        # 트렌딩 점수 기준 정렬
        etfs.sort(key=lambda x: x['trending_score'], reverse=True)
        
        return etfs[:limit]
    
    def get_etf_recommendations(self, user_profile: Dict) -> Dict[str, List[Dict]]:
        """사용자 프로필 기반 ETF 추천"""
        age = user_profile.get('age', 35)
        risk_level = user_profile.get('risk_level', 'moderate')
        investment_amount = user_profile.get('investment_amount', 10000000)
        
        recommendations = {
            'core_holdings': [],      # 핵심 보유 (60-70%)
            'growth_picks': [],       # 성장 픽 (20-30%)
            'diversifiers': [],       # 분산투자 (10-20%)
            'alternative_options': [] # 대안투자 (0-10%)
        }
        
        # 나이대별 추천
        if age < 30:
            # 젊은 투자자: 성장 중심
            recommendations['core_holdings'] = self.get_etfs_by_category('foreign_equity', 'us')[:3]
            recommendations['growth_picks'] = self.get_etfs_by_category('thematic')[:2]
            recommendations['diversifiers'] = self.get_etfs_by_category('domestic_equity', 'large_cap')[:2]
        elif age < 50:
            # 중년 투자자: 균형
            recommendations['core_holdings'] = (
                self.get_etfs_by_category('domestic_equity', 'large_cap')[:2] +
                self.get_etfs_by_category('foreign_equity', 'us')[:2]
            )
            recommendations['growth_picks'] = self.get_etfs_by_category('foreign_equity', 'developed')[:2]
            recommendations['diversifiers'] = self.get_etfs_by_category('bonds', 'government')[:2]
        else:
            # 고령 투자자: 안정 중심
            recommendations['core_holdings'] = self.get_etfs_by_category('bonds')[:3]
            recommendations['growth_picks'] = self.get_etfs_by_category('domestic_equity', 'dividend')[:2]
            recommendations['diversifiers'] = self.get_etfs_by_category('alternatives', 'reits')[:2]
        
        # 위험성향별 조정
        if risk_level == 'aggressive':
            recommendations['alternative_options'] = self.get_etfs_by_category('thematic')[:3]
        elif risk_level == 'conservative':
            recommendations['alternative_options'] = self.get_etfs_by_category('bonds')[:2]
        
        return recommendations
    
    # 기존 메서드들 유지 (get_etf_info, get_etfs_by_category, 등등...)
    def get_etf_info(self, etf_code: str) -> Optional[Dict]:
        """특정 ETF 정보 조회"""
        return self.etf_data.get(etf_code)
    
    def get_etfs_by_category(self, category: str, 
                           subcategory: Optional[str] = None) -> List[Dict]:
        """카테고리별 ETF 목록 조회"""
        etfs = []
        
        for code, etf_info in self.etf_data.items():
            if etf_info['category'] == category:
                if subcategory is None or etf_info.get('subcategory') == subcategory:
                    etf_info_copy = etf_info.copy()
                    etf_info_copy['code'] = code
                    etfs.append(etf_info_copy)
        
        return etfs
    
    def search_etfs(self, **filters) -> List[Dict]:
        """다중 조건 ETF 검색"""
        etfs = []
        
        for code, etf_info in self.etf_data.items():
            match = True
            
            for key, value in filters.items():
                if key in etf_info:
                    if isinstance(value, list):
                        if etf_info[key] not in value:
                            match = False
                            break
                    else:
                        if etf_info[key] != value:
                            match = False
                            break
                else:
                    match = False
                    break
            
            if match:
                etf_info_copy = etf_info.copy()
                etf_info_copy['code'] = code
                etfs.append(etf_info_copy)
        
        return etfs
    
    def get_total_market_info(self) -> Dict:
        """전체 시장 정보 요약"""
        total_aum = sum(etf.get('aum', 0) for etf in self.etf_data.values())
        avg_expense_ratio = sum(etf.get('expense_ratio', 0) for etf in self.etf_data.values()) / len(self.etf_data)
        
        return {
            'total_etfs': len(self.etf_data),
            'total_aum': total_aum,
            'avg_expense_ratio': avg_expense_ratio,
            'categories': len(self.categories),
            'last_updated': datetime.now().isoformat(),
            'market_data_available': MARKET_DATA_AVAILABLE
        }
    
    def save_to_database(self, db_path: str = None) -> bool:
        """ETF 정보를 데이터베이스에 저장 (개선된 버전)"""
        target_db = db_path or self.db_path
        if not target_db:
            logger.warning("데이터베이스 경로가 지정되지 않았습니다")
            return False
        
        try:
            conn = sqlite3.connect(target_db)
            
            # 테이블의 실제 컬럼 구조 확인
            cursor = conn.execute("PRAGMA table_info(etf_info)")
            table_info = cursor.fetchall()
            
            if not table_info:
                # 테이블이 없으면 생성
                conn.execute('''
                    CREATE TABLE IF NOT EXISTS etf_info (
                        code TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        category TEXT,
                        subcategory TEXT,
                        asset_class TEXT,
                        region TEXT,
                        currency TEXT DEFAULT 'KRW',
                        expense_ratio REAL,
                        aum REAL DEFAULT 0,
                        market_price REAL DEFAULT 0,
                        benchmark TEXT,
                        fund_manager TEXT,
                        description TEXT,
                        avg_volume INTEGER DEFAULT 0,
                        last_updated TEXT,
                        is_active BOOLEAN DEFAULT 1
                    )
                ''')
                logger.info("etf_info 테이블 생성 완료")
            
            # 사용 가능한 컬럼 목록
            cursor = conn.execute("PRAGMA table_info(etf_info)")
            available_columns = [column[1] for column in cursor.fetchall()]
            
            # ETF 데이터 저장
            saved_count = 0
            for code, etf_info in self.etf_data.items():
                # 저장할 데이터 준비
                save_data = {
                    'code': code,
                    'name': etf_info.get('name', f'ETF_{code}'),
                    'category': etf_info.get('category', 'unknown'),
                    'last_updated': datetime.now().isoformat(),
                    'is_active': 1
                }
                
                # 선택적 필드들 추가 (컬럼이 존재하는 경우에만)
                optional_fields = {
                    'subcategory': etf_info.get('subcategory'),
                    'asset_class': etf_info.get('asset_class'),
                    'region': etf_info.get('region'),
                    'currency': etf_info.get('currency', 'KRW'),
                    'expense_ratio': etf_info.get('expense_ratio'),
                    'aum': etf_info.get('aum'),
                    'market_price': etf_info.get('market_price'),
                    'benchmark': etf_info.get('tracking_index'),
                    'fund_manager': etf_info.get('fund_company'),
                    'description': etf_info.get('description'),
                    'avg_volume': etf_info.get('avg_volume') or etf_info.get('volume')
                }
                
                # 사용 가능한 컬럼만 추가
                for field, value in optional_fields.items():
                    if field in available_columns and value is not None:
                        save_data[field] = value
                
                # 동적 INSERT/REPLACE 쿼리 생성
                columns = list(save_data.keys())
                placeholders = ['?' for _ in columns]
                values = [save_data[col] for col in columns]
                
                query = f'''
                    INSERT OR REPLACE INTO etf_info 
                    ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                '''
                
                conn.execute(query, values)
                saved_count += 1
            
            conn.commit()
            conn.close()
            
            logger.info(f"ETF 정보 {saved_count}개를 데이터베이스에 저장 완료")
            return True
            
        except Exception as e:
            logger.error(f"데이터베이스 저장 실패: {e}")
            return False


# ==========================================
# 실행 예제 및 테스트 코드
# ==========================================

if __name__ == "__main__":
    print("📊 ETF 유니버스 테스트 (pykrx 연동 버전)")
    print("=" * 60)
    
    # ETF 유니버스 초기화 (자동 새로고침 활성화)
    universe = ETFUniverse(db_path="etf_universe.db", auto_refresh=True)
    
    # 전체 시장 정보
    market_info = universe.get_market_overview()
    print(f"\n🌍 전체 시장 정보:")
    print(f"- 총 ETF: {market_info['total_etfs']}개")
    print(f"- 총 AUM: {market_info['total_aum']:,.0f}억원")
    print(f"- 평균 보수율: {market_info['avg_expense_ratio']:.3f}%")
    print(f"- 데이터 소스: {market_info.get('data_source', 'cached')}")
    
    if 'market_status' in market_info:
        print(f"- 최종 영업일: {market_info['market_status'].get('last_business_day')}")
        print(f"- 거래시간: {'예' if market_info['market_status'].get('is_trading_hours') else '아니오'}")
    
    # 시장 데이터 새로고침 테스트
    print(f"\n🔄 시장 데이터 새로고침 테스트:")
    if universe.refresh_from_market():
        print("✅ 시장 데이터 새로고침 성공")
        print(f"업데이트 후 ETF 개수: {len(universe.etf_data)}개")
    else:
        print("⚠️ 시장 데이터 새로고침 실패 (기본 데이터 사용)")
    
    # 트렌딩 ETF 조회
    print(f"\n🔥 인기 ETF Top 5:")
    trending = universe.get_trending_etfs(5)
    for i, etf in enumerate(trending, 1):
        print(f"{i}. {etf['name']} ({etf['code']}): AUM {etf.get('aum', 0):,.0f}억원")
    
    # 실시간 ETF 정보 테스트
    if trending:
        test_code = trending[0]['code']
        print(f"\n📊 {test_code} 실시간 정보 테스트:")
        live_info = universe.get_live_etf_info(test_code)
        if live_info:
            print(f"- 현재가: {live_info.get('current_price', 0):,.0f}원")
            print(f"- 거래량: {live_info.get('volume', 0):,}주")
            print(f"- 마지막 업데이트: {live_info.get('last_updated', 'Unknown')[:19]}")
    
    # 가격 히스토리 테스트
    if trending:
        print(f"\n📈 가격 히스토리 테스트:")
        price_history = universe.get_etf_price_history(test_code, "1m")
        if not price_history.empty:
            print(f"- 데이터 기간: {len(price_history)}일")
            print(f"- 최신 가격: {price_history['close'].iloc[-1]:,.0f}원")
        else:
            print("- 가격 히스토리 없음")
    
    # 사용자 맞춤 추천 테스트
    print(f"\n🎯 투자 추천 테스트 (35세, 중간 위험성향):")
    user_profile = {
        'age': 35,
        'risk_level': 'moderate',
        'investment_amount': 10000000
    }
    
    recommendations = universe.get_etf_recommendations(user_profile)
    for category, etfs in recommendations.items():
        if etfs:
            print(f"\n{category.replace('_', ' ').title()}:")
            for etf in etfs[:2]:  # 상위 2개만 표시
                print(f"  - {etf['name']} ({etf['code']})")
    
    print(f"\n✅ ETF 유니버스 테스트 완료!")
    print(f"💡 주요 기능:")
    print(f"   - 실시간 시장 데이터 연동 (pykrx)")
    print(f"   - 자동 ETF 유니버스 새로고침")
    print(f"   - 실시간 가격 정보 조회")
    print(f"   - 사용자 맞춤 ETF 추천")