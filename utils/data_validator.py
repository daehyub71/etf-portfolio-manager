"""
데이터 검증 유틸리티 모듈
ETF 데이터, 포트폴리오 구성, 거래 내역 등의 유효성을 검증하는 도구
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import re
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """데이터 검증 클래스"""
    
    def __init__(self):
        """데이터 검증기 초기화"""
        # 한국 ETF 코드 패턴
        self.korean_etf_pattern = re.compile(r'^\d{6}$')
        
        # 유효한 ETF 코드 목록 (주요 ETF)
        self._setup_valid_etf_codes()
        
        # 검증 규칙 설정
        self._setup_validation_rules()
    
    def _setup_valid_etf_codes(self):
        """유효한 ETF 코드 목록 설정"""
        
        self.valid_etf_codes = {
            # 국내 주식 ETF
            '069500', '152100', '229200', '069660', '148020', '091160',
            '233740', '251340', '114800', '252670', '069660',
            
            # 해외 주식 ETF  
            '139660', '117460', '195930', '192090', '160570', '238720',
            '143850', '243890', '381170', '360200', '381180',
            
            # 채권 ETF
            '114260', '136340', '139230', '130730', '153130', '136410',
            '130680', '168470', '130681', '261240',
            
            # 리츠/부동산 ETF
            '157490', '351590', '395160', '269420',
            
            # 원자재 ETF
            '132030', '130680', '261220', '278420',
            
            # 테마/섹터 ETF
            '305540', '091160', '227560', '278420', '305080', '379800'
        }
    
    def _setup_validation_rules(self):
        """검증 규칙 설정"""
        
        self.validation_rules = {
            'portfolio_weight': {
                'min_total': 99.0,      # 최소 총 비중
                'max_total': 101.0,     # 최대 총 비중
                'min_individual': 0.1,  # 개별 종목 최소 비중
                'max_individual': 50.0, # 개별 종목 최대 비중
                'max_etfs': 20,         # 최대 ETF 개수
                'min_etfs': 1           # 최소 ETF 개수
            },
            'price_data': {
                'min_price': 1.0,       # 최소 가격
                'max_price': 1000000.0, # 최대 가격
                'max_daily_change': 0.3, # 최대 일일 변동률 (30%)
                'min_data_points': 10   # 최소 데이터 포인트 수
            },
            'transaction': {
                'min_amount': 1000,     # 최소 거래 금액
                'max_amount': 100000000, # 최대 거래 금액
                'min_shares': 1,        # 최소 거래 주수
                'max_shares': 1000000   # 최대 거래 주수
            },
            'date_range': {
                'min_date': datetime(2000, 1, 1),
                'max_date': datetime(2030, 12, 31)
            }
        }
    
    def validate_etf_code(self, etf_code: str) -> Dict[str, Any]:
        """
        ETF 코드 유효성 검증
        
        Args:
            etf_code: 검증할 ETF 코드
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': False,
            'etf_code': etf_code,
            'errors': [],
            'warnings': []
        }
        
        try:
            # 기본 형식 검증
            if not etf_code or not isinstance(etf_code, str):
                validation_result['errors'].append('ETF 코드가 문자열이 아닙니다')
                return validation_result
            
            etf_code = etf_code.strip()
            
            # 한국 ETF 코드 패턴 검증
            if not self.korean_etf_pattern.match(etf_code):
                validation_result['errors'].append(f'ETF 코드 형식이 올바르지 않습니다: {etf_code} (6자리 숫자여야 함)')
                return validation_result
            
            # 알려진 ETF 코드 목록과 비교
            if etf_code not in self.valid_etf_codes:
                validation_result['warnings'].append(f'알려지지 않은 ETF 코드입니다: {etf_code}')
            
            validation_result['is_valid'] = True
            
        except Exception as e:
            validation_result['errors'].append(f'ETF 코드 검증 중 오류: {str(e)}')
            logger.error(f"ETF 코드 검증 실패: {e}")
        
        return validation_result
    
    def validate_portfolio_weights(self, portfolio: Dict[str, float]) -> Dict[str, Any]:
        """
        포트폴리오 비중 유효성 검증
        
        Args:
            portfolio: ETF별 비중 딕셔너리
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': False,
            'portfolio': portfolio,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }
        
        try:
            if not portfolio or not isinstance(portfolio, dict):
                validation_result['errors'].append('포트폴리오가 올바른 딕셔너리 형태가 아닙니다')
                return validation_result
            
            # ETF 코드 검증
            invalid_codes = []
            for etf_code in portfolio.keys():
                code_validation = self.validate_etf_code(etf_code)
                if not code_validation['is_valid']:
                    invalid_codes.append(etf_code)
            
            if invalid_codes:
                validation_result['errors'].extend([f'유효하지 않은 ETF 코드: {code}' for code in invalid_codes])
            
            # 비중 값 검증
            weights = list(portfolio.values())
            
            # 숫자 타입 검증
            non_numeric_weights = [w for w in weights if not isinstance(w, (int, float))]
            if non_numeric_weights:
                validation_result['errors'].append(f'숫자가 아닌 비중값이 있습니다: {non_numeric_weights}')
                return validation_result
            
            # 음수 검증
            negative_weights = [w for w in weights if w < 0]
            if negative_weights:
                validation_result['errors'].append(f'음수 비중이 있습니다: {negative_weights}')
            
            # 총 비중 검증
            total_weight = sum(weights)
            rules = self.validation_rules['portfolio_weight']
            
            if total_weight < rules['min_total'] or total_weight > rules['max_total']:
                validation_result['errors'].append(
                    f'총 비중이 올바르지 않습니다: {total_weight:.2f}% (99-101% 범위여야 함)'
                )
            
            # 개별 비중 검증
            for etf_code, weight in portfolio.items():
                if weight < rules['min_individual']:
                    validation_result['warnings'].append(
                        f'{etf_code}의 비중이 너무 낮습니다: {weight:.2f}%'
                    )
                
                if weight > rules['max_individual']:
                    validation_result['errors'].append(
                        f'{etf_code}의 비중이 너무 높습니다: {weight:.2f}% (최대 {rules["max_individual"]}%)'
                    )
            
            # ETF 개수 검증
            etf_count = len(portfolio)
            if etf_count < rules['min_etfs']:
                validation_result['errors'].append(
                    f'ETF 개수가 너무 적습니다: {etf_count}개 (최소 {rules["min_etfs"]}개)'
                )
            
            if etf_count > rules['max_etfs']:
                validation_result['warnings'].append(
                    f'ETF 개수가 많습니다: {etf_count}개 (권장 최대 {rules["max_etfs"]}개)'
                )
            
            # 통계 정보
            validation_result['statistics'] = {
                'total_weight': total_weight,
                'etf_count': etf_count,
                'max_weight': max(weights) if weights else 0,
                'min_weight': min(weights) if weights else 0,
                'weight_std': np.std(weights) if weights else 0
            }
            
            # 최종 판정
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f'포트폴리오 검증 중 오류: {str(e)}')
            logger.error(f"포트폴리오 검증 실패: {e}")
        
        return validation_result
    
    def validate_price_data(self, price_data: pd.DataFrame, etf_code: str = None) -> Dict[str, Any]:
        """
        가격 데이터 유효성 검증
        
        Args:
            price_data: 가격 데이터 DataFrame
            etf_code: ETF 코드 (선택사항)
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': False,
            'etf_code': etf_code,
            'errors': [],
            'warnings': [],
            'statistics': {}
        }
        
        try:
            # 기본 검증
            if price_data is None or price_data.empty:
                validation_result['errors'].append('가격 데이터가 비어있습니다')
                return validation_result
            
            # 필수 컬럼 검증
            required_columns = ['close_price']
            missing_columns = [col for col in required_columns if col not in price_data.columns]
            if missing_columns:
                validation_result['errors'].append(f'필수 컬럼이 없습니다: {missing_columns}')
                return validation_result
            
            # 데이터 개수 검증
            rules = self.validation_rules['price_data']
            if len(price_data) < rules['min_data_points']:
                validation_result['errors'].append(
                    f'데이터 포인트가 부족합니다: {len(price_data)}개 (최소 {rules["min_data_points"]}개)'
                )
            
            # 가격 범위 검증
            close_prices = price_data['close_price'].dropna()
            
            if close_prices.empty:
                validation_result['errors'].append('유효한 종가 데이터가 없습니다')
                return validation_result
            
            # 가격 범위 체크
            min_price = close_prices.min()
            max_price = close_prices.max()
            
            if min_price < rules['min_price']:
                validation_result['errors'].append(f'가격이 너무 낮습니다: {min_price}')
            
            if max_price > rules['max_price']:
                validation_result['errors'].append(f'가격이 너무 높습니다: {max_price}')
            
            # 일일 변동률 검증
            if len(close_prices) > 1:
                daily_returns = close_prices.pct_change().dropna()
                extreme_changes = daily_returns[abs(daily_returns) > rules['max_daily_change']]
                
                if not extreme_changes.empty:
                    validation_result['warnings'].append(
                        f'극단적인 일일 변동이 감지되었습니다: {len(extreme_changes)}건'
                    )
            
            # 결측치 검증
            missing_data = price_data.isnull().sum()
            if missing_data.any():
                validation_result['warnings'].append(
                    f'결측 데이터가 있습니다: {missing_data.to_dict()}'
                )
            
            # 날짜 검증 (date 컬럼이 있는 경우)
            if 'date' in price_data.columns:
                date_validation = self._validate_dates(price_data['date'])
                validation_result['errors'].extend(date_validation['errors'])
                validation_result['warnings'].extend(date_validation['warnings'])
            
            # 통계 정보
            validation_result['statistics'] = {
                'data_points': len(price_data),
                'price_range': [float(min_price), float(max_price)],
                'missing_data_ratio': price_data.isnull().sum().sum() / (len(price_data) * len(price_data.columns)),
                'date_range': [
                    price_data['date'].min() if 'date' in price_data.columns else None,
                    price_data['date'].max() if 'date' in price_data.columns else None
                ]
            }
            
            # 최종 판정
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f'가격 데이터 검증 중 오류: {str(e)}')
            logger.error(f"가격 데이터 검증 실패: {e}")
        
        return validation_result
    
    def validate_transaction_data(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        거래 데이터 유효성 검증
        
        Args:
            transaction: 거래 정보 딕셔너리
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': False,
            'transaction': transaction,
            'errors': [],
            'warnings': []
        }
        
        try:
            # 필수 필드 검증
            required_fields = ['etf_code', 'transaction_type', 'shares', 'price']
            missing_fields = [field for field in required_fields if field not in transaction]
            
            if missing_fields:
                validation_result['errors'].append(f'필수 필드가 없습니다: {missing_fields}')
                return validation_result
            
            # ETF 코드 검증
            etf_validation = self.validate_etf_code(transaction['etf_code'])
            if not etf_validation['is_valid']:
                validation_result['errors'].extend(etf_validation['errors'])
            
            # 거래 유형 검증
            valid_types = ['BUY', 'SELL', 'DIVIDEND']
            if transaction['transaction_type'] not in valid_types:
                validation_result['errors'].append(
                    f'유효하지 않은 거래 유형: {transaction["transaction_type"]} (가능한 값: {valid_types})'
                )
            
            # 주수 검증
            shares = transaction['shares']
            rules = self.validation_rules['transaction']
            
            if not isinstance(shares, (int, float)) or shares <= 0:
                validation_result['errors'].append(f'주수가 올바르지 않습니다: {shares}')
            elif shares < rules['min_shares'] or shares > rules['max_shares']:
                validation_result['errors'].append(
                    f'주수가 범위를 벗어났습니다: {shares} ({rules["min_shares"]}-{rules["max_shares"]})'
                )
            
            # 가격 검증
            price = transaction['price']
            if not isinstance(price, (int, float)) or price <= 0:
                validation_result['errors'].append(f'가격이 올바르지 않습니다: {price}')
            
            # 거래 금액 검증
            total_amount = shares * price
            if total_amount < rules['min_amount'] or total_amount > rules['max_amount']:
                validation_result['warnings'].append(
                    f'거래 금액이 일반적인 범위를 벗어났습니다: {total_amount:,.0f}원'
                )
            
            # 날짜 검증 (있는 경우)
            if 'transaction_date' in transaction:
                date_validation = self._validate_single_date(transaction['transaction_date'])
                validation_result['errors'].extend(date_validation['errors'])
                validation_result['warnings'].extend(date_validation['warnings'])
            
            # 수수료 검증 (있는 경우)
            if 'fee' in transaction:
                fee = transaction['fee']
                if not isinstance(fee, (int, float)) or fee < 0:
                    validation_result['errors'].append(f'수수료가 올바르지 않습니다: {fee}')
                elif fee > total_amount * 0.1:  # 거래금액의 10%를 초과하는 수수료
                    validation_result['warnings'].append(f'수수료가 과도합니다: {fee:,.0f}원')
            
            # 최종 판정
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f'거래 데이터 검증 중 오류: {str(e)}')
            logger.error(f"거래 데이터 검증 실패: {e}")
        
        return validation_result
    
    def validate_date_series(self, dates: pd.Series) -> Dict[str, Any]:
        """
        날짜 시리즈 유효성 검증
        
        Args:
            dates: 날짜 시리즈
            
        Returns:
            검증 결과
        """
        return self._validate_dates(dates)
    
    def _validate_dates(self, dates: Union[pd.Series, List]) -> Dict[str, Any]:
        """날짜 데이터 검증"""
        
        validation_result = {
            'errors': [],
            'warnings': []
        }
        
        try:
            if isinstance(dates, list):
                dates = pd.Series(dates)
            
            # 날짜 형식 변환 시도
            try:
                dates = pd.to_datetime(dates)
            except Exception as e:
                validation_result['errors'].append(f'날짜 형식이 올바르지 않습니다: {str(e)}')
                return validation_result
            
            # 날짜 범위 검증
            rules = self.validation_rules['date_range']
            min_date = dates.min()
            max_date = dates.max()
            
            if min_date < rules['min_date']:
                validation_result['errors'].append(f'날짜가 너무 과거입니다: {min_date}')
            
            if max_date > rules['max_date']:
                validation_result['errors'].append(f'날짜가 너무 미래입니다: {max_date}')
            
            # 중복 날짜 검증
            duplicates = dates.duplicated().sum()
            if duplicates > 0:
                validation_result['warnings'].append(f'중복된 날짜가 있습니다: {duplicates}건')
            
            # 날짜 순서 검증
            if not dates.is_monotonic_increasing:
                validation_result['warnings'].append('날짜가 오름차순으로 정렬되지 않았습니다')
            
        except Exception as e:
            validation_result['errors'].append(f'날짜 검증 중 오류: {str(e)}')
        
        return validation_result
    
    def _validate_single_date(self, date_value: Any) -> Dict[str, Any]:
        """단일 날짜 값 검증"""
        
        validation_result = {
            'errors': [],
            'warnings': []
        }
        
        try:
            # 날짜 변환 시도
            if isinstance(date_value, str):
                date_obj = pd.to_datetime(date_value)
            elif isinstance(date_value, datetime):
                date_obj = date_value
            else:
                validation_result['errors'].append(f'지원하지 않는 날짜 형식: {type(date_value)}')
                return validation_result
            
            # 날짜 범위 검증
            rules = self.validation_rules['date_range']
            if date_obj < rules['min_date']:
                validation_result['errors'].append(f'날짜가 너무 과거입니다: {date_obj}')
            
            if date_obj > rules['max_date']:
                validation_result['errors'].append(f'날짜가 너무 미래입니다: {date_obj}')
            
        except Exception as e:
            validation_result['errors'].append(f'날짜 검증 중 오류: {str(e)}')
        
        return validation_result
    
    def validate_performance_data(self, performance_data: Dict[str, float]) -> Dict[str, Any]:
        """
        성과 데이터 유효성 검증
        
        Args:
            performance_data: 성과 지표 딕셔너리
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': False,
            'performance_data': performance_data,
            'errors': [],
            'warnings': []
        }
        
        try:
            if not isinstance(performance_data, dict):
                validation_result['errors'].append('성과 데이터가 딕셔너리 형태가 아닙니다')
                return validation_result
            
            # 수익률 검증
            if 'return' in performance_data:
                ret = performance_data['return']
                if not isinstance(ret, (int, float)):
                    validation_result['errors'].append(f'수익률이 숫자가 아닙니다: {ret}')
                elif ret < -1.0 or ret > 10.0:  # -100% ~ 1000% 범위
                    validation_result['warnings'].append(f'수익률이 극단적입니다: {ret*100:.1f}%')
            
            # 변동성 검증
            if 'volatility' in performance_data:
                vol = performance_data['volatility']
                if not isinstance(vol, (int, float)):
                    validation_result['errors'].append(f'변동성이 숫자가 아닙니다: {vol}')
                elif vol < 0:
                    validation_result['errors'].append(f'변동성이 음수입니다: {vol}')
                elif vol > 1.0:  # 100% 초과
                    validation_result['warnings'].append(f'변동성이 매우 높습니다: {vol*100:.1f}%')
            
            # 샤프 비율 검증
            if 'sharpe_ratio' in performance_data:
                sharpe = performance_data['sharpe_ratio']
                if not isinstance(sharpe, (int, float)):
                    validation_result['errors'].append(f'샤프 비율이 숫자가 아닙니다: {sharpe}')
                elif sharpe < -5 or sharpe > 5:
                    validation_result['warnings'].append(f'샤프 비율이 극단적입니다: {sharpe:.2f}')
            
            # 최대 낙폭 검증
            if 'max_drawdown' in performance_data:
                mdd = performance_data['max_drawdown']
                if not isinstance(mdd, (int, float)):
                    validation_result['errors'].append(f'최대 낙폭이 숫자가 아닙니다: {mdd}')
                elif mdd > 0:
                    validation_result['warnings'].append(f'최대 낙폭이 양수입니다: {mdd}')
                elif mdd < -0.9:  # -90% 미만
                    validation_result['warnings'].append(f'최대 낙폭이 매우 큽니다: {mdd*100:.1f}%')
            
            # 최종 판정
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f'성과 데이터 검증 중 오류: {str(e)}')
            logger.error(f"성과 데이터 검증 실패: {e}")
        
        return validation_result
    
    def validate_configuration(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        설정 데이터 유효성 검증
        
        Args:
            config: 설정 딕셔너리
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': False,
            'config': config,
            'errors': [],
            'warnings': []
        }
        
        try:
            # 리밸런싱 임계치 검증
            if 'rebalancing_threshold' in config:
                threshold = config['rebalancing_threshold']
                if not isinstance(threshold, (int, float)) or threshold < 0:
                    validation_result['errors'].append(f'리밸런싱 임계치가 올바르지 않습니다: {threshold}')
                elif threshold > 20:
                    validation_result['warnings'].append(f'리밸런싱 임계치가 높습니다: {threshold}%')
            
            # 투자 금액 검증
            if 'investment_amount' in config:
                amount = config['investment_amount']
                if not isinstance(amount, (int, float)) or amount <= 0:
                    validation_result['errors'].append(f'투자 금액이 올바르지 않습니다: {amount}')
                elif amount < 100000:  # 10만원 미만
                    validation_result['warnings'].append(f'투자 금액이 적습니다: {amount:,.0f}원')
            
            # 위험 수준 검증
            if 'risk_level' in config:
                risk_level = config['risk_level']
                valid_levels = ['conservative', 'moderate', 'aggressive']
                if risk_level not in valid_levels:
                    validation_result['errors'].append(
                        f'유효하지 않은 위험 수준: {risk_level} (가능한 값: {valid_levels})'
                    )
            
            # 최종 판정
            validation_result['is_valid'] = len(validation_result['errors']) == 0
            
        except Exception as e:
            validation_result['errors'].append(f'설정 검증 중 오류: {str(e)}')
            logger.error(f"설정 검증 실패: {e}")
        
        return validation_result
    
    def get_data_quality_report(self, data: Any, data_type: str) -> Dict[str, Any]:
        """
        데이터 품질 리포트 생성
        
        Args:
            data: 검증할 데이터
            data_type: 데이터 유형 ('portfolio', 'price_data', 'transaction', 'performance')
            
        Returns:
            품질 리포트
        """
        report = {
            'data_type': data_type,
            'timestamp': datetime.now(),
            'validation_results': {},
            'quality_score': 0,
            'recommendations': []
        }
        
        try:
            # 데이터 유형별 검증
            if data_type == 'portfolio':
                validation = self.validate_portfolio_weights(data)
            elif data_type == 'price_data':
                validation = self.validate_price_data(data)
            elif data_type == 'transaction':
                validation = self.validate_transaction_data(data)
            elif data_type == 'performance':
                validation = self.validate_performance_data(data)
            else:
                validation = {'errors': [f'지원하지 않는 데이터 유형: {data_type}']}
            
            report['validation_results'] = validation
            
            # 품질 점수 계산
            error_count = len(validation.get('errors', []))
            warning_count = len(validation.get('warnings', []))
            
            if error_count == 0:
                if warning_count == 0:
                    report['quality_score'] = 100
                elif warning_count <= 2:
                    report['quality_score'] = 85
                else:
                    report['quality_score'] = 70
            else:
                report['quality_score'] = max(0, 50 - error_count * 10)
            
            # 개선 권장사항
            if error_count > 0:
                report['recommendations'].append('오류를 해결하여 데이터 무결성을 확보하세요')
            
            if warning_count > 0:
                report['recommendations'].append('경고사항을 검토하여 데이터 품질을 개선하세요')
            
            if report['quality_score'] < 80:
                report['recommendations'].append('데이터 검증 규칙을 재검토하고 데이터 수집 프로세스를 개선하세요')
            
        except Exception as e:
            report['validation_results'] = {'errors': [f'품질 리포트 생성 실패: {str(e)}']}
            report['quality_score'] = 0
            logger.error(f"데이터 품질 리포트 생성 실패: {e}")
        
        return report
    
    def batch_validate(self, data_list: List[Tuple[Any, str]]) -> List[Dict[str, Any]]:
        """
        배치 데이터 검증
        
        Args:
            data_list: (데이터, 데이터_유형) 튜플의 리스트
            
        Returns:
            검증 결과 리스트
        """
        results = []
        
        for i, (data, data_type) in enumerate(data_list):
            try:
                result = self.get_data_quality_report(data, data_type)
                result['batch_index'] = i
                results.append(result)
            except Exception as e:
                logger.error(f"배치 검증 실패 (인덱스 {i}): {e}")
                results.append({
                    'batch_index': i,
                    'data_type': data_type,
                    'validation_results': {'errors': [f'검증 실패: {str(e)}']},
                    'quality_score': 0
                })
        
        return results