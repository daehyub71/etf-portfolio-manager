import streamlit as st
import sqlite3
import pandas as pd
from pathlib import Path

# 페이지 설정
st.set_page_config(
    page_title="ETF 데이터베이스 탐색기",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 ETF 데이터베이스 탐색기")
st.subheader("데이터베이스 구조와 내용을 상세히 분석합니다")

def explore_database():
    """데이터베이스 전체 구조 탐색"""
    
    # 데이터베이스 파일 찾기
    possible_paths = [
        "etf_universe.db",
        "../etf_universe.db", 
        "data/etf_universe.db",
        "data/etf_data.db",
        "portfolio_data.db"
    ]
    
    db_path = None
    for path in possible_paths:
        if Path(path).exists():
            db_path = path
            st.success(f"✅ 데이터베이스 발견: {path}")
            break
    
    if not db_path:
        st.error("❌ 데이터베이스 파일을 찾을 수 없습니다")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        
        # 1. 모든 테이블 목록 조회
        st.header("📋 데이터베이스 테이블 목록")
        
        tables_query = """
        SELECT name, type, sql 
        FROM sqlite_master 
        WHERE type='table'
        ORDER BY name
        """
        
        tables_df = pd.read_sql_query(tables_query, conn)
        
        if len(tables_df) == 0:
            st.warning("⚠️ 테이블이 없습니다")
            return
        
        st.write(f"**총 {len(tables_df)}개 테이블 발견:**")
        
        for _, table in tables_df.iterrows():
            st.write(f"• **{table['name']}** ({table['type']})")
        
        # 2. 각 테이블의 레코드 수 확인
        st.header("📊 테이블별 레코드 수")
        
        table_stats = []
        
        for _, table in tables_df.iterrows():
            table_name = table['name']
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count_result = conn.execute(count_query).fetchone()
                record_count = count_result[0]
                
                # 컬럼 정보도 가져오기
                pragma_query = f"PRAGMA table_info({table_name})"
                columns_df = pd.read_sql_query(pragma_query, conn)
                column_count = len(columns_df)
                
                table_stats.append({
                    '테이블명': table_name,
                    '레코드수': record_count,
                    '컬럼수': column_count
                })
                
            except Exception as e:
                table_stats.append({
                    '테이블명': table_name,
                    '레코드수': f"오류: {e}",
                    '컬럼수': 0
                })
        
        stats_df = pd.DataFrame(table_stats)
        st.dataframe(stats_df, use_container_width=True)
        
        # 3. 가장 많은 레코드를 가진 테이블 찾기
        if len(stats_df) > 0:
            # 레코드수가 숫자인 것만 필터링
            numeric_stats = stats_df[stats_df['레코드수'].apply(lambda x: isinstance(x, int))]
            
            if len(numeric_stats) > 0:
                max_records_table = numeric_stats.loc[numeric_stats['레코드수'].idxmax()]
                
                st.header(f"🏆 최대 레코드 테이블: {max_records_table['테이블명']}")
                st.write(f"**레코드 수:** {max_records_table['레코드수']:,}개")
                
                # 해당 테이블의 상세 정보
                table_name = max_records_table['테이블명']
                
                # 컬럼 구조
                st.subheader("📋 컬럼 구조")
                pragma_query = f"PRAGMA table_info({table_name})"
                columns_df = pd.read_sql_query(pragma_query, conn)
                st.dataframe(columns_df, use_container_width=True)
                
                # 샘플 데이터
                st.subheader("📊 샘플 데이터 (상위 10개)")
                sample_query = f"SELECT * FROM {table_name} LIMIT 10"
                sample_df = pd.read_sql_query(sample_query, conn)
                st.dataframe(sample_df, use_container_width=True)
                
                # ETF 관련 테이블인지 확인
                if 'etf' in table_name.lower() or 'code' in sample_df.columns or 'name' in sample_df.columns:
                    st.success(f"✅ {table_name}이 ETF 데이터 테이블로 보입니다!")
                    
                    # 올바른 쿼리 제안
                    st.subheader("💡 권장 수정 사항")
                    st.code(f"""
# 대시보드에서 다음과 같이 수정하세요:

# AS-IS (현재)
query = "SELECT ... FROM etf_info ..."

# TO-BE (수정)
query = "SELECT ... FROM {table_name} ..."
                    """)
        
        # 4. ETF 관련 테이블 모두 찾기
        st.header("🔍 ETF 관련 테이블 검색")
        
        etf_tables = []
        for _, table in tables_df.iterrows():
            table_name = table['name']
            if 'etf' in table_name.lower():
                etf_tables.append(table_name)
        
        if etf_tables:
            st.write("**ETF 관련 테이블들:**")
            for etf_table in etf_tables:
                try:
                    count_query = f"SELECT COUNT(*) as count FROM {etf_table}"
                    count_result = conn.execute(count_query).fetchone()
                    record_count = count_result[0]
                    st.write(f"• **{etf_table}**: {record_count:,}개 레코드")
                except Exception as e:
                    st.write(f"• **{etf_table}**: 오류 - {e}")
        else:
            st.warning("⚠️ ETF 관련 테이블을 찾을 수 없습니다")
        
        # 5. 전체 데이터베이스 요약
        st.header("📈 데이터베이스 요약")
        
        total_tables = len(tables_df)
        total_records = sum([stat['레코드수'] for stat in table_stats if isinstance(stat['레코드수'], int)])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("총 테이블 수", f"{total_tables}개")
        with col2:
            st.metric("총 레코드 수", f"{total_records:,}개")
        with col3:
            st.metric("데이터베이스 크기", f"{Path(db_path).stat().st_size / 1024 / 1024:.1f}MB")
        
        conn.close()
        
    except Exception as e:
        st.error(f"❌ 데이터베이스 탐색 실패: {e}")
        st.exception(e)

def main():
    st.markdown("""
    **이 도구의 목적:**
    - 데이터베이스에 어떤 테이블들이 있는지 확인
    - 각 테이블에 몇 개의 레코드가 있는지 확인  
    - ETF 데이터가 실제로 어느 테이블에 있는지 찾기
    - 올바른 테이블명으로 대시보드 수정 방향 제시
    """)
    
    if st.button("🔍 데이터베이스 탐색 시작", type="primary"):
        explore_database()

if __name__ == "__main__":
    main()