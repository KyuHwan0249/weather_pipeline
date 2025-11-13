FROM postgres:15

# init-db 폴더 안의 SQL 스크립트들을 Postgres 초기화 경로로 복사
COPY init-db/*.sql /docker-entrypoint-initdb.d/
