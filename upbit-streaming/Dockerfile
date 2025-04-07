# 1. 경량화된 OpenJDK 17 JRE 기반의 공식 이미지 사용
FROM eclipse-temurin:17-jre-alpine

# 2. 컨테이너 내부의 작업 디렉토리 설정
WORKDIR /app

# 3. 빌드된 jar 파일 복사
COPY build/libs/*.jar app.jar

# 4. 컨테이너에서 실행될 기본 명령어 설정
CMD ["java", "-jar", "app.jar"]
