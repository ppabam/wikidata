# WIKI DATA
위키미디어 재단에서 운영하는 위키백과를 비롯한 다양한 프로젝트는 전 세계적으로 방대한 양의 지식과 정보를 제공하며, 수많은 사용자들이 이를 활발하게 이용하고 있습니다. 이러한 위키미디어 프로젝트의 페이지뷰 데이터는 정보 소비 패턴, 사용자들의 관심사, 그리고 특정 사건이 지식 접근에 미치는 영향 등을 파악할 수 있는 매우 귀중한 자료입니다.

- 아래 공식 기술문서를 참고하여 한국 도메인에 대한 수집/처리/분석 진행
https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews

### CMDs
```bash
# gz 파일만 남기고 지우기
$ find . -maxdepth 1 -type f ! -name "*.gz" -delete

# copy
$ gcloud storage cp -r 2024-07/ gs://sunsin-bucket/wiki/

# copy -m : 다중 스레드로 병렬 복사
$ gsutil -m cp -r 2025-01/ gs://sunsin-bucket/wiki/
```