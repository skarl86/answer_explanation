Answer Explantion
=======
Watson, Siri, Exobrain과 같은 사용자질의에 대한 답을 해주는 QA 시스템에서는 대용량 데이터에 대한 질의 처리 기술이 활용 되고 있다.
이와 같은 시스템에 사용 되는 질의 처리 기술 연구가 현재까지도 계속해서 진행되고 있으며, 이를 통해 대용량 질의문 처리 기술의 수준이 향상 되었다.
하지만 사용자 질의결과에 대한 처리 기술은 많이 연구되고 있지만, 질의결과를 설명해주는 기술 연구는 부족한 상황이다.
따라서 본 연구에서는 사용자 질의결과에 대해 인과관계를 설명해줌으로써, 편의성을 증대시키는 시스템을 제안한다.
기존 온톨로지에 규칙 기반 추론 수행한 후, 종속 구조를 구축하기위한 ATMS를 이용해 질의 처리 결과에 대한 설명 시스템 구축 방안에 대하여 설명한다.
대용량 데이터를 이용한 질의 결과 설명 시스템에 대한 평가를 위해서 벤치마크 데이터인 LUBM(Lehigh University Benchmark)을 사용했으며,
LUBM에서 제공한 14개의 테스트 질의문을 사용하여 평가했다. 실험은 LUBM 데이터셋 별로 질의 결과를 설명하는 시간을 측정하였다.

# 1. 사용 방법
-----------------
## 1.1 Load
1. HDFS에 올라가 있는 "input triple"로 SWRL Reasoner를 실행.<br>참고)추론 결과는 HDFS상에 nk 경로에 저장, ATMS에 넘길 데이터는 atmsOutput 경로에 저장.
2. SWRL Reasoner에 Output이 HDFS 상에 atmsOutput 경로에 저장되고, 이 데이터를 기반으로 ATMS Build를 시작.
3. Reasoner를 통해 추론 된 Triple과 Input Triple를 가지고 HDFS상에 SPARQLGX DB를 생성.

<pre><code>bash bin/load.sh sparqlgx_dbName swrl_rule.txt hdfs_triple_path hdfs_inf_triple_path</code></pre>
- <code>sparqlgx_dbName</code> : SPARQLGX DB 이름.(결과적으로 HDFS상의 경로명이다.)
- <code>swrl_rule.txt</code> : SWRL Reasoner에 사용 될 규칙.
- <code>hdfs_triple_path</code> : HDFS상의 초기 트리플 경로.
- <code>hdfs_inf_triple_path</code> : HDFS상의 추론 트리플 경로.

예)<code> bash bin/load.sh nk-lubm1k swrl_rule/nk-lubm-rule.txt LUBM/lubm1000 nk </code>

## 1.2 Execute
1. Load를 통해 만들어진 SPARQLGX DB를 불러와 Query를 실행 후 Query 결과를 HDFS상에 저장.
2. Query 결과를 가져와 Answer Explanation 실행.

<pre><code>bash bin/run.sh sparqlgx_dbName query_file.rq </code></pre>
- <code>sparqlgx_dbName</code> : HDFS상에 저장되는 SPARQLGX Load 결과 경로.
- <code>query_file.rq</code> : SPARQL 기반의 질의 파일.

예) <code> bash bin/run.sh nk-lubm1k query/lubm/Q1.rq </code>

# 작성자
이 남기
<beohemian@gmail.com>
숭실대학교 [인공지능연구실](http://ailab.ssu.ac.kr/rb/) 2017.
