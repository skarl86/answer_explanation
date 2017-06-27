Answer Explantion
=======
# 1. 사용 방법
-----------------
## 1.1 Load
1. HDFS에 올라가 있는 "input triple"로 SWRL Reasoner를 실행.추론 결과는 HDFS상에 nk 경로에 저장, ATMS에 넘길 데이터는 atmsOutput 경로에 저장.
2. SWRL Reasoner에 Output이 HDFS 상에 atmsOutput 경로에 저장되고, 이 데이터를 기반으로 ATMS Build를 시작.
3. Reasoner를 통해 추론 된 Triple과 Input Triple를 가지고 HDFS상에 SPARQLGX DB를 생성.

<pre><code>bash load.sh dbName swrl_rule.txt hdfs_triple_file.nt hdfs_inf_triple_file.nt</code></pre>
- dbName : HDFS상에 저장되는 SPARQLGX Load 결과 경로.
- swrl_rule.txt : SWRL Reasoner에 사용 될 규칙.
- hdfs_triple_file.nt : HDFS상의 초기 트리플.
- hdfs_inf_triple_file.nt : HDFS상의 추론 트리플.

## 1.2 Execute
1. Load를 통해 만들어진 SPARQLGX DB를 불러와 Query를 실행 후 Query 결과를 HDFS상에 저장.
2. Query 결과를 가져와 Answer Explanation 실행.

<pre><code>bash run.sh dbName query_file.rq </code></pre>
- dbName : HDFS상에 저장되는 SPARQLGX Load 결과 경로.
- query_file.rq : SPARQL 기반의 질의 파일.

# 작성자
이 남기<beohemian@gmail.com>
숭실대학교 [인공지능연구실] 2017.
