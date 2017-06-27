Answer Explantion
=================
1. 사용 방법
-----------------
# 1.1 Load
1. HDFS에 올라가 있는 "input triple"로 SWRL Reasoner를 실행.
    1.1 추론 결과는 HDFS상에 nk 경로에 저장, ATMS에 넘길 데이터는 atmsOutput 경로에 저장.
2. SWRL Reasoner에 Output이 HDFS 상에 atmsOutput 경로에 저장되고, 이 데이터를 기반으로 ATMS Build를 시작.
3. Reasoner를 통해 추론 된 Triple과 Input Triple를 가지고 HDFS상에 SPARQLGX DB를 생성.

<pre><code>bash load.sh "sparqlgx dbname" "swrl rule path" "input triple path in HDFS" "inferred triple path in HDFS"</code></pre>

# 1.2 Execute
<pre><code>bash run.sh "sparqlgx dbname" "query path"</code></pre>