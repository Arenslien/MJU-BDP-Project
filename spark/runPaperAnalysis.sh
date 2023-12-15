# runPaperAnalysis.sh

export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYSPARK_PYTHON="/bin/python3.6"

#spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
	#stop_words.py

spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
	paperAbstractAnalysis.py

mkdir ../analysis_result
hdfs dfs -copyToLocal /user/maria_dev/archive_store/abstract-keyword ../analysis_result/
hdfs dfs -copyToLocal /user/maria_dev/archive_store/Yearly-Publication-Count/ ../analysis_result/

bash pipInstall.sh
python3 visulization.py

hdfs dfs -mkdir /user/maria_dev/archive_store/analysis_result
hdfs dfs -copyFromLocal ./all_keyword_wordcloud.png archive_store/analysis_result/
hdfs dfs -copyFromLocal ./yearly_publication_count.png archive_store/analysis_result/
hdfs dfs -copyFromLocal ./Paper_Analysis_Result_Report.docx archive_store/analysis_result/

