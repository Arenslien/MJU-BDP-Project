# runPaperAnalysis.sh

export LANG=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export PYSPARK_PYTHON="/bin/python3.6"

#spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
	#stop_words.py

spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=/bin/python3.6 \
	paperAbstractAnalysis.py

