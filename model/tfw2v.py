from google.colab import drive
import pandas as pd

# Google Drive 마운트
drive.mount('/content/drive')

# 파일 경로 설정
file_path = '/content/drive/My Drive/BDPgo/paper.csv'

# CSV 파일 읽기
paper = pd.read_csv(file_path)