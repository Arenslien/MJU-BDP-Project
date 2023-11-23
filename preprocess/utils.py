def paper_pd(data, Subject):
  data['Abstract'] = data['Abstract'].str.replace('Abstract : ', '')
  data['Authors'] = data['Authors'].str.replace('Authors: ', '')

  data['Date'] = data['Date'].apply(remove_after_semicolon)

  data['Subjects'] = Subject

  return data


# ';' 이후의 내용을 제거하는 함수 정의
def remove_after_semicolon(text):
    return text.split(';')[0].strip()