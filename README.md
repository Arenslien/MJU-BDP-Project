# 2023년 2학기 빅데이터프로그래밍 팀프로젝트

## 찾았다! 내 Reference! (참고 논문)

* Team : 초록팀(Abstract Team)
* 팀원: 허재석(60171791), 정성훈(60191686), 신지훈(60201687), 신지영(60211674)

--------------

# 🚨중요사항🚨

## 1. 초기 세팅

### 1.1 개인 토큰 발급하기

[참고 URL : https://heytech.tistory.com/393](https://heytech.tistory.com/393)

1. Github Login 하기
2. [개인 Setting으로 이동](https://github.com/settings/tokens)
3. 좌측 하단의 Developer Settings 이동
4. Personal access tokens --> Tokens(classic) 이동
5. Generate new token (classic)
6. repo에 체크 후 생성하기
7. 개인 Token 복사하기(처음에만 복사 가능 이후 안 보여줌!!!)

### 1.2 Git clone (초기 첫 작업 딱 한 번만!!!)

```bash
# 작업할 디렉토리 이동 후(자세한 건 git_setting.ipynb 확인)
git clone https://[Github ID]:[Personal Token]@github.com/Arenslien/MJU-BDP-Project.git
```
```bash
# 각자 Github 사용하는 이메일
!git config --global user.email "Your@example.com"
# Github ID
!git config --global user.name "Your ID"
```

### 1.3 Git 브랜치 설정

1.3.1 작업할 branch 생성

```bash
git branch <각자 사용할 브랜치명>
```

1.3.2 사용할 branch 설정

```bash
git checkout <사용할 브랜치 명>
```

--------------

## 2. 작업 전 main branch에서 코드 pull로 가져오기

### 2.1 git 상태 체크

2.1.1 작업 전 git 상태 체크

```bash
git status
```

git status 결과가 아래와 같으면 pull 받아도 됨

```
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

2.1.2 현재 작업 branch 상태 체크

```bash
git branch
```

git branch 결과가 아래와 같으면 굿

```
* <my branch>
  main
```

### 2.2 git 코드 가져오기

```bash
git pull origin main
```

## 3. 작업 후 코드 git에 업데이트 하기

### 3.1 작업 전 git 상태 체크

```bash
git status
```

아래의 git status 결과로 내가 업데이트 할 파일 목록 확인 가능

```
Untracked files:
  (use "git add <file>..." to include in what will be committed)
        <파일1>
        <파일2>
        ...
```

### 3.2 작업한 코드 add, commit, push하기

아래 내용을 한 라인씩 순차적으로 작성

```bash
git add *
git commit -m "변경사항을 설명하는 메시지"
git push origin <자신이 사용하는 브랜치 명>
```
