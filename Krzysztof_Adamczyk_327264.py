"""
Krzysztof Adamczyk
Rozwiązanie pracy domowej nr 5
"""

# 1.) Przygotowanie danych

# a) wykonaj import potrzebnych pakietów
import pandas as pd
import numpy as np
import os, os.path
import sqlite3

# b) wczytaj ramki danych, na których będziesz dalej pracował
Posts = pd.read_csv("Posts.csv.gz", compression= 'gzip')
Comments = pd.read_csv("Comments.csv.gz", compression='gzip')
Users = pd.read_csv("Users.csv.gz", compression='gzip')


# c) przygotuj bazę danych zgodnie z instrukcją zamieszczoną w treści pracy domowej
baza = 'ex.db' #tworzymy baze
conn = sqlite3.connect(baza) #podlaczamy sql do bazy i pozniej z kazdej ramki kleimy baze sqlowa
Comments.to_sql("Comments", conn)
Posts.to_sql("Posts", conn)
Users.to_sql("Users", conn)


# # 2.) Wyniki zapytań SQL

sql_1 = pd.read_sql_query("""
SELECT Location, SUM(UpVotes) as TotalUpVotes
FROM Users
WHERE Location != ''
GROUP BY Location
ORDER BY TotalUpVotes DESC
LIMIT 10
""", conn)
sql_2 = pd.read_sql_query("""
SELECT STRFTIME('%Y', CreationDate) AS Year, STRFTIME('%m', CreationDate) AS Month,
COUNT(*) AS PostsNumber, MAX(Score) AS MaxScore
FROM Posts
WHERE PostTypeId IN (1, 2)
GROUP BY Year, Month
HAVING PostsNumber > 1000
""", conn)
sql_3 = pd.read_sql_query("""
SELECT Id, DisplayName, TotalViews
FROM (
SELECT OwnerUserId, SUM(ViewCount) as TotalViews
FROM Posts
WHERE PostTypeId = 1
GROUP BY OwnerUserId
) AS Questions
JOIN Users
ON Users.Id = Questions.OwnerUserId
ORDER BY TotalViews DESC
LIMIT 10
""", conn)
sql_4 = pd.read_sql_query("""
SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
FROM (
SELECT *
FROM (
SELECT COUNT(*) as AnswersNumber, OwnerUserId
FROM Posts
WHERE PostTypeId = 2
GROUP BY OwnerUserId
) AS Answers
JOIN
(
SELECT COUNT(*) as QuestionsNumber, OwnerUserId
FROM Posts
WHERE PostTypeId = 1
GROUP BY OwnerUserId
) AS Questions
ON Answers.OwnerUserId = Questions.OwnerUserId
WHERE AnswersNumber > QuestionsNumber
ORDER BY AnswersNumber DESC
LIMIT 5
) AS PostsCounts
JOIN Users
ON PostsCounts.OwnerUserId = Users.Id
""", conn)
sql_5 = pd.read_sql_query("""
SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
FROM (
SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
CmtTotScr.CommentsTotalScore
FROM (
SELECT PostId, SUM(Score) AS CommentsTotalScore
FROM Comments
GROUP BY PostId
) AS CmtTotScr
JOIN Posts ON Posts.Id = CmtTotScr.PostId
WHERE Posts.PostTypeId=1
) AS PostsBestComments
JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
ORDER BY CommentsTotalScore DESC
LIMIT 10
""", conn)


#
# # Uwaga: Zapytania powinny się wykonywać nie dłużej niż kilka sekund każde, jednak czasem występują problemy zależne od systemu, np. pod Linuxem zapytania 3 i 5 potrafią zająć odp. kilka minut i ponad godzinę. Żeby obejść ten problem wyniki zapytań mozna zapisac do tymczasowych plików pickle.
#
# # Zapisanie każdej z ramek danych opisujących wyniki zapytań SQL do osobnego pliku pickle.
for i, df in enumerate([sql_1, sql_2, sql_3, sql_4, sql_5], 1):
    df.to_pickle(f'sql_{i}.pkl.gz')

# # Wczytanie policzonych uprzednio wyników z plików pickle (możesz to zrobić, jeżeli zapytania wykonują się za długo).
sql_1, sql_2, sql_3, sql_4, sql_5 = [
    pd.read_pickle(f'sql_{i}.pkl.gz') for i in range(1, 4 + 1)
]
#
# # 3.) Wyniki zapytań SQL odtworzone przy użyciu metod pakietu Pandas.
#
# # zad. 1
#
try:
    pandas_1 = Users.copy() # kopiujemy zeby nie edytowac starych ramek danych
    pandas_1 = pandas_1[pandas_1.Location != ''] # warunek na location != nulla
    pandas_1 = pandas_1.groupby('Location').agg(TotalUpVotes = ('UpVotes', 'sum')).reset_index() # aggregate po Location, sumujemy UpVotes
    pandas_1.rename(columns = {'UpVotes' : 'TotalUpVotes'}) #zmiana nazw kolumn
    pandas_1 = pandas_1.sort_values(by = 'TotalUpVotes', ascending= False).reset_index() # sortujemy po TOtalUpVotes malejaco
    pandas_1 = pandas_1[['Location', 'TotalUpVotes']] # kolumny do wybrania
    pandas_1 = pandas_1.head(10) # 10 pierwszych
    print(pandas_1)


    # sprawdzenie równoważności wyników
    print (pandas_1.equals(sql_1) ) #true

except Exception as e:
    print("Zad. 1: niepoprawny wynik.")
    print(e)

# # zad. 2

try:
    pandas_2 = Posts.copy()
    pandas_2 = pandas_2[(pandas_2['PostTypeId'] == 1) | (pandas_2['PostTypeId'] == 2)]
    pandas_2['CreationDate'] = pd.to_datetime(pandas_2['CreationDate']) # przenosimy Creation Date na format date time
    pandas_2['Year'] = pandas_2['CreationDate'].dt.strftime('%Y') # tworzymy kolumne z rokiem i miesiacem
    pandas_2['Month'] = pandas_2['CreationDate'].dt.strftime('%m')
    ag1 = pandas_2.groupby(['Year', 'Month']).size().rename('PostsNumber') # grupujemy po roku i miesiacu i liczymy ilosc wystapien
    ag2 = pandas_2.groupby(['Year', 'Month']).agg(MaxScore = ('Score', 'max')) # podobnie ale bierzemy max z Score
    pandas_2 = pd.merge(ag1, ag2, on = ['Year', 'Month']) #laczymy obie ramki po roku i miesiacu
    pandas_2 = pandas_2[pandas_2.PostsNumber > 1000] # warunek na postnumber
    pandas_2 = pandas_2.sort_values(by =['Year', 'Month']).reset_index() # sortujemy i resetujemy index
    print(pandas_2)
    # sprawdzenie równoważności wyników
    print (pandas_2.equals(sql_2) )

except Exception as e:
    print("Zad. 2: niepoprawny wynik.")
    print(e)

# # zad. 3
#
try:
    pandas_3 = Posts.copy()
    pandas_3 = pandas_3[pandas_3.PostTypeId == 1]
    pandas_3 = pandas_3.groupby('OwnerUserId').agg(TotalViews = ('ViewCount', 'sum')).reset_index()
    pandas_3 = pd.merge(Users.copy(), pandas_3, left_on= 'Id', right_on= 'OwnerUserId') # laczymy ramki po id i owneruserid
    pandas_3 = pandas_3.sort_values(by = 'TotalViews', ascending= False).reset_index()
    pandas_3 = pandas_3[['Id', 'DisplayName', 'TotalViews']]
    pandas_3 = pandas_3.head(10)
    print(pandas_3)
    # sprawdzenie równoważności wyników
    print (pandas_3.equals(sql_3) )

except Exception as e:
    print("Zad. 3: niepoprawny wynik.")
    print(e)
#
# # zad. 4
#
try:
    var2 = Posts.copy()
    var2 = var2[var2.PostTypeId == 2]
    var2 = var2.groupby('OwnerUserId').size().rename('AnswersNumber') #ilosc wystapien + zmiana nazwy na AnsewersNumber
    var1 = Posts.copy()
    var1 = var1[var1.PostTypeId == 1]
    var1 = var1.groupby('OwnerUserId').size().rename('QuestionsNumber')
    pandas_4 = pd.merge(var2, var1, left_on='OwnerUserId', right_on= 'OwnerUserId')
    pandas_4 = pandas_4[pandas_4.AnswersNumber > pandas_4.QuestionsNumber] #warunek na AnswersNumber > QuestionsNumber
    pandas_4 = pd.merge(pandas_4, Users.copy(), left_on='OwnerUserId', right_on= 'Id') # laczymy
    pandas_4 = pandas_4.sort_values(by = 'AnswersNumber', ascending= False).reset_index()
    pandas_4 = pandas_4[['DisplayName', 'QuestionsNumber', 'AnswersNumber', 'Location', 'Reputation', 'UpVotes', 'DownVotes']]
    pandas_4 = pandas_4.head(5)
    print(pandas_4)


    # sprawdzenie równoważności wyników
    print (pandas_4.equals(sql_4) )

except Exception as e:
    print("Zad. 4: niepoprawny wynik.")
    print(e)
#
# # zad. 5
#
try:
    CmtTotScr = Comments.copy()
    CmtTotScr = CmtTotScr.groupby('PostId').agg(CommentsTotalScore = ('Score', 'sum')).reset_index()
    Post = Posts.copy()
    Post = Post[Post.PostTypeId == 1]
    pandas_5 = pd.merge(CmtTotScr, Post, left_on= 'PostId', right_on='Id')
    pandas_5 = pd.merge(pandas_5, Users.copy(), left_on='OwnerUserId', right_on='Id')
    pandas_5 = pandas_5.sort_values(by = 'CommentsTotalScore', ascending= False).reset_index()
    pandas_5 = pandas_5[['Title', 'CommentCount', 'ViewCount', 'CommentsTotalScore', 'DisplayName', 'Reputation', 'Location']]
    pandas_5 = pandas_5.head(10)
    print(pandas_5)
    # sprawdzenie równoważności wyników
    #print (pandas_5.equals(sql_5) )

except Exception as e:
    print("Zad. 5: niepoprawny wynik.")
    print(e)
conn.close() # zamykamy polaczenie z baza danych
