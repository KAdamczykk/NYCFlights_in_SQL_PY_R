### Przetwarzanie Danych Ustrukturyzowanych 2023L
### Praca domowa nr. 3
###
### UWAGA:
### nazwy funkcji oraz ich parametrow powinny pozostac niezmienione.
###  
### Wskazane fragmenty kodu przed wyslaniem rozwiazania powinny zostac 
### zakomentowane
###

# -----------------------------------------------------------------------------#
# Wczytanie danych oraz pakietow.
# !!! Przed wyslaniem zakomentuj ten fragment

# -----------------------------------------------------------------------------#
#wczytujemy dane, pakiety i biblioteki
# Posts <- read.csv("D:/Studia/Semestr 2/R/Data frame projekt/Posts.csv.gz")
# Users <- read.csv("D:/Studia/Semestr 2/R/Data frame projekt/Users.csv.gz")
# Comments <- read.csv("D:/Studia/Semestr 2/R/Data frame projekt/Comments.csv.gz")
# options(stringsAsFactors = FALSE)




#install.packages(c("nycflights13", "sqldf", "dplyr", 'data.table'))
#install.packages("RSQLite")

# library("sqldf")
# library("dplyr") 
# library("data.table")

# -----------------------------------------------------------------------------#
# Zadanie 1
# -----------------------------------------------------------------------------#

sql_1 <- function(Users){
 
    sqldf("SELECT Location,SUM(UpVotes) as TotalUpVotes 
         FROM Users WHERE Location != '' GROUP BY Location 
         ORDER BY TotalUpVotes DESC
         LIMIT 10") -> df
  df
}

base_1 <- function(Users){
  #zakladamy ze Location nie jest puste
  Users <- Users[Users$Location != '',]
  #tworzymy ramke o kolumnach Location i UpVotes, gdzie w miescu UpVotes znajduje sie suma UpVotes dla danej Location i pomijamy NA
  us <- aggregate(x = Users["UpVotes"], by = Users["Location"], FUN=sum, na.rm = TRUE)
  #Zmieniamy nazwe UpVotes na TotalUpVotes
  colnames(us)[2] <- "TotalUpVotes"
  #sortujemy malejaco po TotalUpVotes
  us <- us[order(us$TotalUpVotes,decreasing = TRUE),]
  #wybieramy 10 pierwszych wartosci
  us <- head(us,10)
  #resetujemy indexy wierszy
  row.names(us) <- NULL
  #zwracamy data.frame
  us
}

dplyr_1 <- function(Users){
  # %>% - dziala jak przekazanie strumienia danych dalej
    x <- Users %>% #bierzemy data.frame users
      select(Location, UpVotes) %>% #wybieramy z niego pola Location i UpVotes
      filter(Location != "") %>% #warunek na Location
      group_by(Location) %>% #grupujemy ramke po Location
      summarise(TotalUpVotes = sum(UpVotes))%>% #sumujemy wartosci UpVotes dla danych Location i nazywamy je TotalUpVotes
      arrange(desc(TotalUpVotes)) %>% #sortowanie
      slice_head(n = 10) #wybor 10 opcji
      
    x
}

table_1 <- function(Users){
    dt <- setDT(Users) #tworzymy data.table z data.frame Users
    dt <- dt[Location != ""] #warunek na Location
    agg <- dt[, .(TotalUpVotes = sum(UpVotes)), by = Location] #sumujemy UpVotes po Location i nazywamy je TotalUpVotes
    ordered <- setorder(agg, cols = - "TotalUpVotes") #sortowanie po TOtalUpVotes
    top <- ordered[1:10] #wybieramy 10 pierwszych
    ret <- top[,.(Location, TotalUpVotes)] #wybieramy kolumny do wyswietlenia
    

}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
#wywolanie funkcji w 1 zadaniu i sprwdzenie poprawnosci
# s1 <- sql_1(Users)
# #print(s1)
# b1 <- base_1(Users)
# print(b1)
# d1 <- dplyr_1(Users)
# #print(d1)
# t1 <- table_1(Users)
# #print(t1)
#dplyr::all_equal(s1, b1)
#dplyr::all_equal(d1, t1)
#dplyr::all_equal(d1, b1)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
#microbenchmark::microbenchmark(
# s1,b1,d1,t1
#)

# wyniki microbenchmark 1
# expr min lq mean median  uq  max neval
# s1   0  0   49      0 100  500   100
# b1   0  0   48      0 100  600   100
# d1   0  0   49      0 100 1200   100
# t1   0  0   57      0 100  900   100
# -----------------------------------------------------------------------------#
# Zadanie 2
# -----------------------------------------------------------------------------#

sql_2 <- function(Posts){
    sqldf("SELECT STRFTIME('%Y', CreationDate) AS Year, STRFTIME('%m', CreationDate) AS Month,
COUNT(*) AS PostsNumber, MAX(Score) AS MaxScore
FROM Posts
WHERE PostTypeId IN (1, 2)
GROUP BY Year, Month
HAVING PostsNumber > 1000") -> df
  df
}

base_2 <- function(Posts){ 
    Posts <- Posts[(Posts$PostTypeId ==1 | Posts$PostTypeId ==2),] #warunek na PostTypeId =1 lub =2
    Year <- list(format(as.Date(Posts$CreationDate), "%Y")) #formatyjemy CreationDate na typ Date i wybieramy odpowiednio rok i miesiac
    Month <- list(format(as.Date(Posts$CreationDate), "%m")) #lista, bo by moc uzyc aggregate obiekt by= musi byc lista
    l1 <- aggregate(x = Posts[1], by = c(Year, Month), FUN = length) #tworzymy ramke dancyh ktora smuje nam ilosc wystapien unikatowych wartosci Year i Month
    y <- aggregate(x = Posts["Score"], by = c(Year, Month), FUN = max) #tworzymy ramke gdzie wybieramy max Score dla kazdego roku i miesiaca
    wyn <- merge(l1,y,by.l1 = c(Year, Month)) #laczymy ramki po roku i miesiacu
    colnames(wyn) <- c("Year", "Month", "PostsNumber", "MaxScore") #ustawiamy nazwy kolumn
    lepwyn <- wyn[wyn$PostsNumber> 1000,] #warunek na PostsNumber > 1000
    ret <- na.omit(lepwyn[order(c(lepwyn$Year, lepwyn$Month)),]) #pomijamy NA i sortujemy po roku i miesiacu
    row.names(ret) <- NULL #reset indexow
    ret #return
}

dplyr_2 <- function(Posts){
     x <- Posts %>% #korzystamy z data.frame Posts
       filter(PostTypeId %in% c(1,2)) %>% #warunek na PostTypeId rowne 1 lub 2
       mutate(Year = format(as.Date(CreationDate), "%Y"), Month = format(as.Date(CreationDate), "%m"))  %>% #utworzenie kolumn Year i Month po uprzednim skonwertowaniu ich na typ Date i wybraniu odpowiednio roku i miesiaca
       group_by(Year, Month) %>%  #grupowanie po kolumnach Year i Month
       summarize(PostsNumber = n(), MaxScore = max(Score)) %>% #odpowiednik dplyrowy slowa kluczowego COUNT(*) z SQL + wybieramy max Score
       filter(PostsNumber > 1000) #wybieramy wartosci PostsNumber >1000
       
}

table_2 <- function(Posts){
    dt <- setDT(Posts) #ustawienie data.table
    dt <- dt[PostTypeId %in% c(1,2)] #warunek na PostTypeId rowne 1 lub 2
    dt <- dt[, .(Year = format(as.Date(CreationDate), "%Y"), Month = format(as.Date(CreationDate), "%m"), Score)] #rowniez skonwertowanie CreationDate na typ Date i wybor roku i miesiaca
    dt <- dt[, .(PostsNumber = .N, MaxScore = max(Score)), by = .(Year, Month)] #Count i max Score dla danych lat i miesiecy
    dt <- dt[PostsNumber > 1000] #warunek na PostsNumber >1000
    dt #return
    
    
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
#wywolanie funkcji w 2 zadaniu i sprwdzenie poprawnosci
# s2 <- sql_2(Posts)
# #print(s2)
# b2 <- base_2(Posts)
# #print(b2)
# d2 <- dplyr_2(Posts)
# #print(d2)
# t2 <- table_2(Posts)
# #print(t2)
#dplyr::all_equal(s2, b2)
#dplyr::all_equal(d2, b2)
#dplyr::all_equal(d2, t2)
# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# microbenchmark::microbenchmark(
#    s2,b2,d2,t2
# )

# wyniki microbenchmark 2
# expr min lq mean median  uq  max neval
# s2   0  0   53      0 100  600   100
# b2   0  0   62      0 100 2000   100
# d2   0  0   53      0 100  900   100
# t2   0  0   49      0 100 1200   100
# -----------------------------------------------------------------------------#
# Zadanie 3
# -----------------------------------------------------------------------------#

sql_3 <- function(Posts, Users) {
  Questions <- sqldf( 'SELECT OwnerUserId, SUM(ViewCount) as TotalViews
                                      FROM Posts
                                      WHERE PostTypeId = 1
                                      GROUP BY OwnerUserId' )
  sqldf( "SELECT Id, DisplayName, TotalViews
                FROM Questions
                JOIN Users
                ON Users.Id = Questions.OwnerUserId
                ORDER BY TotalViews DESC
                LIMIT 10")
}

base_3 <- function(Posts, Users){ 
    Posts <- as.data.frame(Posts) #konwertyjemy na data.frame, aby nie dzialac na data.table, przynajmniej mi R tak dzialal
    Posts <- Posts[Posts$PostTypeId == 1,] #Warunek na PostTypeId = 1 przecinek po 1 bo wybieramy kolumny
    x <- aggregate(x = Posts$ViewCount, by = Posts["OwnerUserId"], FUN = sum) #tworzymy data frame o kolumnach OwnerUserId i VievCount gdzie ViewCount bedzie zawieralo sume ViewCount dla danej wartosci OwnerUserId
    x_ord <- x[order(x$OwnerUserId),] #sortowanie po OwnerUserId
    colnames(x_ord) = c("OwnerUserId", "TotalViews") #zmiana nazw kolumn
    
    joined <- merge(x = Users, y = x_ord, by.x = "Id", by.y = "OwnerUserId") #laczymy dwa data frame Users i x_ord po Id dla Users i po OwnerUserId dla x_ord
    ordered <- na.omit(joined[order(joined$TotalViews, decreasing = TRUE),])
    ret <- head(ordered, 10) #wybieramy 10 pierwszych wartosci
    row.names(ret) <- NULL #resetujemy indexy wierszy
    ret[,c("Id", "DisplayName", "TotalViews")] #wybieramy kolumny do wyswietlenia
    
}

dplyr_3 <- function(Posts, Users){
    questions <- Posts %>% #tworzymy data frame questions wybierajac wartosci z dataframe Posts
      select(OwnerUserId, ViewCount, PostTypeId) %>% #z Posts bierzemy OwnerUserId, ViewCount i PostTypeId
      group_by(OwnerUserId) %>% #grupujemy po OwnerUserId
      filter(PostTypeId == 1) %>% #warunek na PostTypeId = 1
      summarize(TotalViews = sum(ViewCount))  #sumujemy ViewCount dla unikatowych OwnerUserId i nazywamy je TotalViews
    Users <- Users %>% 
      select(Id, DisplayName) #z Users potrzebujemy tylko Id i DisplayName do dzialania stąd nadpisanie zmiennej o ten sam data frame ale o 2 kolumnach jedynie
    p <- inner_join(Users, questions, by =  join_by(Id == OwnerUserId) ) #laczymy ramki Users i Questions po Id i OwnerUserId opowiednio
    rowz <- p %>% 
      arrange(desc(TotalViews)) %>% #sortujemy malejaco po TotalViews i wybieramy 10 pierwszych wartosci
      slice_head(n =10)
    
      
}

table_3 <- function(Posts, Users){
    dt <- setDT(Posts)
    dt <- dt[PostTypeId ==1]
    Questions <- dt[, .(TotalViews = sum(ViewCount, na.rm = TRUE)), by = OwnerUserId]
    dT <- setDT(Users)
    mer <- na.omit(Users[Questions, on = .(Id = OwnerUserId)])
    ret <- setorder(mer, cols = -"TotalViews")
    ret <- ret[1:10]
    ret[, .(Id,DisplayName, TotalViews)]
    
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
#wywolanie funkcji w 3 zadaniu i sprwdzenie poprawnosci
# s3 <- sql_3(Posts, Users)
# #print(s3)
# b3 <- base_3(Posts, Users)
# #print(b3)
# d3 <- dplyr_3(Posts, Users)
# #print(d3)
# t3 <- table_3(Posts, Users)
# #print(t3)

#dplyr::all_equal(s3, d3)
#dplyr::all_equal(b3, d3)
#dplyr::all_equal(t3, d3)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# microbenchmark::microbenchmark(
#  s3,b3,d3,t3 
# )
# wyniki microbenchmark 3
# expr min lq mean median  uq  max neval
# s3   0  0   61    100 100 1000   100
# b3   0  0   58      0 100 1300   100
# d3   0  0   61      0 100 1800   100
# t3   0  0   79    100 100 1600   100
# -----------------------------------------------------------------------------#
# Zadanie  4
# -----------------------------------------------------------------------------#

sql_4 <- function(Posts, Users){
    sqldf("SELECT DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes
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
") -> df
    df
}

base_4 <- function(Posts, Users){ 
    Posts2 <- as.data.frame(Posts[Posts$PostTypeId ==2,]) #tak samo jak w 3 konwertuje na data frame 
    answers <- aggregate(x = Posts2[1], by = Posts2["OwnerUserId"], FUN = length) #tworze data frame ktory zawiera unikatowe wartosci OwnerUserId i licznosc ich wystepowania
    colnames(answers)[2] <- "AnswersNumber" #nazwy kolumn: OwnerUserId i AnswersNumber
    
    #analogiczne dzialanie dla PostTypeId == 1
    Posts1 <- as.data.frame(Posts[Posts$PostTypeId ==1,]) 
    questions <-aggregate(x = Posts1[1], by = Posts1["OwnerUserId"], FUN = length)
    colnames(questions)[2] <- "QuestionsNumber"
    
    joined <- merge(answers, questions, by.x = "OwnerUserId", by.y = "OwnerUserId") #laczymy ramki answers i questions po OwnerUserId
    joined <- joined[(joined$AnswersNumber > joined$QuestionsNumber),] # wybieramy wiersze gdzie AnswersNuber > QuestionsNumber
    ret <- merge(joined, Users, by.x = "OwnerUserId", by.y = "Id") #laczymy joined i Users opowiednio po OwnerUserId i Id
    ret <- na.omit(ret[order(ret$AnswersNumber, decreasing = TRUE),]) #pomijamy na i sortujemy malejaco po AnswersNumber 
    row.names(ret) <- NULL #reset indexow
    ret <- head(ret, 5) #wybor pierwszych 5 wierszy
    ret[,c("DisplayName", "QuestionsNumber", "AnswersNumber", "Location", "Reputation", "UpVotes", "DownVotes")] #wybieramy kolumny do wyswietlenia
    
}

dplyr_4 <- function(Posts, Users){
    Answers <- Posts %>% 
      select(OwnerUserId, PostTypeId) %>% #z Posts wybieramy OwnerUserId i PostTypeId
      group_by(OwnerUserId) %>% #grupujemy po OwnerUserId
      filter(PostTypeId == 2) %>% #wybieramy wartosci PostTypeId =2
      summarize(AnswersNumber = n()) #sumujemy ilosc wystapien kazdego OwnerUserId jako AnswersNumber
    #analogicznie robiy z Questions
    Questions <- Posts %>%
      select(OwnerUserId, PostTypeId) %>%
      group_by(OwnerUserId) %>%
      filter(PostTypeId == 1) %>%
      summarize(QuestionsNumber = n())
    
    PostsCounts <- inner_join(Answers, Questions, by = join_by(OwnerUserId == OwnerUserId)) #laczymy listy Answers i Questions po OwnerUserId
    PostsCounts<- PostsCounts %>%
      filter(AnswersNumber > QuestionsNumber) %>% #warunek na AnswersNumber > QuestionsNumber
      arrange(desc(AnswersNumber))  #sortujemy malejaco
      
    wyn <- inner_join(PostsCounts, Users, by = join_by(OwnerUserId == Id)) #laczymy ramki PostsCounts i Users po OwnerUserId i Id
    wyn <- wyn %>%
      select(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes) %>% #wybieramy kolumny do wyswietlenia
      filter(!is.na(Location)) %>% #pomijamy NA
      slice_head(n = 5) #pierwsze 5 wartosci
    
      
}

table_4 <- function(Posts, Users){
    Posts <- setDT(Posts) #tworzymy data.table
    Posts1 <- Posts[PostTypeId ==2] #warunek na PostTypeId = 2
    Answers <- Posts1[, .(AnswersNumber = .N), by = OwnerUserId] #data.table kolumnach OwnerUserId i ilosci wystepowania danej wartosci w Posts1
    #to samo robimy dla Questions
    Posts2 <- Posts[PostTypeId == 1]
    Questions <-Posts2[, .(QuestionsNumber = .N), by = OwnerUserId]
    PostsCounts <- na.omit(Answers[Questions, on = .(OwnerUserId = OwnerUserId)]) #omijamy NA i laczymy dwa data.table po OwnerUserId
    PostsCounts <- PostsCounts[AnswersNumber > QuestionsNumber] #warunek AnswersNumber > QuestionsNumber
    ret <- na.omit(PostsCounts[Users, on = .(OwnerUserId = Id)])
    ret <- setorder(ret, cols = -"AnswersNumber") #sortujemy po kolumnie AnswersNumber, malejaco
    ret <- ret[1:5] #pierwsze 5 wartosci
    ret[,.(DisplayName, QuestionsNumber, AnswersNumber, Location, Reputation, UpVotes, DownVotes)] #wybieramy kolumny do wyswietlenia
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
#wywolanie funkcji w 4 zadaniu i sprwdzenie poprawnosci
# s4 <- sql_4(Posts, Users)
# #print(s4)
# b4 <- base_4(Posts, Users)
# #print(b4)
# d4 <- dplyr_4(Posts, Users)
# #print(d4)
# t4 <- table_4(Posts,Users) 
# #print(t4)

#dplyr::all_equal(s4, b4)
#dplyr::all_equal(d4, b4)
#dplyr::all_equal(d4, t4)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# microbenchmark::microbenchmark(
# s4,b4,d4,t4
# )

# wyniki microbenchmark 4
# expr min lq mean median  uq  max neval
# s4   0  0   46      0 100  600   100
# b4   0  0   58      0 100 1400   100
# d4   0  0   81      0 100 3200   100
# t4   0  0   61    100 100  800   100

# -----------------------------------------------------------------------------#
# Zadanie 5
# -----------------------------------------------------------------------------#

sql_5 <- function(Posts, Comments, Users) {
  
  CmtTotScr <- sqldf( 'SELECT PostId, SUM(Score) AS CommentsTotalScore
                                      FROM Comments
                                      GROUP BY PostId' )
  
  PostsBestComments <- sqldf( 'SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,

CmtTotScr.CommentsTotalScore
                                                       FROM CmtTotScr
                                                       JOIN Posts ON Posts.Id = CmtTotScr.PostId
                                                       WHERE Posts.PostTypeId=1' )
  sqldf( 'SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
                 FROM PostsBestComments
                 JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
                 ORDER BY CommentsTotalScore DESC
                 LIMIT 10' )
}

base_5 <- function(Posts, Comments, Users){
    CmtTotScr <- aggregate(x = Comments["Score"], by = Comments["PostId"], FUN = sum) #tworzymy ramke CmtTotScr o wartosciach PostId i sumie Score dla danej unikatowej wartosci PostId
    colnames(CmtTotScr)[2] <- "CommentsTotalScore" #zmiana nazwy kolumny
    CmtTotScr <- CmtTotScr[order(CmtTotScr$PostId),] #sortowanie 
    Posts <- Posts[Posts$PostTypeId ==1,] #warunek na PostTypeId = 1
    
    PostsBestComments <- merge(x = CmtTotScr, y = Posts, by.x = "PostId", by.y = "Id") #laczymy CmtTotScr i Posts po PostId i Id
    joined <- merge(PostsBestComments, Users, by.x = "OwnerUserId", by.y = "Id") #sortujemy po CommentsTotalScore
    joined <- joined[order(joined$CommentsTotalScore, decreasing = TRUE), ] #sortujemy malejaco
    ret <- head(joined, 10) #10 pierwszych
    rownames(ret) <- NULL#reset indexu
    ret[,c("Title", "CommentCount", "ViewCount", "CommentsTotalScore", "DisplayName", "Reputation", "Location")] #wybor kolumn do wyswietlenia
}

dplyr_5 <- function(Posts, Comments, Users){
    CmtTotScr <- Comments %>%
      select(PostId, Score) %>% #wybieramy PostId i Score z Comments
      group_by(PostId) %>% #grupujemy po PostId
      summarize(CommentsTotalScore = sum(Score)) #sumujemy Score dla danych PostId i nazywamy CommentsTotalScore
    Posts <- Posts %>%
      select(OwnerUserId, Title, CommentCount, ViewCount, Id, PostTypeId) #z posts wybieramy takie kolumny
    PostsBestComments <- inner_join(CmtTotScr, Posts, by = join_by(PostId == Id)) %>% #laczymy ramki po PostId i Id
      filter(PostTypeId == 1) #warunek PostTypeId = 1
    zwr <- inner_join(PostsBestComments, Users, by = join_by(OwnerUserId == Id)) %>% #laczymy PostsBestComments i Users Po OwnerUserId i Id odpowiednio
      select(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location) %>% #kolumny do wyswietlenia
      arrange(desc(CommentsTotalScore)) %>% #sortowanie malejace
      filter(!is.na(Location)) %>%#pomijamy na
      slice_head(n = 10) #10 pierwszych
      
      
    
} 


table_5 <- function(Posts, Comments, Users){
  #ustawienie data.table x3
    Comments <- setDT(Comments)
    Posts <- setDT(Posts)
    Users <- setDT(Users)
    CmtTotScr <- Comments[, .(CommentsTotalScore = sum(Score)), by = PostId] #sumujemy Score dla danych PostId i nazywamy CommentsTotalScore
    CmtTotScr <- CmtTotScr[, .(PostId, CommentsTotalScore)] #wybieramy kolumny nam potrzebne
    Posts <- Posts[PostTypeId ==1] #warunek na posttypeid =1
    PostsBestComment <- CmtTotScr[Posts, on = .(PostId = Id)] #laczymy ramki po PostId i Id
    PostsBestComment <- PostsBestComment[, .(OwnerUserId,Title, CommentCount, ViewCount, CommentsTotalScore)] #wybieramy potrzebne kolumny
    ret <- na.omit(PostsBestComment[Users, on= .(OwnerUserId = Id)])  #laczymy PostsBestComments i Users Po OwnerUserId i Id odpowiednio i pomijamy NA
    ret <- setorder(ret, cols = - "CommentsTotalScore") #sortujemy malejaco
    ret <- ret[1:10] #10 pierwszych
    ret <- ret[, .(Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location)] #wybieramy potrzebne kolumny i return
    
}

# Sprawdzenie rownowaznosci wynikow - zakomentuj te czesc przed wyslaniem
#wywolanie funkcji w 5 zadaniu i sprwdzenie poprawnosci
# s5 <- sql_5(Posts, Comments, Users)
# #print(s5)
# b5 <- base_5(Posts, Comments, Users)
# #print(b5)
# d5 <- dplyr_5(Posts, Comments, Users)
# #print(d5)
# t5 <- table_5(Posts,Comments, Users)
# #print(t5)
#dplyr::all_equal(s5, b5)
#dplyr::all_equal(d5, b5)
#dplyr::all_equal(d5, t5)

# Porowanie czasow wykonania - zakomentuj te czesc przed wyslaniem
# microbenchmark::microbenchmark(
#  s5,b5,d5,t5
# )

#wyniki microbenchmark 5
# expr min lq mean median  uq  max neval
# s5   0  0   45      0 100  600   100
# b5   0  0   48      0 100  600   100
# d5   0  0   49      0 100  700   100
# t5   0  0   52      0 100 1300   100