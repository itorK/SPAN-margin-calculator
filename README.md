Kalkulator depozytów SPAN® dla MySQL lub MariaDB
==================

Kalkulator depozytów SPAN® jako przykład użycia procedur składowanych w MySQL lub MariaDB.<br>
Implementacja algorytmu według przepisów na stronie KDPW <br>
http://www.kdpwccp.pl/pl/zarzadzanie/span/Strony/O-SPAN.aspx

Algorytm zwraca wartości identyczne z Kalkulatorem KDPW<br>
http://www.kdpwccp.pl/pl/zarzadzanie/Strony/kalkulator.aspx

Jeśli chciałbyś wersję w PL/SQL, PL/pgSQL, lub w Javie skontaktuj się z autorem :)

Instalacja 
-----------

1. Należy pobrać i zainstalować bazę danych MariaDB/MySQL<br>
https://downloads.mariadb.org/ (rekomendowana a najlepiej w wersji większej niż 10.0.8)<br>
lub<br>
http://dev.mysql.com/downloads/mysql/

2. Pobieramy 3 pliki -> kalk_schema.sql , import_span.sql, kalkulator_derywaty.sql

3. Odpaliamy konsolę MySQL w scieżce gdzie znajdują się pobrane pliki z punktu 2, a następnie wpisujemy 3 komendy:
``` 
MariaDB [(none)]> source kalk_schema.sql;
MariaDB [kalkulator]> source import_span.sql;
MariaDB [kalkulator]> source kalkulator_derywaty.sql;
``` 

Użycie - Import
-----------
Należy zmienić/dodać w pliku my.ini (plik konfiguracyjny MySQL) wartość<br>
```
[mysqld]
max_allowed_packet = 50M 
```

1. Sciągamy plik ze strony KDPW<br>
http://www.kdpwccp.pl/pl/zarzadzanie/Parametry/SPAN/RPNJI_ZRS.xml
2. W konsoli MySQL wpisujemy komendę<br>
  ```
  MariaDB [kalkulator]> INSERT INTO b (col1) VALUES ( LOAD_FILE('RPNJE_ZRS.xml'));
  ```
  <br>Dla sprawdzenia poprawności załadowania pliku<br>
  ```
  MariaDB [kalkulator]> SELECT count(*) FROM b WHERE col1 IS NOT NULL;
  ```
  <br>Powinno zwrócić wartość 1, jeśli 0 to należy wprowadzić poprawną ścieżkę do pliku RPNJE_ZRS.xml
3. Uruchamiamy import<br>
  ``` 
  MariaDB [kalkulator]> call prImportuj();
  ``` 

######UWAGA
Proces może długo trwać(w moim przypadku 30 minut).

Użycie - Kalkulacja
-----------
1. Czyścimy tabelę zlecenia<br>
  ``` 
  MariaDB [kalkulator]> call prCzysc();
  ``` 
2. Dodajemy pozycje do kalkulatora, gdzie 1 parametr to Nazwa Intrumentu, 2 parametr to ilość pozycji, 3 parametr to cena(w przypadku kontraktów jest ignorowana, w przypadku opcji wpływa na premię opcyjną)<br>
```         
MariaDB [kalkulator]> call prDodajZlecenie('FW20Z1420',8,4.5);
MariaDB [kalkulator]> call prDodajZlecenie('FW20U1520',-2,4.5);
```     
3. Uruchamiamy właściwą kalkulację<br>
  ``` 
  MariaDB [kalkulator]> call prOblDep;
  ``` 
4. Odczyt parametrów<br>
  ``` 
  MariaDB [kalkulator]> select @depozyt,@NOD, @DPNO, @premia, @pno;
  ``` 
<br>Właściwym wynikiem jest @depozyt

Źródła
-----------

[KDPW SPAN](http://www.kdpwccp.pl/pl/zarzadzanie/span/Documents/SPAN_depozyty_dla_kontrakt%C3%B3w_terminowych/SPAN_depozyty_dla_kontraktow_terminowych.pdf) 

Autor
-----------
[Karol Przybylski](http://www.esm-technology.pl) <br>
karol.przybylski@esm-technology.pl
