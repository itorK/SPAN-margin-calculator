Kalkulator depozytów SPAN® dla MySQL lub MariaDB
==================

Kalkulator depozytów SPAN® jako przykład użycia procedur składowanych w MySQL lub MariaDB.<br>
Implementacja algorytmu według przepisów na stronie KDPW <br>
http://www.kdpwccp.pl/pl/zarzadzanie/span/Strony/O-SPAN.aspx

Algorytm zwraca wartości identyczne z Kalkulatorem KDPW<br>
http://www.kdpwccp.pl/pl/zarzadzanie/Strony/kalkulator.aspx

Wersja do zopytmalizowania we własnym zakresie, jeśli chcesz by działało to szybciej skontaktuj się z autorem.

Jeśli chciałbyś wersję w PL/SQL, PL/pgSQL, lub w Javie skontaktuj się z autorem :)

Instalacja 
-----------

1. Należy pobrać i zainstalować bazę danych MariaDB/MySQL
https://downloads.mariadb.org/ (rekomendowana)
lub
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
1. Sciągamy plik ze strony KDPW<br>
http://www.kdpwccp.pl/pl/zarzadzanie/Parametry/SPAN/RPNJI_ZRS.xml
2. W konsoli MySQL wpisujemy komendę
``` 
MariaDB [kalkulator]> INSERT INTO b (col1) VALUES ( LOAD_FILE('RPNJE_ZRS.xml'));
``` 

3. Następnie
``` 
MariaDB [kalkulator]> call prImportuj();
``` 

######UWAGA
Proces może długo trwać(w moim przypadku 30 minut).

Użycie - Kalkulacja
-----------
1. Czyścimy tabelę zlecenia
``` 
MariaDB [kalkulator]> call prCzysc();
``` 
2. Dodajemy pozycje do kalkulatora, gdzie 1 parametr to Nazwa Intrumentu, 2 parametr to ilość pozycji, 3 parametr to cena(w przypadku kontraktów jest ignorowana)
``` 
MariaDB [kalkulator]> call prDodajZlecenie('FW20Z1420',8,4.5);
MariaDB [kalkulator]> call prDodajZlecenie('FW20U1520',-2,4.5);
``` 
3. Uruchamiamy właściwą kalkulację
``` 
MariaDB [kalkulator]> call prOblDep;
``` 
4. Odczyt parametrów
``` 
MariaDB [kalkulator]> select @depozyt,@NOD, @DPNO, @premia, @pno;
``` 

Źródła
-----------

[KDPW SPAN](http://www.kdpwccp.pl/pl/zarzadzanie/span/Documents/SPAN_depozyty_dla_kontrakt%C3%B3w_terminowych/SPAN_depozyty_dla_kontraktow_terminowych.pdf) 

Autor
-----------
[Karol Przybylski](http://www.esm-technology.pl) 
