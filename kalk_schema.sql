/* Scehmat Bazy danych dla kalkulatora SPAN
*  Written by Karol Przybylski 2014-11-10
*  Visit http://www.esm-technology.pl or http://github.com/itorK
*/
CREATE DATABASE kalkulator;

USE kalkulator;

CREATE TABLE b (
  col1 longblob NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 CREATE TABLE span_klasy_spready
    ( spks_id   INT NOT NULL AUTO_INCREMENT
    , spks_typ  VARCHAR(2)
    , spks_priorytet    INT DEFAULT 0
    , spks_depozyt     DECIMAL(16,6) DEFAULT 0
    , spks_wsp_korekty DECIMAL(3,2) DEFAULT 0,
	PRIMARY KEY (`spks_id`)
    ) ;   

CREATE TABLE spready_nogi
    ( spn_id   INT  NOT NULL AUTO_INCREMENT
   , spn_spks_id   INT
    , spn_klas_id    INT
    , spn_nr_poziomu INT DEFAULT 0
    , spn_strona    VARCHAR(2)
    , spn_liczba_delt DECIMAL(15,6),
	PRIMARY KEY (`spn_id`)
    ) ;
	
	CREATE INDEX idx_spn_spks ON spready_nogi (spn_spks_id);
	CREATE INDEX idx_spn_klas ON spready_nogi (spn_klas_id);
	
CREATE TABLE klasy (
  klas_id INT NOT NULL AUTO_INCREMENT,
  klas_nazwa varchar(255),
  klas_nazwa_span varchar(255),
  klas_som decimal(15,2) DEFAULT 0,
  PRIMARY KEY (`klas_id`)
);

CREATE UNIQUE INDEX idx_klas ON klasy (klas_nazwa);

 CREATE TABLE span_papiery
 ( 
   sppa_id INT(6) NOT NULL AUTO_INCREMENT,
   sppa_klas_id  INT(6),
   sppa_nazwa VARCHAR(255),
    sppa_data_wygas VARCHAR(10),
    sppa_kurs_wyk decimal(19,4),
    sppa_rodzaj_opcji VARCHAR(3),
    sppa_typ_papieru VARCHAR(5),
    sppa_czas_do_wygas decimal(19,6),
    sppa_psr decimal(19,6),
    sppa_vsr decimal(19,6),
    sppa_mnoznik decimal(19,7),
    sppa_delta decimal(10,6),
    sppa_model_wyc varchar(2),
    sppa_stopa_proc decimal(19,6),
    sppa_zmien_op decimal(19,6),
    sppa_zmien_op_wygasl decimal(19,6),
    sppa_wsp_skal_delty decimal(19,6),
    sppa_stopa_dyw decimal(19,6),
    sppa_nr_poziomu INT(6),
    sppa_Intraday varchar(2),
    sppa_PSR_intra decimal(19,6),
    sppa_godz_waz_intr TIMESTAMP,
    sppa_godz_zamk_intr TIMESTAMP,
    sppa_cena_instr_baz  decimal(19,6),
    sppa_cena_instr decimal(19,6),
	PRIMARY KEY (`sppa_id`)
 );
 
CREATE INDEX idx_sppa_klas ON span_papiery (sppa_klas_id);
  
   CREATE TABLE depozyty_jedn
 ( dep_id INT(6)  NOT NULL AUTO_INCREMENT,
   dep_sppa_id INT,
   dep_numer INT,
   dep_wartosc decimal(10,2),
   PRIMARY KEY (`dep_id`)
  );
  CREATE INDEX idx_depozyty_jedn ON depozyty_jedn (dep_sppa_id);
  
  CREATE TABLE zlecenia (
  zlc_id INT(6) NOT NULL AUTO_INCREMENT,
  zlc_klas_id INT(6),
  zlc_ilosc INT(6),
  zlc_cena_jedn decimal(19,6),
  zlc_typ_operacji varchar(2),
  zlc_sppa_id int(6),
  PRIMARY KEY (`zlc_id`)
);

CREATE INDEX idx_zlc_klas ON zlecenia (zlc_klas_id);
CREATE INDEX idx_zlc_sppa ON zlecenia (zlc_sppa_id);

  CREATE TABLE rach_pw (
  rpw_id INT(6) NOT NULL AUTO_INCREMENT,
  rpw_sppa_id INT(6),
  rpw_ilosc_ma INT(6),
  rpw_ilosc_wn INT(6),
  rpw_suma INT(6),
  PRIMARY KEY (`rpw_id`)
);

  CREATE TABLE papier_korekta
 ( pko_id INT(6)  NOT NULL AUTO_INCREMENT,
   pko_sppa_id INT,
   pko_kor_psr decimal(10,2),
   PRIMARY KEY (`pko_id`)
  );
CREATE UNIQUE INDEX idx_pap_kor ON papier_korekta (pko_sppa_id);