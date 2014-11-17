/* Kalkulator depozytow SPAN
*  Written by Karol Przybylski 2014-11-10
*  Visit http://www.esm-technology.pl or http://github.com/itorK
*/
SET GLOBAL log_bin_trust_function_creators = 1;

delimiter //

DROP PROCEDURE IF EXISTS prRyzykoKlasy//
CREATE PROCEDURE prRyzykoKlasy(p_kod_klasy VARCHAR(15),out p_ryzyko DECIMAL(15,2))
begin
DECLARE v_numer,v_klas_id  INT DEFAULT 0;
DECLARE v_suma_max,v_suma DECIMAL(15,2) DEFAULT 0;
DECLARE v_done INT DEFAULT 0;

DECLARE cs_max_scen CURSOR FOR
select dep_numer,sum(c.ilosc*dep_wartosc) as suma FROM (
select sum(zlc_ilosc) as ilosc,sppa_id,sppa_klas_id 
from zlecenia,span_papiery
where sppa_id=zlc_sppa_id
group by span_papiery.sppa_id) c,depozyty_jedn
where depozyty_jedn.dep_sppa_id=c.sppa_id
and c.sppa_klas_id = v_klas_id 
group by dep_numer;
 
DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

SET p_ryzyko = 0;

SELECT klas_id INTO v_klas_id FROM klasy WHERE klas_nazwa = p_kod_klasy;
DELETE FROM span_obl_risk WHERE sobr_sob_id = (select sob_id from span_obl where sob_klas_id=v_klas_id);
OPEN cs_max_scen;
REPEAT
       FETCH cs_max_scen INTO v_numer,v_suma;
       IF NOT v_done AND v_klas_id IS NOT null THEN
	        INSERT INTO span_obl_risk (sobr_sob_id,sobr_ryzyko,sobr_scen) values ((select sob_id from span_obl where sob_klas_id=v_klas_id),v_suma,v_numer);
			IF v_suma > v_suma_max THEN
				UPDATE span_obl SET sob_ryzyko = round(v_suma,0),sob_akt_scen=v_numer WHERE sob_klas_id=v_klas_id ;
				SET v_suma_max = v_suma;
			END IF;
       END IF;
UNTIL v_done END REPEAT;
CLOSE cs_max_scen;
SET p_ryzyko = v_suma_max;
end;


DROP PROCEDURE IF EXISTS prSpreadWewn//
CREATE PROCEDURE prSpreadWewn(p_kod_klasy VARCHAR(15),out p_depozyt DECIMAL(15,4))
begin
DECLARE v_klasa VARCHAR(255);
DECLARE v_strona,v_tkwew_strona,v_ktora_strona,v_prior_typ VARCHAR(5);
DECLARE v_prior_strona VARCHAR(2) DEFAULT 'N';
DECLARE v_ilosc,v_klas_id,v_priorytet,v_nr_poziomu,v_prior_poprz_poziom INT;
DECLARE v_liczba_delt,v_depozyt,v_tkwew_dd,v_tkwew_du,v_tkwew_strona_wartosc,v_delta,v_depozyt_wew,v_prior_delta FLOAT(20,4) DEFAULT '0.0000';
DECLARE v_done INT DEFAULT 0;
DECLARE cs_du CURSOR FOR
SELECT sum(zlc_ilosc*(-1)*sppa_wsp_skal_delty*sppa_delta),sppa_nr_poziomu from zlecenia,span_papiery WHERE zlc_sppa_id=sppa_id 
and sppa_klas_id=v_klas_id AND zlc_ilosc<0
group by sppa_nr_poziomu;

DECLARE cs_dd CURSOR FOR
SELECT sum(zlc_ilosc*sppa_wsp_skal_delty*sppa_delta),sppa_nr_poziomu from zlecenia,span_papiery WHERE zlc_sppa_id=sppa_id 
and sppa_klas_id=v_klas_id AND zlc_ilosc>0
group by sppa_nr_poziomu;

DECLARE cs_spr CURSOR FOR
select spn_klas_id,spks_priorytet,spn_nr_poziomu,spn_liczba_delt,spks_depozyt,spn_strona from spready_nogi,span_klasy_spready
WHERE spn_spks_id=spks_id AND spn_klas_id=v_klas_id AND spks_typ = 'F' order by spks_priorytet asc;
 
DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

truncate table priorytety;
truncate table klasy_wewn;
SELECT klas_id INTO v_klas_id FROM klasy WHERE klas_nazwa = p_kod_klasy;
	
OPEN cs_du;
REPEAT
       FETCH cs_du INTO v_tkwew_du,v_nr_poziomu;
       IF NOT v_done THEN
		 INSERT INTO klasy_wewn (tkwew_du,tkwew_poziom) values (v_tkwew_du,v_nr_poziomu)
		 ON DUPLICATE KEY UPDATE tkwew_du = v_tkwew_du;
       END IF;
UNTIL v_done END REPEAT;
CLOSE cs_du;

SET v_done = 0;
OPEN cs_dd;
REPEAT
       FETCH cs_dd INTO v_tkwew_dd,v_nr_poziomu;
       IF NOT v_done THEN
	     INSERT INTO klasy_wewn (tkwew_dd,tkwew_poziom) values (v_tkwew_dd,v_nr_poziomu)
		 ON DUPLICATE KEY UPDATE tkwew_dd = v_tkwew_dd;
       END IF;
UNTIL v_done END REPEAT;
CLOSE cs_dd;
		 
SET p_depozyt = 0;		 
SET v_done = 0;		 
OPEN cs_spr;
REPEAT
       FETCH cs_spr INTO v_klas_id,v_priorytet,v_nr_poziomu,v_liczba_delt,v_depozyt,v_strona;
       IF NOT v_done THEN
		  SELECT tkwew_du,tkwew_dd,tkwew_strona,tkwew_strona_wartosc INTO 
		         v_tkwew_du,v_tkwew_dd,v_tkwew_strona,v_tkwew_strona_wartosc 
		  FROM klasy_wewn WHERE tkwew_poziom=v_nr_poziomu;
		  IF FOUND_ROWS() < 1 THEN
		    SET v_done = 0;
		  END IF;
		  SET v_prior_strona = 'N';
		  SET v_depozyt_wew = 0;
		  SELECT prior_strona,prior_delta,prior_typ,prior_poprz_poziom INTO v_prior_strona,v_prior_delta,v_prior_typ,v_prior_poprz_poziom FROM priorytety WHERE prior_nr = v_priorytet;
		   IF FOUND_ROWS() < 1 THEN
		     SET v_done = 0;
		   END IF;
		  IF v_prior_strona = 'N' THEN
		    IF ((v_tkwew_du/v_liczba_delt) < (v_tkwew_dd/v_liczba_delt) ) THEN

				IF v_tkwew_du > 0 THEN
					SET v_tkwew_strona_wartosc = v_tkwew_du/v_liczba_delt;
					
					SET v_prior_typ = 'U';
			    ELSE
					SET v_tkwew_strona_wartosc = v_tkwew_dd/v_liczba_delt;
					SET v_prior_typ = 'D';
				END IF;
			ELSEIF ((v_tkwew_du/v_liczba_delt) > (v_tkwew_dd/v_liczba_delt)) THEN
				IF v_tkwew_dd > 0 THEN
					SET v_tkwew_strona_wartosc = v_tkwew_dd/v_liczba_delt;
					SET v_prior_typ = 'D';
			    ELSE
					SET v_tkwew_strona_wartosc = v_tkwew_du/v_liczba_delt;
					SET v_prior_typ = 'U';
				END IF;
			END IF;
			/*IF v_tkwew_strona_wartosc < 0 THEN
			 SET v_tkwew_strona_wartosc = 0;
			 SET v_prior_typ = 'N';
			END IF;*/
			INSERT INTO priorytety(prior_strona,prior_delta,prior_typ,prior_nr,prior_poprz_poziom) values (v_strona,v_tkwew_strona_wartosc,v_prior_typ, v_priorytet,v_nr_poziomu )
			ON DUPLICATE KEY UPDATE prior_delta =v_tkwew_strona_wartosc,prior_strona=v_strona,prior_nr=v_priorytet,prior_typ=v_prior_typ,prior_poprz_poziom = v_nr_poziomu;
		  ELSEIF (v_prior_strona <> v_strona) THEN
			IF (v_prior_typ = 'D') THEN
				IF ((v_tkwew_du/v_liczba_delt) < v_prior_delta) THEN
					SET v_delta = (v_tkwew_du/v_liczba_delt);
				ELSE
					SET v_delta = v_prior_delta;
				END IF;
				UPDATE klasy_wewn SET tkwew_du = tkwew_du - v_delta WHERE tkwew_poziom=v_nr_poziomu;
				UPDATE klasy_wewn SET tkwew_dd = tkwew_dd - v_delta WHERE tkwew_poziom=v_prior_poprz_poziom;
			ELSEIF (v_prior_typ = 'U') THEN
				IF ((v_tkwew_dd/v_liczba_delt) < v_prior_delta) THEN
					SET v_delta = (v_tkwew_dd/v_liczba_delt);
				ELSE
					SET v_delta = v_prior_delta;
				END IF;
				UPDATE klasy_wewn SET tkwew_dd = tkwew_dd - v_delta WHERE tkwew_poziom=v_nr_poziomu;
				UPDATE klasy_wewn SET tkwew_du = tkwew_du - v_delta WHERE tkwew_poziom=v_prior_poprz_poziom;
			END IF;
			
			SET v_depozyt_wew = v_delta * v_depozyt;
			IF v_depozyt_wew < 0 THEN
			  SET v_depozyt_wew = 0;
			END IF;
			UPDATE priorytety SET prior_depozyt = v_depozyt_wew WHERE prior_nr = v_priorytet;
		  END IF;
		  UPDATE klasy_wewn SET tkwew_strona_wartosc = v_tkwew_strona_wartosc, 
								tkwew_strona = v_strona,
								tkwew_depozyt = v_depozyt_wew
		  WHERE tkwew_poziom=v_nr_poziomu;
		  SET p_depozyt = p_depozyt + v_depozyt_wew;
       END IF;
UNTIL v_done END REPEAT;
CLOSE cs_spr;
UPDATE span_obl SET sob_spread_intra = p_depozyt WHERE sob_klas_id=v_klas_id;
end;

//
DROP PROCEDURE IF EXISTS prSpreadZewn//
CREATE PROCEDURE prSpreadZewn(out p_depozyt DECIMAL(15,4))
begin
DECLARE v_done,v_klas_id,v_priorytet,v_nr_poziomu,v_klas_id_2,v_nr_akt2,v_sob_id,v_nr_akt,v_sob_id2,v_ret,v_prior_id,v_prior_id_2 INT DEFAULT 0;
DECLARE v_strona,v_kod_klasy_2,v_kod_klasy VARCHAR(15);
DECLARE v_liczba_delt,v_depozyt,v_du,v_dd,v_du_2,v_dd_2,v_spar_ryzyko,v_SCRV1,v_SCRV2,v_delta,v_delta_1,v_delta_max,
v_ryzyko,v_ryzyko2,v_spar_ryzyko2,v_TR1,v_TR2,v_liczba_delt_2,v_RZC1,v_RZC2,v_JRZC1,v_JRZC2,v_wylicz_depozyt,v_wylicz_depozyt_2,
v_dd_org,v_dd_2_org,p1 DECIMAL(20,4) DEFAULT 0;
DECLARE cs_spr CURSOR FOR
select k1.klas_nazwa,sp1.spn_klas_id,spks_priorytet,sp1.spn_liczba_delt,spks_depozyt,
	   k2.klas_nazwa,sp2.spn_klas_id,sp2.spn_liczba_delt
from spready_nogi sp1,span_klasy_spready,klasy k1,spready_nogi sp2, klasy k2
WHERE sp1.spn_spks_id=spks_id AND sp1.spn_klas_id=k1.klas_id AND spks_typ = 'W' 
AND sp1.spn_strona='A' AND sp2.spn_strona='B' AND sp2.spn_klas_id=k2.klas_id
AND sp2.spn_spks_id=spks_id and (k2.klas_id in (select zlc_klas_id from zlecenia) or k1.klas_id in (select zlc_klas_id from zlecenia))
order by spks_priorytet asc;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

truncate table priorytety;


UPDATE span_obl SET sob_spread_inter = 0;
SET p_depozyt = 0;
SET v_done = 0;		
	OPEN cs_spr;
	l_spready: REPEAT
		   FETCH cs_spr INTO v_kod_klasy_2,v_klas_id_2,v_priorytet,v_liczba_delt_2,v_depozyt,v_kod_klasy,v_klas_id,v_liczba_delt;
		   IF NOT v_done THEN
		   		    SET v_dd_2 = 0;
					SET v_du_2 = 0;
				    SET v_dd = 0;
					SET v_du = 0;

				call prRyzykoKlasy(v_kod_klasy_2,@p1);
				SET v_delta_max = 0;

				SELECT prior_delta,prior_depozyt INTO v_dd_2,v_wylicz_depozyt_2 FROM priorytety WHERE prior_klas_id=v_klas_id_2 AND  prior_nr < v_priorytet ORDER BY prior_nr DESC LIMIT 1;
				IF FOUND_ROWS() < 1 THEN
					
					SET v_done = 0;	
					SET v_dd_2 = 0;
					SET v_du_2 = 0;
					
					SELECT sum(zlc_ilosc*sppa_wsp_skal_delty*sppa_delta) INTO v_dd_2 from zlecenia,span_papiery WHERE zlc_sppa_id=sppa_id and sppa_klas_id=v_klas_id_2;
					SELECT prior_depozyt INTO v_wylicz_depozyt_2 FROM priorytety WHERE prior_klas_id=v_klas_id_2  AND prior_nr = v_priorytet;
					IF FOUND_ROWS() > 0 THEN
						SET v_dd_2 = 0;
						SET v_du_2 = 0;
						SET v_done = 0;	
						ITERATE l_spready;
					ELSE
					SET v_done = 0;
					INSERT IGNORE INTO priorytety (prior_nr,prior_delta,prior_depozyt,prior_klas_id,prior_klas_id_2) values
										   (v_priorytet,IFNULL(v_dd_2,0),0,v_klas_id_2,v_klas_id);
					END IF;
        			/*IF LAST_INSERT_ID() = 0 THEN
						SET v_dd_2 = 0;
						SET v_du_2 = 0;
						
						ITERATE l_spready;
					END IF;*/            
				END IF;
				call prRyzykoKlasy(v_kod_klasy,@p1);
				
				SELECT prior_delta,prior_depozyt INTO v_dd,v_wylicz_depozyt FROM priorytety WHERE prior_klas_id=v_klas_id  AND prior_nr < v_priorytet ORDER BY prior_nr DESC LIMIT 1;		
				IF FOUND_ROWS() < 1 THEN
					SET v_done = 0;	
					SELECT sum(zlc_ilosc*sppa_wsp_skal_delty*sppa_delta) INTO v_dd from zlecenia,span_papiery WHERE zlc_sppa_id=sppa_id and sppa_klas_id=v_klas_id;
					
					SELECT prior_depozyt INTO v_wylicz_depozyt FROM priorytety WHERE prior_klas_id=v_klas_id AND prior_nr = v_priorytet;
					IF FOUND_ROWS() > 0 THEN
						SET v_dd = 0;
						SET v_du = 0;
						SET v_done = 0;
						ITERATE l_spready;
					ELSE
					SET v_done = 0;
					INSERT IGNORE INTO priorytety (prior_nr,prior_delta,prior_depozyt,prior_klas_id,prior_klas_id_2) values
										   (v_priorytet,IFNULL(v_dd,0),0,v_klas_id,v_klas_id_2);
										   
					END IF;
				END IF;
				IF v_dd_2 = 0 OR v_dd = 0 OR v_dd_2 IS NULL OR v_dd IS NULL THEN
				  SET v_done = 0;	
					ITERATE l_spready;
				END IF;
				IF SIGN(v_dd_2) < 0 THEN
					SET v_du_2 = v_dd_2 * -1;
					SET v_dd_2 = 0;
				END IF; 			
				IF SIGN(v_dd) < 0 THEN
					SET v_du = v_dd * -1;
					SET v_dd = 0;
				END IF; 			
				
				IF v_dd/v_liczba_delt < v_du_2/v_liczba_delt_2 THEN
					SET v_delta = v_dd/v_liczba_delt;
				ELSE
					SET v_delta = v_du_2/v_liczba_delt_2;
				END IF;
				IF v_dd_2/v_liczba_delt_2 < v_du/v_liczba_delt THEN
					SET v_delta_1 = v_dd_2/v_liczba_delt_2;
				ELSE
					SET v_delta_1 = v_du/v_liczba_delt;
				END IF;
				/*
				IF v_dd > 0 AND v_du_2 > 0 THEN
				  IF v_dd < v_du_2 THEN
					SET v_delta = v_dd/v_liczba_delt;
				  ELSE
					SET v_delta = v_du_2/v_liczba_delt_2;
				  END IF;
				END IF;
				*/
				
				/*
				IF v_dd_2 > 0 AND v_du > 0 THEN
				  IF v_dd_2 < v_du THEN
					SET v_delta_1 = v_dd_2/v_liczba_delt_2;
				  ELSE
					SET v_delta_1 = v_du/v_liczba_delt;
				  END IF;
				END IF;
				*/
				IF v_delta > v_delta_1 THEN
				  SET v_delta_max = v_delta;
				ELSE
				  SET v_delta_max = v_delta_1;
				END IF;
				SELECT sob_akt_scen,sob_ryzyko,sob_id INTO v_nr_akt,v_ryzyko,v_sob_id FROM span_obl where sob_klas_id=v_klas_id;
				IF (v_nr_akt >= 15) THEN
				  select sobr_ryzyko INTO v_spar_ryzyko FROM span_obl_risk WHERE sobr_sob_id = v_sob_id and sobr_scen = v_nr_akt;
				  SET v_SCRV1 = (v_ryzyko+v_spar_ryzyko)/2;
				ELSEIF  v_nr_akt % 2 THEN
				  select sobr_ryzyko INTO v_spar_ryzyko FROM span_obl_risk WHERE sobr_sob_id = v_sob_id and sobr_scen = v_nr_akt+1;
				  SET v_SCRV1 = (v_ryzyko+v_spar_ryzyko)/2;
				ELSE
				  select sobr_ryzyko INTO v_spar_ryzyko FROM span_obl_risk WHERE sobr_sob_id = v_sob_id and sobr_scen = v_nr_akt-1;
				  SET v_SCRV1 = (v_ryzyko+v_spar_ryzyko)/2;
				END IF;
				SELECT sob_akt_scen,sob_ryzyko,sob_id  INTO v_nr_akt2,v_ryzyko2,v_sob_id2 FROM span_obl where sob_klas_id=v_klas_id_2;
				IF (v_nr_akt2 >= 15) THEN
				  select sobr_ryzyko INTO v_spar_ryzyko2 FROM span_obl_risk WHERE sobr_sob_id = v_sob_id2 and sobr_scen = v_nr_akt2;
				  SET v_SCRV2 = (v_ryzyko2+v_spar_ryzyko2)/2;
				ELSEIF  v_nr_akt2 % 2 THEN
				  select sobr_ryzyko INTO v_spar_ryzyko2 FROM span_obl_risk WHERE sobr_sob_id = v_sob_id2 and sobr_scen = v_nr_akt2+1;
				  SET v_SCRV2 = (v_ryzyko2+v_spar_ryzyko2)/2;
				ELSE
				  select sobr_ryzyko INTO v_spar_ryzyko2 FROM span_obl_risk WHERE sobr_sob_id = v_sob_id2 and sobr_scen = v_nr_akt2-1;
				  SET v_SCRV2 = (v_ryzyko2+v_spar_ryzyko2)/2;
				END IF;

				select sum(sobr_ryzyko)/2 INTO v_TR1 FROM span_obl_risk WHERE sobr_sob_id = v_sob_id and sobr_scen in (1,2);
				select sum(sobr_ryzyko)/2 INTO v_TR2 FROM span_obl_risk WHERE sobr_sob_id = v_sob_id2 and sobr_scen in (1,2);

				SET v_RZC1 = v_SCRV1 - v_TR1;
				SET v_RZC2 = v_SCRV2 - v_TR2;
		       SELECT sum(zlc_ilosc*sppa_wsp_skal_delty*sppa_delta) INTO v_dd_2_org from zlecenia,span_papiery WHERE zlc_sppa_id=sppa_id and sppa_klas_id=v_klas_id_2;
					
				SELECT sum(zlc_ilosc*sppa_wsp_skal_delty*sppa_delta) INTO v_dd_org from zlecenia,span_papiery WHERE zlc_sppa_id=sppa_id and sppa_klas_id=v_klas_id;
				SET v_JRZC1 = v_RZC1/ABS(v_dd_org);
				SET v_JRZC2 = v_RZC2/ABS(v_dd_2_org);

				IF v_JRZC1 > 0 AND v_JRZC2 > 0 THEN
				    SET v_delta_max = v_delta_max ;
					SET p_depozyt = p_depozyt + Round(IFNULL(v_JRZC1*v_delta_max* v_depozyt *v_liczba_delt,0),4) +Round(IFNULL(v_JRZC2*v_delta_max* v_depozyt *v_liczba_delt_2,0),4);
					/*
					SELECT * from priorytety;
					SELECT CONCAT(v_RZC1,':',v_RZC2,':',v_TR2,':',v_JRZC1,':',v_JRZC2,':',v_delta_max,':',v_depozyt,':',v_klas_id_2,':',v_klas_id,':',v_liczba_delt,':',v_liczba_delt_2,':',v_dd_org,':',v_dd_2_org,':',v_priorytet);
					*/
					
					UPDATE priorytety SET prior_delta =prior_delta - IFNULL((CASE SIGN(prior_delta) WHEN -1 THEN -1*v_delta_max*v_liczba_delt ELSE v_delta_max*v_liczba_delt END),0),
										  prior_depozyt = prior_depozyt + Round(IFNULL(v_JRZC1*v_delta_max* v_depozyt *v_liczba_delt,0),4) WHERE prior_klas_id = v_klas_id /*AND prior_klas_id_2 = v_klas_id_2*/;

					UPDATE priorytety SET prior_delta =prior_delta - IFNULL((CASE SIGN(prior_delta) WHEN -1 THEN -1*v_delta_max*v_liczba_delt_2 ELSE v_delta_max*v_liczba_delt_2 END),0),
										  prior_depozyt= prior_depozyt + Round(IFNULL(v_JRZC2*v_delta_max*v_liczba_delt_2*v_depozyt,0),4) WHERE prior_klas_id = v_klas_id_2 /*AND prior_klas_id_2 = v_klas_id*/;
				END IF;

		   END IF;
	UNTIL v_done END REPEAT l_spready;
	CLOSE cs_spr;
	
UPDATE span_obl SET sob_spread_inter = (select Round(sum(prior_depozyt),0) from priorytety where prior_klas_id=sob_klas_id);
end;

DROP PROCEDURE IF EXISTS prPNO//
CREATE PROCEDURE prPNO()
begin
DECLARE v_klas_id,v_done INT DEFAULT 0;
DECLARE v_pno DECIMAL (15,2) DEFAULT 0;
DECLARE cs_klas CURSOR FOR
select sppa_klas_id,IFNULL(sum(round(zlc_ilosc*sppa_kurs_wyk*sppa_mnoznik,2)),0) from zlecenia,span_papiery where sppa_id=zlc_sppa_id and sppa_typ_papieru='EQTY' group by sppa_klas_id;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
SET @suma_pno = 0;
SET v_done = 0;		
OPEN cs_klas;
REPEAT
	   FETCH cs_klas INTO v_klas_id,v_pno;
	   IF NOT v_done THEN
		 UPDATE span_obl SET sob_pno = v_pno WHERE sob_klas_id=v_klas_id;
		 SET @suma_pno = @suma_pno + v_pno;
	   END IF;
UNTIL v_done END REPEAT;
CLOSE cs_klas;
end;
//
DROP PROCEDURE IF EXISTS prPO//
CREATE PROCEDURE prPO()
begin
DECLARE v_klas_id,v_done INT DEFAULT 0;
DECLARE v_po DECIMAL (15,2) DEFAULT 0;
DECLARE cs_klas CURSOR FOR
select sppa_klas_id,IFNULL(sum(round(zlc_ilosc*zlc_cena_jedn*sppa_mnoznik,2)),0) from zlecenia,span_papiery where sppa_id=zlc_sppa_id and sppa_typ_papieru='EQTY' group by sppa_klas_id;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
SET @suma_po = 0;
SET v_done = 0;		
OPEN cs_klas;
REPEAT
	   FETCH cs_klas INTO v_klas_id,v_po;
	   IF NOT v_done THEN
		 UPDATE span_obl SET sob_po = v_po WHERE sob_klas_id=v_klas_id;
		 SET @suma_po = @suma_pno + v_po;
	   END IF;
UNTIL v_done END REPEAT;
CLOSE cs_klas;
end;
//

DROP PROCEDURE IF EXISTS prMDKO//
CREATE PROCEDURE prMDKO()
begin
DECLARE v_klas_id,v_done INT DEFAULT 0;
DECLARE v_mdko DECIMAL (15,2) DEFAULT 0;
DECLARE cs_klas CURSOR FOR
select sppa_klas_id,IFNULL(sum(zlc_ilosc*klas_som),0) from zlecenia,span_papiery,klasy where sppa_id=zlc_sppa_id and sppa_typ_papieru='EQTY' and sppa_klas_id = klas_id and zlc_ilosc < 0 group by sppa_klas_id;

DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
SET @suma_mdko = 0;
SET v_done = 0;		
OPEN cs_klas;
REPEAT
	   FETCH cs_klas INTO v_klas_id,v_mdko;
	   IF NOT v_done THEN
		 UPDATE span_obl SET sob_mdko = v_mdko WHERE sob_klas_id=v_klas_id;
		 SET @suma_mdko = @suma_mdko + v_mdko;
	   END IF;
UNTIL v_done END REPEAT;
CLOSE cs_klas;
end;
//

DROP PROCEDURE IF EXISTS prTest//
CREATE PROCEDURE prTest()
begin
DECLARE v_ret INT;
DECLARE v_wynik DECIMAL(15,4) DEFAULT 0;
START TRANSACTION;
/* pierwszy test Zewn */
DELETE FROM span_obl_risk;
truncate table zlecenia;
call prDodajZlecenie('FW20Z1420',5,4.5);
call prDodajZlecenie('FW40H15',-8,4.5);
call prDodajZlecenie('F3MWZ14',-4,4.5);
call prDodajZlecenie('F6MWZ14',6,4.5);
call prDodajZlecenie('F1MWZ14',-10,4.5);
call prOblDep;
select @depozyt,':33094';
/* drugi test Zewn*/
DELETE FROM span_obl_risk;
truncate table zlecenia;
call prDodajZlecenie('FW20Z1420',5,4.5);
call prDodajZlecenie('FW40H15',-8,4.5);
call prDodajZlecenie('F3MWZ14',-4,4.5);
call prDodajZlecenie('F6MWZ14',6,4.5);
call prOblDep;
select @depozyt,':28864';

/*3 test test opcji zewn*/
DELETE FROM span_obl_risk;
truncate table zlecenia;
call prDodajZlecenie('OW20L142600',10,4.5);
call prDodajZlecenie('OW20X142650',-6,4.5);
call prDodajZlecenie('FW40H15',-8,4.5);
call prOblDep;
select @depozyt,':20707.30';

/*3 test kontrakty spr wewn*/
DELETE FROM span_obl_risk;
truncate table zlecenia;
call prDodajZlecenie('F3MWH15',8,4.5);
call prDodajZlecenie('F3MWJ15',-2,4.5);
call prDodajZlecenie('F3MWN15',-3,4.5);
call prDodajZlecenie('F3MWU15',6,4.5);
call prDodajZlecenie('F3MWM16',6,4.5);
call prDodajZlecenie('F3MWH16',-5,4.5);
call prOblDep;
select @depozyt,':17909';
/*3 test kontrakty spr wewn W20*/
DELETE FROM span_obl_risk;
truncate table zlecenia;
call prDodajZlecenie('FW20Z1420',8,4.5);
call prDodajZlecenie('FW20U1520',-2,4.5);
call prOblDep;
select @depozyt,':24860';
/*3 test opcje spr wewn W20*/
DELETE FROM span_obl_risk;
truncate table zlecenia;
call prDodajZlecenie('OW20O152900',-2,4.5);
call prDodajZlecenie('OW20F152000',8,4.5);
call prOblDep;
select @depozyt,':-10971.82';
commit;
end;
//

DROP PROCEDURE IF EXISTS prDodajZlecenie//
CREATE PROCEDURE prDodajZlecenie(p_symbol VARCHAR(100),p_ilosc INT, p_cena_jedn DECIMAL(15,4))
begin
INSERT INTO zlecenia (zlc_klas_id,zlc_ilosc,zlc_cena_jedn,zlc_typ_operacji,zlc_sppa_id)
(select sppa_klas_id,p_ilosc,p_cena_jedn,(CASE SIGN(p_ilosc) WHEN -1 THEN 'S' ELSE 'K' END),sppa_id 
from span_papiery WHERE sppa_nazwa=p_symbol);
end;
//

DROP FUNCTION IF EXISTS fnMax//
CREATE FUNCTION fnMax(p_1 DECIMAL(15,4),p_2 DECIMAL(15,4))
RETURNS DECIMAL(15,4)
begin
IF (p_1 > p_2) THEN
	RETURN p_1;
ELSE
	RETURN p_2;
END IF;
end;
//
DROP PROCEDURE IF EXISTS prOblDep//
CREATE PROCEDURE prOblDep()
begin
DECLARE v_ret,v_done INT DEFAULT 0;
DECLARE v_kod_klasy VARCHAR(10);
DECLARE cs_klasy CURSOR FOR
SELECT distinct klas_nazwa from zlecenia,span_papiery,klasy WHERE zlc_sppa_id=sppa_id 
and sppa_klas_id=klas_id;


DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
DROP TABLE IF EXISTS span_obl;
CREATE TEMPORARY TABLE span_obl (
  sob_id INT NOT NULL AUTO_INCREMENT,
  sob_klas_id INT,
  sob_ryzyko DECIMAL(15,4) DEFAULT 0,
  sob_akt_scen INT,
  sob_spread_inter  DECIMAL(15,4) DEFAULT 0,
  sob_spread_intra  DECIMAL(15,4) DEFAULT 0,
  sob_pno  DECIMAL(15,2) DEFAULT 0,
  sob_po  DECIMAL(15,2) DEFAULT 0,
  sob_mdko  DECIMAL(15,2) DEFAULT 0,
  PRIMARY KEY (`sob_id`)
);

CREATE INDEX idx_sob_klas ON span_obl (sob_klas_id);

DROP TABLE IF EXISTS klasy_wewn;
CREATE TEMPORARY TABLE klasy_wewn(
  tkwew_id INT(6) NOT NULL AUTO_INCREMENT,
  tkwew_poziom INT,
  tkwew_du FLOAT(20,4) NOT NULL DEFAULT '0.0000',
  tkwew_dd FLOAT(20,4) NOT NULL DEFAULT '0.0000',
  tkwew_strona VARCHAR(2) DEFAULT 'N',
  tkwew_strona_wartosc FLOAT(20,4) NOT NULL DEFAULT '0.0000',
  tkwew_depozyt FLOAT(20,4) NOT NULL DEFAULT '0.0000',
  PRIMARY KEY (`tkwew_id`)
);
CREATE UNIQUE INDEX idx_klasy_wewn ON klasy_wewn (tkwew_poziom);

DROP TABLE IF EXISTS priorytety;
CREATE TEMPORARY TABLE priorytety (
  prior_id INT(6) NOT NULL AUTO_INCREMENT,
  prior_nr INT,
  prior_strona VARCHAR(2) DEFAULT 'N',
  prior_delta FLOAT(20,4) NOT NULL DEFAULT '0.0000',
  prior_klas_id INT,
  prior_depozyt FLOAT(20,4) NOT NULL DEFAULT '0.0000',
  prior_klas_id_2 INT,
  prior_typ VARCHAR(2) DEFAULT 'N',
  prior_poprz_poziom INT,
  PRIMARY KEY (`prior_id`)
);

CREATE UNIQUE INDEX idx_priorytet ON priorytety (prior_nr,prior_klas_id,prior_klas_id_2);
/*CREATE UNIQUE INDEX idx_priorytet ON priorytety (prior_nr);*/

START TRANSACTION;
INSERT INTO span_obl (sob_klas_id) select klas_id from klasy;
SET @suma_ryzyk = 0;
SET @suma_swew = 0;
DELETE FROM span_obl_risk;
update span_obl set sob_ryzyko=0,sob_spread_inter=0,sob_spread_intra=0,sob_pno=0,sob_po=0,sob_mdko=0;
OPEN cs_klasy;
	 REPEAT
		   FETCH cs_klasy INTO v_kod_klasy;
		   IF NOT v_done THEN
			call  prSpreadWewn(v_kod_klasy,@swew);
			call prRyzykoKlasy(v_kod_klasy,@p1);
			SET @suma_ryzyk = @suma_ryzyk + @p1;
			SET @suma_swew = @suma_swew + @swew;
		   END IF;
	UNTIL v_done END REPEAT;

CLOSE cs_klasy;
call  prSpreadZewn(@szew);
call  prPNO();
call  prPO();
call  prMDKO();
select fnMax(fnMax(round(@suma_ryzyk) + @swew,@suma_mdko) - @szew - @suma_pno,0),@suma_po*-1 INTO @DPNO,@premia;
select fnMax(@suma_pno - fnMax(round(@suma_ryzyk) + @swew,@suma_mdko) - @szew,0) INTO @NOD;
SELECT @DPNO - @NOD INTO @depozyt;
commit;
end;
//

delimiter ;