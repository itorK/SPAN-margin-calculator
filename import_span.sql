/* Import pliku KDPW do Kalkulatora SPAN
*  Written by Karol Przybylski 2014-11-10
*  Visit http://www.esm-technology.pl or http://github.com/itorK
*/

SET GLOBAL log_bin_trust_function_creators = 1;

delimiter //
DROP FUNCTION IF EXISTS fnImportujSpready//
CREATE FUNCTION fnImportujSpready()
RETURNS int
begin
DECLARE v_ilosc_klas,v_ilosc_nog,v_ilosc_spreadow,v_i,v_s,v_n,v_poziom,v_priorytet,v_ilosc_zewn,v_id,v_klas_id,v_ilosc_poziomow,v_poziom_klasy,v_lpz INT DEFAULT 0;
DECLARE v_klasa,v_strona,v_som,v_pocz,v_kon  VARCHAR(255);
DECLARE v_typ_spreadu VARCHAR(5);
DECLARE v_delta FLOAT(20,6) DEFAULT 0;
DECLARE v_depozyt DECIMAL(15,4) DEFAULT 0;

select  extractvalue(col1,'count(//ccDef)') INTO v_ilosc_klas FROM b; 

  l_klasy: LOOP
    SET v_i = v_i + 1;
    IF v_i <= v_ilosc_klas THEN
	  SET v_ilosc_spreadow = 0;
	  SET v_s = 0;
	  select  extractvalue(col1,CONCAT('count(//ccDef[',v_i,']//dSpread)')),
	          extractvalue(col1,CONCAT('//ccDef[',v_i,']//somTiers//tier//rate//val')),
			  extractvalue(col1,CONCAT('count(//ccDef[',v_i,']//intraTiers//tier)'))
	   INTO v_ilosc_spreadow,v_som,v_ilosc_poziomow FROM b; 
	  l_spready: LOOP
	    SET v_s = v_s + 1;
		IF v_s <= v_ilosc_spreadow THEN
		  SET v_n = 0;
		  SET v_ilosc_nog = 0;
		  select  extractvalue(col1,CONCAT('count(//ccDef[',v_i,']//dSpread[',v_s,']//tLeg//cc)')),
				  extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//chargeMeth')),
				  extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//spread')),
				  extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//rate//val'))					  
				  INTO v_ilosc_nog,v_typ_spreadu,v_priorytet,v_depozyt FROM b; 
				  INSERT INTO span_klasy_spready (spks_typ,spks_priorytet,spks_depozyt) values (v_typ_spreadu,v_priorytet,v_depozyt);
				  SET v_id = LAST_INSERT_ID();
		  l_nogi: LOOP
			  SET v_n = v_n + 1;
			  SET v_lpz = 0;
			  IF v_n <= v_ilosc_nog THEN
			    select  extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//tLeg[',v_n,']//cc')),
						extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//tLeg[',v_n,']//tn')),
					    extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//tLeg[',v_n,']//rs')),
						extractvalue(col1,CONCAT('//ccDef[',v_i,']//dSpread[',v_s,']//tLeg[',v_n,']//i'))
						INTO v_klasa,v_poziom,v_strona,v_delta FROM b;
						IF LENGTH(v_som) > 0 AND v_som is not null THEN
							update klasy set klas_som = cast(v_som as decimal(15,4)) WHERE klas_nazwa = v_klasa;
						END IF;
						SELECT klas_id INTO v_klas_id FROM klasy WHERE klas_nazwa = v_klasa;
						INSERT INTO spready_nogi(spn_spks_id , spn_klas_id, spn_nr_poziomu,spn_strona,spn_liczba_delt)
						VALUES (v_id,v_klas_id, v_poziom, v_strona,v_delta);
						l_poziomy: LOOP
						SET v_lpz = v_lpz + 1;
						IF v_lpz <= v_ilosc_poziomow THEN
						  select  extractvalue(col1,CONCAT('//ccDef[',v_i,']//intraTiers//tier[',v_lpz,']//sPe')),
								  extractvalue(col1,CONCAT('//ccDef[',v_i,']//intraTiers//tier[',v_lpz,']//ePe')),
								  extractvalue(col1,CONCAT('//ccDef[',v_i,']//intraTiers//tier[',v_lpz,']//tn'))
						   INTO v_pocz,v_kon,v_poziom_klasy FROM b; 
						   UPDATE span_papiery SET sppa_nr_poziomu=v_poziom_klasy WHERE STR_TO_DATE(sppa_data_wygas,'%Y%m') BETWEEN
						   STR_TO_DATE(v_pocz,'%Y%m') AND STR_TO_DATE(v_kon,'%Y%m') AND sppa_klas_id=v_klas_id;
						   ITERATE l_poziomy;
						ELSE
						  LEAVE l_poziomy;
						END IF;
					  END LOOP l_poziomy;
			    ITERATE l_nogi;
			  ELSE
				LEAVE l_nogi;
			  END IF;
		  END LOOP l_nogi;
		   ITERATE l_spready;
		ELSE
		  LEAVE l_spready;
		END IF;
	  END LOOP l_spready;
      ITERATE l_klasy;
    ELSE
	  LEAVE l_klasy;
	END IF;

  END LOOP l_klasy;
 
  select  extractvalue(col1,'count(//interSpreads//dSpread)') INTO v_ilosc_zewn FROM b; 
  SET v_i = 0;
  l_zewn: LOOP
    SET v_i = v_i + 1;
	SET v_ilosc_nog = 0;
	IF v_i <= v_ilosc_zewn THEN
		SELECT
		extractvalue(col1,CONCAT('count(//interSpreads//dSpread[',v_i,']//tLeg//cc)')),
		extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//chargeMeth')),
		extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//spread')),
		extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//rate//val'))
		INTO v_ilosc_nog,v_typ_spreadu,v_priorytet,v_depozyt
		FROM b;
	    INSERT INTO span_klasy_spready (spks_typ,spks_priorytet,spks_depozyt) values (v_typ_spreadu,v_priorytet,v_depozyt);
		SET v_id = LAST_INSERT_ID();
		SET v_n = 0;
				l_nogi_zew: LOOP
				  SET v_n = v_n + 1;
				  IF v_n <= v_ilosc_nog THEN
				    SET v_klasa = NULL;
					SET v_klas_id = 0;
					select  extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//tLeg[',v_n,']//cc')),
							extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//tLeg[',v_n,']//tn')),
							extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//tLeg[',v_n,']//rs')),
							extractvalue(col1,CONCAT('//interSpreads//dSpread[',v_i,']//tLeg[',v_n,']//i'))
							INTO v_klasa,v_poziom,v_strona,v_delta FROM b;

						SELECT klas_id INTO v_klas_id FROM klasy WHERE klas_nazwa = v_klasa;
						IF v_klas_id = 0 THEN
							INSERT INTO klasy (klas_nazwa) values (v_klasa);
						END IF;
						INSERT INTO spready_nogi(spn_spks_id , spn_klas_id, spn_nr_poziomu,spn_strona,spn_liczba_delt)
						VALUES (v_id,v_klas_id, v_poziom, v_strona,v_delta);
					ITERATE l_nogi_zew;
				  ELSE
					LEAVE l_nogi_zew;
				  END IF;
				END LOOP l_nogi_zew;

      ITERATE l_zewn;
    ELSE
	  LEAVE l_zewn;
	END IF;
  END LOOP l_zewn;

	 RETURN 1;
end;

//

DROP FUNCTION IF EXISTS fnImportujKontrakty//
CREATE FUNCTION fnImportujKontrakty()
RETURNS int
begin
DECLARE v_ilosc_walorow,v_i,v_s,v_n,v_serie,v_poziom,v_klas_id,v_spid,v_ilosc_scen INT DEFAULT 0;
DECLARE v_kod_span,v_kod_klasy,v_waluta,v_data_wyg,v_sppa_nazwa,v_end,v_kod_klasy_tmp VARCHAR(255);
DECLARE v_mnoznik,v_kurs_instr,v_cena_instr,v_scen DECIMAL(15,4) DEFAULT 0;
DECLARE v_wsp_skal,v_stopa_proc,v_psr,v_ryz_zmien,v_ryz_ceny,v_czas_do_wygas,v_delta_ref FLOAT(20,4) DEFAULT 0;
DECLARE v_typ_papieru VARCHAR(5);


select extractvalue(col1,'count(//futPf)') INTO v_ilosc_walorow FROM b; 

  l_walory: LOOP
    SET v_i = v_i + 1;
    IF v_i <= v_ilosc_walorow THEN
	  SET v_s = 0;
	  
		  select extractvalue(col1,CONCAT('count(//futPf[',v_i,']/fut)')),
	          extractvalue(col1,CONCAT('//futPf[',v_i,']/undPf/pfCode')),
			  extractvalue(col1,CONCAT('//futPf[',v_i,']/pfCode')),
			  extractvalue(col1,CONCAT('//futPf[',v_i,']/currency')),
			  extractvalue(col1,CONCAT('//futPf[',v_i,']/cvf')),
			  extractvalue(col1,CONCAT('//futPf[',v_i,']/valueMeth'))
	   INTO v_serie,v_kod_span,v_kod_klasy,v_waluta,v_mnoznik, v_typ_papieru FROM b;  
		   INSERT IGNORE INTO klasy (klas_nazwa,klas_nazwa_span) values (v_kod_span,v_kod_klasy);
		   SELECT klas_id INTO v_klas_id FROM klasy WHERE klas_nazwa = v_kod_span;
	   		l_serie: LOOP
			  SET v_n = 0;
			  SET v_s = v_s + 1;
			  IF v_s <= v_serie THEN
				select extractvalue(col1,CONCAT('count(//futPf[',v_i,']//fut[',v_s,']//ra//a)')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/pe')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/p')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/val')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/sc')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/intrRate/val')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/scanRate/priceScanPct')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/scanRate/volScan')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/scanRate/priceScan')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/t')),
				extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']/ra/d')),
				99
				INTO v_ilosc_scen,v_data_wyg,v_kurs_instr,v_cena_instr,v_wsp_skal,v_stopa_proc,v_psr,v_ryz_zmien,v_ryz_ceny,
				v_czas_do_wygas,v_delta_ref,v_poziom
				FROM b;
				    SET v_end = '';
					SET v_kod_klasy_tmp = v_kod_klasy;
					IF substr(v_kod_klasy,4,2) = '0B' THEN
						SET v_kod_klasy_tmp = substr(v_kod_klasy,1,length(v_kod_klasy)-1);
						SET v_end = '20';
					END IF;
					SET v_sppa_nazwa = CONCAT( v_kod_klasy_tmp,(CASE substr(v_data_wyg,5,2)
					  WHEN '01' THEN 'F'
					  WHEN '02' THEN 'G'
					  WHEN '03' THEN 'H'
					  WHEN '04' THEN 'J'
					  WHEN '05' THEN 'K'
					  WHEN '06' THEN 'M'
					  WHEN '07' THEN 'N'
					  WHEN '08' THEN 'Q'
					  WHEN '09' THEN 'U'
					  WHEN '10' THEN 'V'
					  WHEN '11' THEN 'X'
					  WHEN '12' THEN 'Z'
				    END),substr(v_data_wyg,3,2),v_end);
	  
				INSERT INTO span_papiery (sppa_nazwa,sppa_klas_id,sppa_data_wygas,sppa_kurs_wyk,sppa_typ_papieru,sppa_czas_do_wygas,sppa_psr,
				sppa_vsr,sppa_mnoznik,sppa_delta,sppa_stopa_proc,sppa_wsp_skal_delty,sppa_nr_poziomu,sppa_cena_instr) 
				values (v_sppa_nazwa,v_klas_id,v_data_wyg,v_kurs_instr,v_typ_papieru,v_czas_do_wygas,v_psr,v_ryz_zmien,v_mnoznik,v_delta_ref,
				v_stopa_proc, v_wsp_skal,v_poziom,v_cena_instr);
				SET v_spid = LAST_INSERT_ID();
			    l_scen: LOOP
				  SET v_n = v_n + 1;
				  IF v_n <= v_ilosc_scen THEN
					select  extractvalue(col1,CONCAT('//futPf[',v_i,']//fut[',v_s,']//ra//a[',v_n,']'))
							INTO v_scen FROM b;
					INSERT INTO depozyty_jedn (dep_wartosc,dep_numer,dep_sppa_id) values (v_scen,v_n,v_spid);
					ITERATE l_scen;
				  ELSE
					LEAVE l_scen;
				  END IF;
				END LOOP l_scen;
				ITERATE l_serie;
			  ELSE
				LEAVE l_serie;
			  END IF;
			END LOOP l_serie;  
      ITERATE l_walory;
    ELSE
	  LEAVE l_walory;
	END IF;
  END LOOP l_walory;	   
RETURN 1;
end;

//

DROP FUNCTION IF EXISTS fnImportujOpcje//
CREATE FUNCTION fnImportujOpcje()
RETURNS int
begin
DECLARE v_ilosc_walorow,v_i,v_s,v_n,v_serie,v_sc,v_poziom,v_klas_id,v_spid,v_ilosc_scen,v_ilosc_opcji, v_pId, v_cId INT DEFAULT 0;
DECLARE v_kod_span,v_kod_klasy,v_waluta,v_data_wyg,v_sppa_nazwa,v_end,v_kod_klasy_tmp,v_rodzaj_opcji VARCHAR(255);
DECLARE v_mnoznik,v_kurs_instr,v_cena_instr,v_scen,v_kurs_wyk,v_stopa_dyw,v_cena_baz DECIMAL(15,6) DEFAULT 0;
DECLARE v_wsp_skal,v_stopa_proc,v_psr,v_ryz_zmien,v_ryz_ceny,v_czas_do_wygas,v_delta_ref FLOAT(20,6) DEFAULT 0;
DECLARE v_zmien_op,v_zmien_op_wygasl FLOAT(20,6);
DECLARE v_typ_papieru, v_baz_nazwa VARCHAR(5);


select extractvalue(col1,'count(//oopPf)') INTO v_ilosc_walorow FROM b; 

  l_walory: LOOP
    SET v_i = v_i + 1;
    IF v_i <= v_ilosc_walorow THEN
	  SET v_s = 0;
		  select extractvalue(col1,CONCAT('count(//oopPf[',v_i,']/series)')),
	          extractvalue(col1,CONCAT('//oopPf[',v_i,']/undPf/pfCode')),
			  extractvalue(col1,CONCAT('//oopPf[',v_i,']/pfCode')),
			  extractvalue(col1,CONCAT('//oopPf[',v_i,']/currency')),
			  extractvalue(col1,CONCAT('//oopPf[',v_i,']/cvf')),
			  extractvalue(col1,CONCAT('//oopPf[',v_i,']/valueMeth'))
	   INTO v_serie,v_kod_span,v_kod_klasy,v_waluta,v_mnoznik, v_typ_papieru FROM b;  
		   INSERT IGNORE INTO klasy (klas_nazwa,klas_nazwa_span) values (v_kod_span,v_kod_klasy);
		   SELECT klas_id INTO v_klas_id FROM klasy WHERE klas_nazwa = v_kod_span;
	   		l_serie: LOOP
			  SET v_n = 0;
			  SET v_s = v_s + 1;
			  IF v_s <= v_serie THEN
				select extractvalue(col1,CONCAT('count(//oopPf[',v_i,']//series[',v_s,']//opt)')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/pe')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/sc')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/intrRate/val')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/scanRate/priceScanPct')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/scanRate/volScan')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/scanRate/priceScan')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/t')) - 0.01,
				99,
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/v')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/divRate/val')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/undC/exch')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/undC/pfId')),
				extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']/undC/cId'))
				INTO v_ilosc_opcji,v_data_wyg,v_wsp_skal,v_stopa_proc,v_psr,v_ryz_zmien,v_ryz_ceny,
				v_czas_do_wygas,v_poziom, v_zmien_op_wygasl, v_stopa_dyw, v_baz_nazwa, v_pId, v_cId
				FROM b;
				l_opt: LOOP
				  SET v_sc = 0;
				  SET v_n = v_n + 1;
				  IF v_n <= v_ilosc_opcji THEN
				  select extractvalue(col1,CONCAT('count(//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']//ra//a)')),
				         extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']/k')),
					     extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']/o')),
						 extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']/p')),
						 extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']/val')),
						 extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']/v')),
						 extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']//ra//d')),
						 extractvalue(col1,CONCAT('//exchange[exch/text()="',v_baz_nazwa,'"]//phyPf[pfId/text()=',v_pId,']/phy[cId/text()=',v_cId,']/val'))
				  INTO v_ilosc_scen,v_kurs_wyk,v_rodzaj_opcji,v_kurs_instr,v_cena_instr,v_zmien_op,v_delta_ref,v_cena_baz
				  FROM b;
				    SET v_end = CAST(v_kurs_wyk as int);
					SET v_kod_klasy_tmp = v_kod_klasy;
					
					IF v_rodzaj_opcji = 'C' THEN
						SET v_sppa_nazwa = CONCAT( v_kod_klasy_tmp,(CASE substr(v_data_wyg,5,2)
						  WHEN '01' THEN 'A'
						  WHEN '02' THEN 'B'
						  WHEN '03' THEN 'C'
						  WHEN '04' THEN 'D'
						  WHEN '05' THEN 'E'
						  WHEN '06' THEN 'F'
						  WHEN '07' THEN 'G'
						  WHEN '08' THEN 'H'
						  WHEN '09' THEN 'I'
						  WHEN '10' THEN 'J'
						  WHEN '11' THEN 'K'
						  WHEN '12' THEN 'L'
						END),substr(v_data_wyg,3,2),v_end);
					ELSE
						SET v_sppa_nazwa = CONCAT( v_kod_klasy_tmp,(CASE substr(v_data_wyg,5,2)
						  WHEN '01' THEN 'M'
						  WHEN '02' THEN 'N'
						  WHEN '03' THEN 'O'
						  WHEN '04' THEN 'P'
						  WHEN '05' THEN 'Q'
						  WHEN '06' THEN 'R'
						  WHEN '07' THEN 'S'
						  WHEN '08' THEN 'T'
						  WHEN '09' THEN 'U'
						  WHEN '10' THEN 'V'
						  WHEN '11' THEN 'W'
						  WHEN '12' THEN 'X'
						END),substr(v_data_wyg,3,2),v_end);
					END IF;
					IF v_kod_klasy_tmp = 'MW20' THEN
					  SET v_sppa_nazwa = v_kod_klasy_tmp;
				    END IF;
					
					INSERT INTO span_papiery (sppa_nazwa,sppa_rodzaj_opcji,sppa_klas_id,sppa_data_wygas,sppa_kurs_wyk,sppa_typ_papieru,sppa_czas_do_wygas,sppa_psr,
					sppa_vsr,sppa_mnoznik,sppa_delta,sppa_stopa_proc,sppa_wsp_skal_delty,sppa_nr_poziomu,sppa_cena_instr,
					sppa_zmien_op, sppa_zmien_op_wygasl, sppa_stopa_dyw,sppa_cena_instr_baz) 
					values (v_sppa_nazwa,v_rodzaj_opcji,v_klas_id,v_data_wyg,v_kurs_wyk,v_typ_papieru,v_czas_do_wygas,v_psr,v_ryz_zmien,v_mnoznik,v_delta_ref,
					v_stopa_proc, v_wsp_skal,v_poziom,v_kurs_instr,v_zmien_op, v_zmien_op_wygasl, v_stopa_dyw, v_cena_baz);
					SET v_spid = LAST_INSERT_ID();

					l_scen: LOOP
					  SET v_sc = v_sc + 1;
					  IF v_sc <= v_ilosc_scen THEN
						select  extractvalue(col1,CONCAT('//oopPf[',v_i,']//series[',v_s,']//opt[',v_n,']//ra//a[',v_sc,']'))
								INTO v_scen FROM b;
					    INSERT INTO depozyty_jedn (dep_wartosc,dep_numer,dep_sppa_id) values (v_scen,v_sc,v_spid);
						ITERATE l_scen;
					  ELSE
						LEAVE l_scen;
					  END IF;
					END LOOP l_scen;
					ITERATE l_opt;
				  ELSE
					LEAVE l_opt;
				  END IF;
				END LOOP l_opt;
				ITERATE l_serie;
			  ELSE
				LEAVE l_serie;
			  END IF;
			END LOOP l_serie;  
      ITERATE l_walory;
    ELSE
	  LEAVE l_walory;
	END IF;
  END LOOP l_walory;	   
RETURN 1;
end;

//


DROP PROCEDURE IF EXISTS prImportuj//
CREATE PROCEDURE prImportuj()
begin
DECLARE v_ret INT;
START TRANSACTION;
truncate table span_klasy_spready;
truncate table spready_nogi;
truncate table klasy;
truncate table span_papiery;
truncate table depozyty_jedn;
select fnImportujKontrakty() INTO v_ret;
select fnImportujOpcje() INTO v_ret;
select fnImportujSpready() INTO v_ret;

commit;
end;
//

delimiter ;