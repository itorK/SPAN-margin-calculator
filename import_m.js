/* Node.js Import KDPW data to Oracle DB  
   Author: ESM Technology Karol Przybylski
   Date: 2014-12-01
   Visit http://www.esm-technology.pl or http://github.com/itorK
*/
var select = require('xpath.js')
        , dom = require('xmldom').DOMParser
        , mysql = require('mysql')
        , http = require('http')
        , fs = require('fs');

var download = function(url, dest, cb) {
  console.log('Downloading file RPNJI_ZRS.xml');
  var file = fs.createWriteStream(dest);
  var request = http.get(url, function(response) {
	response.pipe(file);
	file.on('finish', function() {
	  console.log('Download complete');
	  file.close(cb(dest));
	});
  });
}


  connectData = {
    connectionLimit : 100, //important
    host     : 'localhost',
    user     : 'admin',
    password : 'esmtechnology',
    database : 'kalkulator',
	multipleStatements : true,
    debug    :  false
  }
  var connection = mysql.createConnection({
	   connectionLimit : 100, //important
    host     : 'localhost',
    user     : 'admin',
    password : 'esmtechnology',
	multipleStatements : true,
    database : 'kalkulator',
});
var pool      =    mysql.createPool({
    connectionLimit : 100, //important
    host     : 'localhost',
    user     : 'admin',
    password : 'esmtechnology',
	multipleStatements : true,
    database : 'kalkulator',
    debug    :  false
});

    pool.getConnection(function(err,connection){
        if (err) {
          connection.release();
          res.json({"code" : 100, "status" : "Error in connection database"});
          return;
        } 
		
  //connection.connect(connectData, function(err) {
   // if (err) {
   //   console.log("Error connecting to db:", err);
   //   return;
    //}
    function DodajSpready(fhandler1) {
		stmt_s = ""
      klasy = select(fhandler1, "//ccDef");
	  v_data_gen = select(fhandler1, "//created/text()")[0].data;
      for (k = 0; k < klasy.length; k++) {
        nodes = select(klasy[k], "dSpread");
        for (i = 0; i < nodes.length; i++) {
          v_depozyt = select(nodes[i], "rate//val/text()")[0].data;
          v_priorytet = select(nodes[i], "spread/text()")[0].data;
          v_typ_spreadu = select(nodes[i], "chargeMeth/text()")[0].data;
          v_nogi = select(nodes[i], "tLeg");
          v_id = 0;
          for (j = 0; j < v_nogi.length; j++) {
            v_klasa = select(v_nogi[j], "cc/text()")[0].data;
            v_poziom = select(v_nogi[j], "tn/text()")[0].data;
            v_strona = select(v_nogi[j], "rs/text()")[0].data;
            v_delta = select(v_nogi[j], "i/text()")[0].data;
			stmt_s = stmt_s + "call prDodajSpread('W',"+v_depozyt+","+v_priorytet+",'"+v_klasa+"',"+v_poziom+",'"+v_strona+"',"+v_delta+"); "
           /* connection.query(
                    "call prDodajSpread('W',"+v_depozyt+","+v_priorytet+",'"+v_klasa+"',"+v_poziom+",'"+v_strona+"',"+v_delta+");",
                    [ ],
                    function(err, results) {
						
                      if (err) {
						  connection.release();
                        console.error(err);
                        return;
                      }

                    }
            );*/
          }
        }

        v_nazwa_klasy = select(klasy[k], "cc/text()")[0].data;
        v_poziomy = select(klasy[k], "intraTiers//tier");

        for (i = 0; i < v_poziomy.length; i++) {
          try {
            v_pocz = select(v_poziomy[i], "sPe/text()")[0].data;
            v_kon = select(v_poziomy[i], "ePe/text()")[0].data;
            v_poziom_klasy = select(v_poziomy[i], "tn/text()")[0].data;
          } catch (ex) {
            v_pocz = 0;
            v_kon = 0;
            v_poziom_klasy = 0;
          }
          if (v_pocz.length == 6 && v_kon.length == 6 && v_poziom_klasy != 99) {
			  stmt_s = stmt_s + "UPDATE span_papiery SET sppa_nr_poziomu = "+v_poziom_klasy+" WHERE sppa_data_wygas BETWEEN STR_TO_DATE("+v_pocz+",'%Y%m') AND STR_TO_DATE("+v_kon+",'%Y%m') AND sppa_klas_id  = (SELECT klas_id FROM klasy WHERE ifnull(substr(klas_nazwa_span,1,INSTR(klas_nazwa_span,'/')-1),klas_nazwa_span) = '"+v_klasa+"'); ";
            /*connection.query(
                    "UPDATE span_papiery SET sppa_nr_poziomu = "+v_poziom_klasy+" WHERE sppa_data_wygas BETWEEN STR_TO_DATE("+v_pocz+",'%Y%m') AND STR_TO_DATE("+v_kon+",'%Y%m') AND sppa_klas_id  = (SELECT klas_id FROM klasy WHERE ifnull(substr(klas_nazwa_span,1,INSTR(klas_nazwa_span,'/')-1),klas_nazwa_span) = '"+v_klasa+"') ",
                    [ ],
                    function(err, results) {
					
                      if (err) {
						connection.release();
                        console.error(err);
                        console.log(v_klasa);
                        return;
                      }
                    }
            );*/
          }
        }

      }
      nodes = select(fhandler, "//interSpreads//dSpread");
      for (i = 0; i < nodes.length; i++) {
        v_depozyt = select(nodes[i], "rate//val/text()")[0].data;
        v_priorytet = select(nodes[i], "spread/text()")[0].data;
        v_typ_spreadu = select(nodes[i], "chargeMeth/text()")[0].data;
        v_nogi = select(nodes[i], "tLeg");
        for (j = 0; j < v_nogi.length; j++) {
          v_klasa = select(v_nogi[j], "cc/text()")[0].data;
          v_poziom = select(v_nogi[j], "tn/text()")[0].data;
          v_strona = select(v_nogi[j], "rs/text()")[0].data;
          v_delta = select(v_nogi[j], "i/text()")[0].data;
		  stmt_s = stmt_s + "call prDodajSpread('Z',"+v_depozyt+","+v_priorytet+",'"+v_klasa+"',"+v_poziom+",'"+v_strona+"',"+v_delta+"); ";
          /*connection.query(
                  "call prDodajSpread('Z',"+v_depozyt+","+v_priorytet+",'"+v_klasa+"',"+v_poziom+",'"+v_strona+"',"+v_delta+")",
                  [],
                  function(err, results) {
					  
                    if (err) {
						connection.release();
                      console.error(err);
                      return;
                    }
					console.log("end");
                  }
				  
          );*/
        }
		
      }
	  connection.query(
                  stmt_s,
                  [],
                  function(err, results) {
					  
                    if (err) {
						connection.release();
                      console.error(err);
                      return;
                    }
					connection.release();
					//console.log(stmt_s);
					process.exit();
                  }
				  );
     connection.on('error', function(err) {      
              res.json({"code" : 100, "status" : "Error in connection database"});
              return;    
        });
		return;
    }

    function DodajPapier(fhandler, cb) {
      nodes = select(fhandler, "//futPf");
      stmt = "BEGIN; START TRANSACTION; call prCzyscImp(); ";
	 //stmt = "";
      for (i = 0; i < nodes.length; i++) {
        v_klas_id = 0;
        v_kod_span = select(nodes[i], "undPf/pfCode/text()")[0].data;
        v_kod_klasy = select(nodes[i], "pfCode/text()")[0].data;
        v_waluta = select(nodes[i], "currency/text()")[0].data;
        v_mnoznik = select(nodes[i], "cvf/text()")[0].data;
        v_typ_papieru = select(nodes[i], "valueMeth/text()")[0].data;
        v_serie = select(nodes[i], "fut");
        v_sppa_id = 0

        for (j = 0; j < v_serie.length; j++) {
          v_data_wyg = select(v_serie[j], "pe/text()")[0].data;
          v_kurs_instr = select(v_serie[j], "p/text()")[0].data;
          v_cena_instr = select(v_serie[j], "val/text()")[0].data;
          v_wsp_skal = select(v_serie[j], "sc/text()")[0].data;
          v_stopa_proc = select(v_serie[j], "intrRate/val/text()")[0].data;
          v_psr = select(v_serie[j], "scanRate/priceScanPct/text()")[0].data;
          v_ryz_zmien = select(v_serie[j], "scanRate/volScan/text()")[0].data;
          v_ryz_ceny = select(v_serie[j], "scanRate/priceScan/text()")[0].data;
          v_czas_do_wygas = select(v_serie[j], "t/text()")[0].data;
          v_delta_ref = select(v_serie[j], "ra/d/text()")[0].data;
          v_end = '';
          v_kod_klasy_tmp = v_kod_klasy;
          if (v_kod_klasy.substr(3, 2) == "0B") {
            v_kod_klasy_tmp = v_kod_klasy.substr(0, v_kod_klasy.length - 1);
            v_end = '20';
          }
          v_n = 'A';
          switch (v_data_wyg.substr(4, 2)) {
            case '01':
              v_n = 'F';
              break;
            case '02':
              v_n = 'G';
              break;
            case '03':
              v_n = 'H';
              break;
            case '04':
              v_n = 'J';
              break;
            case '05':
              v_n = 'K';
              break;
            case '06':
              v_n = 'M';
              break;
            case '07':
              v_n = 'N';
              break;
            case '08':
              v_n = 'Q';
              break;
            case '09':
              v_n = 'U';
              break;
            case '10':
              v_n = 'V';
              break;
            case '11':
              v_n = 'X';
              break;
            case '12':
              v_n = 'Z';
              break;
          }
          v_dep = select(v_serie[j], "ra/a");
          v_scen1 = v_dep[0].firstChild.data;
          v_scen2 = v_dep[1].firstChild.data;
          v_scen3 = v_dep[2].firstChild.data;
          v_scen4 = v_dep[3].firstChild.data;
          v_scen5 = v_dep[4].firstChild.data;
          v_scen6 = v_dep[5].firstChild.data;
          v_scen7 = v_dep[6].firstChild.data;
          v_scen8 = v_dep[7].firstChild.data;
          v_scen9 = v_dep[8].firstChild.data;
          v_scen10 = v_dep[9].firstChild.data;
          v_scen11 = v_dep[10].firstChild.data;
          v_scen12 = v_dep[11].firstChild.data;
          v_scen13 = v_dep[12].firstChild.data;
          v_scen14 = v_dep[13].firstChild.data;
          v_scen15 = v_dep[14].firstChild.data;
          v_scen16 = v_dep[15].firstChild.data;
          v_sppa_nazwa = v_kod_klasy_tmp + v_n + v_data_wyg.substr(2, 2) + v_end;
          stmt = stmt + " call prDodajPapier('" + v_sppa_nazwa + "','" + v_kod_klasy + "','" + v_data_wyg + "'," + v_kurs_instr + ",'" + v_typ_papieru + "'," + v_czas_do_wygas + "," + v_psr + "," + v_ryz_zmien + "," + v_mnoznik + "," + v_delta_ref + "," + v_stopa_proc + "," + v_wsp_skal + "," + v_cena_instr + "," + v_scen1 + "," + v_scen2 + "," + v_scen3 + "," + v_scen4 + "," + v_scen5 + "," + v_scen6 + "," + v_scen7 + "," + v_scen8 + "," + v_scen9 + "," + v_scen10 + "," + v_scen11 + "," + v_scen12 + "," + v_scen13 + "," + v_scen14 + "," + v_scen15 + "," + v_scen16 + ",'" + v_kod_span + "'); ";
        }


      }
      //OPCJE
      nodes = select(fhandler, "//oopPf");

      for (i = 0; i < nodes.length; i++) {
        v_klas_id = 0;
        v_kod_span = select(nodes[i], "undPf/pfCode/text()")[0].data;
        v_kod_klasy = select(nodes[i], "pfCode/text()")[0].data;
        v_waluta = select(nodes[i], "currency/text()")[0].data;
        v_mnoznik = select(nodes[i], "cvf/text()")[0].data;
        v_typ_papieru = select(nodes[i], "valueMeth/text()")[0].data;
        v_serie = select(nodes[i], "series");
        v_sppa_id = 0

        for (j = 0; j < v_serie.length; j++) {
          v_data_wyg = select(v_serie[j], "pe/text()")[0].data;
          v_wsp_skal = select(v_serie[j], "sc/text()")[0].data;
          v_stopa_proc = select(v_serie[j], "intrRate/val/text()")[0].data;
          v_psr = select(v_serie[j], "scanRate/priceScanPct/text()")[0].data;
          v_ryz_zmien = select(v_serie[j], "scanRate/volScan/text()")[0].data;
          v_ryz_ceny = select(v_serie[j], "scanRate/priceScan/text()")[0].data;
          v_czas_do_wygas = select(v_serie[j], "t/text()")[0].data - 0.01;
          v_poziom = 99;
          v_zmien_op_wygasl = select(v_serie[j], "v/text()")[0].data;
		  try {
			v_stopa_dyw = select(v_serie[j], "divRate/val/text()")[0].data;
		  } catch (ex) { v_stopa_dyw = 0; }
          v_baz_nazwa = select(v_serie[j], "undC/exch/text()")[0].data;
          v_pId = select(v_serie[j], "undC/pfId/text()")[0].data;
          v_cId = select(v_serie[j], "undC/cId/text()")[0].data;
          v_opcje = select(v_serie[j], "opt");
          v_exch = select(fhandler, "//exchange");
          for (k = 0; k < v_exch.length; k++) {
            try {
              if (select(v_exch[k], "exch/text()")[0].data == v_baz_nazwa) {
                v_exchPhyPf = select(v_exch[k], "phyPf");
                for (k1 = 0; k1 < v_exchPhyPf.length; k1++) {
                  if (select(v_exchPhyPf[k1], "pfId/text()")[0].data == v_pId) {
                    v_exchPhyPfPhy = select(v_exchPhyPf[k1], "phy");
                    for (k2 = 0; k2 < v_exchPhyPfPhy.length; k2++) {
                      if (select(v_exchPhyPfPhy[k2], "cId/text()")[0].data == v_cId) {
                        v_cena_baz = select(v_exchPhyPfPhy[k2], "val/text()")[0].data;
                      }
                    }
                  }
                }
              }
            } catch (ex) {
            }
          }
          for (l = 0; l < v_opcje.length; l++) {
            v_kurs_wyk = select(v_opcje[l], "k/text()")[0].data;
            v_rodzaj_opcji = select(v_opcje[l], "o/text()")[0].data;
            v_kurs_instr = select(v_opcje[l], "p/text()")[0].data;
            v_cena_instr = select(v_opcje[l], "val/text()")[0].data;
            v_zmien_op = select(v_opcje[l], "v/text()")[0].data;
            v_delta_ref = select(v_opcje[l], "ra//d/text()")[0].data;
            v_end = Math.round(v_kurs_wyk);

            v_n = 'A';
            if (v_rodzaj_opcji == 'C') {
              switch (v_data_wyg.substr(4, 2)) {
                case '01':
                  v_n = 'A';
                  break;
                case '02':
                  v_n = 'B';
                  break;
                case '03':
                  v_n = 'C';
                  break;
                case '04':
                  v_n = 'D';
                  break;
                case '05':
                  v_n = 'E';
                  break;
                case '06':
                  v_n = 'F';
                  break;
                case '07':
                  v_n = 'G';
                  break;
                case '08':
                  v_n = 'H';
                  break;
                case '09':
                  v_n = 'I';
                  break;
                case '10':
                  v_n = 'J';
                  break;
                case '11':
                  v_n = 'K';
                  break;
                case '12':
                  v_n = 'L';
                  break;
              }
            } else {
              switch (v_data_wyg.substr(4, 2)) {
                case '01':
                  v_n = 'M';
                  break;
                case '02':
                  v_n = 'N';
                  break;
                case '03':
                  v_n = 'O';
                  break;
                case '04':
                  v_n = 'P';
                  break;
                case '05':
                  v_n = 'Q';
                  break;
                case '06':
                  v_n = 'R';
                  break;
                case '07':
                  v_n = 'S';
                  break;
                case '08':
                  v_n = 'T';
                  break;
                case '09':
                  v_n = 'U';
                  break;
                case '10':
                  v_n = 'V';
                  break;
                case '11':
                  v_n = 'W';
                  break;
                case '12':
                  v_n = 'X';
                  break;
              }

            }
            v_sppa_nazwa = v_kod_klasy + v_n + v_data_wyg.substr(2, 2) + v_end;
            if (v_kod_klasy == "MW20") {
              v_sppa_nazwa = "MW20";
            }
            v_dep = select(v_opcje[l], "ra/a");
            v_scen1 = v_dep[0].firstChild.data;
            v_scen2 = v_dep[1].firstChild.data;
            v_scen3 = v_dep[2].firstChild.data;
            v_scen4 = v_dep[3].firstChild.data;
            v_scen5 = v_dep[4].firstChild.data;
            v_scen6 = v_dep[5].firstChild.data;
            v_scen7 = v_dep[6].firstChild.data;
            v_scen8 = v_dep[7].firstChild.data;
            v_scen9 = v_dep[8].firstChild.data;
            v_scen10 = v_dep[9].firstChild.data;
            v_scen11 = v_dep[10].firstChild.data;
            v_scen12 = v_dep[11].firstChild.data;
            v_scen13 = v_dep[12].firstChild.data;
            v_scen14 = v_dep[13].firstChild.data;
            v_scen15 = v_dep[14].firstChild.data;
            v_scen16 = v_dep[15].firstChild.data;

            stmt = stmt + " call prDodajPapierO('" + v_sppa_nazwa + "','" + v_kod_klasy + "','" + v_data_wyg + "'," + v_kurs_instr + ",'" + v_typ_papieru + "'," + v_czas_do_wygas + "," + v_psr + "," + v_ryz_zmien + "," + v_mnoznik + "," + v_delta_ref + "," + v_stopa_proc + "," + v_wsp_skal + "," + v_cena_instr + "," + v_scen1 + "," + v_scen2 + "," + v_scen3 + "," + v_scen4 + "," + v_scen5 + "," + v_scen6 + "," + v_scen7 + "," + v_scen8 + "," + v_scen9 + "," + v_scen10 + "," + v_scen11 + "," + v_scen12 + "," + v_scen13 + "," + v_scen14 + "," + v_scen15 + "," + v_scen16 + ",'" + v_kod_span + "'," + v_zmien_op_wygasl + "," + v_stopa_dyw + "," + v_kurs_wyk + ",'" + v_rodzaj_opcji + "'," + v_zmien_op + "," + v_cena_baz + "); ";

          }
        }
      }

      stmt = stmt + " COMMIT;"
      connection.query(
              stmt, [],
              function(err, results) {
                if (err) {
					connection.release();
                  console.error(err);
                  return;
                }
                DodajSpready(fhandler);
              }
      );
    }
	download('http://www.kdpwccp.pl/pl/zarzadzanie/Parametry/SPAN/RPNJI_ZRS.xml','RPNJE_ZRS.xml', function(dest) {
	console.log('Processing...',dest);
	fs.readFile(dest, 'utf8', function(err, data) {
	  if (err) {
		return console.log(err);
	  }
	  fhandler = new dom().parseFromString(data);
      DodajPapier(fhandler);
	}
	)

  })
  });
