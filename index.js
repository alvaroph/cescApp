const mysql = require('mysql2/promise');

async function main() {
  const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '',
    database: 'cesc'
  });

  // Categorías a procesar (12), sin signos + o -:
  const categories = [
    'soc_POS', 'soc_NEG', 'ar_i', 'pros', 'af', 
    'ar_d', 'pros_2', 'av', 'vf', 'vv', 
    'vr', 'amics'
  ];

   // Generamos las categorías normalizadas
   const normalizedCategories = categories.map(cat => cat + "_norm");

  try {
    // Obtenemos todas las respuestas
    const [rows] = await connection.execute('SELECT * FROM respostes');

    // Estructura intermedia: 
    // Key: `${id_enquesta}-${id_alumne}`, Value: { id_enquesta, id_alumne, soc_POS, soc_NEG, ... }
    const acumulados = {};

    for (const row of rows) {
      const id_enquesta = row.id_enquesta;
      const id_alumne = row.id_alumne;
      
      // Por cada una de las 12 categorías, tenemos 3 columnas: cat_1, cat_2, cat_3
      for (const cat of categories) {
        for (let i = 1; i <= 3; i++) {
          const colName = `${cat}_${i}`;
          const votado = row[colName];
          if (votado && Number.isInteger(votado)) {
            const key = `${id_enquesta}-${votado}`;
            if (!acumulados[key]) {
              acumulados[key] = {
                id_enquesta: id_enquesta,
                id_alumne: votado,
                soc_POS: 0,
                soc_NEG: 0,
                ar_i: 0,
                pros: 0,
                af: 0,
                ar_d: 0,
                pros_2: 0,
                av: 0,
                vf: 0,
                vv: 0,
                vr: 0,
                amics: 0
              };
            }
            // Incrementamos la categoría correspondiente
            acumulados[key][cat] += 1;
          }
        }
      }
    }

    // Ahora volcamos acumulados en la tabla respostes_processades
    for (const key in acumulados) {
      const data = acumulados[key];
      // Verificamos si ya existe el registro en respostes_processades
      const [exist] = await connection.execute(
        'SELECT COUNT(*) as cnt FROM respostes_processades WHERE id_enquesta = ? AND id_alumne = ?',
        [data.id_enquesta, data.id_alumne]
      );

      const existeRegistro = exist[0].cnt > 0;

      if (existeRegistro) {
        // Hacemos un UPDATE
        await connection.execute(
          `UPDATE respostes_processades SET 
            soc_POS = soc_POS + ?, 
            soc_NEG = soc_NEG + ?, 
            ar_i = ar_i + ?, 
            pros = pros + ?, 
            af = af + ?, 
            ar_d = ar_d + ?, 
            pros_2 = pros_2 + ?, 
            av = av + ?, 
            vf = vf + ?, 
            vv = vv + ?, 
            vr = vr + ?, 
            amics = amics + ?
           WHERE id_enquesta = ? AND id_alumne = ?`,
          [
            data.soc_POS, data.soc_NEG, data.ar_i, data.pros, data.af,
            data.ar_d, data.pros_2, data.av, data.vf, data.vv,
            data.vr, data.amics,
            data.id_enquesta, data.id_alumne
          ]
        );
      } else {
        // Hacemos un INSERT
        await connection.execute(
          `INSERT INTO respostes_processades (
            id_enquesta, id_alumne, nom_alumne,
            soc_POS, soc_NEG, ar_i, pros, af, ar_d, 
            pros_2, av, vf, vv, vr, amics
          ) VALUES (?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            data.id_enquesta, data.id_alumne,
            data.soc_POS, data.soc_NEG, data.ar_i, data.pros, data.af,
            data.ar_d, data.pros_2, data.av, data.vf, data.vv,
            data.vr, data.amics
          ]
        );
      }
    }


    console.log('Traspaso y acumulación completados con éxito.');

      // --- PROCESO PARA CALCULAR MEDIA Y DESVIACIÓN ESTÁNDAR ---
      const [procesadas] = await connection.execute('SELECT * FROM respostes_processades');

      const tabla = {};
      const valoresPorColumna = {};
      for (const cat of categories) {
        valoresPorColumna[cat] = [];
      }
  
      // Rellenamos la tabla con los datos de cada alumno
      for (const fila of procesadas) {
        const idAlum = fila.id_alumne.toString();
        tabla[idAlum] = {};
        for (const cat of categories) {
          const valor = fila[cat];
          tabla[idAlum][cat] = valor;
          valoresPorColumna[cat].push(valor);
        }
      }
  
      // Calculamos la media por categoría
      const medias = {};
      for (const cat of categories) {
        const arr = valoresPorColumna[cat];
        const sum = arr.reduce((a, b) => a + b, 0);
        const mean = sum / arr.length;
        medias[cat] = mean;
      }
  
      // Calculamos la desviación estándar por categoría
      const desv = {};
      for (const cat of categories) {
        const arr = valoresPorColumna[cat];
        const mean = medias[cat];
        const variance = arr.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / arr.length;
        const std = Math.sqrt(variance);
        desv[cat] = std;
      }
  
      // Añadimos las filas "media" y "desv" a la tabla
      tabla["media"] = {};
      tabla["desv"] = {};
      for (const cat of categories) {
        tabla["media"][cat] = medias[cat];
        tabla["desv"][cat] = desv[cat];
      }
  
      // --- NORMALIZACIÓN ---
      // Añadimos las columnas normalizadas
      for (const filaId in tabla) {
        if (filaId !== "media" && filaId !== "desv") {
          // Fila de alumno
          for (const cat of categories) {
            const valor = tabla[filaId][cat];
            const mean = tabla["media"][cat];
            const std = tabla["desv"][cat];
            let normVal = 0;
            if (std !== 0) {
              normVal = (valor - mean) / std;
            }
            // Usamos la categoría normalizada de normalizedCategories
            const normCat = cat + "_norm";
            tabla[filaId][normCat] = normVal;
          }
        } else {
          // Fila de media y desv
          for (const cat of categories) {
            const normCat = cat + "_norm";
            if (filaId === "media") {
              // media normalizada = 0
              tabla["media"][normCat] = 0;
            } else if (filaId === "desv") {
              // desv normalizada = 1
              tabla["desv"][normCat] = 1;
            }
          }
        }
      }
  
      

      // Añadimos las nuevas columnas solicitadas:
// Impac (soc_POS + soc_NEG), Prefer (soc_POS - soc_NEG)

const impacArr = [];
const preferArr = [];

for (const filaId in tabla) {
  if (filaId !== "media" && filaId !== "desv") {
    const sp = tabla[filaId].soc_POS;
    const sn = tabla[filaId].soc_NEG;
    const impac = sp + sn;
    const prefer = sp - sn;
    tabla[filaId].Impac = impac;
    tabla[filaId].Prefer = prefer;
    impacArr.push(impac);
    preferArr.push(prefer);
  }
}

// Función auxiliar para calcular media y desviación
function calcMediaDesv(arr) {
  const sum = arr.reduce((a,b) => a+b,0);
  const mean = sum / arr.length;
  const variance = arr.reduce((acc,val)=> acc+Math.pow(val-mean,2), 0) / arr.length;
  const std = Math.sqrt(variance);
  return {mean, std};
}

// Calculamos media y desv para Impac y Prefer
const impacStats = calcMediaDesv(impacArr);
const preferStats = calcMediaDesv(preferArr);

// Asignamos estas estadísticas a la fila media y desv
tabla["media"].Impac = impacStats.mean;
tabla["desv"].Impac = impacStats.std;
tabla["media"].Prefer = preferStats.mean;
tabla["desv"].Prefer = preferStats.std;

// Ahora calculamos Z_Soc_POS, Z_Soc_NEG, Z_Impac, Z_Prefer
for (const filaId in tabla) {
  if (filaId !== "media" && filaId !== "desv") {
    // Z_Soc_POS
    const sp = tabla[filaId].soc_POS;
    const mean_sp = tabla["media"].soc_POS;
    const std_sp = tabla["desv"].soc_POS;
    tabla[filaId].Z_Soc_POS = std_sp !== 0 ? (sp - mean_sp) / std_sp : 0;

    // Z_Soc_NEG
    const sn = tabla[filaId].soc_NEG;
    const mean_sn = tabla["media"].soc_NEG;
    const std_sn = tabla["desv"].soc_NEG;
    tabla[filaId].Z_Soc_NEG = std_sn !== 0 ? (sn - mean_sn) / std_sn : 0;

    // Z_Impac
    const impac = tabla[filaId].Impac;
    const mean_i = tabla["media"].Impac;
    const std_i = tabla["desv"].Impac;
    tabla[filaId].Z_Impac = std_i !== 0 ? (impac - mean_i) / std_i : 0;

    // Z_Prefer
    const prefer = tabla[filaId].Prefer;
    const mean_p = tabla["media"].Prefer;
    const std_p = tabla["desv"].Prefer;
    tabla[filaId].Z_Prefer = std_p !== 0 ? (prefer - mean_p) / std_p : 0;
  } else {
    // Para media y desv
    // Dejamos los Z_ en 0 para media y 1 para desv, o simplemente 0.
    // Aquí: media = 0, desv = 1
    if (filaId === "media") {
      tabla["media"].Z_Soc_POS = 0;
      tabla["media"].Z_Soc_NEG = 0;
      tabla["media"].Z_Impac = 0;
      tabla["media"].Z_Prefer = 0;
    } else if (filaId === "desv") {
      tabla["desv"].Z_Soc_POS = 1;
      tabla["desv"].Z_Soc_NEG = 1;
      tabla["desv"].Z_Impac = 1;
      tabla["desv"].Z_Prefer = 1;
    }
  }
}

// Columnas nuevas a mostrar
const newColumns = ['Z_Soc_POS', 'Z_Soc_NEG', 'Impac', 'Prefer', 'Z_Impac', 'Z_Prefer'];

// Combinar las columnas originales con las nuevas, si lo deseas
// Orden sugerido: primero las originales, luego las nuevas
const allColumns = [...categories,...normalizedCategories, ...newColumns];


// Función auxiliar para calcular media y desviación estándar si no la tienes ya
function calcMediaDesv(arr) {
    const sum = arr.reduce((a,b)=>a+b,0);
    const mean = sum / arr.length;
    const variance = arr.reduce((acc,val)=>acc+Math.pow(val-mean,2),0)/arr.length;
    const std = Math.sqrt(variance);
    return {mean, std};
  }
  
  // Arrays para almacenar valores y poder calcular media y desv
  const arrAR = [];
  const arrTotA = [];
  const arrPros = [];
  const arrTotV = [];
  
  // Calculamos AR, TotA, Pros, TotV para cada alumno
  for (const filaId in tabla) {
    if (filaId !== "media" && filaId !== "desv") {
      const ar_i = tabla[filaId].ar_i;
      const ar_d = tabla[filaId].ar_d;
      const af = tabla[filaId].af;
      const av = tabla[filaId].av;
      const pros = tabla[filaId].pros;
      const pros_2 = tabla[filaId].pros_2;
      const vf = tabla[filaId].vf;
      const vv = tabla[filaId].vv;
      const vr = tabla[filaId].vr;
  
      const AR = ar_i + ar_d;
      const TotA = (ar_i + ar_d) / (2 + af + av);
      const Pros = pros + pros_2;
      const TotV = vf + vv + vr;
  
      tabla[filaId].AR = AR;
      tabla[filaId].TotA = TotA;
      tabla[filaId].Pros = Pros;
      tabla[filaId].TotV = TotV;
  
      arrAR.push(AR);
      arrTotA.push(TotA);
      arrPros.push(Pros);
      arrTotV.push(TotV);
    }
  }
  
  // Calculamos media y desv de AR, TotA, Pros, TotV
  const ARStats = calcMediaDesv(arrAR);
  const TotAStats = calcMediaDesv(arrTotA);
  const ProsStats = calcMediaDesv(arrPros);
  const TotVStats = calcMediaDesv(arrTotV);
  
  // Asignamos estas estadísticas a la fila "media" y "desv"
  tabla["media"].AR = ARStats.mean;
  tabla["desv"].AR = ARStats.std;
  
  tabla["media"].TotA = TotAStats.mean;
  tabla["desv"].TotA = TotAStats.std;
  
  tabla["media"].Pros = ProsStats.mean;
  tabla["desv"].Pros = ProsStats.std;
  
  tabla["media"].TotV = TotVStats.mean;
  tabla["desv"].TotV = TotVStats.std;
  
  // Ahora calculamos las normalizaciones ZAR, ZTotA, ZPros, ZTotV
  for (const filaId in tabla) {
    if (filaId !== "media" && filaId !== "desv") {
      const AR = tabla[filaId].AR;
      const mean_AR = tabla["media"].AR;
      const std_AR = tabla["desv"].AR;
      tabla[filaId].ZAR = std_AR !== 0 ? (AR - mean_AR) / std_AR : 0;
  
      const TotA = tabla[filaId].TotA;
      const mean_TotA = tabla["media"].TotA;
      const std_TotA = tabla["desv"].TotA;
      tabla[filaId].ZTotA = std_TotA !== 0 ? (TotA - mean_TotA) / std_TotA : 0;
  
      const Pros = tabla[filaId].Pros;
      const mean_Pros = tabla["media"].Pros;
      const std_Pros = tabla["desv"].Pros;
      tabla[filaId].ZPros = std_Pros !== 0 ? (Pros - mean_Pros) / std_Pros : 0;
  
      const TotV = tabla[filaId].TotV;
      const mean_TotV = tabla["media"].TotV;
      const std_TotV = tabla["desv"].TotV;
      tabla[filaId].ZTotV = std_TotV !== 0 ? (TotV - mean_TotV) / std_TotV : 0;
    } else {
      // Para la fila media y desv de las Z:
      // media -> 0, desv -> 1
      if (filaId === "media") {
        tabla["media"].ZAR = 0;
        tabla["media"].ZTotA = 0;
        tabla["media"].ZPros = 0;
        tabla["media"].ZTotV = 0;
      } else {
        tabla["desv"].ZAR = 1;
        tabla["desv"].ZTotA = 1;
        tabla["desv"].ZPros = 1;
        tabla["desv"].ZTotV = 1;
      }
    }
  }
  
  console.table(tabla);

  // Nuevas columnas a añadir en esta etapa:

  const newColumns6 = ["AR", "ZAR", "TotA", "ZTotA", "Pros", "ZPros", "TotV", "ZTotV"];
  
  // Suponemos que antes ya tenías 'finalColumns' o 'columnasAcumuladas' con las columnas anteriores.
  // Aquí, por claridad, concatenamos estas nuevas columnas al final.
  // Ajusta según el nombre de tu variable de columnas acumuladas si es necesario.
  // Si no tienes una variable mantenida, puedes crear una con las columnas previas y concatenar estas nuevas.
  //const finalColumnsExtended = finalColumns.concat(newColumns6);
  
  console.log("Nuevas columnas AR, ZAR, TotA, ZTotA, Pros, ZPros, TotV, ZTotV calculadas.");
  //imprimirTabla(tabla, finalColumnsExtended);
  
 

    for (const filaId in tabla) {
      if (filaId !== "media" && filaId !== "desv") {
        const ZSocPos = tabla[filaId].Z_soc_POS;
        const ZSocNeg = tabla[filaId].Z_soc_NEG;
        const ZImpac = tabla[filaId].Z_Impac;
        const ZPrefer = tabla[filaId].Z_Prefer;
    
        let Popular = 0;
        if (ZPrefer > -1) Popular++;
        if (ZSocPos > 0) Popular++;
        if (ZSocNeg < 0) Popular++;
    
        let Rebutjat = 0;
        if (ZPrefer < -1) Rebutjat++;
        if (ZSocNeg > 0) Rebutjat++;
        if (ZSocPos < 0) Rebutjat++;
    
        let Ignorat = 0;
        if (ZImpac < -1) Ignorat++;
        if (ZSocNeg < 0) Ignorat++;
        if (ZSocPos < 0) Ignorat++;
    
        let Controvertit = 0;
        if (ZImpac > 1) Controvertit++;
        if (ZSocPos > 0) Controvertit++;
        if (ZSocNeg > 0) Controvertit++;
    
        let Normal = 0;
        // Se asume que "ZSoc > 0.5" fue un error tipográfico y se refería a ZSoc+
        // Las condiciones de Normal parecen simétricas a las otras:
        // si ZSoc+ < -0.5
        // si ZSoc+ > 0.5
        // si ZSoc- < -0.5
        // si ZSoc- > 0.5
        if (ZSocPos < -0.5) Normal++;
        if (ZSocPos > 0.5) Normal++;
        if (ZSocNeg < -0.5) Normal++;
        if (ZSocNeg > 0.5) Normal++;
    
        tabla[filaId].Popular = Popular;
        tabla[filaId].Rebutjat = Rebutjat;
        tabla[filaId].Ignorat = Ignorat;
        tabla[filaId].Controvertit = Controvertit;
        tabla[filaId].Normal = Normal;
    
      } else {
        // Para "media" y "desv" asignamos 0 (no tiene sentido estadístico)
        tabla[filaId].Popular = 0;
        tabla[filaId].Rebutjat = 0;
        tabla[filaId].Ignorat = 0;
        tabla[filaId].Controvertit = 0;
        tabla[filaId].Normal = 0;
      }
    }
    
    // Agregamos estas columnas a la lista final de columnas.
    // Suponemos que 'finalColumnsExtended' es la lista de columnas tras el último paso.
    // Si es otra variable, ajústalo. Si no tienes esta variable, crea una lista con las columnas previas.
    const newColumns5 = ["Popular", "Rebutjat", "Ignorat", "Controvertit", "Normal"];
    //const finalColumnsExtended2 = finalColumnsExtended.concat(newColumns2);
    
    console.log("Nuevas columnas Popular, Rebutjat, Ignorat, Controvertit, Normal calculadas.");
 //   imprimirTabla(tabla, finalColumnsExtended2);

    
// Convertimos a un array para console.table
const rowsToPrint = [];
for (const filaId in tabla) {
  const rowObj = { id: filaId };
  // Formatear todas las columnas
  for (const cat of categories) {
    rowObj[cat] = tabla[filaId][cat].toFixed(2);
  }
  for (const cat of normalizedCategories) {
    rowObj[cat] = tabla[filaId][cat].toFixed(2);
  }
  for (const nc of newColumns) {
    rowObj[nc] = tabla[filaId][nc].toFixed(2);
  }
  for (const nc of newColumns6) {
    rowObj[nc] = tabla[filaId][nc].toFixed(2);
  }
  for (const nc of newColumns5) {
    rowObj[nc] = tabla[filaId][nc].toFixed(2);
  }
  rowsToPrint.push(rowObj);
}

console.table(rowsToPrint); 

    console.log('Proceso completado con éxito.');

  } catch (error) {
    console.error('Error durante el proceso:', error);
  } finally {
    await connection.end();
  }
}

main();
