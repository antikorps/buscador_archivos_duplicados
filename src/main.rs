use clap::Parser;
use md5;
use sqlite::{Connection,State,Statement};
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs, path::PathBuf, process};
use walkdir::WalkDir;
use csv;


/// Buscador simple de archivos duplicados
#[derive(Parser)]
struct Argumentos {
    /// directorio en el que se buscarán los archivos (incluyendo subdirectorios)
    #[arg(short, long)]
    directorio: PathBuf,

    /// persistir los resultados en archivo sqlite
    #[arg(short, long, default_value_t = false)]
    sqlite: bool,

    /// persistir los resultados en archivo csv
    #[arg(short, long, default_value_t = false)]
    csv: bool,
}

struct Buscador {
    directorio: PathBuf,
    persistencia: Persistencia,
    bbdd: BBDD,
}

struct Persistencia {
    sqlite: bool,
    csv: bool,
}

struct BBDD {
    ruta: PathBuf,
    conexion: Connection,
}

fn obtener_nombre_desde_ruta(ruta: String) -> String {
    let r = Path::new(&ruta);
    match r.file_name() {
        None => {
            let mensaje_error = format!("ADVERTENCIA: no se ha podido obtener el nombre de la ruta: {}", ruta);
            eprintln!("{}", mensaje_error);
            return ruta;
        }
        Some(nombre) => {
            return nombre.to_string_lossy().to_string()
        }
    }
}

fn bytes_a_formato_humano(bytes: i64) -> String {
    // El explorador de archivos Nautilus redondea, no 1073741824, 1048576 o 1024
    let b = bytes as f64;
    if bytes >= 1000000000 {
        let medida = b / 1000000000 as f64;
        return format!("{:.2} GB", medida)
    }

    else if bytes >= 1000000 {
        let medida = b / 1000000 as f64;
        return format!("{:.2} MB", medida)
    }

    else if bytes >= 1000 {
        let medida = b / 1000 as f64;
        return format!("{:.2} KB", medida)
    }

    else {
        return format!("{} bytes", b)
    }
}

fn calcular_tamaño_md5sum(ruta: String) -> Result<(u64,String),String> {
    let archivo: fs::File;
    match fs::File::open(ruta.clone()) {
        Err(error) => {
            let mensaje_error = format!("no se ha podido abrir el archivo {}: {}", ruta, error);
            return Err(mensaje_error);
        }
        Ok(ok) => {
            archivo = ok
        }
    }

    let tamaño: u64;
    match archivo.metadata() {
        Err(error) => {
            let mensaje_error = format!("no se ha podido obtener los metadatos del archivo {}: {}", ruta, error);
            return Err(mensaje_error);
        }
        Ok(ok) => {
            tamaño = ok.len();
        }
    }

    let buf_tam = tamaño.min(1_000_000) as usize;
    let mut buf = BufReader::with_capacity(buf_tam, archivo);
    let mut contexto = md5::Context::new();
    loop {
        let parte: &[u8];
        match buf.fill_buf() {
            Err(error) => {
                let mensaje_error = format!("error de buffer leyendo el archivo {}: {}", ruta, error);
                return Err(mensaje_error);
            }
            Ok(ok) => {
                parte = ok;
            }
        }
        if parte.is_empty() {
            break;
        }
        contexto.consume(parte);
        let parte_tam = parte.len();
        buf.consume(parte_tam)
    }
    let digest = contexto.compute();
    let md5sum = format!("{:x}", digest);
    return Ok((tamaño, md5sum))
}

fn conectar_base_datos() -> Result<(Connection, PathBuf), String> {
    let ruta_ejecutable: PathBuf;
    match env::current_exe() {
        Err(error) => {
            let mensaje_error =
                format!("ERROR: no se ha podido obtener la ruta del ejecutable: {}", error);
            return Err(mensaje_error);
        }
        Ok(ok) => {
            ruta_ejecutable = ok;
        }
    }

    let ruta_raiz: &Path;
    match ruta_ejecutable.parent() {
        None => {
            eprint!("ERROR: no se ha podido obtener el directorio del ejecutable");
            process::exit(1);
        }
        Some(ok) => {
            ruta_raiz = ok;
        }
    }

    let ahora = SystemTime::now();
    let desde_epoch = ahora
        .duration_since(UNIX_EPOCH)
        .expect("ERROR: ¿qué alguien me explique cómo es posible que el tiempo haya ido hacia atrás");
    let nombre_base_datos = format!("{}.sqlite", desde_epoch.as_millis());
    let ruta_base_datos = ruta_raiz.join(nombre_base_datos);

    let ruta_bbdd = ruta_base_datos.clone();

    match sqlite::open(ruta_bbdd.clone()) {
        Err(error) => {
            let mensaje_error = format!("ERROR: no se ha podido crear la base de datos: {}", error);
            return Err(mensaje_error);
        }
        Ok(ok) => {
            return Ok((ok, ruta_bbdd));
        }
    }
}

impl Buscador {
    fn comprobar(&self) {
        if !self.directorio.exists() {
            eprintln!(
                "ERROR: el directorio proporcionado ({}) para el análisis no existe",
                self.directorio.to_string_lossy()
            );
            process::exit(1)
        }
        if !self.directorio.is_dir() {
            eprintln!(
                "ERROR: la ruta proporcionada ({}) no es un directorio",
                self.directorio.to_string_lossy()
            );
            process::exit(1)
        }
    }
    fn eliminar_base_datos(&self) {
        match fs::remove_file(self.bbdd.ruta.clone()) {
            Err(error) => {
                let mensaje_error = format!(
                    "ERROR: no se ha podido borrar el archivo con la base de datos: {}",
                    error
                );
                eprintln!("{}", mensaje_error);
            }
            Ok(_) => (),
        }
    }
    fn crear_tabla_archivos(&self) {
        let consulta = "
            CREATE TABLE 'archivos' (
                'id'	INTEGER,
                'ruta'	TEXT,
                'tamaño'	INTEGER,
                'md5sum' TEXT,
                PRIMARY KEY('id' AUTOINCREMENT)
            );
        ";
        match self.bbdd.conexion.execute(consulta) {
            Err(error) => {
                eprintln!(
                    "ERROR: no se ha podido ejecutar la consulta para crear la tabla: {}",
                    error
                );
                self.eliminar_base_datos();
                process::exit(1)
            }
            Ok(_) => (),
        }
    }
    fn buscar_archivos(&self) {
        for archivo in WalkDir::new(self.directorio.clone())
            .into_iter()
            .filter_map(|archivo: Result<walkdir::DirEntry, walkdir::Error>| archivo.ok())
        {
            let archivo_ruta = archivo.path();
            if archivo_ruta.is_dir() {
                continue;
            }

            let tamaño: u64;
            let md5sum: String;
            match calcular_tamaño_md5sum(archivo_ruta.display().to_string()) {
                Err(error) => {
                    eprintln!("ADVERTENCIA: {}", error);
                    continue
                }
                Ok(ok) => {
                    tamaño = ok.0;
                    md5sum = ok.1;
                }
            }

            let insert_into = format!("INSERT INTO 'archivos' (ruta, tamaño, md5sum) VALUES (?, ?, ?);");
            let mut sentencia_preparada: Statement;
            match self.bbdd.conexion.prepare(insert_into) {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha podido crear la sentencia preparada para el insert del del archivo: {}: {}", archivo_ruta.display().to_string(), error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(ok) => {
                    sentencia_preparada = ok;
                }
            }
            let _ = sentencia_preparada.bind((1, archivo_ruta.display().to_string().as_str()));
            let _ = sentencia_preparada.bind((2, tamaño as i64));
            let _ = sentencia_preparada.bind((3, md5sum.as_str()));
            
            match sentencia_preparada.next() {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha insertar en la base de datos la información relativa al archivo: {}: {}", 
                    archivo_ruta.display().to_string(),
                    error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(_) => ()
            }
        }
    }
    fn buscar_duplicados(&self) {
        let select_duplicados = "
        SELECT md5sum
            FROM archivos
            GROUP BY md5sum
            HAVING COUNT(md5sum) > 1
        ";

        let mut sentencia_preparada: Statement;
        match self.bbdd.conexion.prepare(select_duplicados) {
            Err(error) => {
                eprintln!("ERROR: no se ha podido crear la sentencia preparada para consultar duplicados: {}", error);
                process::exit(1)
            }
            Ok(ok) => {
                sentencia_preparada = ok;
            }
        }

        let mut md5sum_duplicados = Vec::new();
        while let Ok(State::Row) = sentencia_preparada.next() {
            let md5sum: String;
            match sentencia_preparada.read::<String, _>("md5sum") {
                Err(error) => {
                    eprintln!("ERROR: no se ha podido obtener el md5sum en uno de los registros: {}", error);
                    continue
                }
                Ok(ok) => {
                    md5sum = ok
                }
            }
            md5sum_duplicados.push(md5sum);
        }
        
        if md5sum_duplicados.len() == 0 {
            println!("¡ENHORABUENA! No se han encontrado archivos duplicados en el directorio (y subdirectorios) analizado: {}", self.directorio.to_string_lossy());
            return;
        }
        println!("\nÁTENCIÓN: se han encontrado archivos duplicados");

        for md5_duplicado in &md5sum_duplicados {
            let consulta_md5_duplicado = "SELECT ruta, tamaño FROM 'archivos' WHERE md5sum = ?";

            let mut sentencia_preparada: Statement;
            match self.bbdd.conexion.prepare(consulta_md5_duplicado) {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha podido crear la sentencia preparada para el SELECT de un md5sum duplicado: {}: {}", md5_duplicado, error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(ok) => {
                    sentencia_preparada = ok;
                }
            }

            let _ = sentencia_preparada.bind((1, md5_duplicado.as_str()));

            println!("\n### ARCHIVO DUPLICADO ### Un mismo md5sum {} se ha encontrado en varios archivos:", md5_duplicado);

            while let Ok(State::Row) = sentencia_preparada.next() {
                let ruta_bbdd: String;
                match sentencia_preparada.read::<String, _>("ruta") {
                    Err(error) => {
                        let mensaje_error = format!("ERROR: no se ha podido recuperar la ruta de un registro de md5sum duplicado: {}", error);
                        eprintln!("{}", mensaje_error);
                        continue
                    }
                    Ok(ok) => {
                        ruta_bbdd = ok
                    }
                }
                let tamaño_bbdd: i64;
                match sentencia_preparada.read::<i64, _>("tamaño") {
                    Err(error) => {
                        let mensaje_error = format!("ERROR: no se ha podido recuperar el tamaño de un registro de md5sum duplicado: {}", error);
                        eprintln!("{}", mensaje_error);
                        continue
                    }
                    Ok(ok) => {
                        tamaño_bbdd = ok
                    }
                }

                let ruta = Path::new(ruta_bbdd.as_str());
                let nombre = obtener_nombre_desde_ruta(ruta.to_string_lossy().to_string());
                println!("~~~ {} ~~~ Tamaño: {} ~~~ {}", nombre, bytes_a_formato_humano(tamaño_bbdd), ruta_bbdd);
            }
            println!("")
        }
    }
    fn gestionar_persistencia(&self) {
        if self.persistencia.sqlite {
            println!("INFO: en {} dispones de los resultados del análisis", self.bbdd.ruta.to_string_lossy())
        } else {
            self.eliminar_base_datos()
        }
        if self.persistencia.csv {
            self.exportar_csv()
        };
    }
    fn exportar_csv(&self) {
        let ruta_csv = self.bbdd.ruta.display().to_string().replace(".sqlite", ".csv");
        let mut manejador: csv::Writer<fs::File>;
        match csv::Writer::from_path(ruta_csv.clone()) {
            Err(error) => {
                let mensaje_error = format!("ERROR: no se ha podido crear el archivo CSV: {}", error);
                eprintln!("{}", mensaje_error);
                process::exit(1)
            }
            Ok(ok) => {
                manejador = ok
            }
        }
        match manejador.write_record(&["nombre", "ruta", "tamaño", "tamaño_humano", "md5sum"]) {
            Err(error) => {
                let mensaje_error = format!("ERROR: no se ha podido el encabezado en el archivo CSV: {}", error);
                eprintln!("{}", mensaje_error);
                process::exit(1)
            }
            Ok(_) => ()
        }

        let select_registros = "SELECT ruta, tamaño, md5sum FROM archivos";
        let mut sentencia_preparada: Statement;
        match self.bbdd.conexion.prepare(select_registros) {
            Err(error) => {
                let mensaje_error = format!("ERROR: no se ha podido el encabezado en el archivo CSV: {}", error);
                eprintln!("{}", mensaje_error);
                process::exit(1)
            }
            Ok(ok) => {
                sentencia_preparada = ok;
            }
        }

        while let Ok(State::Row) = sentencia_preparada.next() {
            let ruta: String;
            match sentencia_preparada.read::<String, _>("ruta") {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha podido obtener la ruta de uno de los registros: {}", error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(ok) => {
                    ruta = ok
                }
            }

            let tamaño: i64;
            match sentencia_preparada.read::<i64, _>("tamaño") {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha podido obtener el tamaño de uno de los registros: {}", error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(ok) => {
                    tamaño = ok
                }
            }

            let md5sum: String;
            match sentencia_preparada.read::<String, _>("md5sum") {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha podido obtener el md5sum de uno de los registros: {}", error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(ok) => {
                    md5sum = ok
                }
            }

            match manejador.write_record(&[obtener_nombre_desde_ruta(ruta.clone()), ruta.clone(), tamaño.to_string(), bytes_a_formato_humano(tamaño), md5sum]) {
                Err(error) => {
                    let mensaje_error = format!("ERROR: no se ha podido incorporar el registro de {} por: {}", ruta, error);
                    eprintln!("{}", mensaje_error);
                    continue
                }
                Ok(_) => ()
            }
        }

        match manejador.flush() {
            Err(error) => {
                let mensaje_error = format!("ERROR: ha fallado el flush en el manejador por: {}", error);
                eprintln!("{}", mensaje_error);
                return;
            }
            Ok(_) => ()
        }

        println!("INFO: en {} dispones de los resultados del análisis", ruta_csv);
    }
}

fn main() {
    let argumentos = Argumentos::parse();
    let conexion: Connection;
    let ruta: PathBuf;
    match conectar_base_datos() {
        Err(error) => {
            eprintln!("{}", error);
            process::exit(1)
        }
        Ok(ok) => {
            conexion = ok.0;
            ruta = ok.1;
        }
    }

    let buscador = Buscador {
        directorio: argumentos.directorio,
        bbdd: BBDD {
            conexion,
            ruta,
        },
        persistencia: Persistencia {
            sqlite: argumentos.sqlite,
            csv: argumentos.csv,
        }    
    };

    buscador.comprobar();
    buscador.crear_tabla_archivos();
    buscador.buscar_archivos();
    buscador.buscar_duplicados();
    buscador.gestionar_persistencia();
}
