-- Script Pig genérico para wordcount con limpieza básica y stopwords

-- Parámetros:
--  INPUT: ruta en HDFS al archivo de entrada
--  OUTPUT: ruta en HDFS para el resultado
--  STOPWORDS: ruta local al archivo de stopwords (montado en el contenedor)
--  N: cantidad de palabras a dejar en el top

raw = LOAD '$INPUT' USING TextLoader() AS (line:chararray);

lowered = FOREACH raw GENERATE LOWER(line) AS line;

-- Reemplaza todo lo que no sea letra, número o espacio por espacio
clean = FOREACH lowered GENERATE
    REPLACE(line, '[^a-záéíóúñ0-9 ]', ' ') AS line;

-- Tokeniza por espacios
tokens = FOREACH clean GENERATE FLATTEN(TOKENIZE(line)) AS word;

-- Filtra vacíos
non_empty = FILTER tokens BY word IS NOT NULL AND word != '';

-- Cargar stopwords (desde sistema de archivos local del contenedor)
stopwords = LOAD '$STOPWORDS' USING TextLoader() AS (sw:chararray);

-- Join para descartar las que están en stopwords
joined = JOIN non_empty BY word LEFT OUTER, stopwords BY sw;

filtered = FILTER joined BY stopwords::sw IS NULL;

grouped = GROUP filtered BY non_empty::word;

counts = FOREACH grouped GENERATE
    group AS word,
    COUNT(filtered) AS freq;

ordered = ORDER counts BY freq DESC;

topN = LIMIT ordered $N;

STORE topN INTO '$OUTPUT' USING PigStorage('\t');
