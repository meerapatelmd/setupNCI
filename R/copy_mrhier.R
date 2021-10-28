#' @title
#' MRHIER
#'
#' @description
#' Due to perpetual errors, MRHIER is processed in a
#' roundabout manner.
#'
#' @seealso
#'  \code{\link[pg13]{lsSchema}},\code{\link[pg13]{send}}
#'  \code{\link[SqlRender]{render}}
#' @rdname copy_mrhier
#' @export
#' @importFrom glue glue
#' @importFrom pg13 send


copy_mrhier <-
  function(path_to_rrfs,
           conn,
           schema,
           verbose = TRUE,
           render_sql = TRUE) {

    mrhier_path <-
      path.expand(file.path(path_to_rrfs, "MRHIER.RRF"))

    sql_statement <-
      glue::glue(
      "
      DROP TABLE IF EXISTS {schema}.raw_mrhier0;
      CREATE TABLE {schema}.raw_mrhier0 (
          SOURCE text NOT NULL
      );

      COPY {schema}.raw_mrhier0 FROM '{mrhier_path}';

      DROP TABLE IF EXISTS {schema}.raw_mrhier1;
      CREATE TABLE {schema}.raw_mrhier1 AS (
      SELECT
       ROW_NUMBER() OVER() AS row_id,
       m.*
      FROM {schema}.raw_mrhier0 m
      );


      DROP TABLE IF EXISTS {schema}.raw_mrhier2;
      CREATE TABLE {schema}.raw_mrhier2 AS (
      SELECT
       row_id,
       unnest(string_to_array(source, '|')) AS source_split
      FROM {schema}.raw_mrhier1 m
      ORDER BY row_id
      );



      DROP TABLE IF EXISTS {schema}.raw_mrhier3;
      CREATE TABLE {schema}.raw_mrhier3 AS (
      SELECT
       m.row_id,
       ROW_NUMBER() OVER (PARTITION BY row_id) AS col_index,
       CASE WHEN m.source_split = '' THEN NULL ELSE m.source_split END val
      FROM {schema}.raw_mrhier2 m
      ORDER BY row_id
      );


      SELECT *
      FROM {schema}.raw_mrhier3 m
      WHERE col_index = 11
      ;



      DROP TABLE IF EXISTS {schema}.raw_mrhier4;
      CREATE TABLE {schema}.raw_mrhier4 AS (
      SELECT *
      FROM crosstab('SELECT row_id, col_index, val FROM {schema}.raw_mrhier3 WHERE col_index < 11 ORDER BY 1,2', 'SELECT DISTINCT col_index FROM {schema}.raw_mrhier3 WHERE col_index < 11 ORDER BY 1')
      AS (rowid bigint,
      	cui varchar(10),
      	aui varchar(9),
      	cxn int,
      	paui varchar(10),
      	sab varchar(40),
      	rela varchar(100),
      	ptr text,
      	hcd varchar(100),
      	cvf int,
      	filler_col int)
      	);


      DROP TABLE IF EXISTS {schema}.mrhier;
      CREATE TABLE {schema}.mrhier AS (
      SELECT DISTINCT cui, aui, cxn, paui, sab, rela, ptr, hcd, cvf, filler_col FROM {schema}.raw_mrhier4);


      DROP TABLE {schema}.raw_mrhier0;
      DROP TABLE {schema}.raw_mrhier1;
      DROP TABLE {schema}.raw_mrhier2;
      DROP TABLE {schema}.raw_mrhier3;
      DROP TABLE {schema}.raw_mrhier4;
      "
      )

    pg13::send(conn = conn,
               sql_statement = sql_statement,
               verbose = verbose,
               render_sql = render_sql)



  }
