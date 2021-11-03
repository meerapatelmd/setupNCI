#' @title
#' MRHIER
#'
#' @description
#' Due to perpetual errors, MRHIER is processed in a
#' roundabout manner. This is because some rows contain
#' an extra unknown column. In the script, anything
#' with an index of 11 or greater is removed.
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
      DROP TABLE IF EXISTS {schema}.raw_mrhier;
      CREATE TABLE {schema}.raw_mrhier (
          SOURCE text NOT NULL
      );

      COPY {schema}.raw_mrhier FROM '{mrhier_path}';

      DROP TABLE IF EXISTS {schema}.raw_mrhier0;
      CREATE TABLE {schema}.raw_mrhier0 AS (
        SELECT * FROM {schema}.raw_mrhier;
      )

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
               render_sql = render_sql,
               checks = "")


    # Reporting to make sure that the first and last lines are the same
    raw_rows <-
      pg13::query(conn = conn,
                  sql_statement = glue::glue("SELECT COUNT(*) FROM {schema}.raw_mrhier;"),
                  verbose = verbose,
                  render_sql = render_sql,
                  checks = "")
    final_rows <-
      pg13::query(conn = conn,
                  sql_statement = glue::glue("SELECT COUNT(*) FROM {schema}.mrhier;"),
                  verbose = verbose,
                  render_sql = render_sql,
                  checks = "")

    secretary::typewrite("RAW_MRHIER Rows:", raw_rows$count)
    secretary::typewrite("MRHIER Rows:", final_rows$count)


    raw_line1 <-
      pg13::query(conn = conn,
                  sql_statement = glue::glue("SELECT * FROM {schema}.raw_mrhier LIMIT 1;"),
                  verbose = verbose,
                  render_sql = render_sql,
                  checks = "")

    final_line1 <-
      pg13::query(conn = conn,
                  sql_statement = glue::glue("SELECT * FROM {schema}.mrhier LIMIT 1;"),
                  verbose = verbose,
                  render_sql = render_sql,
                  checks = "")

    secretary::typewrite("RAW_MRHIER First Line:")
    huxtable::print_screen(raw_line1)
    secretary::typewrite("MRHIER First Line:")
    huxtable::print_screen(final_line1)

    raw_line_n <-
      pg13::query(conn = conn,
                  sql_statement = glue::glue("SELECT * FROM {schema}.raw_mrhier OFFSET (SELECT count(*) FROM {schema}.raw_mrhier)-1;"),
                  verbose = verbose,
                  render_sql = render_sql,
                  checks = "")

    final_line_n <-
      pg13::query(conn = conn,
                  sql_statement = glue::glue("SELECT * FROM {schema}.mrhier OFFSET (SELECT count(*) FROM {schema}.mrhier)-1;"),
                  verbose = verbose,
                  render_sql = render_sql,
                  checks = "")

    secretary::typewrite("RAW_MRHIER Last Line:")
    huxtable::print_screen(raw_line_n)
    secretary::typewrite("MRHIER Last Line:")
    huxtable::print_screen(final_line_n)




  }
