#' @title
#' Load NCI Metathesaurus
#'
#' @description
#' Load the NCI Metathesaurus RRF files that are downloaded
#' from \href{https://evs.nci.nih.gov/evs-download/metathesaurus-downloads}{NIH EVS}.
#'
#' @inheritParams package_arguments
#' @param ncim_version NCI Metathesaurus version
#'
#' @seealso
#'  \code{\link[metathesaurus]{setup_pg_mth}}
#'  \code{\link[pg13]{table_exists}},\code{\link[pg13]{send}},\code{\link[pg13]{append_table}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[tibble]{tibble}}
#' @rdname load_ncim
#' @export
#' @importFrom metathesaurus setup_pg_mth
#' @importFrom pg13 table_exists send append_table
#' @importFrom SqlRender render
#' @importFrom tibble tibble
load_ncim <-
function(conn,
          schema = "nci",
          path_to_rrfs,
         log_schema = "public",
         log_table = "setup_nci_log",
         ncim_version) {

  if (missing(ncim_version)) {
    stop("`ncim_version` is required.")
  }


  metathesaurus::setup_pg_mth(
    conn = conn,
    schema = schema,
    path_to_rrfs = path_to_rrfs,
    steps = c("reset_schema", "ddl_tables", "copy_rrfs", "add_indexes"),
    log_version = "",
    log_release_date = ""
  )


  if (!pg13::table_exists(conn = conn,
                          schema = log_schema,
                          table_name = log_table)) {

    pg13::send(
      conn = conn,
      sql_statement =
        SqlRender::render(
          "
        CREATE TABLE @log_schema.@log_table (
            sn_datetime TIMESTAMP without TIME ZONE,
            nci_type TEXT,
            nci_version VARCHAR(25)

        )
        ",
          log_schema = log_schema,
          log_table = log_table
        )
    )


  }

  pg13::append_table(conn = conn,
                     schema = log_schema,
                     table = log_table,
                     data = tibble::tibble(
                       sn_datetime = Sys.time(),
                       nci_type = "Metathesaurus",
                       nci_version = ncim_version
                     ))


}
