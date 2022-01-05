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
           steps = c(
             "reset_schema",
             "ddl_tables",
             "copy_rrfs",
             "copy_mrhier",
             "add_indexes",
             "log"
           ),
           log_schema = "public",
           log_table = "setup_nci_log",
           ncim_version,
           mrconso_only = FALSE,
           omop_only = FALSE,
           english_only = TRUE,
           verbose = TRUE,
           render_sql = TRUE,
           render_only = FALSE) {
    if (missing(ncim_version)) {
      stop("`ncim_version` is required.")
    }

    path_to_rrfs <- normalizePath(
      path = path_to_rrfs,
      mustWork = TRUE
    )

    ##### Setup Objects
    errors <- vector()

    tables <-
      c(
        "AMBIGLUI",
        "AMBIGSUI",
        "DELETEDCUI",
        "DELETEDLUI",
        "DELETEDSUI",
        "MERGEDCUI",
        "MERGEDLUI",
        "MRAUI",
        "MRCOLS",
        "MRCONSO",
        "MRCUI",
        "MRCXT",
        "MRDEF",
        "MRDOC",
        "MRFILES",
        "MRHIER",
        "MRHIST",
        "MRMAP",
        "MRRANK",
        "MRREL",
        "MRSAB",
        "MRSAT",
        "MRSMAP",
        "MRSTY",
        "MRXNS_ENG",
        "MRXNW_ENG",
        "MRXW_BAQ",
        "MRXW_CHI",
        "MRXW_CZE",
        "MRXW_DAN",
        "MRXW_DUT",
        "MRXW_ENG",
        "MRXW_EST",
        "MRXW_FIN",
        "MRXW_FRE",
        "MRXW_GER",
        "MRXW_GRE",
        "MRXW_HEB",
        "MRXW_HUN",
        "MRXW_ITA",
        "MRXW_JPN",
        "MRXW_KOR",
        "MRXW_LAV",
        "MRXW_NOR",
        "MRXW_POL",
        "MRXW_POR",
        "MRXW_RUS",
        "MRXW_SCR",
        "MRXW_SPA",
        "MRXW_SWE",
        "MRXW_TUR"
      )

    if (mrconso_only) {
      tables <- "MRCONSO"
    }

    if (omop_only) {
      tables <- c("MRCONSO", "MRHIER", "MRMAP", "MRSMAP", "MRSAT", "MRREL")
    }

    if (english_only) {
      tables <- tables[!(tables %in% c("MRXW_BAQ", "MRXW_CHI", "MRXW_CZE", "MRXW_DAN", "MRXW_DUT", "MRXW_EST", "MRXW_FIN", "MRXW_FRE", "MRXW_GER", "MRXW_GRE", "MRXW_HEB", "MRXW_HUN", "MRXW_ITA", "MRXW_JPN", "MRXW_KOR", "MRXW_LAV", "MRXW_NOR", "MRXW_POL", "MRXW_POR", "MRXW_RUS", "MRXW_SCR", "MRXW_SPA", "MRXW_SWE", "MRXW_TUR"))]
    }

    if ("reset_schema" %in% steps) {
      reset_schema(
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }


    if ("ddl_tables" %in% steps) {
      ddl_tables(
        conn = conn,
        schema = schema,
        tables = tables,
        verbose = verbose,
        render_sql = render_sql
      )
    }

    if ("copy_rrfs" %in% steps) {
      copy_rrfs(
        path_to_rrfs = path_to_rrfs,
        tables = tables,
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }

    if ("copy_mrhier" %in% steps) {
      copy_mrhier(
        path_to_rrfs = path_to_rrfs,
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }


    if ("add_indexes" %in% steps) {
      add_indexes(
        conn = conn,
        schema = schema,
        verbose = verbose,
        render_sql = render_sql
      )
    }

    if ("log" %in% steps) {
      if (!pg13::table_exists(
        conn = conn,
        schema = log_schema,
        table_name = log_table
      )) {
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

      pg13::append_table(
        conn = conn,
        schema = log_schema,
        table = log_table,
        data = tibble::tibble(
          sn_datetime = Sys.time(),
          nci_type = "Metathesaurus",
          nci_version = ncim_version
        )
      )
    }
  }
