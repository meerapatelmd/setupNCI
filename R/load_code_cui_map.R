#' @title
#' Load the Code to CUI Map
#'
#' @description
#' The Code to CUI Map is provided as a `dat` file is written
#' to the `CODE_CUI_MAP` table in the given schema.
#'
#' @inheritParams package_arguments
#'
#' @details
#' The `dat` file contains the map of NCI Thesaurus (NCIt) codes
#' to NCI Metathesaurus (NCIMeta) CUIs (concept unique
#' identifier). The CUIs are either derived from the UMLS
#' Metathesaurus (CUIs that begin with "C-digit") or
#' specific to the NCI Metathesaurus (CUIs that begin with
#' "CL").  This mapping file is created for each release of
#' the NCIMeta, which is published on a quarterly schedule
#' hence is not current with every release of NCIt which is
#' published monthly. Newly created Thesaurus concepts will
#' not appear in this file until the next release of the
#' Metathesaurus, and newly retired Thesaurus concepts will
#' continue to appear until the next release of the
#' Metathesaurus.
#'
#' @seealso
#'  \code{\link[xml2]{read_xml}}
#'  \code{\link[rvest]{html_nodes}},\code{\link[rvest]{html_table}}
#'  \code{\link[dplyr]{select}},\code{\link[dplyr]{mutate}}
#'  \code{\link[rubix]{format_colnames}},\code{\link[rubix]{filter_at_grepl}}
#'  \code{\link[lubridate]{as_date}}
#'  \code{\link[stringr]{str_replace}}
#'  \code{\link[readr]{read_delim}}
#'  \code{\link[pg13]{schema_exists}},\code{\link[pg13]{create_schema}},\code{\link[pg13]{write_table}},\code{\link[pg13]{table_exists}},\code{\link[pg13]{send}},\code{\link[pg13]{append_table}}
#'  \code{\link[SqlRender]{render}}
#'  \code{\link[tibble]{tibble}}
#' @rdname load_code_cui_map
#' @export
#' @importFrom xml2 read_html
#' @importFrom rvest html_node html_table
#' @importFrom dplyr select mutate
#' @importFrom rubix format_colnames filter_at_grepl
#' @importFrom lubridate as_datetime
#' @importFrom stringr str_replace_all
#' @importFrom readr read_delim
#' @importFrom pg13 schema_exists create_schema write_table table_exists send append_table
#' @importFrom SqlRender render
#' @importFrom tibble tibble
#' @importFrom magrittr %>%

load_code_cui_map <-
  function(conn,
           schema = "nci",
           log_schema = "public",
           log_table = "setup_nci_log") {
    if (missing(conn)) {
      stop("`conn` is missing.")
    }

    response <-
      xml2::read_html(x = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/")

    ftp_contents <-
      response %>%
      rvest::html_node("table") %>%
      rvest::html_table() %>%
      dplyr::select(Name, `Last modified`) %>%
      rubix::format_colnames() %>%
      dplyr::mutate(last_modified = lubridate::as_datetime(last_modified,
        format = "%Y-%m-%d %H:%M"
      ))

    dat_file <-
      ftp_contents %>%
      rubix::filter_at_grepl(
        col = name,
        grepl_phrase = "[.]{1}dat$"
      ) %>%
      dplyr::select(name) %>%
      unlist() %>%
      unname()

    dat_file_version <-
      stringr::str_replace_all(
        string = dat_file,
        pattern = "^nci_code_cui_map_(.*?).dat",
        replacement = "\\1"
      )

    td <- tempdir(check = TRUE)

    dl_path <- file.path(td, dat_file)
    download.file(
      url = sprintf(
        "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/%s",
        dat_file
      ),
      destfile = dl_path,
      cacheOK = FALSE
    )

    dat_data <-
      readr::read_delim(
        file = dl_path,
        delim = "|",
        col_names = c(
          "code",
          "cui",
          "concept_name",
          "str",
          "empty"
        )
      ) %>%
      dplyr::select(-empty)

    if (!pg13::schema_exists(
      conn = conn,
      schema = schema
    )) {
      pg13::create_schema(
        conn = conn,
        schema = schema
      )
    }


    pg13::write_table(
      conn = conn,
      schema = schema,
      table_name = "code_cui_map",
      data = dat_data,
      drop_existing = TRUE
    )


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
        nci_type = "Code CUI Map",
        nci_version = dat_file_version
      )
    )
  }
