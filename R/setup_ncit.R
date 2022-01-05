#' @title
#' Run NCI Thesaurus Setup
#'
#' @description
#' Load the NCI Thesaurus and the Code to CUI Map.
#'
#' @inheritParams package_arguments
#' @inheritParams load_ncim
#'
#' @rdname setup_ncit
#'
#' @export

setup_ncit <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "ncit",
           log_schema = "public",
           log_table = "setup_ncit_log") {
    if (missing(conn)) {
      conn <-
        eval(rlang::parse_expr(conn_fun))

      on.exit(pg13::dc(conn = conn))
    }

    log_table_exists <-
      pg13::table_exists(
        conn = conn,
        schema = log_schema,
        table_name = log_table
      )

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


    # Identifying most recent version of the FLAT file
    flat_files <-
      ftp_contents %>%
      rubix::filter_at_grepl(
        col = name,
        grepl_phrase = "^Thesaurus[_]{1}.*FLAT[.]{1}zip$",
        ignore.case = FALSE
      )

    most_recent_flat_file <-
      flat_files %>%
      dplyr::arrange(dplyr::desc(last_modified)) %>%
      dplyr::filter(dplyr::row_number() == 1) %>%
      dplyr::select(name) %>%
      unlist() %>%
      unname()


    flat_file_version <-
      stringr::str_replace_all(
        string = most_recent_flat_file,
        pattern = "^Thesaurus[_]{1}(.*?)[.]{1}FLAT[.]{1}zip$",
        replacement = "\\1"
      )

    if (log_table_exists) {
      version_log_rows <-
        pg13::query(
          conn = conn,
          sql_statement =
            glue::glue(
              "SELECT COUNT(*) FROM {log_schema}.{log_table} WHERE ncit_version = '{flat_file_version}';"
            )
        ) %>%
        unlist() %>%
        unname()
    } else {
      version_log_rows <- 0
    }


    if (version_log_rows == 0) {
      nci_temp_dir <- tempdir(check = TRUE)
      flat_file_dl_path <- file.path(nci_temp_dir, most_recent_flat_file)
      download.file(
        url = sprintf(
          "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/%s",
          most_recent_flat_file
        ),
        destfile = flat_file_dl_path,
        cacheOK = FALSE
      )
      utils::unzip(
        zipfile = flat_file_dl_path,
        exdir = nci_temp_dir
      )

      thesaurus_text_path <- file.path(nci_temp_dir, "Thesaurus.txt")
      thesaurus_text <-
        readr::read_tsv(
          file = thesaurus_text_path,
          col_names = c(
            "code",
            "concept_name",
            "parents",
            "synonyms",
            "definition",
            "display_name",
            "concept_status",
            "semantic_type"
          ),
        )

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
        table_name = "thesaurus",
        data = thesaurus_text,
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
            ncit_version VARCHAR(25),
            row_count BIGINT

        );
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
          ncit_version = flat_file_version,
          row_count = nrow(thesaurus_text)
        )
      )
    }
  }
