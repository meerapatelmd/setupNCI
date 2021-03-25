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
                                                           format = "%Y-%m-%d %H:%M"))

    dat_file <-
      ftp_contents %>%
      rubix::filter_at_grepl(col = name,
                             grepl_phrase = "[.]{1}dat$") %>%
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
      url = sprintf("https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/%s",
                          dat_file),
      destfile = dl_path,
      cacheOK = FALSE
    )

    dat_data <-
            readr::read_delim(
                    file = dl_path,
                    delim = "|",
                    col_names = c("code",
                                  "cui",
                                  "concept_name",
                                  "str",
                                  "empty")
            ) %>%
      dplyr::select(-empty)

    if (!pg13::schema_exists(conn = conn,
                             schema = schema)) {
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
                         nci_type = "Code CUI Map",
                         nci_version = dat_file_version
                       ))


  }
