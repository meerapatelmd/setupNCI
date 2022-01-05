#' @title
#' Run NCI Setup
#'
#' @description
#' Load the NCI Metathesaurus RRF files, followed by the
#' NCI Thesaurus and the Code to CUI Map.
#'
#' @inheritParams package_arguments
#' @inheritParams load_ncim
#'
#' @rdname run_setup
#'
#' @export

run_setup <-
  function(conn,
           conn_fun = "pg13::local_connect()",
           schema = "nci",
           path_to_rrfs,
           ncim_version,
           log_schema = "public",
           log_table = "setup_nci_log") {
    .Deprecated(new = "setup_ncim or setup_ncit")

    secretary::press_enter()

    if (missing(conn)) {
      conn <-
        eval(rlang::parse_expr(conn_fun))

      on.exit(pg13::dc(conn = conn))
    }

    load_ncim(
      conn = conn,
      schema = schema,
      path_to_rrfs = path_to_rrfs,
      log_schema = log_schema,
      log_table = log_table,
      ncim_version = ncim_version
    )


    load_ncit(
      conn = conn,
      schema = schema,
      log_schema = log_schema,
      log_table = log_table
    )


    load_code_cui_map(
      conn = conn,
      schema = schema,
      log_schema = log_schema,
      log_table = log_table
    )
  }
