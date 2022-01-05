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
           path_to_rrfs,
           ncim_version,
           log_schema = "public",
           log_table = "setup_ncit_log") {


    if (missing(conn)) {

      conn <-
        eval(rlang::parse_expr(conn_fun))

      on.exit(pg13::dc(conn = conn))


    }

    load_ncit(conn = conn,
              schema = schema,
              log_schema = log_schema,
              log_table = log_table)


    load_code_cui_map(
      conn = conn,
      schema = schema,
      log_schema = log_schema,
      log_table = log_table
    )


  }
