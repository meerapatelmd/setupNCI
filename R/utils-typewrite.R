ts_typewrite <-
  function(...,
           collapse = "") {

    x <-
    glue::glue(
    paste(
    unlist(rlang::list2(...)),
    collapse = collapse)
    )


    glue::glue(
      "[{as.character(Sys.time())}] {x}"
    )


  }
