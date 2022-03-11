quietly_bind_rows <-
  function(...,
           .id = NULL) {
    suppressMessages(
      dplyr::bind_rows(
        ...,
        .id = .id
      )
    )
  }
