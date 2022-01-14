makedirs <-
  function(folder_path,
           verbose = TRUE) {

folder_path <- path.expand(folder_path)
folder_path_parts <-
stringr::str_split(folder_path,
                   pattern = .Platform$file.sep,
                   n = Inf)[[1]]

for (i in seq_along(folder_path_parts)) {

  if (i>1) {
    x <-
    paste(folder_path_parts[1:i],
          collapse = .Platform$file.sep)

    if (!dir.exists(x)) {

      dir.create(x)

      if (verbose) {

        cli::cli_inform(
          "{cli::symbol$tick} '{x}' created."
        )


      }

    } else {

      if (verbose) {

        cli::cli_inform(
          "{cli::symbol$en_dash} '{x}' already exists."
        )


      }


    }



  }


}

folder_path

}
