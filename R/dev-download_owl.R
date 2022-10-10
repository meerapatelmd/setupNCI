#' @title
#' Download and Extract NCIt OWL Files
#'
#' @description
#' If a diff is detected at the FTP site involving the
#' Thesaurus OWL files or the `owl_folder` argument and its
#' file list, the newest version of both NCI Thesaurus OWL files
#' will be downloaded, extracted, and reported to the console.
#'
#' @rdname download_owl
#'
#'
#' @export
#' @import rvest
#' @import cli
#' @import tidyverse

download_owl <-
  function(output_folder) {
    if (missing(output_folder)) {
      output_folder <-
        pkg_options("output_folder")

      if (is.null(output_folder)) {
        stop(
          "`output_folder` value is required or be globally set using `pkg_options()`.",
          call. = FALSE
        )
      }
    }

    owl_folder <-
      file.path(
        output_folder,
        "owl"
      )

    owl_folder <-
      makedirs(
        folder_path = owl_folder,
        verbose = FALSE
      )


    key <-
      c(
        owl_folder,
        as.list(list.files(path = owl_folder))
      )

    ftp_menu <-
      rvest::read_html(
        x = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus"
      ) %>%
      rvest::html_table() %>%
      quietly_bind_rows() %>%
      select(
        Name,
        `Last modified`
      )

    cached_ftp_menu <-
      R.cache::loadCache(
        key = key,
        dirs = "setupNCI/ftp_menu"
      )

    if (!is.null(cached_ftp_menu)) {
      if (!setequal(
        cached_ftp_menu,
        ftp_menu
      )) {
        diff_df <-
          ftp_menu %>%
          dplyr::filter_at(
            vars(Name),
            ~ grepl(
              pattern = "_[0-9]{2}.[0-9]{2}[a-z]{1}.OWL.zip$",
              x = .
            )
          )
      } else {
        diff_df <-
          tibble::tribble(~`Name`, ~`Last modified`)
      }
    } else {
      diff_df <-
        ftp_menu %>%
        dplyr::filter_at(
          vars(Name),
          ~ grepl(
            pattern = "_[0-9]{2}.[0-9]{2}[a-z]{1}.OWL.zip$",
            x = .
          )
        )
    }

    if (nrow(diff_df) > 0) {
      new_owl_files <-
        diff_df %>%
        extract(
          col = Name,
          into = c(
            "OWL Type",
            "Version"
          ),
          regex = "(^.*?)_([0-9]{1}.*?).OWL.zip",
          remove = FALSE
        ) %>%
        group_by(`OWL Type`) %>%
        arrange(desc(`Version`)) %>%
        dplyr::filter(row_number() == 1) %>%
        ungroup() %>%
        select(Name) %>%
        unlist() %>%
        unname()


      if (length(new_owl_files) == 2) {
        new_version <-
          diff_df %>%
          extract(
            col = Name,
            into = c(
              "OWL Type",
              "Version"
            ),
            regex = "(^.*?)_([0-9]{1}.*?).OWL.zip",
            remove = FALSE
          ) %>%
          distinct(Version) %>%
          arrange(Version) %>%
          dplyr::filter(row_number() == 1) %>%
          unlist() %>%
          unname()

        for (new_owl_file in new_owl_files) {
          local_owl_path <-
            file.path(
              owl_folder,
              new_owl_file
            )
          Sys.sleep(3)
          download.file(
            file.path(
              "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus",
              new_owl_file
            ),
            destfile = local_owl_path
          )

          unzip(
            zipfile = local_owl_path,
            exdir = owl_folder
          )
        }

        key <-
          c(
            owl_folder,
            as.list(list.files(path = owl_folder))
          )

        R.cache::saveCache(
          object = ftp_menu,
          key    = key,
          dirs   = "setupNCI/ftp_menu"
        )

        cli::cli_inform(
          "
    New version {new_version} was programmatically detected (https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/).
    The OWL files were successfully downloaded and extracted to '{owl_folder}'."
        )
      } else {
        cli::cli_inform(
          "There must be 2 OWL files and {length(new_owl_files)} detected.
      The OWL files can be downloaded and extracted manually from https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/."
        )
      }
    } else {
      cli::cli_inform(
        "No diff was programmatically detected from previous update ({owl_folder}).
    The OWL files can be downloaded and extracted manually from https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/."
      )
    }
  }
