#' @title
#' Download and Extract NCIt OWL Files
#'
#' @description
#' If a diff is detected at the FTP site involving the
#' Thesaurus OWL files, the new versions will be downloaded,
#' extracted, and reported to the console.
#'
#' @rdname download_owl
#'
#'
#' @export
#' @import rvest

download_owl <-
  function(owl_folder    = "/Users/mpatel/terminology/NCIT") {
ftp_menu <-
rvest::read_html(
  x = "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus") %>%
  rvest::html_table() %>%
  bind_rows() %>%
  select(Name,
         `Last modified`)

cached_ftp_menu <-
R.cache::loadCache(
  key = list(""),
  dirs = "setupNCI/ftp_menu"
)

diff_df <-
setdiff(cached_ftp_menu,
        ftp_menu) %>%
  dplyr::filter_at(vars(Name),
                   ~grepl(pattern = "_[0-9]{2}.[0-9]{2}[a-z]{1}.OWL.zip$",
                          x = .))

if (nrow(diff_df)>0) {
  new_owl_files <-
    diff_df %>%
      extract(
        col = Name,
        into = c('OWL Type',
                 'Version'),
        regex = "(^.*?)_([0-9]{1}.*?).OWL.zip",
        remove = FALSE
      ) %>%
      group_by(`OWL Type`) %>%
      arrange(desc(`Last modified`)) %>%
      dplyr::filter(row_number()==1) %>%
      ungroup() %>%
      select(Name) %>%
      unlist() %>%
      unname()


  if (length(new_owl_files)==2) {

    new_version <-
    diff_df %>%
      extract(
        col = Name,
        into = c('OWL Type',
                 'Version'),
        regex = "(^.*?)_([0-9]{1}.*?).OWL.zip",
        remove = FALSE
      ) %>%
      distinct(Version) %>%
      unlist() %>%
      unname()

    for (new_owl_file in new_owl_files) {

      local_owl_path <-
        file.path(owl_folder,
                  new_owl_file)
      Sys.sleep(3)
      download.file(
        file.path("https://evs.nci.nih.gov/ftp1/NCI_Thesaurus",
                  new_owl_file),
        destfile = local_owl_path)

      unzip(zipfile = local_owl_path,
            exdir = owl_folder)


    }

    R.cache::saveCache(
      object = ftp_menu,
      key    = list(""),
      dirs   = "setupNCI/ftp_menu"
    )

    cli::cli_inform(
    "
    New version {new_version} was programmatically detected (https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/).
    The OWL files were successfully downloaded and extracted to '{owl_folder}'.")


  } else {


    cli::cli_inform(
      "There must be 2 OWL files and {length(new_owl_files)} detected.
      The OWL files can be downloaded and extracted manually from https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/.")

  }

} else {

  cli::cli_inform(
    "No diff was programmatically detected from previous update.
    The OWL files can be downloaded and extracted manually from https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/.")


}


}


