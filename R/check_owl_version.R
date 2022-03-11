

check_owl_version <-
  function() {
url <-
  "https://evs.nci.nih.gov/ftp1/NCI_Thesaurus"
ftp_owl_files <-
  rvest::read_html(
    x = url
  ) %>%
  rvest::html_table() %>%
  quietly_bind_rows() %>%
  dplyr::select(Name) %>%
  unlist() %>%
  unname() %>%
  grep(
    pattern = "(Thesaurus.*?)([0-9]{2}[.]{1}[0-9]{2}[a-z]{1})([.]{1}OWL.zip$)",
    value = TRUE
  )

ftp_versions <-
ftp_owl_files %>%
  stringr::str_replace_all(
    pattern = "(Thesaurus.*?)([0-9]{2}[.]{1}[0-9]{2}[a-z]{1})([.]{1}OWL.zip$)",
    replacement = "\\2") %>%
  unique() %>%
  sort(decreasing = TRUE)

newest_ftp_version <-
  ftp_versions[1]


installed_versions <-
list.files(
  file.path(
    here::here(),
    "inst",
    "data",
    "omop")
)

if (!(newest_ftp_version %in% installed_versions)) {

  cli::cli_alert_warning(
    text =
      "NCI Thesaurus OWL Version {.emph {newest_ftp_version}} available for download at {.url {url}}.",
    wrap = TRUE
  )

  cli::cli_alert_info(
    text = "Versions in installation directory:"
  )
  names(installed_versions) <- "*"
  cli::cli_bullets(
    text = installed_versions
  )


}

}
