



ncit <- readr::read_tsv(file = "~/Desktop/Thesaurus.txt",
                        col_names = c('code',
                                      'concept_name',
                                      'parents',
                                      'synonyms',
                                      'definition',
                                      'display_name',
                                      'concept_status',
                                      'semantic_type'),
                        )


scrape <-
function (x, encoding = "", ..., options = c("RECOVER", "NOERROR",
                                             "NOBLANKS"), sleep_time = 5, verbose = TRUE) {
  if (verbose) {
    secretary::typewrite("Sleeping...")
  }
  Sys.sleep(sleep_time)
  if (verbose) {
    secretary::typewrite("Scraping...")
  }
  response <- tryCatch(xml2::read_html(x = x, encoding = encoding,
                                       ..., options = options), error = function(e) NULL)
  if (is.null(response)) {
    if (verbose) {
      secretary::typewrite("Scraping...failed.")
      secretary::typewrite("Scraping again...")
    }
    Sys.sleep(sleep_time)
    response <- tryCatch(xml2::read_html(x = x, encoding = encoding,
                                         ..., options = options), error = function(e) NULL)
    secretary::typewrite("Scraping again...complete.")
  }
  else {
    secretary::typewrite("Scraping...complete.")
  }
  closeAllConnections()
  response
}

ncit_code <- "C100862"
ncit_code_url <- sprintf("https://ncithesaurus.nci.nih.gov/ncitbrowser/pages/concept_details.jsf?dictionary=NCI_Thesaurus&code=%s&ns=ncit&type=synonym&key=null&b=1&n=0&vse=null#", ncit_code)
