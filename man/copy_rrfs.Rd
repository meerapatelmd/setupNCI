% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/load_ncim_int.R
\name{copy_rrfs}
\alias{copy_rrfs}
\title{Copy RRF to Table}
\usage{
copy_rrfs(
  path_to_rrfs,
  tables,
  conn,
  schema,
  verbose = TRUE,
  render_sql = TRUE
)
}
\description{
Copy RRF to Table
}
\seealso{
\code{\link[tibble]{as_tibble}}
\code{\link[dplyr]{mutate}},\code{\link[dplyr]{filter}},\code{\link[dplyr]{select}},\code{\link[dplyr]{distinct}}
\code{\link[stringr]{str_remove}}
\code{\link[progress]{progress_bar}}
\code{\link[SqlRender]{render}}
\code{\link[pg13]{send}}
\code{\link[secretary]{c("typewrite", "typewrite")}},\code{\link[secretary]{typewrite_warning}},\code{\link[secretary]{italicize}}
\code{\link[purrr]{map}}
}
