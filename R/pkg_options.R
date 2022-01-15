# Variable, global to package's namespace.

#' @keywords internal
#' @import settings
MYPKGOPTIONS <-
  options_manager(
    output_folder=NULL
  )


#' Set or get options for my package
#'
#' @param ... Option names to retrieve option values or \code{[key]=[value]} pairs to set options.
#'
#' @section Supported options:
#' The following options are supported
#' \itemize{
#'  \item{\code{output_folder}}{(\code{character};1) Folder to which outputs for data processing will be written to. }
#' }
#'
#' @export
#' @import settings
pkg_options <- function(...){
  # protect against the use of reserved words.
  stop_if_reserved(...)
  MYPKGOPTIONS(...)
}
