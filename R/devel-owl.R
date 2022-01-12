library(tidyverse)
library(reticulate)



py_file <-
system.file(package = "setupNCI", "py", "parse_ncit_owl.py")
py_run_file(py_file)
