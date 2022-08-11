library(tidyverse)


fns <-
list.files("dev/omop",
           full.names = TRUE,
           pattern = "csv$")

omop_data <-
  fns %>%
  purrr::map(
    function(x) readr::read_csv(x)) %>%
  purrr::set_names(xfun::sans_ext(basename(fns)))


all(omop_data$CONCEPT_ANCESTOR$ancestor_concept_id %in% omop_data$CONCEPT$concept_id)

omop_data$CONCEPT_ANCESTOR %>%
  dplyr::filter(!(ancestor_concept_id %in% omop_data$CONCEPT$concept_id))

omop_data$CONCEPT %>%
  dplyr::full_join(omop_data$CONCEPT_ANCESTOR,
                   by = c("concept_id" = "ancestor_concept_id")) %>%
  dplyr::distinct() %>%
  dplyr::filter(is.na(concept_id))
