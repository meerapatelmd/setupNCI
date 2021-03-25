



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
