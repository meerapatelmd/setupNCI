tbl_to_gfm <-
  function(csv_data) {
    # csv to df to gfm
    # echo '| prior_cai_domain | prior_cai_domain_count | new_cai_domain | new_cai_domain_count |  ' >> tmp/README.md
    # echo '| prior_cai_domain | prior_cai_domain_count | new_cai_domain | new_cai_domain_count |  ' >> tmp/README.md
    col_names_a <- glue::glue('| {paste(colnames(csv_data), collapse = " | ")} |  \n')
    col_names_b <- str_replace_all(col_names_a, pattern = "[^| ]", "-")

    gfm_tbl <-
      c(col_names_a,
        col_names_b)

    for (i in 1:nrow(csv_data)) {

      gfm_tbl <-
        c(gfm_tbl,
          glue::glue('| {paste(unname(unlist(csv_data[i,])), collapse = " | ")} |  \n'))

    }

    gfm_tbl
  }
