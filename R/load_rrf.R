





#https://evs.nci.nih.gov/evs-download/metathesaurus-downloads

load_rrf <-
function(conn,
          schema = "nci",
          path_to_rrfs,
         steps = c("reset_schema", "ddl_tables", "copy_rrfs", "add_indexes")) {


  metathesaurus::setup_pg_mth(
    conn = conn,
    schema = schema,
    path_to_rrfs = path_to_rrfs,

  )


}
