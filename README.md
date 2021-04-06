
<!-- README.md is generated from README.Rmd. Please edit that file -->

# setupNCI

<!-- badges: start -->

<!-- badges: end -->

The goal of setupNCI is to instantiate the latest versions of the NCI
Metathesaurus and NCI Thesaurus along with the map between the two in a
Postgres database.

## Installation

You can install the development version from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("meerapatelmd/setupNCI")
```

## Execution

A complete instance of instantiation involves 3 steps:

``` r
conn <- pg13::local_connect()

# 1. Load NCI Metathesaurus  
load_ncim(conn = conn, 
          path_to_rrfs = "~/Desktop/Metathesaurus.RRF/META",
          ncim_version = "202008")

# 2. Load NCI Thesaurus  
load_ncit(conn = conn)

# 3. Load Code to CUI Map  
load_code_cui_map(conn = conn)  
```

## Code of Conduct

Please note that the setupNCI project is released with a [Contributor
Code of
Conduct](https://contributor-covenant.org/version/2/0/CODE_OF_CONDUCT.html).
By contributing to this project, you agree to abide by its terms.
