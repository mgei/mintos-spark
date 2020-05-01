library(tidyverse)
library(dbplyr)


con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), 
                      host = "localhost",
                      user = "postgres",
                      # port = 5432,
                      password = "my") # rstudioapi::askForPassword("Database password")

DBI::dbListTables(con)

mtcars_tbl <- mtcars %>% 
  rownames_to_column("model") %>% 
  as_tibble()
  
copy_to(con, mtcars_tbl, "mtcars",
        temporary = FALSE, 
        indexes = list(
          colnames(mtcars_tbl)
        )
)

mtcars_db <- tbl(con, "mtcars")

mtcars_db %>% 
  group_by(cyl) %>% 
  nest()

mintos <- readRDS("~/Documents/R/mintos-spark/data/data.RDS")

copy_to(con, mintos, "mintos",
        temporary = FALSE, 
        indexes = list(
          colnames(mintos)
        )
)

mtcars_db <- tbl(con, "mtcars")