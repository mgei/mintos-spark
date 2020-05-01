library(tidyverse)
library(dbplyr)


con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(), 
                      host = "localhost",
                      user = "postgres",
                      # port = 5432,
                      password = "my") # rstudioapi::askForPassword("Database password")

DBI::dbListTables(con)

mintos_db <- tbl(con, "mintos")


mintos_db %>% summarize(n())

lc <- mintos_db %>% 
  group_by(`Loan Originator`) %>% 
  count()

lc

mintos_db %>% sample_n(100)

DBI::dbGetQuery(con, "SELECT * FROM mintos ORDER BY random() LIMIT 10;")


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



mintos_db %>% 
  summarise(n = n())

mtcars_db %>% 
  group_by(cyl) %>% 
  nest()

mintos <- read_rds("~/Documents/R/mintos-spark/data/data.RDS")

copy_to(con, 
        mintos, 
        "mintos",
        temporary = FALSE, 
        indexes = list(
          colnames(mintos)
        )
)

                                                                                                                                                                                                                                                                                                                                                      mtcars_db <- tbl(con, "mtcars")