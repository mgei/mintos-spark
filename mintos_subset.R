library(tidyverse)
library(lubridate)

mintos_ss <- readRDS("data/subset.RDS")

mintos_ss %>% 
  group_by(`Loan Originator`) %>% 
  count() %>% 
  arrange(desc(n)) %>% 
  print(n = 1000)

mintos_ss <- mintos_ss %>% 
  mutate(`Issue Date` = as.Date(`Issue Date`),
         `Closing Date` = as.Date(`Closing Date`),
         `Listing Date` = as.Date(`Listing Date`)) %>% 
  glimpse()


source("helper/get_payment_schedule.R")

get_payment_schedule(loan_number = "4315982-01", tibble = T)

mintos_ss %>% 
  sample_n(1) %>% 
  # glimpse()

mintos_ss %>% 
  group_by(`Loan Status`) %>% 
  count()
