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

# get_payment_schedule(loan_number = "4315982-01", tibble = T)

mintos_ss %>% 
  sample_n(1) %>% 
  # glimpse()

mintos_ss %>% 
  group_by(`Loan Status`) %>% 
  count()

mintos_ss_shuffled <-  mintos_ss[sample(nrow(mintos_ss)),]

data <- tibble()
t0 <- Sys.time()
for (i in 1:nrow(mintos_ss_shuffled)) {
  # print(paste(scales::percent(i/nrow(mintos_ss)), i, format(Sys.time() - t0)))
  print(i)
  id <- mintos_ss_shuffled$Id[i]
  
  d <- get_payment_schedule_cache(id)
  
  # data <- data %>% 
  #   bind_rows(d %>% mutate(Id = id))
}

mintos_ss_ps <- mintos_ss %>% 
  filter(paste0(Id, ".RDS") %in% list.files("data/payment_cache/"))

payment_schedules <- tibble()
for (i in 1:nrow(mintos_ss_ps)) {
  print(i)
  
  ps <- get_payment_schedule_cache(loan_number = mintos_ss_ps$Id[i], cache_only = T) %>% 
    mutate(Id = mintos_ss_ps$Id[i])
  
  payment_schedules <- payment_schedules %>% 
    bind_rows(ps)
}



mintos_ss_ps_unnested <- mintos_ss_ps %>% 
  left_join(payment_schedules, by = "Id")

mintos_ss_ps_unnested %>% 
  saveRDS("data/mintos_ss_ps_unnested.RDS")

####

mintos_ss_ps_unnested <- readRDS("data/mintos_ss_ps_unnested.RDS")

mintos_ss_ps_nested <- mintos_ss_ps_unnested %>% 
  group_by(index, Id, `Issue Date`, `Closing Date`, `Listing Date`, 
           Country, `Loan Originator`, `Mintos Rating`, `Loan Type`, `Loan Rate Percent`, 
           Term, Collateral, `Initial LTV`, LTV, `Loan Status`, `Buyback reason`, 
           `Initial Loan Amount`, `Remaining Loan Amount`, Currency, Buyback, 
           `Extendable schedule`) %>% 
    nest()

mintos_ss_ps_nested_lateness <- mintos_ss_ps_nested %>% 
  mutate(data = map(data, ~.x %>% 
                      mutate(Date_R = if_else(str_detect(Status, "^Late") & is.na(Received), as.Date("2020-05-02"), Received),
                             lateness = Date - Date_R)))

mintos_ss_ps_nested_lateness %>% saveRDS("data/mintos_ss_ps_nested_lateness.RDS")

mintos_ss_ps_unnested_lateness <- mintos_ss_ps_nested_lateness %>% 
  mutate(dataX = map(data, ~.x %>% 
                       group_by(Date_m = floor_date(Date, unit = "month"),
                                late20d = if_else(lateness <= -30, "late20d", "notlate")) %>% 
                       summarise(n = n()) %>% 
                       filter(!is.na(late20d)) %>% 
                       pivot_wider(names_from = "late20d", values_from = "n"))) %>% 
  select(-data) %>% 
  unnest("dataX") %>% 
  group_by(Date_m) %>% 
  summarise(late20d = sum(late20d, na.rm = T), notlate = sum(notlate, na.rm = T)) %>% 
  mutate(perc_late20d = late20d/(late20d+notlate))
  

mintos_ss_ps_unnested_lateness %>% 
  ggplot(aes(x = Date_m, y = perc_late20d)) + 
  geom_line()
  
x$data[[4]] %>% 
  print(n = 100)

mintos_ss_ps_nested$data[[4]] %>% print(n = 100)

