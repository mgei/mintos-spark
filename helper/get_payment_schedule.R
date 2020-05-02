get_payment_schedule <- function(loan_number, tibble = F) {
  require(rvest)
  require(tidyverse)
  
  url1<-paste0('https://www.mintos.com/webapp/en/', loan_number ,'/?locale=en&referrer=https%3A%2F%2Fwww.mintos.com&hash=&openDoc=false')
  
  table<-read_html(url1) %>%
    html_nodes('.m-labeled-co , .m-labeled-col') %>%
    html_text()
  
  # table<-gsub('\u20b8','',table)
  # table<-gsub(' ₸','',table)
  table <- table %>% str_remove_all("\n") %>% str_trim(side = "both")
  data <- data.frame(matrix(unlist(table), ncol=7, byrow=T), stringsAsFactors = F)
  colnames(data) <- c("Date", "Principal", "Interest", "Total", "Payment", "Received", "Status")
  
  if (tibble) {
    data <- as_tibble(data)
    
    data <- data %>%
      mutate(Date = lubridate::dmy(Date),
             Received = lubridate::dmy(Received),
             Status = str_squish(Status)) %>%
      separate(Principal, into = c("Principal_currency", "Principal"), sep = " ", extra = "merge", fill = "right") %>%
      separate(Interest, into = c("Interest_currency", "Interest"), sep = " ", extra = "merge", fill = "right") %>%
      separate(Total, into = c("Total_currency", "Total"), sep = " ", extra = "merge", fill = "right") %>%
      separate(Payment, into = c("Payment_currency", "Payment"), sep = " ", extra = "merge", fill = "right") %>%
      mutate(Principal = Principal %>% str_remove_all(" ") %>% as.double(),
             Interest = Interest %>% str_remove_all(" ") %>% as.double(),
             Total = Total %>% str_remove_all(" ") %>% as.double(),
             Payment = Payment %>% str_remove_all(" ") %>% as.double())
  }
  
  return(data)
}

get_payment_schedule_cache <- function(loan_number, tibble = T,
                                       reload_if_older_than = "1 month", cache_dir = "data/payment_cache/") {
  
  if (paste0(loan_number, ".RDS") %in% list.files(cache_dir)) {
    cached <- readRDS(paste0(cache_dir, loan_number, ".RDS"))
    
    if (cached$loaddate + period(reload_if_older_than) >= Sys.Date()) {
      out <- cached$data
      
      return(out)
    }
  }
  
  # get from Mintos
  print("get payment schedule from mintos")
  suppressWarnings(out <- get_payment_schedule(loan_number = loan_number, tibble = tibble))
  
  # save to cached files
  list(loaddate = Sys.Date(),
       data = out) %>% 
    saveRDS(paste0(cache_dir, loan_number, ".RDS"))
  
  return(out)
}


# payment_schedule("13813937-02", tibble = T)