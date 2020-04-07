library(rvest)
library(tidyverse)

payment_schedule <- function(loan_number, tibble = F) {
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

# payment_schedule("13813937-02", tibble = T)