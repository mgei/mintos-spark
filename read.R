# 0. setup ----
library(tidyverse)
library(readxl)
library(sparklyr)
library(lubridate)

# need this for sparklyr
Sys.getenv("JAVA_HOME")
Sys.setenv(JAVA_HOME = "/usr/lib/jvm/java-1.8.0-openjdk-amd64")

## 1. convert xlsx files to csv's ----
files <- list.files("data/xlsx")

length(files) # how many files are there?

for (i in 1:length(files)) {
  print(i)
  data <- read_excel(paste0("data/", files[i]))
  data %>% write_csv(paste0("data/", str_replace(files[i], ".xlsx", ".csv")))
  print("done")
}
rm(data)


## 2. spark connnect & load data ----
sc <- spark_connect(master = "local")

# sc_ub <- spark_connect(master = "spark://192.168.1.133")

# iris_tbl <- copy_to(sc, iris)

src_tbls(sc)

# read in csv files to spark
csvfiles <- list.files("data/csv")

# get full dataset in R as tibble
data <- tibble()
for (i in 1:length(csvfiles)) {
  print(i)
  data <- bind_rows(data, read_csv(paste0("data/csv/", csvfiles[i])))
  print("done")
}
# data %>% saveRDS("data/data.RDS")


for (i in 1:length(csvfiles)) {
  print(i)
  sparklyr::spark_read_csv(sc, 
                           name = paste0("data", i),
                           path = paste0("data/csv/", csvfiles[i]))
  assign(paste0("sparkdata", i), tbl(sc, paste0("data", i)))
  print("done")
}

data <- sdf_bind_rows(sparkdata1, sparkdata2, sparkdata3, sparkdata4, sparkdata5, sparkdata6, sparkdata7, sparkdata8, sparkdata9, sparkdata10,
                      sparkdata11, sparkdata12, sparkdata13, sparkdata14, sparkdata15, sparkdata16, sparkdata17, sparkdata18, sparkdata19, sparkdata20,
                      sparkdata21, sparkdata22, sparkdata23, sparkdata24, sparkdata25, sparkdata26, sparkdata27)

object.size(data)

# 3. first glance at data ----

data %>% count()

# data %>% sdf_nrow() # same as count()

data <- data %>% arrange(Issue_Date, Id)

countbymonth <- data %>% 
  # mutate(MonYr = floor_date(unit = "months")) %>% 
  group_by(Loan_Originator, Currency) %>% 
  summarise(n = n())

plotdata <- countbymonth %>% collect() 

# https://stackoverflow.com/questions/35113873/combine-result-from-top-n-with-an-other-category-in-dplyr
plotdata %>% 
  arrange(desc(n)) %>% 
  group_by(Loan_Originator = factor(c(Loan_Originator[1:10], rep("Other", n() - 10)),
                                    levels = c(Loan_Originator[1:10], "Other")),
           Currency) %>% 
  tally() %>% 
  ggplot(aes(x = Loan_Originator, y = n, fill = Currency)) +
  geom_col() +
  scale_y_continuous(labels = scales::number) +
  theme(legend.position = "bottom")

# let's have a look at random credits to get an idea
data %>% dplyr::sample_n(1)

data <- data %>% 
  mutate(Issue_Year = case_when(Issue_Date >= as.Date("2017-01-01") & Issue_Date <= as.Date("2017-12-31") ~ 2017,
                                Issue_Date >= as.Date("2018-01-01") & Issue_Date <= as.Date("2018-12-31") ~ 2018,
                                Issue_Date >= as.Date("2019-01-01") & Issue_Date <= as.Date("2019-12-31") ~ 2019,
                                T ~ 2016))

data %>% 
  group_by(Issue_Year) %>% count()

%>% sdf_sample(0.01)

sample %>% collect()

data %>% 
  mutate(MonYr = floor_date(Issue_Date))


floor_date(as.Date("2018-03-05"), unit = "months")

file1 %>% sdf_bind_rows(file2)

object.size(file1) %>% format(units = "auto")

fileX <- sdf_bind_rows(file1, file2, file3, file4, file5) %>% 
  ggplot(aes(x = Country, y = n)) %>% 
  geom_col()

fileX %>% group_by(Country) %>% count()



data <- tibble()
for (i in 1:length(csvfiles)) {
  print(i)
  d <- read_csv(paste0("data/csv/", csvfiles[i]), progress = F)
  data <- bind_rows(data, d)
  print("done")
  print(format(object.size(data), units = "Mb"))
}



spark_disconnect(sc)


sc <- spark_connect(master = "spark://192.168.1.133")


# conventional dplyr
data %>% 
  sample_n(1000) %>% 
  filter(`Loan Status` == "Finished prematurely") %>% 
  mutate(TermActual = interval(`Issue Date`, `Closing Date`) %/% months(1)) %>% 
  ggplot(aes(x = Term, y = TermActual, text = paste("Issue: ", `Issue Date`, 
                                                    "<br>Cl Date: ", `Closing Date`),
             size = `Initial Loan Amount`)) + 
  geom_point() -> p

ggplotly(p, tooltip = "text")

# total loan amount on Mintos
data %>% filter(Currency == "EUR") %>% 
  summarise(Amount = sum(`Initial Loan Amount`)) %>% 
  mutate(AmountMillion = scales::number(Amount/1000000))

data %>% filter(Currency == "EUR") %>% 
  group_by(Year = year(`Issue Date`)) %>% 
  summarise(Amount = sum(`Initial Loan Amount`)) %>% 
  mutate(AmountMillion = scales::number(Amount/1000000)) -> a

a

a %>% ggplot(aes(x = Year, y = Amount)) + geom_line()

# how much is currently open?
data %>% filter(Currency == "EUR") %>% 
  group_by(`Loan Status`) %>% 
  summarise(Amount = sum(`Initial Loan Amount`)) %>% 
  mutate(AmountMillion = scales::number(Amount/1000000))

