# Analyzing loans from Mintos P2P

The dataset contains >13 million(1) loans and is too big for conventional analysis in R. Therefore we shall be using **sparklyr**.

(1) 28 million as of May 2020!

## AWS EC2

https://spark.rstudio.com/examples/yarn-cluster-emr/

## Creating a cluster on Microsoft Azure

(did now work)

Following [this tutorial](https://blog.revolutionanalytics.com/2018/02/aztk-sparklyr.html?utm_campaign=News&utm_medium=Community&utm_source=DataCamp.com).

1. Install [Azure Distributed Data Engineering Toolbox](https://github.com/Azure/aztk/blob/master/docs/00-getting-started.md) with `pip install aytk`. Done in my main Python 3.5 conda environment.

2. Signed up to Microsoft Azure, there's also a link to [get $200 credit for free](http://cda.ms/7v). It's required to insert personal details as well as credit card details anyway.

3. Create an Azure Batch following the [official documentation](https://docs.microsoft.com/en-us/azure/batch/quick-create-portal). In https://portal.azure.com click on *Create a resource*, select category *Compute*, and search for *Batch Resource*.

Copy and save the details!

Go to the batch resource and select *Pools*, *Add*.
