library(data.table)
library(forecast)

# parse args
args = commandArgs(trailingOnly=T)
in_fn = args[1]
out_fn = args[2]

# read data
x = data.frame(fread(in_fn, sep='\t', header=T))
y = as.matrix(x[,2:ncol(x)])
x[,2:ncol(x)] = apply(y, 2, function(a){tryCatch({fitted(auto.arima(a))}, error=function(b){return(NA)})})
write.table(x, file=ofn, quote=F, sep='\t')
