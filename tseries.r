library(data.table)

read_tseries = function(fn, std=TRUE){
    # Read time series as zoo object
    x = fread(fn, sep='\t', header=TRUE)
    x = zoo(x[,-1,with=F], x[,1,with=F])
    # Standardize if necessary
    if(std == TRUE){
        x = scale(x)
    }
    # Return zoo object
    return(x)
}

fit_arima = function(x){
    require(forecast)
    # Fit ARIMA model to each column of x
    y = as.data.frame(apply(x, 2, function(xi) fitted(auto.arima(xi
    # Get residuals
    z = x - y
    return(list(fitted=y, residuals=z))
}