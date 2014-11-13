library(data.table)
library(zoo)

read_tseries = function(fn, std=TRUE){
    
    # Read time series as zoo object
    x = fread(fn, sep='\t', header=TRUE)
    x = zoo(x[,-1,with=F], x[[1]])
    
    # Fix zeros @ .5*min
    x = fix_zeros(x, method='min')
    
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
    y = as.data.frame(apply(x, 2, function(xi) fitted(auto.arima(xi))))
    # Get residuals
    z = x - y
    return(list(fitted=y, residuals=z))
}

fix_zeros = function(x, method='min'){
    if(method == 'min'){
        y = as.numeric(x)
        x[x < -20] = log(.5)*min(y[y > -20])
    }
    return x
}