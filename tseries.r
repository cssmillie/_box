read_tseries = function(fn, std=TRUE){
    require(data.table)
    # Read time series as zoo object
    x = fread(fn, sep='\t', header=TRUE)
    x = data.frame(x[,-1,with=FALSE], row.names=x[[1]])
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
    y = apply(x, 2, function(a) auto.arima(a))
    z = list()
    z$fitted = sapply(y, function(a) fitted(a)[-1])
    z$residuals = sapply(y, function(a) a$residuals)

    #y = as.data.frame(apply(x, 2, function(xi) fitted(auto.arima(xi))[-1]))
    # Get residuals
    #z = x - y
    #return(list(fitted=y, residuals=z))
}

fix_zeros = function(x, method='min'){
    if(method == 'min'){
        x = as.matrix(x)
        x[x < -20] = log(.5) + min(y[y > -20])
        x = as.data.frame(x)
    }
    return(x)
}