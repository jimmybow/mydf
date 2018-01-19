from .base import *
import collections

# ------------------------------------------------------------------------------
# series ordering
# ------------------------------------------------------------------------------

@make_symbolic
def order_series_by(series, order_series):
    """
    Orders one series according to another series, or a list of other
    series. If a list of other series are specified, ordering is done hierarchically
    like when a list of columns is supplied to `.sort_values()`.

    Args:
        series (:obj:`pandas.Series`): the pandas Series object to be reordered.
        order_series: either a pandas Series object or a list of pandas Series
            objects. These will be sorted using `.sort_values()` with
            `ascending=True`, and the new order will be used to reorder the
            Series supplied in the first argument.

    Returns:
        reordered `pandas.Series` object
    """

    if isinstance(order_series, (list, tuple)):
        sorter = pd.concat(order_series, axis=1)
        sorter_columns = ['_sorter'+str(i) for i in range(len(order_series))]
        sorter.columns = sorter_columns
        sorter['series'] = series.values
        sorted_series = sorter.sort_values(sorter_columns)['series']
        return sorted_series
    else:
        sorted_series = pd.DataFrame({
            'series':series.values,
            'order':order_series.values
        }).sort_values('order', ascending=True)['series']
        return sorted_series


@make_symbolic
def desc(series):
    """
    Mimics the functionality of the R desc function. Essentially inverts a
    series object to make ascending sort act like descending sort.

    Args:
        series (:obj:`pandas.Series`): pandas series to be inverted prior to
            ordering/sorting.

    Returns:
        inverted `pandas.Series`. The returned series will be numeric (integers),
            regardless of the type of the original series.

    Example:

        First group by cut, then find the first value of price when ordering by
        price ascending, and ordering by price descending using the `desc` function.

        diamonds >> group_by(X.cut) >> summarize(carat_low=first(X.price, order_by=X.price),
                                                 carat_high=first(X.price, order_by=desc(X.price)))

                 cut  carat_high  carat_low
        0       Fair       18574        337
        1       Good       18788        327
        2      Ideal       18806        326
        3    Premium       18823        326
        4  Very Good       18818        336
    """

    return series.rank(method='min', ascending=False)



# ------------------------------------------------------------------------------
# coalesce
# ------------------------------------------------------------------------------

@make_symbolic
def coalesce(*series):
    """
    Takes the first non-NaN value in order across the specified series,
    returning a new series. Mimics the coalesce function in dplyr and SQL.

    Args:
        *series: Series objects, typically represented in their symbolic form
            (like X.series).

    Example:
        df = pd.DataFrame({
            'a':[1,np.nan,np.nan,np.nan,np.nan],
            'b':[2,3,np.nan,np.nan,np.nan],
            'c':[np.nan,np.nan,4,5,np.nan],
            'd':[6,7,8,9,np.nan]
        })
        df >> transmute(coal=coalesce(X.a, X.b, X.c, X.d))

             coal
        0       1
        1       3
        2       4
        3       5
        4  np.nan
    """

    series = [pd.Series(s) for s in series]
    coalescer = pd.concat(series, axis=1)
    min_nonna = np.argmin(pd.isnull(coalescer).values, axis=1)
    min_nonna = [coalescer.columns[i] for i in min_nonna]
    return coalescer.lookup(np.arange(coalescer.shape[0]), min_nonna)


# ------------------------------------------------------------------------------
# case_when
# ------------------------------------------------------------------------------

@make_symbolic
def case_when(*args):
    L = len(args)
    if isinstance(args[L-1], list): 
        tmp = np.nan
        L_for_loop = L-1
    else: 
        tmp = args[L-1]
        L_for_loop = L-2
    for i in np.arange(L_for_loop , -1, -1) :
        tmp = np.where(args[i][0], args[i][1], tmp)
    return tmp


# ------------------------------------------------------------------------------
# if_else
# ------------------------------------------------------------------------------

@make_symbolic
def if_else(condition, when_true, otherwise):
    """
    Wraps creation of a series based on if-else conditional logic into a function
    call.

    Provide a boolean vector condition, value(s) when true, and value(s)
    when false, and a vector will be returned the same length as the conditional
    vector according to the logical statement.

    Args:
        condition: A boolean vector representing the condition. This is often
            a logical statement with a symbolic series.
        when_true: A vector the same length as the condition vector or a single
            value to apply when the condition is `True`.
        otherwise: A vector the same length as the condition vector or a single
            value to apply when the condition is `False`.

    Example:
    df = pd.DataFrame
    """

    if not isinstance(when_true, collections.Iterable) or isinstance(when_true, str):
        when_true = np.repeat(when_true, len(condition))
    if not isinstance(otherwise, collections.Iterable) or isinstance(otherwise, str):
        otherwise = np.repeat(otherwise, len(condition))
    assert (len(condition) == len(when_true)) and (len(condition) == len(otherwise))

    output = np.array([when_true[i] if c else otherwise[i]
                       for i,c in enumerate(condition)])
    return output


# ------------------------------------------------------------------------------
# na_if
# ------------------------------------------------------------------------------

@make_symbolic
def na_if(series, *values):
    """
    If values in a series match a specified value, change them to `np.nan`.

    Args:
        series: Series or vector, often symbolic.
        *values: Value(s) to convert to `np.nan` in the series.
    """

    series = pd.Series(series)
    series[series.isin(values)] = np.nan
    return series
    
# ------------------------------------------------------------------------------
# Len
# ------------------------------------------------------------------------------

@make_symbolic
def Len(series):
    return len(series)
