from .base import *

# ------------------------------------------------------------------------------
# SQL-style joins
# ------------------------------------------------------------------------------

def get_join_parameters(join_kwargs):
    """
    Convenience function to determine the columns to join the right and
    left DataFrames on, as well as any suffixes for the columns.
    """

    by = join_kwargs.get('by', None)
    suffixes = join_kwargs.get('suffixes', ('_x', '_y'))
    indicator = join_kwargs.get('indicator', False)
    if by is None:
        left_on, right_on = None, None
    else:
        if isinstance(by, str):
            left_on, right_on = by, by
        else:
            left_on = [col if isinstance(col, str) else col[0] for col in by]
            right_on = [col if isinstance(col, str) else col[1] for col in by]
    return left_on, right_on, suffixes, indicator


@pipe
def inner_join(df, other, **kwargs):
    """
    Joins on values present in both DataFrames.

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.
        suffixes (list): String suffixes to append to column names in left
            and right DataFrames.

    Example:
        a >> inner_join(b, by='x1')

          x1  x2     x3
        0  A   1   True
        1  B   2  False
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    joined = df.merge(other, how='inner', left_on=left_on,
                      right_on=right_on, suffixes=suffixes, indicator=indicator)
    return joined


@pipe
def full_join(df, other, **kwargs):
    """
    Joins on values present in either DataFrame. (Alternate to `outer_join`)

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.
        suffixes (list): String suffixes to append to column names in left
            and right DataFrames.

    Example:
        a >> outer_join(b, by='x1')

          x1   x2     x3
        0  A  1.0   True
        1  B  2.0  False
        2  C  3.0    NaN
        3  D  NaN   True
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    joined = df.merge(other, how='outer', left_on=left_on,
                      right_on=right_on, suffixes=suffixes, indicator=indicator)
    return joined


@pipe
def outer_join(df, other, **kwargs):
    """
    Joins on values present in either DataFrame. (Alternate to `full_join`)

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.
        suffixes (list): String suffixes to append to column names in left
            and right DataFrames.

    Example:
        a >> full_join(b, by='x1')

          x1   x2     x3
        0  A  1.0   True
        1  B  2.0  False
        2  C  3.0    NaN
        3  D  NaN   True
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    joined = df.merge(other, how='outer', left_on=left_on,
                      right_on=right_on, suffixes=suffixes, indicator=indicator)
    return joined


@pipe
def left_join(df, other, **kwargs):
    """
    Joins on values present in in the left DataFrame.

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.
        suffixes (list): String suffixes to append to column names in left
            and right DataFrames.

    Example:
        a >> left_join(b, by='x1')

          x1  x2     x3
        0  A   1   True
        1  B   2  False
        2  C   3    NaN
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    joined = df.merge(other, how='left', left_on=left_on,
                      right_on=right_on, suffixes=suffixes, indicator=indicator)
    return joined


@pipe
def right_join(df, other, **kwargs):
    """
    Joins on values present in in the right DataFrame.

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.
        suffixes (list): String suffixes to append to column names in left
            and right DataFrames.

    Example:
        a >> right_join(b, by='x1')

          x1   x2     x3
        0  A  1.0   True
        1  B  2.0  False
        2  D  NaN   True
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    joined = df.merge(other, how='right', left_on=left_on,
                      right_on=right_on, suffixes=suffixes, indicator=indicator)
    return joined


@pipe
def semi_join(df, other, **kwargs):
    """
    Returns all of the rows in the left DataFrame that have a match
    in the right DataFrame.

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.

    Example:
        a >> semi_join(b, by='x1')

          x1  x2
        0  A   1
        1  B   2
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    if not right_on:
        right_on = [col_name for col_name in df.columns.values.tolist() if col_name in other.columns.values.tolist()]
        left_on = right_on
    elif not isinstance(right_on, (list, tuple)):
        right_on = [right_on]
    other_reduced = other[right_on].drop_duplicates()
    joined = df.merge(other_reduced, how='inner', left_on=left_on,
                      right_on=right_on, suffixes=('', '_y'),
                      indicator=True).query('_merge=="both"')[df.columns.values.tolist()]
    return joined


@pipe
def anti_join(df, other, **kwargs):
    """
    Returns all of the rows in the left DataFrame that do not have a
    match in the right DataFrame.

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe)
        other (pandas.DataFrame): Right DataFrame

    Kwargs:
        by (str or list): Columns to join on. If a single string, will join
            on that column. If a list of lists which contain strings or
            integers, the right/left columns to join on.

    Example:
        a >> anti_join(b, by='x1')

          x1  x2
        2  C   3
    """

    left_on, right_on, suffixes, indicator = get_join_parameters(kwargs)
    if not right_on:
        right_on = [col_name for col_name in df.columns.values.tolist() if col_name in other.columns.values.tolist()]
        left_on = right_on
    elif not isinstance(right_on, (list, tuple)):
        right_on = [right_on]
    other_reduced = other[right_on].drop_duplicates()
    joined = df.merge(other_reduced, how='left', left_on=left_on,
                      right_on=right_on, suffixes=('', '_y'),
                      indicator=True).query('_merge=="left_only"')[df.columns.values.tolist()]
    return joined


# ------------------------------------------------------------------------------
# Binding
# ------------------------------------------------------------------------------

@pipe
def bind_rows(df, other, join='outer', ignore_index=False):
    """
    Binds DataFrames "vertically", stacking them together. This is equivalent
    to `pd.concat` with `axis=0`.

    Args:
        df (pandas.DataFrame): Top DataFrame (passed in via pipe).
        other (pandas.DataFrame): Bottom DataFrame.

    Kwargs:
        join (str): One of `"outer"` or `"inner"`. Outer join will preserve
            columns not present in both DataFrames, whereas inner joining will
            drop them.
        ignore_index (bool): Indicates whether to consider pandas indices as
            part of the concatenation (defaults to `False`).
    """

    df = pd.concat([df, other], join=join, ignore_index=ignore_index, axis=0)
    return df


@pipe
def bind_cols(df, other, join='outer', ignore_index=False):
    """
    Binds DataFrames "horizontally". This is equivalent to `pd.concat` with
    `axis=1`.

    Args:
        df (pandas.DataFrame): Left DataFrame (passed in via pipe).
        other (pandas.DataFrame): Right DataFrame.

    Kwargs:
        join (str): One of `"outer"` or `"inner"`. Outer join will preserve
            rows not present in both DataFrames, whereas inner joining will
            drop them.
        ignore_index (bool): Indicates whether to consider pandas indices as
            part of the concatenation (defaults to `False`).
    """

    df = pd.concat([df, other], join=join, ignore_index=ignore_index, axis=1)
    return df

# ------------------------------------------------------------------------------
# Expand
# ------------------------------------------------------------------------------
@pipe
def expand(df, by, name = None):
    """
    Expand DataFrame by the target column, the elements of the target column must be array-like.

    Args:
        by: The target column (passed in via pipe).
        name: new column name, new column will replace the target column if name = None. 
    """
    df = df.reset_index(drop = True)
    r = range(len(df))
    target = df[by]
    if name == None :
        df_tmp = pd.concat([pd.DataFrame({by :target.iloc[i]}) for i in r] )
        df_tmp.index = np.repeat(r, target.str.len())    
        df = pd.concat([df[df.columns[df.columns != by]], df_tmp], axis = 1)
    else :
        df_tmp = pd.concat([pd.DataFrame({name :target.iloc[i]}) for i in r] )
        df_tmp.index = np.repeat(r, target.str.len())    
        df = pd.concat([df, df_tmp], axis = 1)
    df = df.reset_index(drop = True)	
    return df
