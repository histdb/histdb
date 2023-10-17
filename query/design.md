#
# selections
#

the query language lets you express sets of metrics to include.
each metric is identified uniquely by its tag set, which is
an unsorted set of key=value pairs. the primary method of
specifying a set of metrics is with a selection. the general
syntax for a selection is, for some tag keys t1, t2, ..., TN
and some boolean expression

    {t1,t2,...,tN | expr}

the expression has in scope variables with identifiers equal
to the specified tags. for example `t1 == "foo"` is an expression
that evaluates if the value for the tag key t1 is equal to "foo".
selections have a canonical form where the tag keys specified on
the left are sorted and deduplicated.

if all of the tags appear in the expression then they need not
be specified and will be inferred. for example

    {t1 == "foo" && t2 == "bar"} -> {t1,t2 | t1 == "foo" && t2 == "bar"}

the other special selection form is where only the tag
name is specified. this is the same as having true as
the expression.

    {t1} -> {t1 | true}

finally, this generalizes to no tag at all and we have

    {} -> {| true}

which is the empty set as it specifies no tags.

if there is only one selection, the braces around the
selection can be elided. this is the case for all of the
provided examples so far.

#
# expressions
#

    (e1 op e2) op e3   # grouping

    tag     # tag variable reference

    lit     # string/number literal
    "lit"   # quoted string/number literal (with \ escapes)
    'lit'   # quoted string/number literal (with \ escapes)

    e1 || e2   # logical or
    e1 |  e2

    e1 && e2   # logical and
    e1 &  e2
    e1 ,  e2

    tag == lit    # equality
    tag =  lit    # equality
    tag != lit    # inequality

    tag <  lit    # less than
    tag <= lit    # less than or equal
    tag >  lit    # greater than
    tag >= lit    # greater than or equal

    tag =~ lit    # regex matching
    tag !~ lit    # regex not matching

    tag =* lit    # glob matching
    tag !* lit    # glob not matching

#
# selection expressions
#

    ({} op {}) op {}         # grouping

    {t1 | e1} | {t2 | e2}    # union
    {t1 | e1} & {t2 | e2}    # intersection
    {t1 | e1} ^ {t2 | e2}    # symmetric difference
    {t1 | e1} % {t2 | e2}    # difference

#
# selection decomposition
#

any selection expression can be decomposed to
a single comparison with some set of tag keys.
for example

    {foo == 'foo' && bar == 'bar'} ->
        { foo,bar | foo == 'foo' } & { foo,bar | bar == 'bar' }

in general we have

    { tags | e1 && e2 } -> { tags | e1 } & { tags | e2 }
    { tags | e1 || e2 } -> { tags | e1 } | { tags | e2 }

this is done for implemenation efficiency because
a selection involving a single tag can be computed
in linear time with respect to the tag, and the
selection operations are also linear, making the total
runtime linear, whereas computing the compound
expressions naively is exponential
