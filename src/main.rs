use nom::branch::alt;
use nom::bytes::complete::{is_a, is_not, tag, tag_no_case, take_while1};
use nom::character::complete::{digit1, multispace0, multispace1};
use nom::combinator::{map, not, opt, peek, recognize};
use nom::error::VerboseError;
use nom::multi::{many1, separated_list1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::IResult;

#[derive(Debug)]
struct Select<'a> {
    body: SelectBody<'a>,
    // orderby: Option<Vec<?????>>, // TODO
    // limit: Option<?????>, // TODO
}

#[derive(Debug)]
enum SelectBody<'a> {
    Select {
        distinct: bool,
        columns: Option<CommaList<Column<'a>>>,
        from: Option<CommaList<Table<'a>>>,
        where_: Option<Expr<'a>>,
        groupby: Option<CommaList<Expr<'a>>>,
        having: Option<Expr<'a>>,
        //  window: Option<?????>, // TODO
    },
    Values(Vec<Vec<Expr<'a>>>),
    Compound {
        operator: CompoundOperator,
        left: Box<SelectBody<'a>>,
        right: Box<SelectBody<'a>>,
    },
}

#[derive(Debug)]
enum CompoundOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

fn select_stmt(i: &str) -> IResult<&str, Select, VerboseError<&str>> {
    map(alt((values_body, select_body)), |body| Select { body })(i)
}

fn select_body(i: &str) -> IResult<&str, SelectBody, VerboseError<&str>> {
    let (remaining, (select, from, where_, groupby_having)) = tuple((
        opt(select_part),
        opt(preceded(multispace0, from_part)),
        opt(preceded(multispace0, where_part)),
        opt(preceded(
            multispace0,
            tuple((groupby_part, opt(preceded(multispace1, having_part)))),
        )),
    ))(i)?;

    let (distinct, columns) = match select {
        Some((d, c)) => (d, Some(c)),
        None => (false, None),
    };

    let (groupby, having) = match groupby_having {
        Some((g, h)) => (Some(g), h),
        None => (None, None),
    };

    Ok((
        remaining,
        SelectBody::Select {
            distinct,
            columns,
            from,
            where_,
            groupby,
            having,
        },
    ))
}

fn values_body(i: &str) -> IResult<&str, SelectBody, VerboseError<&str>> {
    preceded(
        tuple((tag_no_case("values"), multispace1)),
        map(
            separated_list1(
                tuple((multispace0, tag(","), multispace0)),
                delimited(
                    tuple((tag("("), multispace0)),
                    separated_list1(tuple((multispace0, tag(","), multispace0)), expression),
                    tuple((multispace0, tag(")"))),
                ),
            ),
            |xs| SelectBody::Values(xs),
        ),
    )(i)
}

fn select_part(i: &str) -> IResult<&str, (bool, CommaList<Column>), VerboseError<&str>> {
    preceded(
        tuple((tag_no_case("select"), multispace1)),
        tuple((distinct_mode, column_list)),
    )(i)
}

fn from_part(i: &str) -> IResult<&str, CommaList<Table>, VerboseError<&str>> {
    preceded(tuple((tag_no_case("from"), multispace1)), table_list)(i)
}

fn where_part(i: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    preceded(tuple((tag_no_case("where"), multispace1)), expression)(i)
}

fn groupby_part(i: &str) -> IResult<&str, CommaList<Expr>, VerboseError<&str>> {
    preceded(
        tuple((
            tag_no_case("group"),
            multispace1,
            tag_no_case("by"),
            multispace1,
        )),
        comma_list(expression),
    )(i)
}

fn having_part(i: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    preceded(tuple((tag_no_case("having"), multispace1)), expression)(i)
}

fn distinct_mode(i: &str) -> IResult<&str, bool, VerboseError<&str>> {
    let (remaining, mode) = opt(terminated(
        alt((tag_no_case("distinct"), tag_no_case("all"))),
        multispace1,
    ))(i)?;

    Ok((
        remaining,
        match mode {
            Some("distinct") => true,
            _ => false,
        },
    ))
}

#[derive(Debug)]
struct CommaList<T>(Vec<T>);

fn comma_list<'a, O, E, F>(f: F) -> impl FnMut(&'a str) -> IResult<&'a str, CommaList<O>, E>
where
    F: nom::Parser<&'a str, O, E>,
    E: nom::error::ParseError<&'a str>,
{
    map(
        separated_list1(tuple((multispace0, tag(","), multispace0)), f),
        |xs| CommaList(xs),
    )
}

#[derive(Debug)]
enum Column<'a> {
    Expr {
        expr: Expr<'a>,
        alias: Option<&'a str>,
    },
    All,
    AllFrom(&'a str),
}

fn column_list(i: &str) -> IResult<&str, CommaList<Column>, VerboseError<&str>> {
    comma_list(alt((
        map(
            terminated(
                identifier,
                tuple((multispace0, tag("."), multispace0, tag("*"))),
            ),
            |x| Column::AllFrom(x),
        ),
        map(tag("*"), |_| Column::All),
        map(
            tuple((
                expression,
                opt(preceded(
                    tuple((multispace0, opt(tuple((tag_no_case("as"), multispace0))))),
                    identifier,
                )),
            )),
            |(e, a)| Column::Expr { expr: e, alias: a },
        ),
    )))(i)
}

#[derive(Debug)]
enum Table<'a> {
    Name {
        name: &'a str,
        alias: Option<&'a str>,
    },
    Func {
        func: &'a str,
        expr: Expr<'a>,
        alias: Option<&'a str>,
    },
    Select(Box<Select<'a>>),
    Join {
        natural: bool,
        operator: JoinOperator,
        constraint: JoinConstraint<'a>,
        left: Box<Table<'a>>,
        right: Box<Table<'a>>,
    },
}

#[derive(Debug)]
enum JoinOperator {
    Left,
    Inner,
    Cross,
}

#[derive(Debug)]
enum JoinConstraint<'a> {
    None,
    On(Expr<'a>),
    Using(Vec<Column<'a>>),
}

fn table_list(i: &str) -> IResult<&str, CommaList<Table>, VerboseError<&str>> {
    comma_list(map(
        tuple((
            identifier,
            opt(preceded(
                tuple((multispace0, opt(tuple((tag_no_case("as"), multispace0))))),
                identifier,
            )),
        )),
        |(name, alias)| Table::Name { name, alias },
    ))(i)
}

fn keywords(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    terminated(
        alt((
            tag_no_case("as"),
            tag_no_case("group"),
            tag_no_case("having"),
            tag_no_case("window"),
            tag_no_case("order"),
            tag_no_case("limit"),
            operator,
        )),
        multispace1,
    )(i)
}

fn identifier(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    preceded(
        not(peek(alt((keywords, digit1)))),
        alt((
            delimited(tag("["), is_not("]"), tag("]")),
            delimited(tag("\""), is_not("\""), tag("\"")),
            delimited(tag("`"), is_not("`"), tag("`")),
            take_while1(|c| !" \t\r\n,()".contains(c)),
        )),
    )(i)
}

#[derive(Debug)]
struct Expr<'a>(Vec<&'a str>);

fn expression(i: &str) -> IResult<&str, Expr, VerboseError<&str>> {
    map(
        many1(preceded(
            multispace0,
            alt((
                function_call,
                identifier,
                operator,
                preceded(not(peek(keywords)), is_not(",() \t\r\n")),
            )),
        )),
        |xs| Expr(xs),
    )(i)
}

fn operator(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    terminated(
        alt((
            alt((tag("~"), tag("+"), tag("-"))),
            alt((tag("||"), tag("->"), tag("->>"))),
            alt((tag("*"), tag("/"), tag("%"))),
            alt((tag("&"), tag("|"), tag("<<"), tag(">>"))),
            alt((tag("<"), tag(">"), tag("<="), tag(">="))),
            alt((
                tag("="),
                tag("=="),
                tag("<>"),
                tag("!="),
                tag_no_case("is"),
                recognize(tuple((tag_no_case("is"), multispace1, tag_no_case("not")))),
                tag_no_case("between"),
                tag_no_case("in"),
                tag_no_case("match"),
                tag_no_case("like"),
                tag_no_case("regexp"),
                tag_no_case("glob"),
                tag_no_case("isnull"),
                tag_no_case("notnull"),
                recognize(tuple((
                    tag_no_case("not"),
                    multispace1,
                    tag_no_case("null"),
                ))),
            )),
            alt((tag_no_case("not"), tag_no_case("and"), tag_no_case("or"))),
        )),
        is_a(" \t\r\n,()"),
    )(i)
}

fn function_call(i: &str) -> IResult<&str, &str, VerboseError<&str>> {
    recognize(tuple((
        identifier,
        tag("("),
        multispace0,
        expression,
        multispace0,
        tag(")"),
    )))(i)
}

fn main() {
    let inp = "select [ab c] as a, def as d from [g hi], ./jkl.csv as j where max(abc) >= 5 group by mno, pqr having mno > 0";
    match select_stmt(inp) {
        Err(nom::Err::Error(err)) => println!("{}", nom::error::convert_error(inp, err)),
        x => println!("{:?}", x),
    }
}
