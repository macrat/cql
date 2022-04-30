use std::fmt;

use nom::branch::alt;
use nom::bytes::complete::{is_a, is_not, tag, tag_no_case, take_while1};
use nom::character::complete::{digit1, multispace0, multispace1};
use nom::combinator::{map, not, opt, peek, recognize};
use nom::error::VerboseError;
use nom::multi::{many1, separated_list1};
use nom::sequence::{delimited, preceded, terminated, tuple};
use nom::{IResult, Finish};

#[derive(Debug)]
pub enum ParseError<'a> {
    Failure(String),
    Extra(&'a str),
}

impl ParseError<'_> {
    fn from_verbose<'a>(input: &'a str, err: VerboseError<&'a str>) -> ParseError<'a> {
        ParseError::Failure(nom::error::convert_error(input, err))
    }

    fn from_extra<'a>(extra: &'a str) -> ParseError<'a> {
        ParseError::Extra(extra)
    }
}

impl fmt::Display for ParseError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::Failure(msg) => write!(f, "{}", msg),
            ParseError::Extra(remain) => write!(f, "found unknown extra text: {}", remain),
        }
    }
}

#[derive(Debug)]
pub struct Select<'a> {
    body: SelectBody<'a>,
    // orderby: Option<Vec<?????>>, // TODO
    // limit: Option<?????>, // TODO
}

impl<'a> fmt::Display for Select<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.body)
    }
}

pub fn parse(query: &str) -> Result<Select, ParseError> {
    match select_stmt(query).finish() {
        Err(err) => Err(ParseError::from_verbose(query, err)),
        Ok(("", select)) => Ok(select),
        Ok((extra, _)) => Err(ParseError::from_extra(extra)),
    }
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

impl<'a> fmt::Display for SelectBody<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SelectBody::Select{ distinct, columns, from, where_, groupby, having } => {
                write!(
                    f,
                    "SELECT{} {} FROM {}",
                    (if *distinct { " DISTINCT" } else { "" }),
                    (if let Some(cs) = columns { cs.to_string() } else { "".to_string() }),
                    (if let Some(from) = from { from.to_string() } else { "".to_string() }),
                )
            }
            SelectBody::Values(values) => {
                write!(
                    f,
                    "VALUES {}",
                    values.iter().map(|x| {
                        format!("({})", x.iter().map(|e| e.to_string()).collect::<Vec<_>>().join(", "))
                    }).collect::<Vec<_>>().join(", "),
                )
            }
            SelectBody::Compound{ operator, left, right } => {
                write!(f, "{} {} {}", left, operator, right)
            }
        }
    }
}

#[derive(Debug)]
enum CompoundOperator {
    Union,
    UnionAll,
    Intersect,
    Except,
}

impl fmt::Display for CompoundOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match self {
            CompoundOperator::Union => "UNION",
            CompoundOperator::UnionAll => "UNION ALL",
            CompoundOperator::Intersect => "INTERSECT",
            CompoundOperator::Except => "EXCEPT",
        })
    }
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
struct CommaList<T>(Vec<T>) where T: fmt::Display;

impl<T> fmt::Display for CommaList<T>
where T: fmt::Display {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(", "))
    }
}

fn comma_list<'a, O, E, F>(f: F) -> impl FnMut(&'a str) -> IResult<&'a str, CommaList<O>, E>
where
    O: fmt::Display,
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

impl<'a> fmt::Display for Column<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Column::Expr{expr, alias: None} => write!(f, "[{}]", expr),
            Column::Expr{expr, alias: Some(alias)} => write!(f, "[{}] AS [{}]", expr, alias),
            Column::All => write!(f, "*"),
            Column::AllFrom(table) => write!(f, "[{}].*", table),
        }
    }
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

impl<'a> fmt::Display for Table<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Table::Name{name, alias: None} => write!(f, "[{}]", name),
            Table::Name{name, alias: Some(alias)} => write!(f, "[{}] AS [{}]", name, alias),
            Table::Func{func, expr, alias: None} => write!(f, "{}({})", func, expr),
            Table::Func{func, expr, alias: Some(alias)} => write!(f, "{}({}) AS [{}]", func, expr, alias),
            Table::Select(select) => write!(f, "({})", select),
            Table::Join{..} => {
                write!(f, "JOIN IS NOT SUPPORTED YET") // TODO
            }
        }
    }
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

impl<'a> fmt::Display for Expr<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.join(" "))
    }
}

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

